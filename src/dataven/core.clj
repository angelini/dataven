(ns dataven.core
  (:require [clojure.java.jdbc :as jdbc])
  (:import [com.facebook.presto.jdbc
            PrestoConnection PrestoDriver PrestoResultSet PrestoResultSetMetaData]
           [java.sql Timestamp]
           [java.time LocalDateTime]
           [java.util Properties])
  (:gen-class))

(def local-db {:dbtype "postgresql"
               :dbname "dataven"
               :user "dataven"
               :password "dv"})

(def schema "
  connector - db conn info
  source - table definition
    has 1 connector
  cache - local cache table definition
    has 1 source
  column - column name and type
    has 1 cache
  frame - frame def associated with a book
    has 1 cache
  log - input and output command log entry
  book - log of commands and a set of live frames
    has n frames
    has n logs
  ")

(def connectors-def
  {:name "connectors"
   :columns [["id"       :serial :primary]
             ["name"     :str]
             ["type"     :str]
             ["host"     :str]
             ["port"     :int]
             ["database" :str]
             ["username" :str]
             ]})

(def sources-def
  {:name "sources"
   :columns [["id"           :serial :primary]
             ["schema"       :str]
             ["table_name"   :str]
             ["connector_id" :int]
             ["count"        :int    :nullable]
             ["stats_time"   :time   :nullable]
             ]})

(def caches-def
  {:name "caches"
   :columns [["id"          :serial :primary]
             ["local_table" :str]
             ["sample"      :bool]
             ["source_id"   :int]
             ["time"        :time]
             ]})

(def columns-def
  {:name "columns"
   :columns [["id"        :serial :primary]
             ["name"      :str]
             ["position"  :int]
             ["data_type" :str]
             ["nullable"  :bool]
             ["cache_id"  :int]
             ]})

(def frames-def
  {:name "frames"
   :columns [["id"       :serial :primary]
             ["name"     :str]
             ["cache_id" :int]
             ["time"     :time]
             ["book_id"  :int]
             ]})

(def logs-def
  {:name "logs"
   :columns [["id"          :serial :primary]
             ["input"       :str    :nullable]
             ["output"      :str    :nullable]
             ["input_time"  :time   :nullable]
             ["output_time" :time   :nullable]
             ["book_id"     :int]
             ]})

(def books-def
  {:name "books"
   :columns [["id"   :serial]
             ["name" :str]
             ["time" :time]
             ]})

(def type-stmts
  {:serial "SERIAL"
   :str    "VARCHAR(128)"
   :bigstr "VARCHAR(1024)"
   :int    "INTEGER"
   :bigint "BIGINT"
   :bool   "BOOLEAN"
   :time   "TIMESTAMP"
   })

(def inv-type-stmts
  (clojure.set/map-invert type-stmts))

(defn column-stmt
  ([n t] (column-stmt n t nil))
  ([n t attr] (case attr
                :nullable (format "%s %s" n (get type-stmts t))
                :primary (format "%s %s PRIMARY KEY" n (get type-stmts t))
                nil (format "%s %s NOT NULL" n (get type-stmts t)))))

(defn create-table-stmt [table-def]
  (let [{:keys [name columns]} table-def
        columns (map #(str "  " (apply column-stmt %)) columns)
        lines [(format "CREATE TABLE IF NOT EXISTS %s (" name)
               (clojure.string/join ",\n" columns)
               ");"]]
    (clojure.string/join "\n" lines)))

(defn drop-table-stmt [table-def]
  (format "DROP TABLE IF EXISTS %s;" (:name table-def)))

(defn create-tables []
  (let [tables [connectors-def
                sources-def
                caches-def
                columns-def
                frames-def
                logs-def
                books-def]]
    (jdbc/db-do-commands local-db (mapv drop-table-stmt tables))
    (jdbc/db-do-commands local-db (mapv create-table-stmt tables))))

(defn create-table [d]
  (jdbc/execute! local-db (create-table-stmt d)))

(defn select-local
  ([d] (select-local d ["*"]))
  ([d cols] (jdbc/query local-db [(format "
SELECT %s
  FROM %s" (clojure.string/join ", " cols) (:name d))])))

(defn insert [table row]
  (first (jdbc/insert! local-db table row)))

(defn insert-multi [table rows]
  (jdbc/insert-multi! local-db table rows))

(defn truncate [table-name]
  (jdbc/db-do-commands local-db [(format "TRUNCATE TABLE %s;" table-name)
                                 (format "ALTER SEQUENCE %s_id_seq RESTART WITH 1;" table-name)]))

(defn now []
  (Timestamp/valueOf (LocalDateTime/now)))

(defn to-jdbc-spec [host port database username]
  {:classname "org.postgresql.Driver"
   :subprotocol type
   :subname (format "//%s:%d/%s" host port database)
   :user username})

(defn get-row [columns results]
  (->> columns
       (map (fn [[i key class-name]]
              [key (case class-name
                     "java.lang.String" (.getString results i)
                     "java.lang.Long"   (.getLong results i))]))
       (into {})))

(defn ^PrestoConnection presto-connect [host port database user]
  (let [uri (format "jdbc:presto://%s:%d/%s" host port database)
        props (Properties.)
        driver (PrestoDriver.)]
    (.put props "user" user)
    (.connect driver uri props)))

(defn presto-query [conn sql]
  (let [^PrestoResultSet results (.executeQuery (.createStatement conn) sql)
        ^PrestoResultSetMetaData metadata (.getMetaData results)
        columns (->> (range 1 (inc (.getColumnCount metadata)))
                     (map (fn [i]
                            [i
                             (keyword (.getColumnName metadata i))
                             (.getColumnClassName metadata i)])))]
    (loop [has-next (.next results)
           rows []]
      (if-not has-next
        rows
        (let [row (get-row columns results)]
          (recur (.next results) (conj rows row)))))))

(defprotocol QueryExec
  (exec [self sql params])
  (select [self schema table])
  (get-table-names [self schema])
  (get-table-size [self schema table])
  (get-columns [self schema table]))

(deftype PrestoQueryExec [host port database user]
  QueryExec
  (exec [self sql params]
    (presto-query (presto-connect host port database user)
                  (apply format sql params)))

  (select [self schema table]
    (exec self "
SELECT *
  FROM %s.%s" [schema table]))

  (get-table-names [self schema]
    (->> (exec self "
SELECT table_name
  FROM information_schema.tables
 WHERE table_type = 'BASE TABLE'
   AND table_schema = '%s'" [schema])
         (map :table_name)))

  (get-table-size [self schema table]
    (->> (exec self "
SELECT count(*) as count
  FROM %s.%s" [schema table])
         (map :count)
         first))

  (get-columns [self schema table]
    (let [convert-type (fn [t]
                         (subs (->> (if (= "varchar" t)
                                "VARCHAR(1024)"
                                (clojure.string/upper-case t))
                              (get inv-type-stmts)
                              str) 1))]
      (->> (exec self "
SELECT column_name, ordinal_position, is_nullable, data_type
  FROM information_schema.columns
 WHERE table_schema = '%s'
   AND table_name = '%s'" [schema table])
           (map (fn [{:keys [column_name ordinal_position, is_nullable, data_type]}]
                  {:name column_name
                   :position ordinal_position
                   :nullable (= is_nullable "YES")
                   :data_type (convert-type data_type)}))))))

(deftype JdbcQueryExec [host port database user]
  QueryExec
  (exec [self sql params]
    (jdbc/query (to-jdbc-spec host port database user)
                (concat [sql] params)))

  (select [self schema table]
    (exec self "
SELECT *
  FROM ?" [(format "%s.%s" schema table)]))

  (get-table-names [self schema]
    (->> (exec self "
SELECT table_name
  FROM information_schema.tables
 WHERE table_type = 'BASE TABLE'
   AND table_schema = ?" [schema])
         (map :table_name)))

  (get-table-size [self schema table]
    (->> (exec self "
SELECT count(*) as count
  FROM ?" [(format "%s.%s" schema table)])
         (map :count)
         first))

  (get-columns [self schema table] "TODO"))

(defn new-query-exec [attrs]
  (let [{:keys [type host port database username]} attrs]
    (case type
      "postgresql" (JdbcQueryExec. host port database username)
      "presto" (PrestoQueryExec. host port database username))))

(defn insert-connectors []
  (let [table-name (:name connectors-def)]
    (truncate table-name)
    (insert-multi table-name [{:name "local"
                               :type "postgresql"
                               :host "localhost"
                               :port 5432
                               :database "scratch"
                               :username "dataven"}
                              {:name "warehouse"
                               :type "presto"
                               :host "presto.example.com"
                               :port 8082
                               :database "scratch"
                               :username "dataven"}])))

(defn fetch-stats [query-exec schema table]
  (try
    [(get-table-size query-exec schema table) (now)]
    (catch Exception e
      (println e) [nil nil])))

(defn insert-sources [connector schema]
  (let [query-exec (new-query-exec connector)
        sources (map (fn [table]
                       (let [[count time] (fetch-stats query-exec schema table)]
                         {:schema schema
                          :table_name table
                          :connector_id (:id connector)
                          :count count
                          :stats_time time}))
                     (get-table-names query-exec schema))]
    (insert-multi (:name sources-def) sources)))

(defn insert-columns [query-exec schema table cache-id]
  (println schema)
  (println table)
  (let [columns (get-columns query-exec schema table)]
    (insert-multi (:name columns-def)
                  (map #(assoc % :cache_id cache-id) columns))))

(defn to-table-def [name cache-id]
  (let [columns (select-local columns-def ["name" "nullable" "data_type"])]
    {:name name
     :columns (mapv (fn [{:keys [name nullable data_type]}]
                      [name (keyword data_type) (if nullable :nullable nil)])
                    columns)}))

(defn download-cache [connector source]
  (let [{:keys [count schema table_name]} source
        query-exec (new-query-exec connector)
        local-table (format "dt_%07d" (int (rand 10000000)))
        _ (assert (and count (< count 100)))
        rows (select query-exec schema table_name)
        cache (insert (:name caches-def) {:local_table local-table
                                          :sample false
                                          :source_id (:id source)
                                          :time (now)})]
    (insert-columns query-exec schema table_name (:id cache))
    (create-table (to-table-def local-table (:id cache)))
    (insert-multi local-table rows)
    cache))

;; --------------------------------------------------------

;; (create-tables)

;; (def connectors (insert-connectors))

;; (def jdbc-conn (first connectors))
;; (def presto-conn (second connectors))

;; (def sources (insert-sources presto-conn "discovery"))

;; (download-cache presto-conn (nth (vec (select-local sources-def)) 4))
