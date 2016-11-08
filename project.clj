(defproject dataven "0.1.0-SNAPSHOT"
  :dependencies [[org.clojure/clojure "1.8.0"]
                 [org.clojure/java.jdbc "0.6.1"]
                 [org.postgresql/postgresql "9.4.1211"]
                 [com.facebook.presto/presto-jdbc "0.155"]]
  :plugins [[cider/cider-nrepl "0.15.0-SNAPSHOT"]]
  :main ^:skip-aot dataven.core
  :target-path "target/%s"
  :profiles {:uberjar {:aot :all}})
