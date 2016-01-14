(defproject biodividx "0.0.1"
  :description "Run analysis DarwinCore taxa and occurrences aggregated"
  :url "http://github.com/diogok/biodivagg"
  :license {:name "MI"}
  :main biodividx.core
  :source-paths ["src"]
  :dependencies [[org.clojure/clojure "1.7.0"]
                 [org.clojure/core.async "0.2.374"]
                 [com.taoensso/timbre "4.1.4"]
                 [org.clojure/data.json "0.2.5"]
                 [clj-http-lite "0.3.0"]]
  :profiles {:uberjar {:aot :all}
             :jar {:aot :all}})
