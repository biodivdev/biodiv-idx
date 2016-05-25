(defproject biodividx "0.0.1"
  :description "Run analysis on DarwinCore taxa and occurrences aggregated"
  :url "http://github.com/diogok/biodiv-idx"
  :license {:name "MIT"}
  :main biodividx.core
  :source-paths ["src"]
  :dependencies [[org.clojure/clojure "1.8.0"]
                 [org.clojure/core.async "0.2.374"]
                 [com.taoensso/timbre "4.3.1"]
                 [org.clojure/data.json "0.2.6"]
                 [clj-http-lite "0.3.0"]
                 [environ "1.0.2"]]
  :profiles {:uberjar {:aot :all}
             :jar {:aot :all}})
