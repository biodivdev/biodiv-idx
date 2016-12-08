(defproject rrapp-idx "0.0.3"
  :description "Run analysis on DarwinCore taxa and occurrences aggregated"
  :url "http://github.com/biodivdev/rrapp-idx"
  :license {:name "MIT"}
  :main rrapp-idx.core
  :source-paths ["src"]
  :dependencies [[org.clojure/clojure "1.8.0"]
                 [org.clojure/core.async "0.2.391"]
                 [com.taoensso/timbre "4.7.4"]
                 [org.clojure/data.json "0.2.6"]
                 [clj-http-lite "0.3.0"]
                 [dwc-analysis "0.0.31"]
                 [environ "1.1.0"]]
  :repositories [["osgeo" "http://download.osgeo.org/webdav/geotools/"]
                 ["clojars" {:sign-releases false}]]
  :profiles {:uberjar {:aot :all}
             :jar {:aot :all}})
