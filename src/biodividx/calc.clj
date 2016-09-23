(ns biodividx.calc
  (:use biodividx.http)
  (:require [clojure.core.async :refer [<! <!! >! >!! chan close! go-loop go]])
  (:require [taoensso.timbre :as log])
  (:require [environ.core :refer [env]])
  (:gen-class))

(def dwc (or (env :dwc-services) "http://localhost:8181/api/v1"))

(defn now
  [] (System/currentTimeMillis))

(defn get-results
  [[spp-name occs] out]
  (go
    (log/info "Got occs for" spp-name (count occs))
    (try
      (let [result (post-json (str dwc "/analysis/all") occs)]
        (log/info "Got result for" spp-name)
        (>! out
           (map
             #(assoc % :scientificNameWithoutAuthorship spp-name :timestamp (now))
             [{:type "count" 
               :occurrences (dissoc (:occurrences result) :all :recent :historic)
               :points (dissoc (:points result) :all :recent :historic :geo)}
              {:type "geo" :geo (get-in result [:points :geo])}
              (assoc (get-in result [:eoo :historic]) :type "eoo_historic")
              (assoc (get-in result [:eoo :historic]) :type "eoo_historic")
              (assoc (get-in result [:eoo :recent]) :type "eoo_recent")
              (assoc (get-in result [:eoo :all]) :type "eoo")
              (dissoc (assoc (get-in result [:aoo-variadic :historic]) :type "aoo_variadic_historic") :grid)
              (dissoc (assoc (get-in result [:aoo-variadic :recent]) :type "aoo_variadic_recent") :grid)
              (dissoc (assoc (get-in result [:aoo-variadic :all]) :type "aoo_variadic") :grid)
              (dissoc (assoc (get-in result [:aoo :historic]) :type "aoo_historic") :grid)
              (dissoc (assoc (get-in result [:aoo :recent]) :type "aoo_recent") :grid)
              (dissoc (assoc (get-in result [:aoo :all]) :type "aoo") :grid)
              (assoc (get-in result [:clusters :historic]) :type "clusters_historic")
              (assoc (get-in result [:clusters :recent]) :type "clusters_recent")
              (assoc (get-in result [:clusters :all]) :type "clusters")
              {:risk-assessment (:risk-assessment result) :main-risk-assessment (first (:risk-assessment result)) :type "risk_assessment"}
             ])))
      (catch Exception e
       (do (log/error "Error calculating results" spp-name e))))
    (close! out)))

