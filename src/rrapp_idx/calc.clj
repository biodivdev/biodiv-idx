(ns rrapp-idx.calc
  (:require [dwc-analysis.all :refer [all-analysis]])
  (:require [clojure.core.async :refer [<! <!! >! >!! chan close! go-loop go]])
  (:require [taoensso.timbre :as log])
  (:require [environ.core :refer [env]])
  (:gen-class))

(defn now
  [] (System/currentTimeMillis))

(defn get-results
  [[spp occs] out]
  (go
    (log/info "Got occs for" (:scientificNameWithoutAuthorship spp) (count occs))
    (try
      (let [result (all-analysis occs)]
        (log/info "Got result for" (:scientificNameWithoutAuthorship spp))
        (>! out
           (-> result
                (merge spp)
                (assoc :type "analysis")
                (assoc :timestamp (now))
                (assoc :main-risk-assessment (first (:risk-assessment result)))
                (assoc :occurrences
                  (dissoc (:occurrences result) :historic :recent))
                (assoc :points
                  (dissoc (:points result) :historic :recent :geo))
                (dissoc :quality)
                (update-in [:aoo :all] dissoc :grid)
                (update-in [:aoo :recent] dissoc :grid)
                (update-in [:aoo :historic] dissoc :grid)
                (update-in [:aoo-variadic :all] dissoc :grid)
                (update-in [:aoo-variadic :recent] dissoc :grid)
                (update-in [:aoo-variadic :historic] dissoc :grid))))
      (catch Exception e
       (do (log/error "Error calculating results" spp e))))
    (close! out)))

