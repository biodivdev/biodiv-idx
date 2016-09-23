(ns biodividx.db
  (:use biodividx.http)
  (:require [clj-http.lite.client :as client])
  (:require [clojure.data.json :as json])
  (:require [clojure.core.async :refer [<! <!! >! >!! chan close! go-loop go]])
  (:require [taoensso.timbre :as log])
  (:require [environ.core :refer [env]])
  (:gen-class))

(def es  (or (env :elasticsearch) "http://localhost:9200"))
(def idx (or (env :index) "dwc"))
(def index idx)

(defn get-families
  [] 
  (->> {:size 0
        :aggs 
         {:families
           {:aggs {:families {:terms {:field "family" :size 0}}}
            :filter {:term {:taxonomicStatus "accepted"}}}}}
    (post-json (str es "/" idx "/taxon/_search"))
    :aggregations
    :families
    :families
    :buckets
    (map :key)
    (map #(.toUpperCase %))
    (sort)))

(defn get-species-0
  [family]
  (->> {:size 999
        :_source [:scientificNameWithoutAuthorship]
        :query 
          {:bool
            {:must 
             {:query {:match {:family family}}}
              :filter {:term {:taxonomicStatus "accepted"}}}}}
    (post-json (str es "/" idx "/taxon/_search"))
    :hits
    :hits
    (map :_source)
    (map :scientificNameWithoutAuthorship)
    (sort)))

(defn get-names-0
  [spp-name] 
  (->> {:size 999
        :_source [:scientificNameWithoutAuthorship]
        :query  {:match {:acceptedNameUsage {:query spp-name :type "phrase"}}}}
    (post-json (str es "/" idx "/taxon/_search"))
    :hits
    :hits
    (map :_source)
    (map :scientificNameWithoutAuthorship)
    (sort)))

(defn get-occurrences-0
  [names] 
  (->> {:size 9999
        :query
         {:bool
          {:should 
            (for [spp-name names]
              {:match 
               {:scientificNameWithoutAuthorship 
                {:query spp-name :type "phrase"}}})}}}
    (post-json (str es "/" idx "/occurrence/_search"))
    :hits
    :hits
    (map :_source)))

(defn get-species
  [family out] 
  (go
    (log/info "Get species of" family)
    (doseq [spp-name (get-species-0 family)]
       (>! out spp-name))
    (close! out)))

(defn get-names
  [spp-name out] 
  (go
    (log/info "Get names of" spp-name)
    (>! out [spp-name (get-names-0 spp-name)])
    (close! out)))

(defn get-occurrences
  [[spp-name names] out]
   (go
     (log/info "Get occurrences of" spp-name (count names))
     (>! out [spp-name (get-occurrences-0 names)])
     (close! out)))

(defn prepare-doc
  [doc]
   [{:index {:_index idx :_type (:type doc) :_id (:scientificNameWithoutAuthorship doc)}}
      doc])

(defn make-body
  [docs] 
  (->> docs
    (map prepare-doc)
    (flatten)
    (map json/write-str)
    (interpose "\n")
    (apply str)))

(defn put-results
  [results]
  (go-loop [result (<! results)]
   (when-not (nil? result)
     (when-not (empty? result)
       (log/info "Will save" (:scientificNameWithoutAuthorship (first result)) (count result))
       (let [body (str (make-body result) "\n")]
         (try
             (client/post (str es "/_bulk") {:body body})
             (log/info "Saved " (:scientificNameWithoutAuthorship (first result)))
           (catch Exception e
             (do (log/warn "Error saving" (:scientificNameWithoutAuthorship (first result)))
                 (log/warn "Error body" body)
                 (log/warn e))))))
     (recur (<! results)))))

(defn setup
  []
  (let [mappings (json/read-str (slurp (clojure.java.io/resource "mappings.json")) :key-fn keyword)]
    (doseq [[map-type map-body] mappings]
      (log/info map-type map-body)
      (log/info 
        (:body
          (client/put (str es "/" index "/_mapping/" (name map-type))
            {:body  (json/write-str map-body)
             :throw-exceptions false
             :headers {"Content-Type" "application/json"}}))))))

