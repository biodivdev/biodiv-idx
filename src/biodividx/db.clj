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
(def source (or (env :taxon_resource) "lista_especies_flora_brasil"))

(defn get-families
  [] 
  (->> {:size 0
        :aggs 
         {:families
           {:aggs {:families {:terms {:field "family" :size 0}}}
            :filter {:bool {:must [{:term {:taxonomicStatus "accepted"}} {:term {:source source}}]}}} } }
    (post-json (str es "/" idx "/taxon/_search"))
    :aggregations
    :families
    :families
    :buckets
    (map :key)
    (sort)))

(defn get-species-0
  [family]
  (->> {:size 999
        :query 
          {:bool
            {:must 
             [{:term {:family family}}
              {:term {:taxonRank "species"}}
              {:term {:taxonomicStatus "accepted"}}
              {:term {:source source}}]}}}
    (post-json (str es "/" idx "/taxon/_search"))
    :hits
    :hits
    (map :_source)))

(defn get-synonyms-0
  [spp-name] 
  (->> {:size 999
        :query 
          {:bool
            {:must 
             [{:bool
                {:should
                  [{:query {:match {:acceptedNameUsage {:query spp-name :type "phrase"}}}}
                   {:query {:match {:scientificName {:query spp-name :type "phrase"}}}}]}}
              {:term {:taxonRank "species"}} 
              {:term {:taxonomicStatus "synonym"}} 
              {:term {:source source}}]}}}
    (post-json (str es "/" idx "/taxon/_search"))
    :hits
    :hits
    (map :_source)))

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
    (doseq [spp (get-species-0 family)]
       (>! out spp))
    (close! out)))

(defn get-names
  [spp out] 
  (go
    (log/info "Get names of" (:scientificNameWithoutAuthorship spp))
    (>! out (assoc spp :synonyms (get-synonyms-0 (:scientificNameWithoutAuthorship spp))))
    (close! out)))

(defn get-names-1
  [spp] 
  (cons
    (:scientificNameWithoutAuthorship spp)
    (map :scientificNameWithoutAuthorship (:synonyms spp))))

(defn get-occurrences
  [spp out]
   (go
     (log/info "Get occurrences of"  (:scientificNameWithoutAuthorship spp))
     (>! out [spp (get-occurrences-0 (get-names-1 spp))])
     (close! out)))

(defn put-results
  [results]
  (go-loop [result (<! results)]
   (when-not (nil? result)
     (do
       (log/info "Will save" (:scientificNameWithoutAuthorship result))
       (try
         (client/post (str es "/" index "/" (:type result) "/" (:id result)) {:body (json/write-str result)})
         (log/info "Saved " (:scientificNameWithoutAuthorship result))
         (catch Exception e
           (log/error "Error saving" (:scientificNameWithoutAuthorship result) e)))
       (recur (<! results))))))

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

