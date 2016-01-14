(ns biodividx.core
  (:require [clj-http.lite.client :as http])
  (:require [clojure.data.json :as json])
  (:require [clojure.core.async :refer [<! <!! >! >!! chan close! go-loop]])
  (:require [taoensso.timbre :as log])
  (:gen-class))

(def taxadata "http://taxadata/api/v2")
(def dwc-bot "http://dwcbot:8383")
(def dwc "http://dwcservices:8080/api/v1")
(def es "http://elasticsearch:9200/biodiv")

(defn get-json
  [& url] 
  (log/info "Get JSON"(apply str url ))
  (:result (json/read-str (:body (http/get (apply str url))) :key-fn keyword)))

(defn post-json
  [url body] 
  (log/info "POST JSON" url)
  (-> 
    (http/post url
      {:content-type :json :body (json/write-str body)})
    :body
    (json/read-str :key-fn keyword)
    :result))

(defn create-db
  [] (try
      (http/post "http://elasticsearch:9200/biodiv")
        (catch Exception e (.printStackTrace e))))

(defn sources->families
  [sources families]
  (go-loop [src (<! sources)]
    (when-not (nil? src)
      (log/info "Got source" src)
      (doseq [family (get-json taxadata "/" src "/families")]
        (>! families [src family]))
       (recur (<! sources))))) 

(defn families->species
  [families species results] 
  (go-loop [[src family] (<! families)]
    (when-not (nil? family)
      (log/info "Got family" family)
      (doseq [spp (get-json taxadata "/" src "/" family "/species")]
        (>! results (assoc spp :type "taxon"))
        (>! species
          [(:scientificNameWithoutAuthorship spp)
           (map :scientificNameWithoutAuthorship (cons spp (:synonyms spp)))]))
      (recur (<! families)))))

(defn species->occurrences
  [species occurrences]
  (go-loop [[spp names] (<! species)]
   (when-not (nil? names)
     (log/info "Got names" spp "->" (apply str (interpose "," names )))
     (>! occurrences 
         [spp
          (reduce concat
            (map (fn [n] (get-json dwc-bot "/search?q=" (.replace n " " "%20" ))) names))])
     (recur (<! species)))))

(defn occurrences->results
  [occurrences results]
  (go-loop [[spp occs] (<! occurrences)]
    (when-not (nil? occs)
      (log/info "Got occs for" spp (count occs))
      (try
        (doseq [occ occs]
          (>! results (assoc occ :type "occurrence" :scientificNameWithoutAuthorship spp)))
        (let [result (post-json (str dwc "/analysis/all" occs))]
          (log/info "Got result for" spp)
          (>! results (assoc (get-in result [:eoo :historic]) :type "eoo_historic" :scientificNameWithoutAuthorship spp))
          (>! results (assoc (get-in result [:eoo :recent]) :type "eoo_recent" :scientificNameWithoutAuthorship spp))
          (>! results (assoc (get-in result [:eoo :all]) :type "eoo" :scientificNameWithoutAuthorship spp))
          (>! results (dissoc (assoc (get-in result [:aoo :historic]) :type "aoo_historic" :scientificNameWithoutAuthorship spp) :grid))
          (>! results (dissoc (assoc (get-in result [:aoo :recent]) :type "aoo_recent" :scientificNameWithoutAuthorship spp) :grid))
          (>! results (dissoc (assoc (get-in result [:aoo :all]) :type "aoo" :scientificNameWithoutAuthorship spp) :grid))
          (>! results (assoc (get-in result [:clusters :historic]) :type "clusters_historic" :scientificNameWithoutAuthorship spp))
          (>! results (assoc (get-in result [:clusters :recent]) :type "clusters_recent" :scientificNameWithoutAuthorship spp))
          (>! results (assoc (get-in result [:clusters :all]) :type "clusters" :scientificNameWithoutAuthorship spp))
          (>! results {:risk-assessment (:risk-assessment result) :type "risk_assessment" :scientificNameWithoutAuthorship spp}))
        (catch Exception e
         (do (log/warn "Error calculating results" spp e) (.printStackTrace e))))
      (recur (<! occurrences)))))

(defn results->db
  [results]
  (go-loop [result (<! results)]
   (when-not (nil? result)
     (log/info "Will save" (:scientificNameWithoutAuthorship result) (:type result))
       (let [url (str es "/" (:type result) "/" (.replace (:scientificNameWithoutAuthorship result) " " "%20"))
             doc (assoc result :timestamp (System/currentTimeMillis) :id (:scientificNameWithoutAuthorship result)) 
             json-doc (json/write-str doc)]
         (try
           (log/info "Sending" url)
           (http/post url
            {:content-type :json
             :body json-doc})
           (log/info "Saved " (:scientificNameWithoutAuthorship result) (:type result))
           (catch Exception e
             (do (log/warn "Error saving " (:scientificNameWithoutAuthorship result) (:type result) (.getMessage e))
                 (log/warn json-doc)
               (.printStackTrace e)))))
     (recur (<! results)))))

(defn run
  [] 
  (let [sources     (chan 1)
        families    (chan 2)
        species     (chan 5)
        occurrences (chan 2)
        results     (chan 20)]

  (sources->families sources families)
  (families->species families species results)
  (species->occurrences species occurrences)
  (occurrences->results occurrences results)
  (results->db results)

  (doseq [source (get-json "http://taxadata/api/v2/sources")]
    (>!! sources source))))

(defn -main 
  [ & args ]
  (create-db)
  (while true 
    (run)
    (Thread/sleep (* 12 60 60 1000))))

