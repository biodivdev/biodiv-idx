(ns biodividx.core
  (:require [clj-http.lite.client :as http])
  (:require [clojure.data.json :as json])
  (:require [clojure.core.async :refer [<! <!! >! >!! chan close! go-loop]])
  (:require [taoensso.timbre :as log])
  (:gen-class))

(def taxadata "http://taxadata/api/v2")
(def dwc-bot "http://dwcbot:8383")
(def dwc "http://dwcservices/api/v1")
(def es "http://elasticsearch:9200/biodiv")

(defn get-json
  [& url] 
  (log/info "Get JSON"(apply str url ))
  (:result (json/read-str (:body (http/get (apply str url))) :key-fn keyword)))

(defn post-json
  [url body] 
  (log/info "POST JSON" url)
  (-> 
    (http/post url {:content-type :json :body (json/write-str body)})
    :body
    (json/read-str :key-fn keyword)))

(defn create-db
  [] (try
      (http/post "http://elasticsearch:9200/biodiv")
        (catch Exception e (.printStackTrace e))))

(defn wat
  [w] (log/info w) w)

(defn delete [spp]
  (try
    (->> 
      (-> spp
        :scientificNameWithoutAuthorship
        (.replace " " "%20"))
      (str es "/_query?q=scientificNameWithoutAuthorship:")
      (wat)
      (http/delete)
      (:body)
      (log/info))
    (catch Exception e (log/warn (.getMessage e)))))

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
        (delete spp)
        (>! results [(assoc spp :type "taxon")])
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
        (>! results (map #(assoc % :type "occurrence" :scientificNameWithoutAuthorship spp) occs))
        (let [result (post-json (str dwc "/analysis/all") occs)]
          (log/info "Got result for" spp)
          (>! results
             (map #(assoc % :scientificNameWithoutAuthorship spp)
              [ {:type "count" 
                 :occurrences (dissoc (:occurrences result) :all :recent :historic)
                 :points (dissoc (:points result) :all :recent :historic :geo)}
                {:type "geo" :geo (get-in result [:points :geo])}
                (assoc (get-in result [:eoo :historic]) :type "eoo_historic")
                (assoc (get-in result [:eoo :historic]) :type "eoo_historic")
                (assoc (get-in result [:eoo :recent]) :type "eoo_recent")
                (assoc (get-in result [:eoo :all]) :type "eoo")
                (dissoc (assoc (get-in result [:aoo :historic]) :type "aoo_historic") :grid)
                (dissoc (assoc (get-in result [:aoo :recent]) :type "aoo_recent") :grid)
                (dissoc (assoc (get-in result [:aoo :all]) :type "aoo") :grid)
                (assoc (get-in result [:clusters :historic]) :type "clusters_historic")
                (assoc (get-in result [:clusters :recent]) :type "clusters_recent")
                (assoc (get-in result [:clusters :all]) :type "clusters")
                {:risk-assessment (:risk-assessment result) :main-risk-assessment (first (:risk-assessment result)) :type "risk_assessment"}
               ])))
        (catch Exception e
         (do (log/warn "Error calculating results" spp e) (.printStackTrace e))))
      (recur (<! occurrences)))))

(defn results->db
  [results]
  (go-loop [result (<! results)]
   (when-not (nil? result)
     (log/info "Will save" (:scientificNameWithoutAuthorship (first result)) (count result))
     (loop [docs result items []]
       (if (empty? docs)
         (try
           (let [body (apply str (interpose "\n" (map json/write-str items)))]
             (http/post (str es "/_bulk") {:body (str body "\n")}))
           (log/info "Saved " (:scientificNameWithoutAuthorship (first result)) (count result))
           (catch Exception e
             (do (log/warn "Error saving " (:scientificNameWithoutAuthorship (first result)) (.getMessage e))
               (log/warn "Error body" (apply str (map json/write-str items) ))
               (.printStackTrace e))))
         (let [doc  (first docs)
               id   (or (:occurrenceID doc) (:scientificNameWithoutAuthorship doc))]
           (recur (rest docs)
              (conj items 
                {:index {:_index "biodiv" :_type (:type doc) :_id id}}
                (assoc doc :timestamp (System/currentTimeMillis) :id id))))))
     (recur (<! results)))))

(defn run
  [] 
  (let [sources     (chan 1)
        families    (chan 2)
        species     (chan 5)
        occurrences (chan 2)
        results     (chan 5)]
  (sources->families sources families)
  (families->species families species results)
  (species->occurrences species occurrences)
  (occurrences->results occurrences results)
  (results->db results)

  (doseq [source (get-json "http://taxadata/api/v2/sources")]
    (>!! sources source))))

(defn -main 
  [ & args ]
  (log/info "Starting...")
  (Thread/sleep (* 15 1000 ))
  (create-db)
  (while true 
    (log/info "Will run")
    (run)
    (Thread/sleep (* 12 60 60 1000))
    (log/info "Will rest.")))

