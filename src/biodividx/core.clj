(ns biodividx.core
  (:require [clj-http.lite.client :as http])
  (:require [clojure.data.json :as json])
  (:require [clojure.core.async :refer [<! <!! >! >!! chan close! go-loop]])
  (:require [taoensso.timbre :as log])
  (:require [environ.core :refer [env]])
  (:gen-class))

(def taxadata (or (env :taxadata-url) "http://taxadata/api/v2"))
(def dwc-bot (or (env :dwc-bot-url) "http://dwcbot"))
(def dwc (or (env :dwc-services-url) "http://dwcservices/api/v1"))
(def es (or (env :elasticsearch-url) "http://elasticsearch:9200/biodiv"))

(defn get-json
  [& url] 
  (log/info "Get JSON"(apply str url ))
  (try
    (:result (json/read-str (:body (http/get (apply str url))) :key-fn keyword))
    (catch Exception e 
      (log/warn (str "Failled get JSON " (apply str url)  (.getMessage e))))))

(defn post-json
  [url body] 
  (log/info "POST JSON" url)
  (try
    (-> 
      (http/post url {:content-type :json :body (json/write-str body)})
      :body
      (json/read-str :key-fn keyword))
    (catch Exception e 
      (log/warn (str "Failled POST JSON " (apply str url)  (.getMessage e))))))

(defn create-db
  [] (try
      (http/post es)
      (catch Exception e (log/info (.getMessage e)))))

(defn wat
  [w] (log/info w) w)

(defn delete [spp]
  (log/info (str "Will delete " (:scientificNameWithoutAuthorship spp)))
  (try
    (do
      (->> 
        (str "\""
          (-> spp
            :scientificNameWithoutAuthorship
            (.replace " " "%20"))
          "\"")
        (str es "/_query?q=scientificNameWithoutAuthorship:")
        (http/delete)
        (:body))
        (log/info (str "Deleted " (:scientificNameWithoutAuthorship spp))))
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

(defn mksearch
  [spp]
  (str dwc-bot
    (->
      (str "/search?q="
           (str "("
             (apply str
                (interpose " AND "
                  (for [part (.split spp " ")]
                    (str "scientificName:" part))))
                                       ")")
           "%20OR%20"
           (str "("
             (apply str
                (interpose " AND "
                  (for [part (.split spp " ")]
                    (str "scientificNameWithoutAuthorship:" part))))
                ")"))
    (.replaceAll " " "%20")
    (.replaceAll ":" "%3A"))))

(defn species->occurrences
  [species occurrences]
  (go-loop [[spp names] (<! species)]
   (when-not (nil? names)
     (log/info "Got names" spp "->" (apply str (interpose "," names )))
     (>! occurrences 
         [spp
          (reduce concat
            (map (fn [n] (get-json (mksearch n))) names))])
     (recur (<! species)))))

(defn occurrences->results
  [occurrences results]
  (go-loop [[spp occs] (<! occurrences)]
    (when-not (nil? occs)
      (log/info "Got occs for" spp (count occs))
      (>! results (map #(assoc % :type "occurrence" :scientificNameWithoutAuthorship spp) occs))
      (try
        (let [result (post-json (str dwc "/analysis/all") occs)]
          (log/info "Got result for" spp)
          (>! results
             (map
               #(assoc % :scientificNameWithoutAuthorship spp)
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
         (do (log/warn "Error calculating results" spp e))))
      (recur (<! occurrences)))))

(defn prepare-doc
  [doc]
   (let [id (or (:occurrenceId doc) (:scientificNameWithoutAuthorship doc))]
     [{:index {:_index "biodiv" :_type (:type doc) :_id id}}
      (assoc doc :timestamp (System/currentTimeMillis) :id id)]))

(defn make-body
  [docs] 
  (->>
    (map prepare-doc docs)
    (flatten)
    (map json/write-str)
    (interpose "\n")
    (apply str)))

(defn results->db
  [results]
  (go-loop [result (<! results)]
   (when-not (nil? result)
     (when-not (empty? result)
       (log/info "Will save" (:scientificNameWithoutAuthorship (first result)) (:type (first result)) (count result))
       (let [body (str (make-body result) "\n")]
         (try
             (http/post (str es "/_bulk") {:body body})
             (log/info "Saved " (:scientificNameWithoutAuthorship (first result)) (count result))
           (catch Exception e
             (do (log/warn "Error saving " (:scientificNameWithoutAuthorship (first result)) (.getMessage e))
                 (log/warn "Error body" body))))))
     (recur (<! results)))))

(defn run-all
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

  (doseq [source (get-json (str taxadata "/sources" ))]
    (>!! sources source))))

(defn run-single
  [spp]
  (let [species     (chan 5)
        occurrences (chan 2)
        results     (chan 5)]

  (log/info (str "Will do only " spp))

  (species->occurrences species occurrences)
  (occurrences->results occurrences results)
  (results->db results)

  (let [taxon (get-json (str taxadata "/flora/specie/" (.replace spp " " "%20")))]
    (delete taxon)
    (>!! results [(assoc taxon :type "taxon")])
    (>!! species
        [(:scientificNameWithoutAuthorship taxon)
         (map :scientificNameWithoutAuthorship (cons taxon (:synonyms taxon)))]))))

(defn wait-es
  "Wait for ElasticSearch to be ready"
  []
  (let [done (atom false)]
    (while (not @done)
      (try 
        (log/info (str "Waiting: " es))
        (let [r (http/get es {:throw-exceptions false})]
          (if (= 200 (:status r))
            (reset! done true)
            (Thread/sleep 1000)))
        (catch Exception e 
          (do
            (log/warn (.toString e))
            (Thread/sleep 1000)))))
    (log/info (str "Done: " es))))

(defn wait-taxadata
  "Wait for Taxadata to be ready"
  []
  (let [done (atom false)]
    (while (not @done)
      (try 
        (log/info (str "Waiting: " taxadata))
        (let [r (http/get (str taxadata "/status") {:throw-exceptions false})]
          (if (= "done" (:status (json/read-str (:body r) :key-fn keyword)))
            (reset! done true)
            (do
              (log/info (json/read-str (:body r)))
              (Thread/sleep 1000))))
        (catch Exception e 
          (do
            (log/warn (.toString e))
            (Thread/sleep 1000)))))
    (log/info (str "Done: " taxadata))))

(defn wait-dwcbot
  "Wait for DWC-BOT to be ready"
  []
  (let [done (atom false)]
    (while (not @done)
      (try 
        (log/info (str "Waiting: " dwc-bot))
        (let [r (http/get (str dwc-bot "/status") {:throw-exceptions false})]
          (if (= "idle" (:status (json/read-str (:body r) :key-fn keyword)))
            (reset! done true)
            (do
              (log/info (json/read-str (:body r)))
              (Thread/sleep 15000))))
        (catch Exception e 
          (do
            (log/warn (.toString e))
            (Thread/sleep 15000)))))
    (log/info (str "Done: " dwc-bot))))

(defn -main 
  [ & args ]
  (log/info "Starting...")

  (log/info taxadata)
  (log/info dwc-bot)
  (log/info dwc)
  (log/info es)

  (Thread/sleep (* 5 1000))

  (wait-es)
  (wait-taxadata)
  (wait-dwcbot)

  #_(create-db)
  (log/info "Will start now")
  (if (first args)
    (run-single (first args))
    (run-all))
  (Thread/sleep (* 24 60 60 1000)))

