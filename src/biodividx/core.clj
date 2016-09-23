(ns biodividx.core
  (:use biodividx.db)
  (:use biodividx.calc)
  (:require [clj-http.lite.client :as client])
  (:require [clojure.core.async :refer [<! <!! >! >!! chan close! go go-loop pipeline pipeline-blocking pipeline-async]])
  (:require [taoensso.timbre :as log])
  (:require [environ.core :refer [env]])
  (:gen-class))

(def procs (Integer/valueOf (or (env :procs) "4")))

(defn pipe
  [from fun to]
  (pipeline-async procs to fun from))

(defn run-all
  [] 
  (let [families    (chan 2)
        species     (chan 5)
        names       (chan 5)
        occurrences (chan 2)
        results     (chan 5)]

  (pipe families get-species species)
  (pipe species get-names names)
  (pipe names get-occurrences occurrences)
  (pipe occurrences get-results results)
  (put-results results)

  (doseq [family (get-families)]
    (>!! families family))))

(defn wait-es
  "Wait for ElasticSearch to be ready"
  []
  (let [done (atom false)]
    (while (not @done)
      (try 
        (log/info (str "Waiting: " es))
        (let [r (client/get (str es "/" idx) {:throw-exceptions false})]
          (if (= 200 (:status r))
            (reset! done true)
            (Thread/sleep 1000)))
        (catch Exception e 
          (do
            (log/warn (.toString e))
            (Thread/sleep 1000)))))
    (log/info (str "Done: " es))))

(defn -main 
  [ & args ]
  (log/info "Starting...")
  (log/info dwc)
  (log/info es)
  (Thread/sleep (* 5 1000))
  (wait-es)
  (log/info "Will start now")
  (setup)
  (Thread/sleep (* 5 1000))
  (let [keep (atom true)]
    (while @keep
      (run-all)
      (swap! keep (fn [_] (or (= (env :loop) "true") false)))
      (when @keep
        (Thread/sleep (* 24 60 60 1000))))))

