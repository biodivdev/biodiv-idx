(ns rrapp-idx.core
  (:use rrapp-idx.db)
  (:use rrapp-idx.calc)
  (:use rrapp-idx.config)
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
        families-d  (chan 2)
        species     (chan 5)
        names       (chan 5)
        occurrences (chan 2)
        results     (chan 5)]

  (pipe families clean-family families-d)
  (pipe families-d get-species species)
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
        (log/info (str "Waiting: " (config :elasticsearch)))
        (let [r (client/get (str (config :elasticsearch) "/" (config :index)) {:throw-exceptions false})]
          (if (= 200 (:status r))
            (reset! done true)
            (Thread/sleep 1000)))
        (catch Exception e 
          (do
            (log/warn (.toString e))
            (Thread/sleep 1000)))))
    (log/info (str "Done: " (config :elasticsearch)))))

(defn -main 
  [ & args ]
  (log/info "Starting...")
  (log/info (config :elasticsearch))
  (Thread/sleep (* 5 1000))
  (wait-es)
  (log/info "Will start now")
  (setup)
  (Thread/sleep (* 5 1000))
  (let [keep (atom true)]
    (while @keep
      (run-all)
      (swap! keep (fn [_] (or (= (config :loop) "true") false)))
      (when @keep
        (Thread/sleep (* 24 60 60 1000))))))

