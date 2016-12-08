(ns rrapp-idx.config
  (:require [taoensso.timbre :as log])
  (:require [clojure.java.io :as io])
  (:require [environ.core :refer [env]])
  (:gen-class))

(defn config-file
  []
  (let [env   (io/file (or (env :config) "/etc/biodiv/config.ini"))
        base  (io/resource "config.ini")]
    (if (.exists env)
      env
      base)))

(defn config
  ([] 
    (with-open [rdr (io/reader (config-file))]
      (->> (line-seq rdr)
           (map #(.trim %))
           (filter #(and (not (nil? %)) (not (empty? %))))
           (map (fn [line] ( .split line "=" )))
           (map (fn [pair] [(keyword (.toLowerCase (.trim (first pair)))) (.trim (last pair))]))
           (map (fn [kv] {(first kv) (or (env (first kv)) (last kv))}))
           (reduce merge {}))))
  ([k] ((config) k)))

