(ns biodividx.http
  (:require [clj-http.lite.client :as client])
  (:require [clojure.data.json :as json])
  (:require [taoensso.timbre :as log])
  (:gen-class))

(defn get-json
  [& url] 
  (log/info "Get JSON" (apply str url))
  (try
    (:result (json/read-str (:body (client/get (apply str url))) :key-fn keyword))
    (catch Exception e 
      (do (log/error (str "Failled get JSON " (apply str url)))
          (log/error e)))))

(defn post-json
  [url body] 
  (log/info "POST JSON" url)
  (try
    (-> 
      (client/post url {:content-type :json :body (json/write-str body)})
      :body
      (json/read-str :key-fn keyword))
    (catch Exception e 
      (do (log/error (str "Failed POST JSON " (apply str url)) body)
          (log/error e)))))

