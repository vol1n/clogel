(ns vol1n.clogel.client
  (:require [cheshire.core :as json]
            [clojure.string :as str])
  (:import [com.geldata.driver GelClientPool]
           [java.util HashMap]))

(def client-pool (atom (GelClientPool.)))



(defn java-map
  [m]
  (let [hm (HashMap.)]
    (doseq [[k v] m] (.put hm (str/join (rest (str k))) v))
    hm))

(comment
  (java-map {})
  (java-map {:key1 "val1"}))

(defn query
  ([query-str] (query query-str {}))
  ([query-str params]
   (-> (.queryJson @client-pool query-str (java-map params))
       (.toCompletableFuture)
       (.get)
       (.getValue)
       (json/parse-string true))))
