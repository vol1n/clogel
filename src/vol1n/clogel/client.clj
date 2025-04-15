(ns vol1n.clogel.client
  (:require [cheshire.core :as json])
  (:import [com.geldata.driver GelClientPool]
           [java.util HashMap]))

(def client-pool (atom (GelClientPool.)))


(defn java-map
  [m]
  (let [hm (HashMap.)]
    (doseq [[k v] m] (.put hm (str k) v))
    hm))


(defn query
  ([query-str] (query query-str {}))
  ([query-str params]
   (-> (.queryJson @client-pool query-str (java-map params))
       (.toCompletableFuture)
       (.get)
       (.getValue)
       (json/parse-string true))))
