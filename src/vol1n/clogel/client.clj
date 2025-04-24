(ns vol1n.clogel.client
  (:require [cheshire.core :as json]
            [clojure.string :as str])
  (:import [com.geldata.driver GelClientPool]
           [java.util HashMap]))

(def client-pool (atom (GelClientPool.)))

(defn dehyphenate-symbol [sym] (symbol (str/replace (str sym) "-" "_")))

(defn java-map
  [m]
  (let [hm (HashMap.)]
    (doseq [[k v] m] (.put hm (str/join (rest (str (dehyphenate-symbol k)))) v))
    (println "hm" hm)
    hm))

(comment
  (java-map {})
  (java-map {"key-1" "val1"}))

(defn query
  ([query-str] (query query-str {}))
  ([query-str params]
   (-> (.queryJson @client-pool query-str (java-map params))
       (.toCompletableFuture)
       (.get)
       (.getValue)
       (json/parse-string true))))
