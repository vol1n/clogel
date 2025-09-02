(ns vol1n.clogel.client
  (:require [cheshire.core :as json]
            [clojure.string :as str]
            [clojure.walk :as walk]
            [clojure.pprint :refer [pprint]])
  (:import [com.geldata.driver GelClientPool]
           [java.util HashMap]
           [java.util.function Function BiConsumer BiFunction]
           [java.util.concurrent CompletionStage CompletableFuture TimeUnit]))

(def client-pool
  (atom
   (if (System/getenv "GEL_INSTANCE")
     ;; the Java library is not set up for production connections as
     ;; outlined in the docs. this is a workaround.
     (let [_ (println "getting env vars"
                      (System/getenv "GEL_SECRET_KEY")
                      (System/getenv "GEL_INSTANCE"))
           sk (System/getenv "GEL_SECRET_KEY")
           tmp (doto (java.io.File/createTempFile "gel-cred" ".json") (.deleteOnExit))
           json-data {:secret_key sk}
           ]
       (println "tmp" (.getAbsolutePath tmp))
       ;; write JSON
       (spit tmp (json/encode json-data))
       ;; set GEL_CREDENTIALS_FILE for this JVM
       (System/setProperty "GEL_CREDENTIALS_FILE" (.getAbsolutePath tmp))
       ;; unset the others (affects only this process, not the parent shell)
       (System/clearProperty "GEL_INSTANCE")
       (println "credentials file" (slurp (.getAbsolutePath tmp)))
       (let [pool (GelClientPool.)]
         (try (.get (.toCompletableFuture (.queryJson pool "select 42;")))
              (catch Exception e
                (println "ERROR INITIALIZING GEL CLOUD CONNECTION")
                ;; raw stack trace
                (.printStackTrace e)
                ;; message from the server (usually the important bit)
                (println "Message:" (.getMessage e))
                ;; Gel-specific code, if it exposes one
                (when-let [code (try (.getCode e) (catch Exception _ nil))] (println "Code:" code))
                ;; optional: turn stack trace into a seq for easier reading
                (doseq [frame (.getStackTrace e)] (println frame))))
         pool))
     (do (println "GEL_INSTANCE NOT SET") (GelClientPool.)))))

;; (def client-pool (atom (GelClientPool.)))

(defn dehyphenate-symbol [sym] (symbol (str/replace (str sym) "-" "_")))

(def ^:dynamic *clogel-tx* nil)

(defn java-map
  [m]
  (let [hm (HashMap.)]
    (doseq [[k v] m] (.put hm (str/join (rest (str (dehyphenate-symbol k)))) v))
    hm))

(comment
  (java-map {})
  (java-map {"key-1" "val1"}))

;; (defn -query-stage
;;   [client query-str params]
;;   (try (println "-query-stage" client query-str (java-map params))
;;        (-> (.queryJson client query-str (java-map params))
;;            (.thenApply (reify
;;                         java.util.function.Function
;;                           (apply [_ raw-json]
;;                             (cheshire.core/parse-string (.getValue raw-json) true)))))
;;        (catch Exception e (println "-query-stage exception" e))))
;;

(defn -query-stage
  [client query-str params]
  (-> (.queryJson client query-str (java-map params))
      (.thenApply (reify
                   Function
                     (apply [_ raw-json] (cheshire.core/parse-string (.getValue raw-json) true))))
      ;; log any async failure
  ))

(defn -query
  [client query-str params]
  (let [raw (-> (.queryJson client query-str (java-map params))
                (.toCompletableFuture)
                (.get)
                (.getValue))
        res (json/parse-string raw true)]
    res))

(defn query
  ([query-str] (query query-str {}))
  ([query-str params]
   (if *clogel-tx*
     (-query-stage *clogel-tx* query-str params)
     (-query @client-pool query-str params))))
