(ns vol1n.clogel.util
  (:require [clojure.string :as str]))

(def ^:dynamic *clogel-dot-access-context* nil)

(def ^:dynamic *clogel-with-bindings* {})

(def ^:dynamic *clogel-param-bindings* {})

(defrecord Overload [validator compile-fn])
(defrecord Node [kw generate-children build-type-form overloads])

(defn remove-colon-kw [kw] (str/join (rest (str kw))))

(declare parse-generic-type gel-type->clogel-type) ;; :)

(defn max-card
  [cards]
  (reduce (fn [acc c]
            (case acc
              :singleton c
              :empty :empty
              :many (if (= :empty c) :empty :many)
              :optional (if (or (= :empty c) (= :many c)) c :optional)))
          :singleton
          cards))

(defn parse-generic-type
  [s]
  (letfn [(split-generic [s]
            (loop [chars (seq s)
                   level 0
                   curr ""
                   acc []]
              (if (empty? chars)
                (conj acc curr)
                (let [c (first chars)]
                  (cond (= c \<) (recur (rest chars) (inc level) (str curr c) acc)
                        (= c \>) (recur (rest chars) (dec level) (str curr c) acc)
                        (and (= c \,) (= level 0)) (recur (rest chars) level "" (conj acc curr))
                        :else (recur (rest chars) level (str curr c) acc))))))]
    (if-let [[_ outer inner] (re-matches #"^([^<]+)<(.+)>$" s)]
      {(keyword outer) (mapv gel-type->clogel-type (split-generic inner))}
      (gel-type->clogel-type s))))

(defn gel-type->clogel-type
  [s]
  (if (re-find #"<" s)
    (parse-generic-type s)
    (let [kw (-> s
                 (str/replace-first #"::" "/")
                 keyword)]
      (if (or (= (namespace kw) "std") (= (namespace kw) "default")) (keyword (name kw)) kw))))
