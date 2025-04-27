(ns vol1n.clogel.collection
  (:require [vol1n.clogel.util :refer [->Node ->Overload]]
            [vol1n.clogel.types :refer [lub]]
            [clojure.string :as str]))

(def clogel-collections
  (->Node
   :collection
   (fn [coll] (seq coll))
   (fn [coll types]
     (println "types" types)
     (if (set? coll) (into '() types) (into (empty coll) types)))
   [(->Overload #(if (and (vector? %) (= :tuple (first %)))
                   {:type {:tuple (mapv {:type (:type %) :card :singleton} %)}
                    :card (if (every? (fn [item] (= (:card item) :singleton)) %) :singleton :many)}
                   {:error/error true :error/message "Not a valid tuple form"})
                (fn [_ & compiled-children] (str "(" (str/join ", " compiled-children) ")")))
    (->Overload #(if (vector? %)
                   {:type {:array [(lub (map :type %))]}}
                   {:error/error true :error/message "Not a vector"})
                (fn [_ & compiled-children] (str \[ (str/join ", " compiled-children) \])))
    (->Overload #(if (or (set? %) (list? %))
                   (let [lub-type (lub (map :type %))]
                     {:type        lub-type
                      :card        (cond (= (count %) 0) :empty
                                         (= (count %) 1) :singleton
                                         :else :many)
                      :insertable  (every? (fn [t] (get-in t [:type :insertable])) %)
                      :object-type (if (every? :object-type %) lub-type nil)
                      :updatable   (every? (fn [t] (get-in t [:type :updatable])) %)})
                   {:error/error true :error/message "Not a set (or list)"})
                (fn [_ & compiled-children] (str \{ (str/join ", " compiled-children) \})))]))


(defn tuple [& args] (into [:tuple] args))
