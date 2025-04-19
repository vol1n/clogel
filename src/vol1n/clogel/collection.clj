(ns vol1n.clogel.collection
  (:require [vol1n.clogel.util :refer [->Node ->Overload]]
            [vol1n.clogel.types :refer [lub]]
            [clojure.string :as str]
            [clj-kondo.hooks-api :as api]))

(def clogel-collections
  (->Node
   :collection
   (fn [coll] (seq coll))
   (fn [kondo-node _]
     (if (api/list-node? kondo-node)
       (let [sexpr (api/sexpr kondo-node)]
         (if (= (first sexpr) 'set)
           (-> kondo-node
               :children
               last
               :children)
           (if (= (first sexpr) 'list) (rest (:children kondo-node)) (:children kondo-node))))
       (:children kondo-node)))
   (fn [coll types]
     (println "types" types)
     (if (set? coll) (into '() types) (into (empty coll) types)))
   [(->Overload #(if (vector? %)
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
