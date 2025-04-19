(ns vol1n.clogel-kondo.hooks
  (:require [clojure.edn :as edn]
            [vol1n.clogel.util :refer
             [map->Node map->Overload *clogel-dot-access-context* *clogel-param-bindings*
              *clogel-with-bindings*]]
            [vol1n.clogel.object-types :refer [dot-access build-object-registry]]
            [vol1n.clogel.top-level :refer [clogel-top-level-statements]]
            [vol1n.clogel.scalar :refer [clogel-scalars]]
            [vol1n.clogel.collection :refer [clogel-collections]]
            [vol1n.clogel.functions-operators :refer [gel-index build-func-registry]]
            [vol1n.clogel.cast :refer [build-cast-registry]]
            [clj-kondo.hooks-api :as api]
            [sci.core :as sci]
            [clojure.walk]))

(def readers
  {:readers {'vol1n.clogel.util/Node map->Node 'vol1n.clogel.util/Overload map->Overload}})

(defn restore-keys
  [data]
  (clojure.walk/postwalk (fn [x] (if (= x :__value) (keyword nil "@value") x)) data))

(defonce gelobject-registry
  (->> (slurp "clj-kondo.exports/objects.edn")
       (edn/read-string readers)
       restore-keys
       build-object-registry))

(defonce gelcast-registry
  (->> (slurp "clj-kondo.exports/casts.edn")
       (edn/read-string readers)
       restore-keys
       build-cast-registry))

(defonce gelfunc-registry
  (do (println "slurped" (slurp "clj-kondo.exports/functions.edn"))
      (->> (slurp "clj-kondo.exports/functions.edn")
           (edn/read-string readers)
           restore-keys
           build-func-registry)))

(defonce gelop-registry
  (->> (slurp "clj-kondo.exports/operators.edn")
       (edn/read-string readers)
       restore-keys
       build-func-registry))

(defonce node-registry
  (merge dot-access
         gelfunc-registry
         gelop-registry
         gelobject-registry
         gelcast-registry
         clogel-top-level-statements
         {:scalar clogel-scalars}
         {:collection clogel-collections}
         gel-index))

(defn match-overload
  [call overloads]
  (let [matched (some (fn [ovl]
                        (let [result ((:validator ovl) call)]
                          (when (clojure.core/not (:error/error result))
                            {:overload ovl :annotations result})))
                      overloads)]
    matched))

(def mod-keys #{:limit :order-by :filter :offset})

(defn find-projected-path
  [proj-kondo-node path]
  (loop [node proj-kondo-node
         p path]
    (if (= (count p) 1)
      (some #(when (= (api/sexpr %) (first p)) %) (:children node))
      (recur (some #(when (when (api/map-node %)
                            (let [sexpr (api/sexpr %)
                                  main-key (some (fn [entry]
                                                   (when (not (contains? mod-keys (key entry)))
                                                     (key entry)))
                                                 sexpr)]
                              (and (map? sexpr) (= main-key (first path)))))
                      %)
                   (:children node))
             (rest path)))))

(defn validate-clogel-form
  [edn kondo-node]
  (let [node-key (cond (clojure.core/and (map? edn) (:with edn)) :with
                       (map? edn) (some #(when (not (contains? mod-keys %)) %) (keys edn))
                       (clojure.core/and (vector? edn) (keyword? (first edn))) (first edn)
                       (keyword? edn) edn
                       (symbol? edn) :dot-access
                       (coll? edn) :collection
                       :else :scalar)
        node (get node-registry node-key)]
    (if (clojure.core/not node)
      (throw (ex-info (str "invalid keyword" node-key)
                      {:valid-keys (keys node-registry) :passed node-key}))
      (let [children ((:generate-children node) edn)
            kondo-children ((:generate-kondo-children node) kondo-node)
            compiled-children
            (if (nil? children)
              (seq [])
              ;; if it's an object, we need to know what object we're in for children of this
              ;; node
              (if (contains? gelobject-registry node-key)
                (binding [*clogel-dot-access-context* {:type node-key :card :many}]
                  (map (bound-fn [[child kondo-node]] (validate-clogel-form child kondo-node))
                       (map vector children kondo-children)))
                ;; if it's a top level statement (i.e. select)
                ;; we need to know what object we're selecting for modifier statements
                ;; ex. select User filter .name = "John";
                (if (contains? clogel-top-level-statements node-key)
                  (cond
                    (clojure.core/= node-key :for)
                    (let [compiled-binding (validate-clogel-form (first children)
                                                                 (first kondo-children))]
                      (binding [*clogel-with-bindings*
                                (assoc *clogel-with-bindings* (first (:for edn)) compiled-binding)]
                        [compiled-binding
                         (validate-clogel-form (last children) (last kondo-children))]))
                    (clojure.core/= node-key :group)
                    (let [compiled-group-statement (validate-clogel-form (first children)
                                                                         (first kondo-children))
                          {compiled-with-children :compiled bindings :with-bindings}
                          (reduce (fn [compiled [b c k]]
                                    (let [result (validate-clogel-form c k)]
                                      (binding [*clogel-with-bindings*
                                                (assoc *clogel-with-bindings* b result)]
                                        {:compiled      (conj (:compiled compiled) result)
                                         :with-bindings *clogel-with-bindings*})))
                                  {:compiled [] :with-bindings *clogel-with-bindings*}
                                  (map vector
                                       (map first (:using edn))
                                       (clojure.core/take (count (:using edn)) (rest children))
                                       (clojure.core/take (count (:using edn))
                                                          (rest kondo-children))))]
                      (binding [*clogel-with-bindings* bindings]
                        (conj (cons compiled-group-statement compiled-with-children)
                              (map #(validate-clogel-form %1 %2)
                                   (clojure.core/drop (inc (count (:using edn))) children)
                                   (clojure.core/drop (inc (count (:using edn))) kondo-children)))))
                    (clojure.core/= node-key :with)
                    (let [{compiled-with-children :compiled bindings :with-bindings}
                          (reduce (fn [compiled [b c k]]
                                    (let [result (validate-clogel-form c k)]
                                      (binding [*clogel-with-bindings*
                                                (assoc *clogel-with-bindings* b result)]
                                        {:compiled      (conj (:compiled compiled) result)
                                         :with-bindings *clogel-with-bindings*})))
                                  {:compiled [] :with-bindings *clogel-with-bindings*}
                                  (map vector
                                       (map first (:with edn))
                                       (butlast children)
                                       (butlast kondo-children)))]
                      (binding [*clogel-with-bindings* bindings]
                        (conj compiled-with-children
                              (validate-clogel-form (last children) (last kondo-children)))))
                    :else (let [compiled-first (validate-clogel-form (first children)
                                                                     (first kondo-children))]
                            (binding [*clogel-dot-access-context* {:type (:type compiled-first)
                                                                   :card (:card compiled-first)}]
                              (into [compiled-first]
                                    (reduce (fn [acc [c k]] (conj acc (validate-clogel-form c k)))
                                            []
                                            (map vector (rest children) (rest kondo-children)))))))
                  (map #(validate-clogel-form %1 %2) children kondo-children))))
            type-form ((:build-type-form node) edn compiled-children)
            {annotations :annotations overload :overload} (match-overload type-form
                                                                          (:overloads node))]
        (if (clojure.core/not overload)
          (throw (ex-info (str "No overload for node of type " node-key " for value " edn)
                          {:node edn :valid (:overloads node)}))
          annotations)))))

(defn dynamic-eval-clogel [full-form])

(defn clogel-hook [] ())
