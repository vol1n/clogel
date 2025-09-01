(ns vol1n.clogel.object-util
  (:require [vol1n.clogel.util :refer [gel-type->clogel-type]]
            [vol1n.clogel.client :refer [query]]))

(defn card-str->clogel-card
  [s]
  (case s
    "Many" :many
    "One" :singleton))

(defn parse-object-registry
  [raw]
  (reduce (fn [acc obj]
            (assoc acc
                   (let [name (gel-type->clogel-type (:name obj))]
                     (if (= name :Object) :GelObject name))
                   (let [parse-properties
                         (fn [properties]
                           (reduce (fn [acc prop]
                                     (assoc acc
                                            (gel-type->clogel-type (:name prop))
                                            {:card     (card-str->clogel-card (:cardinality prop))
                                             :type     (gel-type->clogel-type
                                                        (get-in prop [:target :name]))
                                             :computed (boolean (seq (:expr prop)))
                                             :required (:required prop)
                                             :default  (:default prop)}))
                                   {}
                                   properties))]
                     (merge (parse-properties (:links obj)) (parse-properties (:properties obj))))))
          {}
          raw))

(defn get-object-types
  []
  (query
   "
select schema::ObjectType {
    name,
    abstract,
    bases: { name },
    ancestors: { name },
    annotations: { name, @value },
    links: {
        name,
        cardinality,
        required,
        target: { name },
        expr
    },
    properties: {
        name,
        cardinality,
        required,
        default,
        target: { name },
        expr
    },
    constraints: { name },
    indexes: { expr },
}"))


(def raw-objects (get-object-types))

;; { :User { :firstName {:type :str :card :singleton} :lastName {:type :str :card :singleton} } }
(def object-registry (parse-object-registry raw-objects))
