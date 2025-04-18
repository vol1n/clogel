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
                   (gel-type->clogel-type (:name obj))
                   (let [parse-properties
                         (fn [properties]
                           (reduce (fn [acc prop]
                                     (assoc acc
                                            (gel-type->clogel-type (:name prop))
                                            {:card     (card-str->clogel-card (:cardinality prop))
                                             :type     (gel-type->clogel-type
                                                        (get-in prop [:target :name]))
                                             :required (:required prop)}))
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
    },
    properties: {
        name,
        cardinality,
        required,
        target: { name },
    },
    constraints: { name },
    indexes: { expr },
}"))


(def raw-objects (get-object-types))

;; { :User { :firstName {:type :str :card :singleton} :lastName {:type :str :card :singleton} } }
(def object-registry (parse-object-registry raw-objects))


