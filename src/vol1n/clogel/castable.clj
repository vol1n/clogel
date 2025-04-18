(ns vol1n.clogel.castable
  (:require [vol1n.clogel.client :refer [query]]
            [vol1n.clogel.object-util :refer [object-registry]]
            [vol1n.clogel.util :refer [gel-type->clogel-type]]))

(declare implicit-castable? validate-object-type-cast)


(defn get-casts
  []
  (query
   "
select schema::Cast {
    allow_assignment,
    allow_implicit,
    from_type: { name },
    to_type: { name },
}"))


(defonce casts (get-casts))

(def parsed-casts
  (map #(-> %
            (assoc :to-type (gel-type->clogel-type (get-in % [:to_type :name])))
            (assoc :from-type (gel-type->clogel-type (get-in % [:from_type :name]))))
       casts))

(defn casts->from-to
  [casts]
  (reduce (fn [acc c] (update acc (:from-type c) (fnil conj #{}) (:to-type c))) {} casts))

(def casts-map (casts->from-to parsed-casts))

(def implicit-casts-map (casts->from-to (filter #(:allow_implicit %) parsed-casts)))

(defn path-exists?
  [g start end]
  (loop [visited #{start}
         q (seq [start])]
    (if (empty? q)
      nil
      (if (contains? (get g (first q)) end)
        true
        (recur (conj visited (first q))
               (concat (rest q) (filter #(not (contains? visited %)) (get g (first q)))))))))


(comment
  (let [g {:a #{:b :c} :b #{:c} :c #{:a}}] (path-exists? g :b :a))
  (let [g {:a #{:b :c} :b #{:c} :c #{:a}}] (path-exists? g :a :d)))

(defn castable?
  [from to]
  (if (= from to) true (if (= to :anytype) true (path-exists? casts-map from to))))

(defn implicit-castable?
  [from to]

  (if (some map? [from to])
    (if (not (every? map? [from to]))
      false
      (if (and (= (key (first from)) (key (first to)))
               (every? #(implicit-castable? (first %) (last %))
                       (map vector (val (first from)) (val (first to)))))
        true
        false))
    (let [from (or (:object-type from) from)
          to (or (:object-type to) to)
          result (if (= from to)
                   true
                   (if (contains? object-registry to)
                     (validate-object-type-cast to from)
                     (if (= to :anytype) true (path-exists? implicit-casts-map from to))))]

      result)))

(comment
  (println "casts-map" casts-map)
  (println implicit-casts-map)
  (implicit-castable? :int32 :int64))

(defn card-gte?
  [l r]
  (case l
    :singleton (= r :singleton)
    :empty (= r :empty)
    :optional (#{:empty :singleton :optional} r)
    :many true))

(defn validate-object-type-cast
  [object-type cast-type]
  (let [object-type-map (get object-registry object-type)]
    (cond
      (nil? object-type-map) {:error/error   true
                              :error/message (str "Object type " object-type "not in registry. ")}
      (keyword? cast-type)
      (if (contains? object-registry cast-type)
        (recur object-type (get object-registry cast-type))
        {:error/error true
         :error/message
         (str
          "Type"
          cast-type
          "not an object type so not castable to object-type.
Did you try and use a scalar as an object?"
          object-type)})
      :else
      (let [required-fields (filter (fn [[k v]] (and (:required v) (not= k :id))) object-type-map)
            fields (reduce (fn [acc [k v]]
                             (let [type-def (get object-type-map k)]
                               (if (and (implicit-castable? (:type v) (:type (get cast-type k)))
                                        (card-gte? (:card v) (:card type-def)))
                                 (conj acc k)
                                 (reduced {:error/error   true
                                           :error/message (str
                                                           "Key with cardinality " (:card v)
                                                           " and type " (:type v)
                                                           "does not cast onto object field with "
                                                           {:card (:card type-def)
                                                            :type (:type type-def)})}))))
                           []
                           cast-type)]
        (if (:error/error fields)
          fields
          (do
              (if (not (every? (set required-fields) fields))
                {:error/error   true
                 :error/message (str "Not every required field is there for " object-type
                                     " Required: " required-fields
                                     " Received " fields)}
                true)))))))
