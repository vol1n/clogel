(ns vol1n.clogel.castable
  (:require [vol1n.clogel.client :refer [query]]
            [vol1n.clogel.object-util :refer [object-registry raw-objects]]
            [vol1n.clogel.util :refer [gel-type->clogel-type]]
            [clojure.set :as set]))

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


(defn objects->ancestors-map
  [entries]
  (reduce (fn [acc obj]
            (update acc
                    (:clogel-name obj)
                    #(reduce (fn [acc a] (conj acc a)) (or % #{}) (:ancestor-names obj))))
          {}
          entries))

(defn parse-objects
  [objects]
  (map #(-> %
            (assoc :clogel-name
                   (let [name (gel-type->clogel-type (:name %))]
                     (if (= name :Object) :GelObject name)))
            (assoc :ancestor-names
                   (reduce (fn [acc a]
                             (conj acc
                                   (let [name (-> a
                                                  :name
                                                  gel-type->clogel-type)]
                                     (if (= name :Object) :GelObject name))))
                           #{}
                           (:ancestors %))))
       objects))

(def ancestry-map (objects->ancestors-map (parse-objects raw-objects)))

(comment
  (let [g {:a #{:b :c} :b #{:c} :c #{:a}}] (path-exists? g :b :a))
  (let [g {:a #{:b :c} :b #{:c} :c #{:a}}] (path-exists? g :a :d)))

(defn is-ancestor?
  [ancestor child]
  (or (= ancestor child) (path-exists? ancestry-map child ancestor)))

(comment
  (is-ancestor? :GelObject :user/User)
  (is-ancestor? :user/User :GelObject))

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
  (cond
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
    (keyword? object-type)
    (if (contains? object-registry object-type)
      (recur (get object-registry object-type) cast-type)
      {:error/error true
       :error/message
       (str
        "Type"
        object-type
        "not an object type so not castable to object-type.
Did you try and use a scalar as an object?"
        object-type)})
    :else (let [required-fields
                (keep (fn [[k v]]
                        (when (and (not (:default v)) (:required v) (not (#{:id :__type__} k))) k))
                      object-type)
                fields (reduce (fn [acc [k v]]
                                 (let [type-def (get object-type k)]
                                   (if type-def
                                     (if (and (implicit-castable? (:type v)
                                                                  (:type (get cast-type k)))
                                              (card-gte? (:card v) (:card type-def)))
                                       (conj acc k)
                                       (reduced {:error/error true
                                                 :error/message
                                                 (str "Key with cardinality "
                                                      (:card v)
                                                      " and type "
                                                      (:type v)
                                                      "does not cast onto object field with "
                                                      "type "
                                                      (:type type-def)
                                                      {:card (:card type-def)
                                                       :type (:type type-def)})}))
                                     (conj acc k))))
                               []
                               cast-type)]
            (if (:error/error fields)
              fields
              (if (not (set/superset? (set fields) (set required-fields)))
                {:error/error   true
                 :error/message (str "Not every required field is there for " object-type
                                     " Required: " (vec required-fields)
                                     " Received " fields)}
                true)))))
