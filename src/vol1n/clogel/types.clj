(ns vol1n.clogel.types
  (:require [vol1n.clogel.util :refer [gel-type->clogel-type]]
            [vol1n.clogel.castable :refer
             [implicit-castable? implicit-casts-map validate-object-type-cast]]
            [vol1n.clogel.object-util :refer [object-registry]]
            [vol1n.clogel.client :refer [query]]))

;; A real number x is called an upper bound for S if x ≥ s for all s ∈ S.
;; A real number x is the least upper bound (or supremum) for S if x is an
;; upper bound for S and x ≤ y for every upper bound y of S.
;;
;; x ≥ s iff (castable? s x)
(defn lub
  [types]
  (if (empty? types)
    :empty
    (let [types (map #(or (:object-type %) %) types)
          lubbed (if (apply = types)
                   (first types)
                   (reduce (fn [acc x]
                             (println "acc" acc "x" x)
                             (if (or (nil? acc) (implicit-castable? acc x)) x acc))
                           nil
                           (filter #(every? (fn [s] (implicit-castable? s %))
                                            (filter (fn [t] (not (or (nil? t) (:error/error t))))
                                                    types))
                                   (keys implicit-casts-map))))]
      lubbed)))

(comment
  (lub [:int64 :int32]))

(defrecord AbstractType [kw children])

(defn get-abstract-children
  [generic]
  (let
    [decoloned (apply str (rest (str generic)))
     q
     (str
      "
SELECT schema::ScalarType {
  name
}
FILTER EXISTS (
  SELECT .ancestors FILTER .name = 'std::"
      decoloned
      "'
);
")]
    (query q)))

(defn get-all-objects [] (query "select schema::ObjectType{ name }"))

(comment
  (get-abstract-children :anyscalar)) ;; all upper bounds

(defn build-abstract-types
  []
  (into {:anytype   (->AbstractType :anytype (fn [_] true))
         :empty     (->AbstractType :empty (fn [_] true))
         :anyobject (->AbstractType :anyobject
                                    (set (map #(gel-type->clogel-type (:name %))
                                              (get-all-objects))))}
        (vec (pmap (fn [abstract-type] [abstract-type
                                        (->AbstractType abstract-type
                                                        (set (map #(gel-type->clogel-type (:name %))
                                                                  (get-abstract-children
                                                                   abstract-type))))])
                   #{:anyscalar :anyenum :anytuple :anyint :anyfloat :anyreal :anypoint :anydiscrete
                     :anycontiguous}))))

(def gel-abstract-types (build-abstract-types))

(defn is-abstract-type? [t] (contains? gel-abstract-types t))

(defn child-of-abstract-type?
  [abstract concrete]
  (boolean ((:children (get gel-abstract-types abstract)) concrete)))

(comment
  (child-of-abstract-type? :anyreal :int64))
