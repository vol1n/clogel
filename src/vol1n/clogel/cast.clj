(ns vol1n.clogel.cast
  (:require [vol1n.clogel.util :refer [->Overload remove-colon-kw ->Node gel-type->clogel-type]]
            [clojure.string :as str]
            [vol1n.clogel.object-util :refer [raw-objects object-registry]]
            [vol1n.clogel.castable :refer [castable? implicit-castable? casts parsed-casts]])
  (:refer-clojure :exclude [cast]))


(defn build-cast-validator
  [cast]
  (fn [[_ target]]
    (if (= (:from-type cast) (:type target))
      {:type (:to-type cast) :card (:card target)}
      {:error/error   true
       :error/message (str (:type target) "does not satisfy" (:from-type cast))})))

(defn build-cast-compiler
  [cast]
  (fn [_ & [compiled-child]] (str \< (:name (:to_type cast)) \> compiled-child)))

(defn build-cast-overload-form
  [cast]
  (->Overload (build-cast-validator cast) (build-cast-compiler cast)))

(def cast-children-generator (fn [[_ from]] [from]))

(def cast-type-form (fn [cast types] [(first cast) (first types)]))

(defn build-object-cast-validator
  [object-type-keyword]
  (fn [[_ expr]]
    (if (or (contains? object-registry (:type expr))
            (:object-type expr)
            (map? (:type expr))
            (#{:uuid :empty :anyobject :anytype} (:type expr)))
      {:type object-type-keyword :card (:card expr)}
      {:error/error true :error/message "Can only cast object types or UUIDs to object type"})))

(defn build-object-cast-compiler
  [object-type]
  (fn [_ & [compiled-child]] (str \< object-type \> compiled-child)))

(defn cast-op-keyword
  [typename]
  (keyword (str "cast_" (str/replace (remove-colon-kw typename) #"::|\/" "__"))))

(defmacro defgelcasts
  []
  (let [no-ns #(symbol (str %))
        grouped (group-by :to-type parsed-casts)
        reg (into {}
                  (map (fn [[to-type casts]] [(cast-op-keyword to-type)
                                              (->Node (cast-op-keyword to-type)
                                                      cast-children-generator
                                                      cast-type-form
                                                      (vec (map build-cast-overload-form casts)))])
                       grouped))
        parsed-objects (mapv #(-> %
                                  (assoc :clogel-name (gel-type->clogel-type (:name %))))
                             raw-objects)
        object-casts (into {}
                           (map (fn [obj-type] [(cast-op-keyword (:clogel-name obj-type))
                                                (->Node (cast-op-keyword (:clogel-name obj-type))
                                                        cast-children-generator
                                                        cast-type-form
                                                        [(->Overload (build-object-cast-validator
                                                                      (:clogel-name obj-type))
                                                                     (build-object-cast-compiler
                                                                      (:name obj-type)))])])
                                parsed-objects))
        all-casts (merge object-casts reg)
        funcs (map (fn [[to-type _]]
                     (let [kw (if (map? to-type)
                                (let [[k v] (first to-type)]
                                  (keyword (str (remove-colon-kw k)
                                                "-"
                                                (str/join "," (map remove-colon-kw v)))))
                                to-type)
                           sym (symbol (str "cast-"
                                            (if (namespace kw)
                                              (str/join "::" [(namespace kw) (name kw)])
                                              (str (name kw)))))
                           arg-vec ['to-cast]]
                       `(def ~sym (fn ~arg-vec [~(cast-op-keyword to-type) ~(symbol "to-cast")]))))
                   grouped)
        def-registry `(def ~(no-ns 'gelcast-registry) ~all-casts)]
    `(do ~def-registry ~@funcs)))

(defn cast [to expr] [(cast-op-keyword (keyword to)) expr])


(comment
  (casts->from-to (get-casts))
  (macroexpand (defgelcasts)))
