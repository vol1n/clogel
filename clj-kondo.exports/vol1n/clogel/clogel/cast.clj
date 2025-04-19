(ns vol1n.clogel.cast
  (:require [vol1n.clogel.util :refer [->Overload remove-colon-kw ->Node gel-type->clogel-type]]
            [clojure.string :as str]
            [vol1n.clogel.castable :refer [castable? implicit-castable? casts]]))


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

(defn build-cast-registry
  [casts]
  (let [parsed (map #(-> %
                         (assoc :to-type (gel-type->clogel-type (get-in % [:to_type :name])))
                         (assoc :from-type (gel-type->clogel-type (get-in % [:from_type :name]))))
                    casts)
        grouped (group-by :to-type parsed)]
    (into {}
          (map (fn [[to-type casts]] [(keyword (str "cast-" (remove-colon-kw to-type)))
                                      (->Node (keyword (str "cast-" (remove-colon-kw to-type)))
                                              cast-children-generator
                                              cast-type-form
                                              (vec (map build-cast-overload-form casts)))])
               grouped))))

(defonce gelcast-registry (build-cast-registry casts))

(defmacro defgelcasts
  []
  (let [parsed (map #(-> %
                         (assoc :to-type (gel-type->clogel-type (get-in % [:to_type :name])))
                         (assoc :from-type (gel-type->clogel-type (get-in % [:from_type :name]))))
                    casts)
        grouped (group-by :to-type parsed)
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
                       `(def ~sym
                          (fn ~arg-vec
                            (println ~(symbol "to-cast"))
                            [~(keyword (str "cast-" (remove-colon-kw kw))) ~(symbol "to-cast")]))))
                   grouped)]
    `(do ~@funcs)))




(comment
  (macroexpand (defgelcasts)))
