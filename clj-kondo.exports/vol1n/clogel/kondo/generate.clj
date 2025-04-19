(ns vol1n.clogel.kondo.generate
  (:require [vol1n.clogel.functions-operators :refer [gel-index ops fns edgename->keyword]]
            [vol1n.clogel.top-level :refer [clogel-top-level-statements]]
            [vol1n.clogel.scalar :refer [clogel-scalars]]
            [vol1n.clogel.collection :refer [clogel-collections]]
            [vol1n.clogel.object-util :refer [raw-objects]]
            [vol1n.clogel.cast :refer [gelcast-registry]]
            [vol1n.clogel.util :refer [remove-colon-kw gel-type->clogel-type]]
            [clojure.string :as str]
            [vol1n.clogel.castable :refer [parsed-casts]]))



(defn get-all-names [keys] (map #(str/replace (remove-colon-kw %) "/" "::") keys))

(defn get-fn-name
  [f]
  (let [kw (edgename->keyword (:name f))
        replaced (if (= kw :=) :equals kw)]
    (if (#{"://"} (str kw))
      nil
      (if (namespace replaced)
        (str/join "::" [(namespace replaced) (name replaced)])
        (str (name replaced))))))

(defn get-op-alias
  [f]
  (let [alias (some (fn [a]
                      (when (= (:name a) "std::identifier") (keyword (get a (keyword "@value")))))
                    (:annotations f))]
    (if (nil? alias)
      nil
      (str/lower-case (if (namespace alias)
                        (str/join "::" [(namespace alias) (name alias)])
                        (str (name alias)))))))

(defn get-cast-name
  [c]
  (let [to-type (:to-type c)
        kw (if (map? to-type)
             (let [[k v] (first to-type)]
               (keyword (str (remove-colon-kw k) "-" (str/join "," (map remove-colon-kw v)))))
             to-type)]
    (str "cast-" (if (namespace kw) (str/join "::" [(namespace kw) (name kw)]) (str (name kw))))))

(defn get-object-name
  [o]
  (let [kw (gel-type->clogel-type (:name o))
        object-string
        (if (namespace kw) (str/join "::" [(namespace kw) (name kw)]) (str (name kw)))]
    (if (= object-string "Object")
      "GelObject" ;; mapping to Object breaks Java
      object-string)))

(defn write-kondo-config!
  []
  (let [dynamic-names (concat (map get-fn-name fns)
                              (mapcat (fn [op] [(get-fn-name op) (get-op-alias op)]) ops)
                              (map get-cast-name parsed-casts)
                              (map get-object-name raw-objects))]
    (spit "clj-kondo.exports/clogel/objects.edn" (pr-str raw-objects))
    (spit "clj-kondo.exports/clogel/casts.edn" (pr-str parsed-casts))
    (spit "clj-kondo.exports/clogel/functions.edn" (pr-str fns))
    (spit "clj-kondo.exports/clogel/operators.edn" (pr-str ops))
    ;;(spit "clj-kondo.exports/clogel/top_level.edn" (pr-str clogel-top-level-statements))
    ;;(spit "clj-kondo.exports/clogel/scalars.edn" (pr-str {:scalars clogel-scalars}))
    ;;(spit "clj-kondo.exports/clogel/collections.edn" (pr-str {:collection
    ;;clogel-collections}))
    ;;(spit "clj-kondo.exports/clogel/index.edn" (pr-str gel-index))
    (spit "clj-kondo.exports/clogel/config.edn"
          (pr-str {:hooks {:analyze-call (map #(str "vol1n.clogel.core/" %)
                                              (concat dynamic-names
                                                      (keys clogel-top-level-statements)))}}))))

(defn -main [] (write-kondo-config!))
