(ns vol1n.clogel.object-cast)


(defn card-gte?
  [l r]
  (case l
    :singleton (= r :singleton)
    :empty (= r :empty)
    :optional (#{:empty :singleton :optional} r)
    :many true))

(defn validate-object-type-cast
  [object-registry object-type cast-type]
  (let [object-type-map (get object-registry object-type)]
    (cond
      (nil? object-type-map) {:error/error   true
                              :error/message (str "Object type " object-type "not in registry. ")}
      (keyword? cast-type)
      (if (contains? object-registry cast-type)
        (recur object-registry object-type (get object-registry cast-type))
        {:error/error true
         :error/message
         (str
          "Type"
          cast-type
          "not an object type so not castable to object-type.
Did you try and use a scalar as an object?"
          object-type)})
      :else (let [required-fields (filter (fn [[k v]] (and (:required v) (not= k :id)))
                                          object-type-map)
                  fields (reduce (fn [acc [k v]]
                                   (let [type-def (get object-type-map k)]
                                     (if (or (implicit-castable? (:type v) (get cast-type k))
                                             (card-gte? (:card v) (:card type-def)))
                                       (reduced {:error/error true
                                                 :error/message
                                                 (str "Key with cardinality " (:card v)
                                                      " and type " (:type v)
                                                      "does not cast onto object field with "
                                                      {:card (:card type-def)
                                                       :type (:type type-def)})})
                                       (;conj acc k
                                       ))))
                                 []
                                 cast-type)]
              (if (not (every? (set fields) required-fields))
                {:error/error   true
                 :error/message (str "Not every required field is there for " object-type
                                     " Required: " required-fields
                                     " Received " fields)}
                true)))))
