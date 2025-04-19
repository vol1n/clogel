(ns vol1n.clogel.object-types
  (:require [clojure.string :as str]
            [vol1n.clogel.util :refer
             [gel-type->clogel-type ->Node ->Overload remove-colon-kw max-card
              *clogel-dot-access-context* *clogel-with-bindings*]]
            [vol1n.clogel.castable :refer [implicit-castable? validate-object-type-cast]]
            [vol1n.clogel.object-util :refer [object-registry raw-objects]]))


(def mod-keys #{:limit :order-by :filter :offset})

(def modifier-validators
  {:order-by {:fn #(or
                    (= (:card %) :singleton)
                    (and (vector? %) (= (:card (first %)) :singleton) (#{"asc" "desc"} (last %))))
              :message
              "Order by statement must be a singleton or vector [singleton, 'asc' | 'desc'"}
   :limit    {:fn      #(and (#{:int16 :int32 :int64} (:type %))
                             (#{:optional :singleton} (:card %)))
              :message "Limit statement must be an int singleton"}
   :offset   {:fn      #(and (#{:int16 :int32 :int64} (:type %))
                             (#{:optional :singleton} (:card %)))
              :message "Offset statement must be a singleton"}
   :filter   {:fn #(implicit-castable? (:type %) :bool) :message "Filter statement must be bool"}})


(defn build-object-validator
  [object-type]
  (fn validator [object]
    {:type
     (cond (keyword? object) (if (= object (keyword (:clogel-name object-type)))
                               {:card :many :type (:clogel-name object-type) :deletable true}
                               (throw (ex-info "We've got problems" {})))
           (and (map? object) (vector? (last (first object))))
           (do
             (let [vec (last (first object))
                   type (get object-registry (key (first object)))
                   only-assignments (every? (fn [item] (and (map? item) (= (key (first item)) :=)))
                                            vec)]
               (if (not type)
                 (throw (ex-info (str "Cannot project non-object type "
                                      (or (key (first object)) object))
                                 {:error/type type :error/error true}))
                 (let [reduced
                       (reduce
                        (fn [acc item]
                          (cond (and (map? item) (= (key (first item)) :=))
                                ;; assignment
                                (assoc acc
                                       (key (first (val (first item))))
                                       (val (first (val (first item)))))
                                (map? item)
                                ;; nesting
                                (let [main-key
                                      (some #(when (not (contains? mod-keys (key %))) (key %)) item)
                                      modifiers (dissoc item main-key)]
                                  (doseq [[mod val] modifiers]
                                    (when (not ((get-in modifier-validators [mod :fn]) val))
                                      (throw (ex-info
                                              (str "Value " val " for modifier " mod "invalid")
                                              {:error/error true})))
                                    modifiers)
                                  (assoc acc
                                         main-key
                                         (if (true? (get item main-key))
                                           main-key
                                           (validator
                                            {(or (:type (get type main-key))
                                                 (throw (ex-info
                                                         (str "Key " main-key " not found on type")
                                                         {})))
                                             (get item main-key)}))))
                                (keyword? item)
                                (if (contains? type item)
                                  (assoc acc item (get type item))
                                  (throw (ex-info (str "Key " item " does not exist on type" type)
                                                  {:error/error true})))
                                :else
                                (throw
                                 (ex-info
                                  "Invalid type within projection vector expected map or keyword"
                                  {}))))
                        {}
                        vec)]
                   (assoc (cond-> reduced
                            only-assignments (assoc :updatable true)
                            (and only-assignments
                                 (validate-object-type-cast (:clogel-name object-type) reduced))
                            (assoc :insertable true))
                          :object-type
                          (:clogel-name object-type)))))))
     :card :many}))

(defn compile-modifier [k child] (str (remove-colon-kw k) " " child))

(defn sort-keys
  [m]
  (->> (keys m)
       (sort-by (fn [k] [(name k)])) ;; secondary: alphabetical
       vec))

(defn compile-order-by
  [order-by-statement child]
  (if (vector? order-by-statement)
    (str "order by " child " " (last order-by-statement))
    (compile-modifier :order-by child)))

(defn compile-projection
  [proj & compiled-children]
  (let [red (reduce
             (fn [acc item]
               (cond (keyword? item) {:compiled  (str (:compiled acc) (remove-colon-kw item) ",\n")
                                      :remaining (:remaining acc)}
                     (and (map? item) (= (key (first item)) :=))
                     {:compiled  (str (:compiled acc)
                                      (remove-colon-kw (key (first (get item :=))))
                                      " := "
                                      (first (:remaining acc))
                                      ",\n")
                      :remaining (rest (:remaining acc))}
                     :else
                     (let [main-key (some #(when (not (contains? mod-keys (key %))) (key %)) item)
                           modifiers (dissoc item main-key)
                           result (apply compile-projection
                                         (into [(get item main-key)] (:remaining acc)))]
                       {:compiled  (str (:compiled acc)
                                        (remove-colon-kw main-key)
                                        ": "
                                        \{
                                        "\n"
                                        (:compiled result)
                                        (reduce
                                         (fn [acc [k child]]
                                           (if (= k :order-by)
                                             (str acc (compile-order-by (get item :order-by) child))
                                             (str acc (compile-modifier k child))))
                                         ""
                                         (map vector
                                              (sort-keys modifiers)
                                              (take (count modifiers) (:remaining result))))
                                        \})
                        :remaining (drop (count modifiers) (:remaining result))})))
             {:compiled "" :remaining compiled-children}
             proj)]
    red))

(defn build-object-compiler
  [object-type]
  (fn [object & compiled-children]
    (cond (keyword? object) (str (:name object-type) " ")
          (and (map? object) (vector? (last (first object))))
          (str (:name object-type)
               " "
               \{
               "\n"
               (:compiled (apply compile-projection
                                 (into [(val (first object))] compiled-children)))
               \}))))

(defn build-projection-type-form
  [proj & types]
  (reduce (fn [acc item]
            (cond (keyword? item) {:vec (conj (:vec acc) item) :remaining (:remaining acc)}
                  (and (map? item) (= (key (first item)) :=))
                  {:vec       (conj (:vec acc)
                                    {:= {(key (first (val (first item)))) (first (:remaining
                                                                                  acc))}})
                   :remaining (rest (:remaining acc))}
                  :else (let [result (apply build-projection-type-form
                                            (into [(val (first item))] (:remaining acc)))]
                          {:vec       (conj (:vec acc) {(key (first item)) (:vec result)})
                           :remaining (:remaining result)})))
          {:vec [] :remaining types}
          proj))

(def build-type-form
  (fn [object types]
    (cond (keyword? object) object
          (map? object) {(key (first object)) (:vec (apply build-projection-type-form
                                                           (into [(val (first object))] types)))}
          :else (throw (ex-info "Botch" {})))))

(defn get-projection-children
  [proj]
  (reduce (fn [acc item]
            (cond (keyword? item) acc
                  (and (map? item) (= (key (first item)) :=)) (conj acc (val (first (get item :=))))
                  :else (concat acc (get-projection-children item))))
          []
          proj))

(def generate-object-children
  (fn [object]
    (cond (keyword? object) nil
          (map? object) (get-projection-children object))))

(defn resolve-path
  [path type]
  (loop [p path
         t type]
    (let [resolved-type
          (if (contains? object-registry (:type t)) (get object-registry (:type t)) t)]
      (if (= (count p) 0)
        t
        (if t
          (recur (rest p)
                 (let [type (get resolved-type (first p))]
                   (if (:type type)
                     type
                     (throw (ex-info (str "Type " t " does not have field " (first p))
                                     {:error/error true})))))
          (throw (ex-info (str "Type " resolved-type " does not have field " (first path)) {})))))))

(defn validate-dot-access
  [sym]
  (if (symbol? sym)
    (let
      [as-str (str sym)
       access-path
       (if (= (first as-str) \.)
         (if *clogel-dot-access-context*
           (cons (or (:object-type (:type *clogel-dot-access-context*))
                     (or (:type (:type *clogel-dot-access-context*))
                         (:type *clogel-dot-access-context*)))
                 (rest (map keyword (str/split as-str #"\."))))
           (throw (ex-info "Tried to use leading dot access with no object context"
                           {:dot-access-form sym})))
         (let [kws (mapv keyword (str/split as-str #"\."))]
           (if-let [with-binding (get *clogel-with-bindings*
                                      (symbol (remove-colon-kw (first kws))))]
             (into [(or (:type (:type with-binding)) (:type with-binding))] (rest kws))
             (if (not (contains? object-registry (first kws)))
               (throw
                (ex-info
                 "Invalid dot access form, leading with neither a dot nor a valid object type not a valid symbol"
                 {:error/error true}))
               kws))))]
      (resolve-path (rest access-path)
                    {:type (first access-path)
                     :card (if (and *clogel-dot-access-context*
                                    (= (first access-path) (:type *clogel-dot-access-context*)))
                             (:card *clogel-dot-access-context*)
                             :many)}))
    (throw (ex-info "Not a symbol somehow" {}))))

(defn compile-dot-access [sym] (str sym))

(def dot-access
  {:dot-access (->Node :dot-access
                       (fn [_] nil)
                       (fn [call _] call)
                       [(->Overload validate-dot-access compile-dot-access)])})

(defmacro defgelobjects
  []
  (let [parsed (mapv #(-> %
                          (assoc :clogel-name (gel-type->clogel-type (:name %))))
                     raw-objects)
        reg (into {}
                  (map (fn [obj-type] [(:clogel-name obj-type)
                                       (->Node (:clogel-name obj-type)
                                               generate-object-children
                                               build-type-form
                                               [(->Overload (build-object-validator obj-type)
                                                            (build-object-compiler obj-type))])])
                       parsed))
        funcs
        (map (fn [obj-type]
               (let [kw (:clogel-name obj-type)
                     sym (symbol (let [object-symbol (if (namespace kw)
                                                       (str/join "::" [(namespace kw) (name kw)])
                                                       (str (name kw)))]
                                   (if (= (str object-symbol) "Object")
                                     (symbol "GelObject") ;; mapping to Object breaks
                                     ;; Java lol
                                     object-symbol)))
                     arg (symbol "proj")]
                 `(def ~sym
                    (fn (~[] ~(:clogel-name obj-type)) (~[arg] {~(:clogel-name obj-type) ~arg})))))
             parsed)
        sym (symbol "gelobject-registry")
        def-reg `(def ~sym ~reg)]
    `(do ~def-reg ~@funcs)))
