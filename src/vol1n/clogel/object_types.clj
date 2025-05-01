(ns vol1n.clogel.object-types
  (:require [clojure.string :as str]
            [vol1n.clogel.util :refer
             [gel-type->clogel-type ->Node ->Overload remove-colon-kw max-card sanitize-kw
              *clogel-dot-access-context* *clogel-with-bindings* *clogel-param-bindings*]]
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

(def assignment-operators #{:= :+=})

(defn build-object-validator
  [object-type]
  (fn validator [object]
    {:type
     (cond
       (keyword? object)
       (if (= (sanitize-kw object) (:clogel-name object-type))
         {:card :many :type (:clogel-name object-type) :deletable true :updatable true}
         (throw (ex-info "We've got problems BOV" {})))
       (and (map? object) (vector? (last (first object))))
       (do
         (let [vec (last (first object))
               type (if (= (key (first object)) :free-object)
                      :free-object
                      (get object-registry (sanitize-kw (key (first object)))))
               only-assignments
               (every? (fn [item] (and (map? item) (assignment-operators (key (first item))))) vec)
               only-existing-assignments (and only-assignments
                                              (every? (fn [item]
                                                        (get type
                                                             (-> item
                                                                 first
                                                                 val
                                                                 first
                                                                 key
                                                                 sanitize-kw)))
                                                      vec))
               only= (and only-assignments (every? (fn [item] (#{:=} (key (first item)))) vec))]
           (if (not type)
             (throw (ex-info (str "Cannot project non-object type "
                                  (or (key (first object)) object))
                             {:error/type type :error/error true}))
             (let [reduced
                   (reduce
                    (fn [acc item]
                      (cond
                        (and (map? item) (assignment-operators (key (first item))))
                        ;; assignment
                        (if (= (key (first item)) :=)
                          (assoc acc
                                 (sanitize-kw (key (first (val (first item)))))
                                 (val (first (val (first item)))))
                          (let [assign-to (-> item
                                              first
                                              val
                                              first
                                              key
                                              sanitize-kw)
                                assign-value (-> item
                                                 first
                                                 val
                                                 first
                                                 val)
                                attribute-type (get type assign-to)]
                            (if (nil? attribute-type)
                              (throw (ex-info (str "Cannot use assignment operator (other than :=)"
                                                   "on non-existent field "
                                                   assign-to)
                                              {:error/error true}))
                              (if (validate-object-type-cast (:type assign-value)
                                                             (:type attribute-type))
                                (assoc acc assign-to attribute-type)
                                (throw (ex-info (str "Tried to assign type " (:type assign-value)
                                                     " to " (:type attribute-type)
                                                     " for field: " assign-to)
                                                {:error/error true}))))))
                        (map? item)
                        ;; nesting
                        (let [main-key (some #(when (not (contains? mod-keys (key %))) (key %))
                                             item)
                              modifiers (dissoc item main-key)]
                          (doseq [[mod val] modifiers]
                            (when (not ((get-in modifier-validators [mod :fn]) val))
                              (throw (ex-info (str "Value " val " for modifier " mod "invalid")
                                              {:error/error true})))
                            modifiers)
                          (assoc acc
                                 main-key
                                 (if (true? (get item main-key))
                                   main-key
                                   (validator
                                    {(or (:type (get type main-key))
                                         (throw (ex-info (str "Key " main-key " not found on type")
                                                         {})))
                                     (get item main-key)}))))
                        (keyword? item)
                        (if (or (contains? type item) (contains? type (sanitize-kw item)))
                          (assoc acc item (get type item))
                          (throw (ex-info (str "Key " item " does not exist on type" type)
                                          {:error/error true})))
                        :else (throw
                               (ex-info
                                "Invalid type within projection vector expected map or keyword"
                                {}))))
                    {}
                    vec)]
               (let [fin
                     (assoc
                      (let [validation-result (validate-object-type-cast
                                               (or (:clogel-name object-type) object-type)
                                               reduced)]
                        (cond-> reduced
                          only-existing-assignments (assoc :settable true)
                          (and only=
                               (:clogel-name object-type)
                               (not (:error/error validation-result)))
                          (assoc :insertable true)
                          (not only=)
                          (with-meta {:not-insertable-because
                                      "Can only insert an object with assignment fields provided"})
                          (and only= (:clogel-name object-type) (:error/error validation-result))
                          (with-meta {:not-insertable-because
                                      (str "type " (:clogel-name object-type)
                                           " " (:error/message validation-result))})))
                      :object-type
                      (or (:clogel-name object-type) :free-object))]
                 fin))))))
     :card :many}))

(defn validate-free-object
  [object] ;; object is a vector
  (let [object-type (or (:object-type (:type *clogel-dot-access-context*))
                        (or (:type (:type *clogel-dot-access-context*))
                            (:type *clogel-dot-access-context*))
                        :free-object)
        validator (if (= object-type :free-object)
                    (build-object-validator nil)
                    (build-object-validator
                     (let [parsed (mapv #(-> %
                                             (assoc :clogel-name (gel-type->clogel-type (:name %))))
                                        raw-objects)]
                       (some #(when (= (:clogel-name %) object-type) %) parsed))))
        val-result (validator {object-type object})]
    val-result))

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
               (cond (keyword? item) {:compiled  (str (:compiled acc)
                                                      (-> item
                                                          sanitize-kw
                                                          remove-colon-kw)
                                                      ",\n")
                                      :remaining (:remaining acc)}
                     (and (map? item) (assignment-operators (key (first item))))
                     (let [op (key (first item))
                           op-string (if (= op :=) ":=" (remove-colon-kw op))]
                       {:compiled  (str (:compiled acc)
                                        (-> (get item op)
                                            first
                                            key
                                            sanitize-kw
                                            remove-colon-kw)
                                        " "
                                        op-string
                                        " "
                                        (first (:remaining acc))
                                        ",\n")
                        :remaining (rest (:remaining acc))})
                     :else
                     (let [main-key (some #(when (not (contains? mod-keys (key %))) (key %)) item)
                           modifiers (dissoc item main-key)
                           result (apply compile-projection
                                         (into [(get item main-key)] (:remaining acc)))]
                       {:compiled  (str (:compiled acc)
                                        (-> main-key
                                            sanitize-kw
                                            remove-colon-kw)
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

(defn compile-free-object
  [obj & compiled-children]
  (str "{\n" (:compiled (apply compile-projection (into [obj] compiled-children))) "\n}"))

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

(defn build-modifier-type-form
  [modifiers types]
  (:type-form (reduce (fn [acc k]
                        (cond (and (= k :group-by) (vector? (get modifiers :group-by)))
                              {:type-form (assoc (:type-form acc)
                                                 :group-by
                                                 [(first (:types acc))
                                                  (last (get modifiers :group-by))])
                               :types     (rest (:types acc))}
                              :else {:type-form (assoc (:type-form acc) k (first (:types acc)))
                                     :types     (rest (:types acc))}))
                      {:type-form {} :types types}
                      (sort-keys modifiers))))

(defn build-projection-type-form
  [proj & types]
  (reduce (fn [acc item]
            (cond (keyword? item) {:vec (conj (:vec acc) item) :remaining (:remaining acc)}
                  (and (map? item) (assignment-operators (key (first item))))
                  {:vec       (conj (:vec acc)
                                    {(key (first item)) {(key (first (val (first item))))
                                                         (first (:remaining acc))}})
                   :remaining (rest (:remaining acc))}
                  :else
                  (let [main-key (some #(when (not (contains? mod-keys (key %))) (key %)) item)
                        modifiers (dissoc item main-key)
                        modifier-type-form (build-modifier-type-form modifiers
                                                                     (take (count (keys modifiers))
                                                                           (:remaining acc)))
                        result (apply build-projection-type-form
                                      (into [(val (first item))]
                                            (drop (count (keys modifiers)) (:remaining acc))))]
                    {:vec       (conj (:vec acc)
                                      (merge modifier-type-form {main-key (:vec result)}))
                     :remaining (:remaining result)})))
          {:vec [] :remaining types}
          proj))

(defn build-free-object-type-form
  [proj types]
  (:vec (apply build-projection-type-form (into [proj] types))))

(def build-type-form
  (fn [object types]
    (cond (keyword? object) object
          (map? object) {(key (first object)) (:vec (apply build-projection-type-form
                                                           (into [(val (first object))] types)))}
          :else (throw (ex-info "Botch" {})))))

(defn get-modifier-children
  [modifiers current-type]
  (let [children (map #(let [meta-attached
                             (cond (and (= % :group-by) (vector? (get modifiers :group-by)))
                                   (with-meta (first (get modifiers :group-by))
                                              {:modifies current-type})
                                   :else (with-meta (get modifiers %) {:modifies current-type}))]
                         meta-attached)
                      (sort-keys modifiers))]
    children))

(defn get-projection-children
  [proj type]
  (reduce (fn [acc item]
            (cond (keyword? item) acc
                  (and (map? item) (assignment-operators (key (first item))))
                  (conj acc (val (first (get item (key (first item))))))
                  (map-entry? item) (get-projection-children (last item) type)
                  :else
                  (let [main-key (some #(when (not (contains? mod-keys (key %))) (key %)) item)
                        modifiers (dissoc item main-key)]
                    (let [modifier-children (get-modifier-children
                                             modifiers
                                             (get object-registry (:type (get type main-key))))]
                      (concat acc
                              modifier-children
                              (get-projection-children {main-key (get item main-key)}
                                                       (get object-registry
                                                            (:type (get type main-key)))))))))
          []
          proj))

(defn get-free-object-children
  [obj]
  (let [children (get-projection-children {:free-object obj} nil)] children))

(def generate-object-children
  (fn [object]
    (let [children (cond (keyword? object) nil
                         (map? object) (get-projection-children object
                                                                (get object-registry
                                                                     (key (first object)))))]
      children)))

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
                 (let [type (or (get resolved-type (first p))
                                (get (:type resolved-type) (first p)))]
                   (if (:type type)
                     type
                     (throw (ex-info (str "Type " (:type t) " does not have field " (first p))
                                     {:error/error true})))))
          (throw (ex-info (str "Type " resolved-type " does not have field " (first path)) {})))))))

(defn validate-dot-access
  [sym]
  (if (symbol? sym)
    (let [as-str (str/replace (str sym) #"-" "_")
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
              (if-let [with-binding
                       (or (get *clogel-with-bindings* (symbol (remove-colon-kw (first kws))))
                           (get *clogel-param-bindings* (symbol (remove-colon-kw (first kws)))))]
                (into [(or (:type (:type with-binding)) (:type with-binding))] (rest kws))
                (if (not (contains? object-registry (first kws)))
                  (throw (ex-info (str "Invalid symbol form: "
                                       sym
                                       " does not lead with a dot or valid object type")
                                  {:error/error true}))
                  kws))))]
      (resolve-path (rest access-path)
                    {:type (first access-path)
                     :card (if (and *clogel-dot-access-context*
                                    (= (first access-path) (:type *clogel-dot-access-context*)))
                             (:card *clogel-dot-access-context*)
                             :many)}))
    (throw (ex-info "Not a symbol somehow" {}))))

(defn compile-dot-access [sym] (str/replace (str sym) #"-" "_"))

(def dot-access
  {:dot-access (->Node :dot-access
                       (fn [_] nil)
                       (fn [call _] call)
                       [(->Overload validate-dot-access compile-dot-access)])})

(def clogel-free-object
  (->Node :free-object
          get-free-object-children
          build-free-object-type-form
          [(->Overload validate-free-object compile-free-object)]))

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
