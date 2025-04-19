(ns vol1n.clogel.functions-operators
  (:require [vol1n.clogel.types :refer
             [lub gel-abstract-types is-abstract-type? child-of-abstract-type?]]
            [vol1n.clogel.cardinality :refer [gel-typemod->clogel-cardinalities]]
            [vol1n.clogel.castable :refer [implicit-castable?]]
            [clojure.string :as str]
            [vol1n.clogel.util :refer [gel-type->clogel-type ->Overload ->Node max-card]]
            [vol1n.clogel.client :refer [query]]
            [clj-kondo.hooks-api :as api]))


(defn edgename->keyword
  [s]
  (as-> s $
    (str/split $ #"::")
    (map #(str/replace % " " "-") $)
    (if (= (first $) "std") (rest $) $)
    (if (> (count $) 1) (str/join "/" [(first $) (str/join "::" (rest $))]) (str (first $)))
    (keyword $)))

(comment
  (edgename->keyword "std::cal::NOT IN"))

(comment
  (edgename->keyword "sys::get_instance_name")
  (edgename->keyword "std::cal::date_fn")
  (let [kw (edgename->keyword "std::json_get")] (symbol (str/join "::" [(namespace kw) (name kw)])))
  (let [kw (edgename->keyword "std::math::abs")]
    (symbol (str/join "::" [(namespace kw) (name kw)])))
  (let [kw (edgename->keyword "std::cal::date_fn")]
    (symbol (str/join "::" [(namespace kw) (name kw)]))))

(defn build-validator
  [p]
  (let [param-type (gel-type->clogel-type (:name (:type p)))]
    (fn [arg]
      (let [card (case (:card p)
                   :many :singleton
                   :singleton (:card arg)
                   :optional (if (= (:card arg) :empty) :singleton (:card arg)))]
        (if (is-abstract-type? param-type)
          (if (child-of-abstract-type? param-type (:type arg))
            {:type (:type arg) :card card}
            {:error/error     true
             :error/message   (str "Cannot cast argument of type" (:type arg)
                                   "to generic parameter of type" param-type)
             :error/parameter p
             :error/argument  arg})
          (if (implicit-castable? (:type arg) param-type)
            {:type (:type arg) :card card}
            {:error/error     true
             :error/message   (str "Cannot implicitly cast argument of type" (:type arg)
                                   " to parameter of type " param-type)
             :error/parameter p
             :error/argument  arg}))))))

(defn build-variadic-validator
  [p]
  (let [param-type (gel-type->clogel-type (:name (:type p)))]
    (fn [& passed]
      (let [type (lub (map :type passed))
            card (case (:card p)
                   :many :singleton
                   :singleton (max-card (map :card passed))
                   :optional (max-card (filter #(not= % :empty) (map :card passed))))]
        (if (nil? type)
          {:error/error     true
           :error/message   "Incompatible types in variadic args"
           :error/parameter p
           :error/passed    passed}
          (if (is-abstract-type? param-type)
            (if (child-of-abstract-type? param-type type)
              {:type type :card card}
              {:error/error     true
               :error/message   (str "Cannot cast argument of type" type
                                     "to generic parameter of type" param-type)
               :error/parameter p
               :error/passed    passed})
            (if (implicit-castable? type param-type)
              {:type type :card card}
              {:error/error     true
               :error/message   (str "Cannot implicitly cast argument of type" type
                                     " to parameter of type " param-type)
               :error/parameter p
               :error/passed    passed})))))))

(defn is-type-generic?
  [t]
  (if (map? t)
    (any? (map #(contains? gel-abstract-types %) (last (first t))))
    (contains? gel-abstract-types t)))

(defn build-fn-validator
  [f]
  (let [is-return-generic (is-type-generic? (:return-type f))
        update-annotations
        (fn [p old new]
          (let [is-param-generic (is-type-generic? (:param-type p))
                card (max-card [(:card old) (:card new)])]
            (if is-return-generic
              {:card card
               :type (if is-param-generic
                       (if (nil? (:type old)) (:type new) (lub [(:type old) (:type new)]))
                       (:type old))}
              {:card card})))
        positional-params (filter #(= "PositionalParam" (:kind %)) (:params f))
        variadic-param (some #(when (= "VariadicParam" (:kind %)) %) (:params f))
        named-only-params (filter #(= "NamedOnlyParam" (:kind %)) (:params f))
        positional-validator
        (reduce (fn [acc p]
                  (let [validator (build-validator p)]
                    (fn [& args]
                      (let [old (apply acc (butlast args))
                            new (validator (last args))]
                        (if (:error/error new) (reduced new) (update-annotations p old new))))))
                (fn [& _] {:card :singleton :type nil})
                positional-params)
        named-only-validators
        (reduce (fn [acc np] (assoc acc (gel-type->clogel-type (:name np)) (build-validator np)))
                {}
                named-only-params)
        args-validator
        (if variadic-param
          (fn [args]
            (let [old (apply positional-validator (take (count positional-params) args))
                  new ((build-variadic-validator variadic-param)
                       (drop (count positional-params) args))]
              (if (:error/error @old)
                @old
                (if (:error/error new) new (update-annotations variadic-param old new)))))
          positional-validator)]
    (fn [call] ;; call => [:fn pos-arg1 pos-arg2 ... { named only args }]
      ;;
      (let [{assignment-args true positional-args false}
            (group-by #(and (map? %) (= (key (first %)) :=)) (rest call))
            deref-if-needed #(if (instance? clojure.lang.IDeref %) @% %)
            arg-result (deref-if-needed (apply args-validator positional-args))]
        (if (:error/error arg-result)
          arg-result
          (let [assignment-result (reduce (fn [acc a]
                                            (let [name (key (first (get a :=)))
                                                  validator (get named-only-validators name)]
                                              (update-annotations
                                               (some
                                                #(when (= (gel-type->clogel-type (:name %)) name) %)
                                                named-only-params)
                                               acc
                                               (validator (val (first (get a :=)))))))
                                          {:card :singleton :type nil}
                                          assignment-args)]
            (if (:error/error assignment-result)
              assignment-result
              (if is-return-generic
                {:type (lub [(:type arg-result) (:type assignment-result)])
                 :card (max-card [(:card arg-result) (:card assignment-result)])}
                {:type (:return-type f)
                 :card (max-card [(:card arg-result) (:card assignment-result)])}))))))))

(defn chop-last [s n] (let [chars (count s)] (subs s 0 (- chars n))))

(defn chop-prefix [s prefix] (if (clojure.string/starts-with? s prefix) (subs s (count prefix)) s))

(defn build-fn-compiler
  [f]
  (if (:operator_kind f)
    (case (:operator_kind f)
      "Infix" (fn [[_ & _] & compiled-children]
                (str (first compiled-children)
                     " " (chop-prefix (:name f) "std::")
                     " " (last compiled-children)))
      "Prefix" (fn [[_ & args] & compiled-children]
                 (str (chop-prefix (:name f) "std::") " " (first compiled-children)))
      "Ternary" (fn [[_ & args] & compiled-children]
                  (str (first compiled-children)
                       "if " (second compiled-children)
                       " else " (nth compiled-children 2))))
    (fn [[_ & args] & compiled-children]
      (println "compiled-children" compiled-children)
      (println "args" args)
      (str (:name f)
           "("
           (chop-last (str/join ",\n"
                                (map #(if (and (map? (first %)) (= (key (first (first %))) :=))
                                        (str (key (first (val (first (first %))))) " := " (last %))
                                        (str (last %) ",\n"))
                                     (map vector args compiled-children)))
                      2)
           ")"))))
(def fn-children-generator
  (fn [[_ & args]]
    (map #(if (and (map? %) (= (key (first %)) :=)) (val (first (get % :=))) %) args)))

{:= {:something 'something}}
'something

(def fn-kondo-children-generator
  (fn [kondo-node _]
    (let [[_ & arg-nodes] (:children kondo-node)]
      (map #(let [sexpr (api/sexpr %)]
              (if (and (map? sexpr) (= (key (first sexpr)) :=))
                (some-> %
                        :children second
                        :children second)
                %))
           arg-nodes))))

(def build-type-form
  (fn [call types]
    (into [(first call)]
          (map (fn [[a t]] (if (and (map? a) (= (key (first a)) :=)) {:= {(key (first a)) t}} t))
               (map vector (rest call) types)))))

(defn build-fn-overload-form [f] (->Overload (build-fn-validator f) (build-fn-compiler f)))


(defn build-func-registry
  [items]
  (let [parsed (mapv #(-> %
                          (assoc :return-type (gel-type->clogel-type (:name (:return_type %))))
                          (assoc
                           :params
                           (map
                            (fn [p]
                              (-> p
                                  (assoc :card (get gel-typemod->clogel-cardinalities (:typemod p)))
                                  (assoc :param-type (gel-type->clogel-type (:name (:type p))))))
                            (:params %)))
                          (assoc :alias
                                 (some (fn [a]
                                         (when (= (:name a) "std::identifier")
                                           (keyword (get a (keyword "@value")))))
                                       (:annotations %))))
                     items)
        grouped (group-by :name parsed)
        grouped-by-alias (group-by :alias parsed)
        reg (into {}
                  (map (fn [[fname overloads]] [(edgename->keyword fname)
                                                (->Node (edgename->keyword fname)
                                                        fn-children-generator
                                                        fn-kondo-children-generator
                                                        build-type-form
                                                        (vec (pmap build-fn-overload-form
                                                                   overloads)))])
                       grouped))
        aliases (reduce (fn [acc [alias overloads]]
                          (if alias
                            (assoc acc
                                   alias
                                   (->Node alias
                                           fn-children-generator
                                           fn-kondo-children-generator
                                           build-type-form
                                           (vec (pmap build-fn-overload-form overloads))))
                            acc))
                        {}
                        grouped-by-alias)]
    (merge aliases reg)))

(defn function-helper
  [items]
  (let [parsed (mapv #(-> %
                          (assoc :return-type (gel-type->clogel-type (:name (:return_type %))))
                          (assoc
                           :params
                           (map
                            (fn [p]
                              (-> p
                                  (assoc :card (get gel-typemod->clogel-cardinalities (:typemod p)))
                                  (assoc :param-type (gel-type->clogel-type (:name (:type p))))))
                            (:params %)))
                          (assoc :alias
                                 (some (fn [a]
                                         (when (= (:name a) "std::identifier")
                                           (keyword (get a (keyword "@value")))))
                                       (:annotations %))))
                     items)
        grouped (group-by :name parsed)
        grouped-by-alias (group-by :alias parsed)
        funcs (map (fn [[fname _]]
                     (let [kw (edgename->keyword fname)
                           replaced (if (= kw :=) :equals kw)
                           args-vec ['& 'args]]
                       (if (#{"://"} (str kw))
                         nil
                         `(def ~(symbol (if (namespace replaced)
                                          (str/join "::" [(namespace replaced) (name replaced)])
                                          (str (name replaced))))
                            (fn ~args-vec (into [~replaced] ~(symbol "args")))))))
                   grouped)
        alias-funcs
        (reduce (fn [acc [alias _]]
                  (if (nil? alias)
                    acc
                    (let [kw alias
                          sym (symbol (str/lower-case (if (namespace kw)
                                                        (str/join "::" [(namespace kw) (name kw)])
                                                        (str (name kw)))))
                          args-vec ['& 'args]]
                      (conj acc `(def ~sym (fn ~args-vec (into [~kw] ~(symbol "args"))))))))
                []
                grouped-by-alias)]
    `(do ~@alias-funcs ~@funcs))) ;;   ~@funcs


(defn get-fns
  []
  (query
   "
select schema::Function {
    name,
    annotations: { name, @value },
    params: {
        kind,
        name,
        num,
        typemod,
        type: { name },
        default,
    },
    return_typemod,
    return_type: { name },
}"))

(defonce fns (get-fns))

(defmacro defgelfuncs [] (function-helper fns))

(defonce gelfunc-registry (build-func-registry fns))

(defn get-ops
  []
  (query
   "
select schema::Operator {
  name,
  operator_kind,
  return_type: { name },
  return_typemod,
  params: {
    kind,
    name,
    typemod,
    type: { name },
    default
  },
  annotations: {
    name,
    @value
  } filter .name = 'std::identifier'
};
"))

(defn build-index-slice-validator
  [f]
  (let [param-types (map #(gel-type->clogel-type (get-in % [:type :name])) (:params f))
        parsed-params (reduce (fn [acc t]
                                (if (and (map? t) (= :tuple (key (first t))))
                                  (concat acc (val (first t)))
                                  (conj acc t)))
                              []
                              param-types)]
    (fn [[_ & args]]
      (if (not= (count args) (count (:params f)))
        {:error/error true :error/message "Wrong number of params passed"}
        (let [error (some (fn [[a t]]
                            (when (not (implicit-castable? (:type a) t))
                              {:error/error   true
                               :error/message (str (:type a) " not castable to " t)}))
                          (map vector args parsed-params))]
          (if error
            error
            (let [input-type (:type (first args))
                  output-type (gel-type->clogel-type (:name (:return_type f)))
                  handle-collection-type #(if (= (count (:params f)) 2) (first (val (first %))) %)]
              {:type (if (is-type-generic? output-type)
                       (if (map? input-type) (handle-collection-type input-type) input-type)
                       (if (map? output-type) (handle-collection-type input-type) output-type))
               :card (get gel-typemod->clogel-cardinalities (:return_typemod f))})))))))

(defn index-slice-compiler
  [call & compiled-children]
  (if (= (count compiled-children) 3)
    (str (first compiled-children) \[ (second compiled-children) \: (nth compiled-children 2) \])
    (str (first compiled-children) \[ (second compiled-children) \])))

(defonce ops (get-ops))

(defmacro defgeloperators [] (function-helper ops))

(defonce gelop-registry (build-func-registry ops))

(defn get-slices
  []
  (query
   "
select schema::Operator {
  name,
  operator_kind,
  return_type: { name },
  return_typemod,
  params: {
    kind,
    name,
    typemod,
    type: { name },
    default
  },
  annotations: {
    name,
    @value
  } filter .name = 'std::identifier'
}
filter .name = 'std::[]';
"))

(comment
  (get-slices))

(def gel-index
  {:access (->Node :access
                   (fn [[_ & args]] (println "args" args) (into [] args))
                   (fn [kondo-node _] (into [] (rest (:children kondo-node))))
                   (fn [_ types] (into [:index] types))
                   (mapv #(->Overload (build-index-slice-validator %) index-slice-compiler)
                         (get-slices)))})

(defn access ([item i] [:access item i]) ([item l r] [:access item l r]))
