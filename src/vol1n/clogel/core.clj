(ns vol1n.clogel.core
  (:require [malli.core :as m]
            [vol1n.clogel.scalar :refer [clogel-scalars]]
            [vol1n.clogel.cast :refer [defgelcasts]]
            [vol1n.clogel.cast :as casts]
            [vol1n.clogel.collection :refer [clogel-collections]]
            [vol1n.clogel.collection :as colls]
            [vol1n.clogel.functions-operators :refer [defgelfuncs defgeloperators gel-index]]
            [vol1n.clogel.functions-operators :as func]
            [vol1n.clogel.object-types :refer
             [defgelobjects dot-access assignment-operators clogel-free-object validate-dot-access]]
            [vol1n.clogel.util :refer
             [*clogel-dot-access-context* *clogel-with-bindings* *clogel-param-bindings*
              remove-colon-kw gel-type->clogel-type sanitize-kw sanitize-symbol]]
            [vol1n.clogel.top-level :refer [clogel-top-level-statements]]
            [vol1n.clogel.top-level :as top]
            [vol1n.clogel.client :as client]
            [cheshire.core :as json]
            [clojure.string :as str]
            [clojure.walk :refer [postwalk]])
  (:import [com.geldata.driver.exceptions GelErrorException])
  (:refer-clojure :exclude
                  [update for filter group-by min range find max assert count concat mod or not
                   distinct destructure and <= / > < + >= - * set cast]))
; select filter offset group-by limit update insert
              ;delete
(def select top/select)
(def project top/project)
(def filter top/filter)
(def offset top/offset)
(def group-by top/group-by)
(def order-by top/order-by)
(def limit top/limit)
(def update top/update)
(def insert top/insert)
(def set top/set)
(def for top/for)
(def delete top/delete)
(def with top/with)
(def access func/access)
(def tuple-access func/tuple-access)
(def unless-conflict top/unless-conflict)
(def cast casts/cast)
(def tuple colls/tuple)
(def else top/else)
(defmacro tx "Re-exposed tx macro" [& forms] `(client/tx ~@forms))

(defn match-overload
  [call overloads]
  (if (= (clojure.core/count overloads) 1)
    (let [result ((:validator (first overloads)) call)]
      (if (:error/error result) result {:overload (first overloads) :annotations result}))
    (let [matched (some (fn [ovl]
                          (let [result ((:validator ovl) call)]
                            (when (clojure.core/not (:error/error result))
                              {:overload ovl :annotations result})))
                        overloads)]
      matched)))

(defgelfuncs)
(defgelcasts)
(defgeloperators)
(defgelobjects)

(def node-registry
  (merge dot-access
         gelfunc-registry
         gelop-registry
         gelobject-registry
         gelcast-registry
         clogel-top-level-statements
         {:free-object clogel-free-object}
         {:scalar clogel-scalars}
         {:collection clogel-collections}
         gel-index))

(def mod-keys #{:limit :order-by :filter :offset :set :by :union})

(defn dehyphenate-symbol [sym] (symbol (str/replace (str sym) "-" "_")))

(defn clogel->edgeql
  [edn]
  (if (contains? (meta edn) :modifies)
    (binding [*clogel-dot-access-context* {:type (:modifies (meta edn)) :card :many}]
      (clogel->edgeql (with-meta edn (dissoc (meta edn) :modifies))))
    (if (clojure.core/and (symbol? edn)
                          (clojure.core/or (contains? *clogel-with-bindings* edn)
                                           (contains? *clogel-with-bindings*
                                                      (dehyphenate-symbol edn))))
      (assoc (clojure.core/or (get *clogel-with-bindings* edn)
                              (get *clogel-with-bindings* (dehyphenate-symbol edn)))
             :value
             (str (dehyphenate-symbol edn)))
      (if (clojure.core/and (symbol? edn) (contains? *clogel-param-bindings* edn))
        (let [binding (get *clogel-param-bindings* edn)
              type (remove-colon-kw (:type binding))]
          (assoc binding
                 :type (if (str/starts-with? type "optional")
                         (keyword (last (str/split type #"-")))
                         (:type binding))
                 :value
                 (if (str/starts-with? type "optional")
                   (str "<optional " (last (str/split type #"-")) \> (dehyphenate-symbol edn))
                   (str \< type \> (dehyphenate-symbol edn)))))
        (let [node-key
              (sanitize-kw
               (cond (clojure.core/and (map? edn) (:with edn)) :with
                     (map? edn) (some #(when (clojure.core/not (contains? mod-keys %)) %)
                                      (keys edn))
                     (clojure.core/and (vector? edn)
                                       (every? map? edn)
                                       (every? #(assignment-operators (key (first %))) edn))
                     :free-object
                     (clojure.core/or (symbol? edn)
                                      (clojure.core/and (vector? edn)
                                                        (contains? gelobject-registry (first edn))
                                                        (every? (complement map?) edn)))
                     :dot-access
                     (clojure.core/and (vector? edn) (keyword? (first edn)))
                     (let [key (first edn)]
                       (if (contains? node-registry (sanitize-kw key))
                         key
                         (if *clogel-dot-access-context*
                           :free-object
                           (throw (ex-info (str "invalid keyword" key)
                                           {:valid-keys (keys node-registry) :passed key})))))
                     (keyword? edn) edn
                     (coll? edn) :collection
                     :else :scalar))
              node (get node-registry node-key)]
          (if (clojure.core/not node)
            (throw (ex-info (str "invalid keyword" node-key)
                            {:valid-keys (keys node-registry) :passed node-key}))
            (let [children ((:generate-children node) edn)
                  compiled-children
                  (if (nil? children)
                    (seq [])
                    ;; if it's an object, we need to know what object wse're in for children of
                    ;; this node
                    (if (contains? gelobject-registry node-key)
                      (binding [*clogel-dot-access-context* {:type node-key :card :many}]
                        (map (bound-fn [child] (clogel->edgeql child)) children))
                      ;; if it's a top level statement (i.e. select)
                      ;; we need to know what object we're selecting for modifier statements
                      ;; ex. select User filter .name = "John";
                      (if (contains? clogel-top-level-statements node-key)
                        (cond
                          (clojure.core/= node-key :for)
                          (let [compiled-binding (clogel->edgeql (first children))]
                            (binding [*clogel-with-bindings*
                                      (assoc *clogel-with-bindings*
                                             (sanitize-symbol (first (:for edn)))
                                             (let [nb (merge compiled-binding {:card :singleton})]
                                               nb))]
                              [(merge compiled-binding {:card :singleton})
                               (clogel->edgeql (last children))]))
                          (clojure.core/= node-key :group)
                          (let [compiled-group-statement (clogel->edgeql (first children))
                                {compiled-with-children :compiled bindings :with-bindings}
                                (reduce
                                 (fn [compiled [b c]]
                                   (let [result (clogel->edgeql c)]
                                     (binding [*clogel-with-bindings* (assoc *clogel-with-bindings*
                                                                             (sanitize-symbol b)
                                                                             result)]
                                       {:compiled      (conj (:compiled compiled) result)
                                        :with-bindings *clogel-with-bindings*})))
                                 {:compiled [] :with-bindings *clogel-with-bindings*}
                                 (map vector
                                      (map first (:using edn))
                                      (clojure.core/take (count (:using edn)) (rest children))))]
                            (binding [*clogel-with-bindings* bindings]
                              (conj (cons compiled-group-statement compiled-with-children)
                                    (map clogel->edgeql
                                         (clojure.core/drop (inc (count (:using edn))) children)))))
                          (clojure.core/= node-key :with)
                          (let [{compiled-with-children :compiled bindings :with-bindings}
                                (binding [*clogel-with-bindings* *clogel-with-bindings*]
                                  (reduce (fn [compiled [b c]]
                                            (let [result (clogel->edgeql c)]
                                              (set! *clogel-with-bindings*
                                                    (assoc *clogel-with-bindings*
                                                           (sanitize-symbol b)
                                                           result))
                                              {:compiled      (conj (:compiled compiled) result)
                                               :with-bindings *clogel-with-bindings*}))
                                          {:compiled [] :with-bindings *clogel-with-bindings*}
                                          (map vector (map first (:with edn)) (butlast children))))]
                            (binding [*clogel-with-bindings* bindings]
                              (conj compiled-with-children (clogel->edgeql (last children)))))
                          :else
                          (let [compiled-first (clogel->edgeql (first children))]
                            (binding [*clogel-dot-access-context* {:type (:type compiled-first)
                                                                   :card (:card compiled-first)}]
                              (into [compiled-first]
                                    (reduce #(conj %1 (clogel->edgeql %2)) [] (rest children))))))
                        (map clogel->edgeql children))))
                  type-form ((:build-type-form node) edn (map #(dissoc % :value) compiled-children))
                  {annotations :annotations overload :overload error :error/message}
                  (match-overload type-form (:overloads node))]
              (if error
                (throw (ex-info (str "Error for " node-key " statement: " error ", got: " edn)
                                {:node edn :error error}))
                (if (clojure.core/not overload)
                  (throw (ex-info (str "No overload for node of type " node-key " for value " edn)
                                  {:node edn :valid (:overloads node)}))
                  (assoc annotations
                         :value
                         (apply (:compile-fn overload)
                                (into [edn] (map :value compiled-children)))))))))))))

(defn hyphenate-keywords
  [data]
  (postwalk (fn [y]
              (if (keyword? y)
                (-> y
                    name
                    (str/replace "_" "-")
                    keyword)
                y))
            data))

(comment
  (hyphenate-keywords {:hello_world {:foo_bar :baz_baz}}))

(defn query
  ([q] (query q true))
  ([q hyphenate?]
   (if (some #(#{:select :group :delete :insert :for :with} (key %)) q)
     (let [compiled (:value (clogel->edgeql q))]
       (try (if hyphenate?
              (hyphenate-keywords (client/query
                                   (subs compiled 1 (dec (clojure.core/count compiled)))))
              (client/query (subs compiled 1 (dec (clojure.core/count compiled)))))
            (catch Throwable ge
              (throw (ex-info "Exception in Gel query execution: "
                              {:message (.getMessage ge)
                               :cause   (.getCause ge)
                               :stack   (map #(str "  at" %) (.getStackTrace ge))})))))
     (throw (ex-info "Missing top-level statement in query" {:error/error true :error/query q})))))

(defn compile-query
  [q]
  (let [result (clogel->edgeql q)
        compiled (:value result)]
    (subs compiled 1 (dec (clojure.core/count compiled)))))

(comment
  (clogel->edgeql [:math/abs -5])
  (clogel->edgeql [:cast-int32 5])
  (clogel->edgeql [1 2 3])
  (clogel->edgeql #{1 2 3})
  (clogel->edgeql (cast "user::User" '()))
  (clogel->edgeql :User)
  (clogel->edgeql {:User [:name]})
  (clogel->edgeql {:User [{:best_friend [:name]} {:= {:myAssignment 42}}]})
  (clogel->edgeql {:select {:User [{:best_friend [:name]} {:= {:my_assignment 42}}]}})
  (clogel->edgeql {:select {:User [{:bestFriend [:firstName]} {:= {:myAssignment 42}}]} :limit 1})
  (clogel->edgeql {:with [['x 100]] :select 'x})
  (clogel->edgeql {:select {:User [:name]} :filter [:= '.is_active true]})
  (clogel->edgeql {:delete :User})
  (clogel->edgeql (list 42 69))
  (clogel->edgeql {:insert (User [{:= {:email "alice@example.com"}} {:= {:is_active true}}
                                  {:= {:name "Alice"}} {:= {:rating 10}}])})
  (println (clogel->edgeql
            (for ['u (json_array_unpack
                      [:cast-json
                       (json/generate-string
                        [{:name "Alice" :email "alice@example.com" :is_active true :rating 10}
                         {:name "Bob" :email "bob@example.com" :is_active false :rating 8}])])]
              {:insert {:User [{:= {:name (cast-str (access 'u "name"))}}
                               {:= {:email (cast-str (access 'u "email"))}}
                               {:= {:is_active (cast-bool (access 'u "is_active"))}}
                               {:= {:rating (cast-int64 (access 'u "rating"))}}]}})))
  (println
   (clogel->edgeql
    {:for
     ['u
      [:cast-json
       "[{\"email\":\"colin@example.com\",\"name\":\"Colin\",\"is_active\":true},
               {\"email\":\"dev@example.com\",\"name\":\"Dev\",\"is_active\":false}]"]]
     :union [:cast-str [:access 'u "email"]]}))
  (clogel->edgeql (and (eq "colin@example.com" "hi") (eq true true)))
  (clogel->edgeql (-> (top/select :User)
                      (top/filter (and (eq "colin@example.com" '.email) (eq "Colin" '.name)))))
  (clogel->edgeql
   (top/for
     ['u
      [:cast-json
       "[{\"email\":\"colin@example.com\",\"name\":\"Colin\",\"is_active\":true},
               {\"email\":\"dev@example.com\",\"name\":\"Dev\",\"is_active\":false}]"]]
     (cast-str (access 'u "email"))))
  (clogel->edgeql
   {:for
    ['u
     [:cast-json
      "[{\"email\":\"colin@example.com\",\"name\":\"Colin\",\"is_active\":true},
               {\"email\":\"dev@example.com\",\"name\":\"Dev\",\"is_active\":false}]"]]
    :union {:insert {:User [{:= {:email [:cast-str [:access 'u "email"]]}}
                            {:= {:name [:cast-str [:access 'u "name"]]}}
                            {:= {:is_active [:cast-bool [:access 'u "is_active"]]}}]}}})
  (clogel->edgeql {:select :User :filter [:= '.name "John"]})
  (clogel->edgeql {:select {:User [{:= {:doubled [:* '.rating 2]}}]}})
  (clogel->edgeql (top/for ['u :User] {:select 'u.email}))
  (clogel->edgeql (top/select {:User [:name] :filter [:= '.name "Alice"]}))
  (clogel->edgeql (-> (top/insert {:user/User [{:= {:email "colin@example.com"}}
                                               {:= {:passwordHash "hashed"}}]})
                      (top/unless-conflict '.email)))
  (clogel->edgeql (-> (top/with [['x (select :user/User)]])
                      (top/select 'x.name)))
  (clogel->edgeql [{:= {:hello "world"}}])
  (clogel->edgeql {:user/User [{:= {:hello "world"}}]})
  (clogel->edgeql {:update :user/User :set [{:= {:hello "world"}}]}) ;;throws
  (clogel->edgeql {:update :user/User :set [{:= {:email "hello@world.com"}}]})
  (clogel->edgeql {:update :user/User :set [{:+= {:apiKeys {:select :user/ApiKey}}}]})
  (println (clogel->edgeql {:select  {:update :user/User :set [{:= {:name "Chudley"}}]}
                            :project [:name :id]}))
  (if-else '() true '()))

(defn dequote [form] (if (clojure.core/and (seq? form) (= 'quote (first form))) (second form) form))

(defmacro defquery
  [name params & body]
  (let [[query hyphenate?] body
        required-params (mapv (fn [[name type]] [name {:type type :card :singleton}])
                              ;; filter for not starting with optional
                              (clojure.core/filter #(clojure.core/not (str/starts-with?
                                                                       (clojure.core/name (last %))
                                                                       "optional-"))
                                                   params))
        optional-params (mapv (fn [optional-param] [(first optional-param)
                                                    {:type (last optional-param) :card :optional}])
                              (clojure.core/filter #(str/starts-with? (clojure.core/name (last %))
                                                                      "optional-")
                                                   params))
        param-symbols (into #{} (map first (clojure.core/concat required-params optional-params)))
        hyphenate? (if (nil? hyphenate?) true hyphenate?) ;; if not passed, default to
        ;; hyphenate. Quote parameter symbols for clogel compilation
        quote-params
        (fn quote-params [form]
          (when (= (str name) "delete-instance-ids!") (println "visiting " form))
          (let [QP (cond (clojure.core/and (symbol? form) (contains? param-symbols form))
                         (list 'quote form)
                         ;; Skip processing (quote ()) forms
                         (clojure.core/and (seq? form)
                                           (= 'quote (first form))
                                           (seq? (second form))
                                           (empty? (second form)))
                         form
                         (clojure.core/and (seq? form) (seq form)) (map quote-params form)
                         (vector? form) (mapv quote-params form)
                         (map? form)
                         (into {} (map (fn [[k v]] [(quote-params k) (quote-params v)]) form))
                         (set? form) (into #{} (map quote-params form))
                         :else form)]
            (when (= (str name) "delete-instance-ids!") (println "QP" QP))
            QP))
        quoted-query (quote-params query)
        evaled-query (eval quoted-query)]
    (if (every? #(clojure.core/and (symbol? (first %)) (str/starts-with? (str (first %)) "$"))
                (clojure.core/concat required-params optional-params))
      (if (every? #(keyword? (:type (last %)))
                  (clojure.core/concat optional-params required-params))
        (try
          (let [args
                (let [required (vec (map first required-params))]
                  (if (seq optional-params) (into required ['& (symbol "optionals")]) required))
                param-binding (into {} (clojure.core/concat optional-params required-params))
                compiled (binding [*clogel-param-bindings* param-binding]
                           (compile-query evaled-query))]
            `(defn ~(dequote name)
               ~args
               (try
                 ~(let [merged-params
                        `(merge ~(into {}
                                       (map (fn [[p a]] [(str (first p)) a])
                                            (map vector required-params (butlast args))))
                                ~(if (clojure.core/= (str (last args)) "optionals")
                                   `(merge ~(into {}
                                                  (mapv (fn [i] [(list 'quote
                                                                       (clojure.core/first i)) nil])
                                                        optional-params))
                                           (first ~(last args)))
                                   {(str (first (last required-params))) (last args)}))]
                    (let [exp
                          `(let [~'query-result (client/query ~compiled ~merged-params)]
                             ~(if hyphenate? `(hyphenate-keywords ~'query-result) ~'query-result))]
                      exp))
                 (catch Throwable e#
                   (throw (ex-info "Exception in Gel query execution: "
                                   {:message (.getMessage e#)
                                    :cause   (.getCause e#)
                                    :stack   (map #(str "  at" %) (.getStackTrace e#))}))))))
          (catch Exception e
            (println "Error during query compilation in defquery: " (.getMessage e)
                     "\n" (reduce #(str %1 " at " %2 "\n") (.getStackTrace e)))
            (throw e)))
        (throw (ex-info (str
                         "Every param binding for a defquery must have the form [$symbol :type],"
                         "bindings received: "
                         params)
                        {})))
      (throw
       (ex-info
        "Every parameter for defquery must be a symbol starting with $ (EdgeQL parameter syntax)"
        {})))))

(comment
  (defquery 'auth-user
            [['$email :str] ['$hashed-password :str]]
            (-> (top/select 42)
                (top/filter (and (eq '$email "email") (eq '$hashed "email")))))
  (prn (macroexpand (defquery test-optional
                              [[$email :str] [$optional-param :optional-str]]
                              (top/select $optional-param))))
  ((fn []
     (defquery test-optional
               [[$email :str] [$optional-param :optional-str]]
               (top/select (++ $optional-param $email)))
     (println "res1" (test-optional "colin@example.com" {'$optional-param "yo"}))
     (println "res2" (test-optional "bolin@example.com"))))
  (defquery 'test-query
            [['$test :int64] ['$test2 :int64]]
            (-> (top/select ['$test '$test2])))
  (defquery 'store-api-key!
            [['$hashed-key :str] ['$user-id :str]]
            (-> (top/with [['user
                            (assert-single (-> (top/select {:user/User [:api-keys]})
                                               (top/filter [:= '.id '$user-id])))]])
                (top/select (if_else
                             '()
                             (gte (count 'user.api-keys) 5)
                             (-> (top/with [['new-key
                                             (top/insert {:user/ApiKey [{:= {:key '$hashed-key}}
                                                                        {:= {:user 'user}}]})]])
                                 (top/select (-> (top/update :user/User)
                                                 (top/set [{:+= {:api-keys 'new-key}}]))))))))
  (defquery 'get-keys
            [['$user-id :str]]
            (-> (top/select {:user/ApiKey [:created-at
                                           :last-used
                                           :id]})
                (top/filter (eq '.user.id (cast-uuid '$user-id)))))
  (defquery
   'insert-instance-type-unless-exists!
   [['$family-name :str] ['$size-name :str] ['$vcpu :int16] ['$price :float32]]
   (-> (top/with [['existing
                   (-> (top/select :infra/InstanceType)
                       (top/filter (and (eq '.family '$family-name) (eq '.size '$size-name))))]])
       (top/select (if-else
                    (-> (top/update :infra/InstanceType)
                        (top/filter (and (eq '.family '$family-name) (eq '.size '$size-name)))
                        (top/set [{:= {:hourly-price '$price}}]))
                    (exists 'existing)
                    (top/insert {:infra/InstanceType
                                 [{:= {:family '$family-name}} {:= {:size '$size-name}}
                                  {:= {:vcpu-count '$vcpu}} {:= {:hourly-price '$price}}]}))))))


(comment
  (query "select 42")
  (auth-user "test@example.com" "hashed-password")
  (test-query 64 32))

(defn resolve-dot-access-for-object
  [object-type dot-access-expr]
  (binding [*clogel-dot-access-context* {:type object-type :card :many}]
    (validate-dot-access dot-access-expr)))
