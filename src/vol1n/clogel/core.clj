(ns vol1n.clogel.core
  (:require [malli.core :as m]
            [vol1n.clogel.scalar :refer [clogel-scalars]]
            [vol1n.clogel.cast :refer [defgelcasts]]
            [vol1n.clogel.collection :refer [clogel-collections]]
            [vol1n.clogel.functions-operators :refer [defgelfuncs defgeloperators gel-index]]
            [vol1n.clogel.functions-operators :as func]
            [vol1n.clogel.object-types :refer [defgelobjects dot-access]]
            [vol1n.clogel.util :refer
             [*clogel-dot-access-context* *clogel-with-bindings* *clogel-param-bindings*]]
            [vol1n.clogel.top-level :refer [clogel-top-level-statements]]
            [vol1n.clogel.top-level :as top]
            [vol1n.clogel.client :as client]
            [cheshire.core :as json]
            [clojure.string :as str])
  (:import [com.geldata.driver.exceptions GelErrorException])
  (:refer-clojure :exclude
                  [update for filter group-by min range find max assert count concat mod or not
                   distinct destructure and <= / > < + >= - *]))
; select filter offset group-by limit update insert
              ;delete
(def select top/select)
(def filter top/filter)
(def offset top/offset)
(def group-by top/group-by)
(def limit top/limit)
(def update top/update)
(def insert top/insert)
(def for top/for)
(def delete top/delete)
(def with top/with)
(def access func/access)
(def unless-conflict top/unless-conflict)

(defn match-overload
  [call overloads]
  (let [matched (some (fn [ovl]
                        (let [result ((:validator ovl) call)]
                          (when (clojure.core/not (:error/error result))
                            {:overload ovl :annotations result})))
                      overloads)]
    matched))

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
         {:scalar clogel-scalars}
         {:collection clogel-collections}
         gel-index))

(def mod-keys #{:limit :order-by :filter :offset})

(defn clogel->edgeql
  [edn]
  (if (clojure.core/and (symbol? edn) (contains? *clogel-with-bindings* edn))
    (assoc (get *clogel-with-bindings* edn) :value (str edn))
    (if (clojure.core/and (symbol? edn) (contains? *clogel-param-bindings* edn))
      (assoc (clojure.core/get *clogel-param-bindings* edn)
             :value
             (str \< (:type (get *clogel-param-bindings* edn)) \> edn))
      (let [node-key (cond (clojure.core/and (map? edn) (:with edn)) :with
                           (map? edn) (some #(when (not (contains? mod-keys %)) %) (keys edn))
                           (clojure.core/and (vector? edn) (keyword? (first edn))) (first edn)
                           (keyword? edn) edn
                           (symbol? edn) :dot-access
                           (coll? edn) :collection
                           :else :scalar)
            node (get node-registry node-key)]
        (if (clojure.core/not node)
          (throw (ex-info (str "invalid keyword" node-key)
                          {:valid-keys (keys node-registry) :passed node-key}))
          (let [children ((:generate-children node) edn)
                compiled-children
                (if (nil? children)
                  (seq [])
                  ;; if it's an object, we need to know what object we're in for children of
                  ;; this node
                  (if (contains? gelobject-registry node-key)
                    (binding [*clogel-dot-access-context* {:type node-key :card :many}]
                      (map (bound-fn [child] (clogel->edgeql child)) children))
                    ;; if it's a top level statement (i.e. select)
                    ;; we need to know what object we're selecting for modifier statements
                    ;; ex. select User filter .name = "John";
                    (if (contains? clogel-top-level-statements node-key)
                      (cond (clojure.core/= node-key :for)
                            (let [compiled-binding (clogel->edgeql (first children))]
                              (binding [*clogel-with-bindings* (assoc *clogel-with-bindings*
                                                                      (first (:for edn))
                                                                      compiled-binding)]
                                [compiled-binding (clogel->edgeql (last children))]))
                            (clojure.core/= node-key :group)
                            (let [compiled-group-statement (clogel->edgeql (first children))
                                  {compiled-with-children :compiled bindings :with-bindings}
                                  (reduce (fn [compiled [b c]]
                                            (let [result (clogel->edgeql c)]
                                              (binding [*clogel-with-bindings*
                                                        (assoc *clogel-with-bindings* b result)]
                                                {:compiled      (conj (:compiled compiled) result)
                                                 :with-bindings *clogel-with-bindings*})))
                                          {:compiled [] :with-bindings *clogel-with-bindings*}
                                          (map vector
                                               (map first (:using edn))
                                               (clojure.core/take (count (:using edn))
                                                                  (rest children))))]
                              (binding [*clogel-with-bindings* bindings]
                                (conj (cons compiled-group-statement compiled-with-children)
                                      (map clogel->edgeql
                                           (clojure.core/drop (inc (count (:using edn)))
                                                              children)))))
                            (clojure.core/= node-key :with)
                            (let [{compiled-with-children :compiled bindings :with-bindings}
                                  (reduce (fn [compiled [b c]]
                                            (let [result (clogel->edgeql c)]
                                              (binding [*clogel-with-bindings*
                                                        (assoc *clogel-with-bindings* b result)]
                                                {:compiled      (conj (:compiled compiled) result)
                                                 :with-bindings *clogel-with-bindings*})))
                                          {:compiled [] :with-bindings *clogel-with-bindings*}
                                          (map vector (map first (:with edn)) (butlast children)))]
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
                {annotations :annotations overload :overload} (match-overload type-form
                                                                              (:overloads node))]
            (if (clojure.core/not overload)
              (throw (ex-info (str "No overload for node of type " node-key " for value " edn)
                              {:node edn :valid (:overloads node)}))
              (assoc annotations
                     :value
                     (apply (:compile-fn overload)
                            (into [edn] (map :value compiled-children)))))))))))

(defn query
  [q]
  (if (some #(#{:select :group :delete :insert :for :with} (key %)) q)
    (let [compiled (:value (clogel->edgeql q))]
      (try (client/query (subs compiled 1 (dec (clojure.core/count compiled))))
           (catch Throwable ge
             (throw (ex-info "Exception in Gel query execution: "
                             {:message (.getMessage ge)
                              :cause   (.getCause ge)
                              :stack   (map #(str "  at" %) (.getStackTrace ge))})))))
    (throw (ex-info "Missing top-level statement in query" {:error/error true :error/query q}))))

(defn compile-query
  [q]
  (let [compiled (:value (clogel->edgeql q))]
    (subs compiled 1 (dec (clojure.core/count compiled)))))

(comment
  (clogel->edgeql [:math/abs -5])
  (clogel->edgeql [:cast-int32 5])
  (clogel->edgeql [1 2 3])
  (clogel->edgeql #{1 2 3})
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
  (query {:delete :User}))

(defn dequote [form] (if (clojure.core/and (seq? form) (= 'quote (first form))) (second form) form))

(defmacro defquery
  [name params query]
  (let [params (mapv (fn [[name type]] [(dequote name) {:type type :card :singleton}]) params)
        query (eval query)]
    (if (every? #(str/starts-with? (str (first %)) "$") params)
      (let [args (vec (map first params))
            param-binding (into {} params)
            compiled (binding [*clogel-param-bindings* param-binding] (clogel->edgeql query))]
        `(defn ~(dequote name)
           ~args
           (client/query ~compiled
                         ~(into {}
                                (map (fn [[p a]] [(str (first p)) a]) (map vector params args))))))
      (throw (ex-info "Every symbol for defquery must start with $ (EdgeQL parameter syntax)"
                      {})))))

(comment
  (macroexpand-1 (defquery 'auth-user
                           [['$email :str] ['$hashed :str]]
                           (-> (top/select :user/User)
                               (top/filter (and (eq '$email '.email)
                                                (eq '$hashed '.passwordHash)))))))
