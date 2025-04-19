(ns vol1n.clogel.top-level
  (:require [vol1n.clogel.castable :refer [implicit-castable?]]
            [vol1n.clogel.util :refer [->Node ->Overload remove-colon-kw]])
  (:refer-clojure :exclude [update for filter]))

(defn validate-map
  [m validator-map]
  (if (every? #(contains? validator-map %) (keys m))
    (some (fn [[k v]]
            (let [{:keys [fn message]} (get validator-map k)]
              (when (and fn (not (fn v))) {:error/error true :error/message message})))
          m)
    {:error/message "extraneous key"
     :error/error   true
     :error/input   m
     :error/allowed (set (keys validator-map))}))

(def modifier-validators
  {:order-by {:fn      #(or (= (:card %) :singleton)
                            (and (vector? %)
                                 (= (:card (first %)) :singleton)
                                 (#{"asc" "desc"} (last %))))
              :message "Filter statement must be a singleton"}
   :limit    {:fn      #(and (#{:int16 :int32 :int64} (:type %))
                             (#{:optional :singleton} (:card %)))
              :message "Limit statement must be a singleton"}
   :offset   {:fn      #(and (#{:int16 :int32 :int64} (:type %))
                             (#{:optional :singleton} (:card %)))
              :message "Offset statement must be a singleton"}
   :filter   {:fn #(implicit-castable? (:type %) :bool)}})

(defn select-validator
  [select-statement]
  (let [select (:select select-statement)
        limit (:limit select-statement)]
    (if (not select)
      (throw (ex-info "should be unreachable" {}))
      (let [failure (validate-map select-statement
                                  (merge {:select {:fn      (fn [_] true)
                                                   :message "Should be unreachable, right?"}}
                                         modifier-validators))]
        (if failure
          (do (println "failure" failure) failure)
          {:type (:type select)
           :card (if (and (= limit 1) (not= :singleton (:card select)))
                   :optional
                   (:card select))})))))

(defn insert-validator
  [insert-statement]
  (println "ins" insert-statement)
  (let [insert (:insert insert-statement)
        limit (:limit insert-statement)]
    (if (not insert)
      (throw (ex-info "should be unreachable" {}))
      (let [failure (validate-map insert-statement
                                  {:insert {:fn      #(or (:insertable (:type %)) (:insertable %))
                                            :message "Should be unreachable, right?"}
                                   :unless-conflict {:fn      (fn [_] true)
                                                     :message "should be unreachable"}
                                   :else   {:fn (fn [_] true) :message "should be unreachable"}})]
        (if failure
          failure
          {:type (:type insert)
           :card (if (and (= limit 1) (not= :singleton (:card insert)))
                   :optional
                   (:card insert))})))))

(defn update-validator
  [update-statement]
  (let [update (:update update-statement)
        limit (:limit update-statement)]
    (if (not update)
      (throw (ex-info "should be unreachable" {}))
      (let [failure (validate-map update-statement
                                  (merge {:update {:fn      #(:updatable (:type %))
                                                   :message "Should be unreachable, right?"}}
                                         modifier-validators))]
        (if failure
          failure
          {:type (:type update)
           :card (if (and (= limit 1) (not= :singleton (:card update)))
                   :optional
                   (:card update))})))))

(defn delete-validator
  [delete-statement]
  (let [delete (:delete delete-statement)
        limit (:limit delete-statement)]
    (if (not delete)
      (throw (ex-info "should be unreachable" {}))
      (let [failure (validate-map delete-statement
                                  (merge {:delete {:fn      #(:deletable (:type %))
                                                   :message "Not deletable"}}
                                         modifier-validators))]
        (if failure
          failure
          {:type (:type delete)
           :card (if (and (= limit 1) (not= :singleton (:card delete)))
                   :optional
                   (:card delete))})))))

(defn with-validator
  [with-entry]
  (let [bindings (:with with-entry)
        rest (:rest with-entry)
        error (map
               #(when (not (and (vector? %) (= (count %) 2) (symbol? (first %)) (:type (last %))))
                  (throw (ex-info "Malformed with statement"
                                  {:error/error true :error/message (str "Malformed binding " %)})))
               bindings)]
    (if (not error) rest nil)))

(defn for-validator
  [for-statement]
  (let [for (:for for-statement)
        union (:union for-statement)
        failure
        (validate-map
         for-statement
         {:for   {:fn      #(and (vector? %) (= (count %) 2) (symbol? (first %)) (:type (last %)))
                  :message "For should be in the form :for [sym value]"}
          :union {:fn #(:type %) :message "Union should contain a valid value"}})]
    (if failure
      failure
      (if (or (not for) (not union))
        (throw (ex-info ":for and :union required for for statements" {}))
        {:type (:type (:union for-statement)) :card :many}))))

(defn group-by-validator
  [group-by-statement]
  (let [group (:group group-by-statement)
        by (:by group-by-statement)
        failure (validate-map group-by-statement
                              (merge {:group {:fn (fn [_] true) :message "none"}
                                      :by    {:fn (fn [_] true) :message "none"}
                                      :using {:fn (fn [bindings]
                                                    (map #(when (not (and (vector? %)
                                                                          (= (count %) 2)
                                                                          (symbol? (first %))
                                                                          (:type (last %))))
                                                            (throw (ex-info
                                                                    "Malformed with statement"
                                                                    {:error/error true
                                                                     :error/message
                                                                     (str "Malformed binding "
                                                                          %)})))
                                                         bindings))}}
                                     modifier-validators))]
    (if (not (and group by))
      (throw (ex-info "Group by requires :group and :by" {:error/error true}))
      (if failure
        failure
        {:type {:grouping {:type :str :card :many} :elements {:type (:type group) :card :many}}}))))

(defn sort-keys
  [m]
  (let [priority {:select          1
                  :group           2
                  :using           2.25
                  :by              2.3
                  :insert          4
                  :update          5
                  :unless-conflict 4.5
                  :else            4.75
                  :order-by        6}]
    (->> (keys m)
         (sort-by (fn [k] [(get priority k 999) ;; primary: custom priority
                           (name k)]))          ;; secondary: alphabetical
         vec)))

(defn compile-modifier [k child] (str (remove-colon-kw k) " " child))

(defn compile-order-by
  [order-by-statement child]
  (if (vector? order-by-statement)
    (str "order by " child " " (last order-by-statement))
    (compile-modifier :order-by child)))

(defn compile-unless-conflict
  [val child]
  (str "unless conflict" (when (not (true? val)) (str " on " child "\n"))))

(defn build-top-level-compiler
  [type]
  (fn compile [statement & children]
    (str \(
         "\n" (reduce
               (fn [acc [k child]]
                 (cond (= k type) (str (remove-colon-kw type) " " child "\n" acc)
                       (= k :order-by) (str acc (compile-order-by (get statement :order-by) child))
                       (= k :unless-conflict)
                       (str acc (compile-unless-conflict (get statement :unless-conflict) child))
                       :else (str acc (compile-modifier k child))))
               ""
               (map vector (sort-keys statement) children))
         "\n" \))))


(defn compile-with
  [with-statement & children]
  (let [bindings (:with with-statement)]
    (str \(
         "\n"
         "with "
         (:str (reduce (fn [acc [sym _]]
                         {:str       (str (:str acc) sym " := " (first (:remaining acc)) ",\n")
                          :remaining (rest children)})
                       {:str "" :remaining (butlast children)}
                       bindings))
         (subs (last children) 1 (dec (count (last children)))))))

(defn compile-for
  [for-statement & children]
  (str \(
       "\n"
       "for "
       (first (:for for-statement))
       " in "
       (first children)
       "\nunion "
       (last children)
       \)))

(defn compile-group-by
  [group-by-statement & children]
  (let [bindings (:using group-by-statement)
        {remaining :remaining compiled-using :str}
        (reduce (fn [acc [sym _]]
                  {:str       (str (:str acc) sym " := " (first (:remaining acc)) ",\n")
                   :remaining (rest children)})
                {:str "" :remaining (rest children)}
                bindings)]
    (str \(
         "\n"
         "group "
         (first children)
         "\n"
         "using "
         compiled-using
         "\n"
         "by "
         (first remaining)
         (reduce (fn [acc [k child]]
                   (cond (= k type) (str (remove-colon-kw type) " " child "\n" acc)
                         (= k :order-by)
                         (str acc (compile-order-by (get group-by-statement :order-by) child))
                         :else (str acc (compile-modifier k child))))
                 ""
                 (map vector (sort-keys group-by-statement) (rest remaining))))))


(def clogel-top-level-statements
  {:select   (->Node
              :select
              (fn [select-statement]
                (map (fn [k] (get select-statement k)) (sort-keys select-statement)))
              (fn [select-statement types]
                (into {} (map (fn [[k t]] [k t]) (map vector (sort-keys select-statement) types))))
              [(->Overload select-validator (build-top-level-compiler :select))])
   :insert   (->Node
              :insert
              (fn [insert-statement]
                (map (fn [k] (get insert-statement k)) (sort-keys insert-statement)))
              (fn [insert-statement types]
                (into {} (map (fn [[k t]] [k t]) (map vector (sort-keys insert-statement) types))))
              [(->Overload insert-validator (build-top-level-compiler :insert))])
   :update   (->Node
              :update
              (fn [update-statement]
                (map (fn [k] (get update-statement k)) (sort-keys update-statement)))
              (fn [update-statement types]
                (into {} (map (fn [[k t]] [k t]) (map vector (sort-keys update-statement) types))))
              [(->Overload update-validator (build-top-level-compiler :update))])
   :delete   (->Node
              :delete
              (fn [delete-statement]
                (map (fn [k] (get delete-statement k)) (sort-keys delete-statement)))
              (fn [delete-statement types]
                (into {} (map (fn [[k t]] [k t]) (map vector (sort-keys delete-statement) types))))
              [(->Overload delete-validator (build-top-level-compiler :delete))])
   :with     (->Node :with
                     (fn [with-statement]
                       (into (mapv #(last %) (:with with-statement))
                             [(dissoc with-statement :with)]))
                     (fn [with-statement types]
                       (merge {:with (mapv vector (map first (:with with-statement)) types)
                               :rest (last types)}))
                     [(->Overload with-validator compile-with)])
   :group-by (->Node :group-by
                     (fn [group-by-statement]
                       (into [(:group group-by-statement)]
                             (concat (mapv last (:using group-by-statement))
                                     [(:by group-by-statement)]
                                     (mapv (fn [k] (get group-by-statement k))
                                           (clojure.core/filter
                                            (fn [k] (not (contains? #{:group :using :by} k)))
                                            (sort-keys group-by-statement))))))
                     (fn [group-by-statement types]
                       (:type-form
                        (reduce (fn [acc [k _]]
                                  {:type-form (assoc acc k (first (:remaining acc)))
                                   :remaining (rest (:remaining acc))})
                                {:type-form (if (contains? group-by-statement :using)
                                              {:group (first types)
                                               :using (mapv vector
                                                            (map first (:using group-by-statement))
                                                            (rest types))
                                               :by    (first (drop (inc (count
                                                                         (:using
                                                                          group-by-statement)))
                                                                   types))}
                                              {:group (first types) :by (second types)})
                                 :remaining (drop (+ (count (:using group-by-statement)) 2))}
                                (dissoc group-by-statement :group :using :by))))
                     [(->Overload group-by-validator compile-group-by)])
   :for      (->Node :for
                     (fn [for-statement] [(second (:for for-statement)) (:union for-statement)])
                     (fn [for-statement types]
                       {:for [(first (:for for-statement)) (first types)] :union (last types)})
                     [(->Overload for-validator compile-for)])})

(defn select ([val] (select {} val)) ([statement val] (assoc statement :select val)))

(defn filter ([by] (filter {} by)) ([statement by] (assoc statement :filter by)))

(defn offset ([amount] (offset {} amount)) ([statement amount] (assoc statement :offset amount)))

(defn group-by
  ([group by] (vol1n.clogel.top-level/group-by {} group by))
  ([statement group by]
   (-> statement
       (assoc :group group)
       (assoc :by by))))

(defn limit ([amount] (limit {} amount)) ([statement amount] (assoc statement :limit amount)))

(defn update ([val] (update {} val)) ([statement val] (assoc statement :update val)))

(defn insert ([val] (insert {} val)) ([statement val] (assoc statement :insert val)))

(defn delete ([val] (delete {} val)) ([statement val] (assoc statement :delete val)))

(defn for ([binding union] {:for binding :union union}))

(defn with ([bindings] (with {} bindings)) ([statement bindings] (assoc statement :with bindings)))

(defn unless-conflict
  ([on] (unless-conflict {} on))
  ([statement on] (assoc statement :unless-conflict on)))
