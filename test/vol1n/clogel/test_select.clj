(ns vol1n.clogel.select-test
  (:require [clojure.test :refer :all]
            [vol1n.clogel.core :as g]))

(defn run-query [q] (g/query q))

(defn result=
  [expected actual]
  ;; For now, shallow = comparison; can add sorting later
  (= expected actual))

;; ----------------------------------------
;; TEST SUITE
;; ----------------------------------------
(defn seed
  []
  (doall (g/delete (g/User))
         (g/insert (list
                    (g/User {:email "alice@example.com" :is_active true :name "Alice" :rating 10})
                    (g/User {:email "bob@example.com" :is_active false :name "Bob" :rating 8})))))

(seed)
(println "loading test suite")

(deftest select-basic-fields
  (is (result= #{{:name "Alice"} {:name "Bob"}} (run-query (g/select {:User [:name]})))))

(deftest select-filtered-users
  (is (result= #{{:name "Alice"}}
               (run-query (g/select {:User [:name] :filter [:= '.is_active true]})))))

(deftest select-user-rating
  (is (result= #{{:name "Alice" :rating 10} {:name "Bob" :rating 8}}
               (run-query (g/select {:User [:name :rating]})))))

(deftest select-nested-projection
  (is (result= #{{:title "Hello World" :author {:name "Alice"}}}
               (run-query (g/select {:Post [:title {:author [:name]}]})))))

(deftest select-dot-access
  (is (result= #{{:name "Alice"}}
               (run-query (g/select {:User [:name] :filter [:= '.name "Alice"]})))))

(deftest select-with-multi-link
  (is (result= #{{:name "Alice" :friends []} {:name "Bob" :friends []}}
               (run-query (g/select {:User [:name {:friends [:name]}]})))))

(deftest select-group-by
  ;; This is a stub — update expected result when implemented
  (is (not (nil? (run-query (g/select {:User [:rating] :group-by [:rating]}))))))

(deftest select-with-function-call
  ;; Expecting :rating to be multiplied
  (is (result= #{{:doubled 20} {:doubled 16}}
               (run-query (g/select {:User [{:doubled [:* '.rating 2]}]})))))

(deftest for-loop-style
  (is (result= #{{:email "alice@example.com"} {:email "bob@example.com"}}
               (run-query (g/for ['u :User] {:select ('u :email)})))))

(deftest with-call
  (is (result= 42
               (run-query (-> (g/with [['x 42]])
                              (g/select 'x))))))

;; ----------------------------------------
;; Run All
;; ----------------------------------------

(deftest full-suite
  (testing "Run all query compilation tests"
    (select-basic-fields)
    (select-filtered-users)
    (select-user-rating)
    (select-nested-projection)
    (select-dot-access)
    (select-with-multi-link)
    (select-group-by)
    (select-with-function-call)
    (for-loop-style)))
