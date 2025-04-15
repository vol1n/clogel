(ns vol1n.clogel.select-test
  (:require [clojure.test :refer :all]
            [vol1n.clogel.core :as g]
            [cheshire.core :as json]))

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
  (g/query (g/delete (g/Post)))
  (g/query (g/delete (g/User)))
  (g/query (g/for ['u (g/json_array_unpack
                       [:to_json
                        (json/generate-string
                         [{:name "Alice" :email "alice@example.com" :is_active true :rating 10}
                          {:name "Bob" :email "bob@example.com" :is_active false :rating 8}])])]
             {:insert {:User [{:= {:name (g/cast-str (g/access 'u "name"))}}
                              {:= {:email (g/cast-str (g/access 'u "email"))}}
                              {:= {:is_active (g/cast-bool (g/access 'u "is_active"))}}
                              {:= {:rating (g/cast-int64 (g/access 'u "rating"))}}]}}))
  (g/query (g/insert (g/Post [{:= {:title "Hello World"}}
                              {:= {:author (g/assert_single
                                            (-> (g/select :User)
                                                (g/filter [:= '.email "alice@example.com"])))}}]))))

(seed)
(println "loading test suite")

(deftest select-basic-fields
  (let [got (run-query (g/select {:User [:name]}))]
    (println "got" got)
    (is (result= (list {:name "Alice"} {:name "Bob"}) got))))

(deftest select-filtered-users
  (let [got (run-query (-> (g/select {:User [:name]})
                           (g/filter [:= '.is_active true])))]
    (println "got" got)
    (is (result= (list {:name "Alice"}) got))))

(deftest select-user-rating
  (let [got (run-query (g/select {:User [:name :rating]}))]
    (println "got" got)
    (is (result= (list {:name "Alice" :rating 10} {:name "Bob" :rating 8}) got))))

(deftest select-nested-projection
  (let [got (run-query (g/select {:Post [:title {:author [:name]}]}))]
    (println "got" got)
    (is (result= (list {:title "Hello World" :author {:name "Alice"}}) got))))

(deftest select-dot-access
  (let [got (run-query (-> (g/select {:User [:name]})
                           (g/filter [:= '.name "Alice"])))]
    (println "got" got)
    (is (result= (list {:name "Alice"}) got))))

(deftest select-with-multi-link
  (let [got (run-query (g/select {:User [:name {:friends [:name]}]}))]
    (println "got" got)
    (is (result= (list {:name "Alice" :friends []} {:name "Bob" :friends []}) got))))

(deftest select-with-function-call
  ;; Expecting :rating to be multiplied
  (let [got (run-query (g/select {:User [{:= {:doubled [:* '.rating 2]}}]}))]
    (println "got" got)
    (is (result= (list {:doubled 20} {:doubled 16}) got))))

(deftest for-loop-style
  (let [got (run-query (g/for ['u :User] {:select 'u.email}))]
    (println "got" got)
    (is (result= (list "alice@example.com" "bob@example.com") got))))

(deftest with-call
  (let [got (run-query (-> (g/with [['x 42]])
                           (g/select 'x)))]
    (println "got" got)
    (is (result= (list 42) got))))

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
    (select-with-function-call)
    (for-loop-style)))
