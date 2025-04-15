(ns vol1n.clogel.assignment
  (:require [clojure.string :as str]
            [vol1n.clogel.util :refer [remove-colon-kw]]))



(defn sort-kws-alpha [& kws] (map keyword (sort (map remove-colon-kw kws))))

(defn generate-assignment-children
  [assignment-map]
  (let [sorted-keys (apply sort-kws-alpha (keys assignment-map))]
    (map #(do (println assignment-map %) (get assignment-map %)) sorted-keys)))

(comment
  (generate-assignment-children {:x 100 :lol "lmao"}))

(defn compile-assignment
  [assignment-map compiled-children]
  (let [sorted-keys (apply sort-kws-alpha (keys assignment-map))]
    (str/join ",\n"
              (reduce (fn [acc [k v]] (conj acc (str (remove-colon-kw k) " := " v)))
                      []
                      (map vector sorted-keys compiled-children)))))

(defn build-type-form-assignment
  [assignment-map types]
  (let [sorted-keys (apply sort-kws-alpha (keys assignment-map))]
    (into {} (map vector sorted-keys types))))

(comment
  (let [test {:x 100 :lol "lmao"}] (compile-assignment test ["100" "'lmao'"])))
