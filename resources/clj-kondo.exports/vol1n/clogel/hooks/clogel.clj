(ns hooks.clogel
  (:require [clj-kondo.hooks-api :as api]))

(defn g-defquery
  [{:keys [node]}]
  (let [children (:children node)
        _defquery-symbol (first children) ; g/defquery
        fn-name (second children)         ; function name
        params (nth children 2)           ; [[$user-id :str] [$customer-id :str]]
        body (drop 3 children)            ; rest of the body
        ;; Extract parameter symbols from nested vectors like [[$user-id :str] [$customer-id
        ;; :str]]
        param-symbols (when params (map #(first (:children %)) (:children params)))]
    ;; Transform to a defn that takes the parameters as function arguments
    (let [result (api/list-node (list* (api/token-node 'defn)
                                       fn-name
                                       ;; Parameter vector with the actual parameters
                                       (api/vector-node (or param-symbols []))
                                       body))]
      {:node result})))

(defn g-with
  [{:keys [node]}]
  (let [children (:children node)
        _with-symbol (first children)  ; g/with
        bindings (second children)     ; [['user (-> ...)] ['other (-> ...)]]
        body (drop 2 children)         ; rest of the body
        ;; Extract binding symbols from nested vectors like [['user ...] ['other ...]]
        binding-pairs (when bindings 
                        (mapcat (fn [binding-vec]
                                  (let [binding-children (:children binding-vec)]
                                    [(first binding-children)  ; the symbol like 'user
                                     (second binding-children)])) ; the value expression
                                (:children bindings)))]
    ;; Transform to a let binding so clj-kondo understands the local bindings
    (let [result (api/list-node (list* (api/token-node 'let)
                                       (api/vector-node binding-pairs)
                                       body))]
      (println "=== G/WITH HOOK ===")
      (println "Binding pairs:" (map api/sexpr binding-pairs))
      (println "Body:" (map api/sexpr body))
      (println "TRANSFORMED:")
      (clojure.pprint/pprint (api/sexpr result))
      {:node result})))

(defn g-for
  [{:keys [node]}]
  (let [children (:children node)
        _for-symbol (first children)  ; g/for
        bindings (second children)    ; ['u :User] 
        body (drop 2 children)        ; rest of the body like {:select 'u.email}
        ;; Extract binding symbol from vector like ['u :User] -> [u :User]
        binding-children (:children bindings)
        binding-symbol (first binding-children)
        binding-value (second binding-children)]
    ;; Transform to a let binding so clj-kondo understands the local binding
    (let [result (api/list-node (list* (api/token-node 'let)
                                       (api/vector-node [binding-symbol binding-value])
                                       body))]
      (println "=== G/FOR HOOK ===")
      (println "Binding symbol:" (api/sexpr binding-symbol))
      (println "Binding value:" (api/sexpr binding-value))
      (println "Body:" (map api/sexpr body))
      (println "TRANSFORMED:")
      (clojure.pprint/pprint (api/sexpr result))
      {:node result})))
