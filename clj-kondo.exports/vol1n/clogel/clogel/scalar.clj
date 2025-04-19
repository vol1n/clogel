(ns vol1n.clogel.scalar
  (:require [vol1n.clogel.client :refer [query]]
            [vol1n.clogel.util :refer [->Node ->Overload]]))

(def primitives-map
  {java.lang.String         :str
   java.lang.Boolean        :bool
   java.lang.Byte           :int64
   java.lang.Short          :int64
   java.lang.Integer        :int64
   java.lang.Long           :int64
   clojure.lang.BigInt      :bigintn
   java.math.BigInteger     :bigint
   java.lang.Float          :float64
   java.lang.Double         :float64
   java.math.BigDecimal     :decimal
   java.util.UUID           :uuid
   java.time.Instant        :datetime
   java.time.OffsetDateTime :datetime
   java.time.LocalDateTime  :cal/local-datetime
   java.time.LocalDate      :cal/local-date
   java.time.LocalTime      :cal/local-time
   java.time.Duration       :cal/duration
   java.time.Period         :cal/relative-duration})

(def gel-type->predicate
  {:str            string?
   :bool           boolean?
   :int64          integer?
   :bigint         #(or (instance? clojure.lang.BigInt %) (instance? java.math.BigInteger %))
   :float64        number?
   :decimal        #(instance? java.math.BigDecimal %)
   :uuid           #(or (instance? java.util.UUID %)
                        (and (string? %)
                             (re-matches
                              #"(?i)[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}"
                              %)))
   :datetime       #(or (instance? java.time.Instant %) (instance? java.time.OffsetDateTime %))
   :cal/local-datetime #(instance? java.time.LocalDateTime %)
   :cal/local-date #(instance? java.time.LocalDate %)
   :cal/local-time #(instance? java.time.LocalTime %)
   :cal/duration   #(instance? java.time.Duration %)
   :cal/relative-duration #(instance? java.time.Period %)})

(def clogel-scalars
  (->Node
   :scalar
   (fn [_] nil)
   (fn [scalar & _] scalar)
   [(->Overload #(if (string? %)
                   {:type :str :card :singleton}
                   {:error/error true :error/message "Not a string"})
                #(pr-str %))
    (->Overload #(if (boolean? %)
                   {:type :bool :card :singleton}
                   {:error/error true :error/message "Not a boolean"})
                str)
    (->Overload #(if (integer? %)
                   {:type :int64 :card :singleton}
                   {:error/error true :error/message "Not a integer"})
                str)
    (->Overload #(if (or (instance? clojure.lang.BigInt %) (instance? java.math.BigInteger %))
                   {:type :bigint :card :singleton}
                   {:error/error true :error/message "Not a bigint"})
                #(str %))
    (->Overload #(if (and (number? %) (not (integer? %)))
                   {:type :float64 :card :singleton}
                   {:error/error true :error/message "Not a float"})
                #(str %))
    (->Overload #(if (instance? java.math.BigDecimal %)
                   {:type :decimal :card :singleton}
                   {:error/error true :error/message "Not a decimal"})
                #(str %))
    (->Overload #(if (instance? java.util.UUID %)
                   {:type :uuid :card :singleton}
                   {:error/error true :error/message "Not a UUID"})
                #(str "<uuid>" %))
    (->Overload #(if (or (instance? java.time.Instant %) (instance? java.time.OffsetDateTime %))
                   {:type :datetime :card :singleton}
                   {:error/error true :error/message "Not a valid Datetime"})
                #(str "<datetime>" %))
    (->Overload #(if (instance? java.time.LocalDateTime %)
                   {:type :cal/local-datetime :card :singleton}
                   {:error/error true :error/message "Not a valid LocalDatetime"})
                #(str "<cal::local_datetime>" %))
    (->Overload #(if (instance? java.time.LocalDate %)
                   {:type :cal/local-date :card :singleton}
                   {:error/error true :error/message "Not a valid Local date"})
                #(str "<cal::local_datetime>" %))
    (->Overload #(if (instance? java.time.LocalTime %)
                   {:type :cal/local-time :card :singleton}
                   {:error/error true :error/message "Not a valid Local time"})
                #(str "<cal::local_datetime>" %))
    (->Overload #(if (instance? java.time.Duration %)
                   {:type :cal/duration :card :singleton}
                   {:error/error true :error/message "Not a valid duration"})
                #(str "<cal::duration>" %))
    (->Overload #(if (instance? java.time.Period %)
                   {:type :cal/relative-duration :card :singleton}
                   {:error/error true :error/message "Not a valid relative duration"})
                #(str "<cal::relative_duration>" %))]))



(defn get-scalars
  []
  (query
   "
select schema::ScalarType {
    name,
    enum_values,
    arg_values,
    bases: {
        name,
        enum_values,
        arg_values,
    },
    ancestors: {
        name,
        enum_values,
        arg_values,
    }
}"))
