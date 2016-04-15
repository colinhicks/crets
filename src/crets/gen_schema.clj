(ns crets.gen-schema
  (:require [crets
             [transform :as t]
             [protocols :as p]
             [type-extensions :as ext]]
            [schema.core :as s]))

(def rets-data-type->stype
  {"Int"      's/Int
   "Small"    's/Int
   "Long"     's/Num
   "Double"   's/Num
   "Decimal"  's/Num
   "DateTime" 's/Inst
   "Date"     's/Inst})

(defn stype [{:keys [data-type lookup? lookup-multiple?]}]
  (if lookup?
    (if lookup-multiple?
      ['s/Str]
      's/Str)
    (get rets-data-type->stype data-type 's/Str)))

(defn key+stype [field]
  [(t/keywordize (:id field))
   (stype field)])

(defn schema [metadata resource-id class-id]
  (->> (p/classes metadata resource-id)
       (some #(when (= class-id (:id %)) %))
       :tables
       (map key+stype)
       (into {})))

(defn ->schema-vec [schema]
  (reduce (fn [r [k v]]
            (apply conj r [(symbol (name k)) :- v]))
          []
          schema))

(defn schema-record-form [name schema]
  `(~'s/defrecord ~name ~(->schema-vec schema)))

(defn record-form [name schema]
  `(~'defrecord ~name ~(->> schema
                            ->schema-vec
                            (partition 3)
                            (map first)
                            (vec))))
