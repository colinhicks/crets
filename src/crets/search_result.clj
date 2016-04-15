(ns crets.search-result
  (:require [clj-time
             [coerce :as timec]
             [format :as timef]]
            [clojure
             [edn :as edn]
             [string :as str]]
            [crets
             [metadata :as m]
             [protocols :as p]
             [utils :as utils]])
  (:import org.realtors.rets.client.SearchResult))

(extend-protocol p/Interop
  SearchResult
  (->clj [obj]
    {:column-names (.getColumns obj)
     :rows         (iterator-seq (.iterator obj))}))

(defn field-value-fn [f]
  (fn [[k v]] [k ((f k) v)]))

(defn field-key-fn [f]
  (fn [[k v]] [(f k) v]))

(def field-keywordize (map (field-key-fn utils/keywordize)))

(defn search-result->fields
  ([search-result]
   (search-result->fields (map identity) search-result))
  ([xf search-result]
   (let [{:keys [column-names rows]} (p/->clj search-result)]
     (map #(into [] xf (map vector column-names %)) rows))))

(def default-type-converters
  {"Int"      edn/read-string
   "Small"    edn/read-string
   "Long"     edn/read-string
   "Double"   edn/read-string
   "Decimal"  edn/read-string
   "DateTime" #(if-not (seq %)
                nil
                (timec/to-date (timef/parse
                                (timef/formatters :date-hour-minute-second) %)))
   "Date"     #(if-not (seq %)
                nil
                (timec/to-date (timef/parse (timef/formatters :date) %)))})

(defn field-converter
  ([search-spec schema]
   (field-converter search-spec schema default-type-converters))
  ([{:keys [resource-id class-id]} schema converters]
   (let [fields (m/fields schema resource-id class-id)
         converter (memoize (fn [field-id]
                              (let [field (some #(when (= field-id (:id %)) %) fields)]
                                (when-not (:lookup? field)
                                  (->> field
                                       :data-type
                                       (get converters))))))]
     (map (field-value-fn #(or (converter %) identity))))))


(defn field-lookup-resolver [{:keys [resource-id class-id]} schema]
  (let [lookups (p/lookups schema resource-id)
        fields (m/fields schema resource-id class-id)
        resolver (memoize
                  (fn [field-id]
                    (let [field (some #(when (= field-id (:id %)) %) fields)
                          resolve (fn [v]
                                    (->> lookups
                                         (some #(when (= (:lookup-name field) (:id %)) %))
                                         :lookup-types
                                         (some #(when (= v (:id %)) %))
                                         :long-value))]
                      (when (:lookup? field)
                        (fn [v]
                          (if (:lookup-multiple? field)
                            (->> (str/split v #",")
                                 (map resolve)
                                 (remove nil?)
                                 vec)
                            (resolve v)))))))]
    (map (field-value-fn #(or (resolver %) identity)))))
