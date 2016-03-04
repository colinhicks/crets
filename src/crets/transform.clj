(ns crets.transform
  (:require [clojure.set :refer [difference]]
            [clojure.string :as str]
            [crets.client :as client]
            [crets.query-syntax :as query]
            [crets.extensions :as ext]
            [clj-time.format :as timef]
            [clj-time.coerce :as timec]
            [clojure.edn :as edn]))

(def search-spec client/search-spec)
(def fetch-search client/fetch-search)

(defn validate-search [{:keys [query resource-id class-id]} schema]
  (let [required (set (query/required-fields query))
        available (->> (ext/fields schema resource-id class-id)
                       (map :id)
                       set)]
    {:missing-fields (difference required available)
     :valid-query-syntax? (seq required)
     :valid-resource-id-and-class-id? (seq available)}))

(defn valid-search?
  ([{:as validation-result :keys [valid-resource-id-and-class-id?
                                  valid-query-syntax?
                                  missing-fields]}]
   (and valid-resource-id-and-class-id?
        valid-query-syntax?
        (not (seq missing-fields))))  
  ([search-spec schema]
   (valid-search? (validate-search search-spec schema))))

(defn field-value-fn [f]
  (fn [[k v]] [k ((f k) v)]))

(defn field-key-fn [f]
  (fn [[k v]] [(f k) v]))

(def keywordize (comp keyword
                      str/lower-case
                      #(str/replace % #"_" "-")))

(def field-keywordize (map (field-key-fn keywordize)))

(defn search-result->fields
  ([search-result]
   (search-result->fields (map identity) search-result))
  ([xf search-result]
   (let [{:keys [column-names rows]} (ext/values search-result)]
     (map #(transduce xf conj (map vector column-names %)) rows))))

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
   (let [fields (ext/fields schema resource-id class-id)
         converter (memoize (fn [field-id]
                              (let [field (some #(when (= field-id (:id %)) %) fields)]
                                (when-not (:lookup? field)
                                  (->> field
                                       :data-type
                                       (get converters))))))]
     (map (field-value-fn #(or (converter %) identity))))))


(defn field-lookup-resolver [{:keys [resource-id class-id]} schema]
  (let [lookups (ext/lookups schema resource-id)
        fields (ext/fields schema resource-id class-id)
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
