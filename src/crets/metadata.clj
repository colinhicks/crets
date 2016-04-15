(ns crets.metadata
  (:require [clojure.edn :as edn]
            [clojure.pprint :refer [pprint]]
            [crets.protocols :as p])
  (:import org.realtors.rets.common.metadata.Metadata
           [org.realtors.rets.common.metadata.types MClass MLookup MLookupType MObject MResource MSystem MTable]))

(extend-protocol p/Interop
  Metadata
  (->clj [obj]
    {:system (p/->clj (.getSystem obj))})

  MSystem
  (->clj [obj]
    {:resources (map p/->clj (.getMResources obj))})

  MResource
  (->clj [obj]
    {:id            (.getId obj)
     :resource-id   (.getResourceID obj)
     :standard-name (.getStandardName obj)
     :visible-name  (.getVisibleName obj)
     :description   (.getDescription obj)
     :key-field     (.getKeyField obj)
     :lookups       (map p/->clj (.getMLookups obj))
     :classes       (map p/->clj (.getMClasses obj))
     :objects       (map p/->clj (.getMObjects obj))})

  MLookup
  (->clj [obj]
    {:id           (.getId obj)
     :lookup-name  (.getLookupName obj)
     :visible-name (.getVisibleName obj)
     :lookup-types (map p/->clj (.getMLookupTypes obj))})

  MLookupType
  (->clj [obj]
    {:id          (.getId obj)
     :long-value  (.getLongValue obj)
     :short-value (.getShortValue obj)
     :value       (.getValue obj)})

  MClass
  (->clj [obj]
    {:id            (.getId obj)
     :class-name    (.getClassName obj)
     :standard-name (.getStandardName obj)
     :description   (.getDescription obj)
     :tables        (map p/->clj (.getMTables obj))})

  MTable
  (->clj [obj]
    {:id               (.getId obj)
     :system-name      (.getSystemName obj)
     :long-name        (.getLongName obj)
     :lookup?          (.isLookup obj)
     :lookup-name      (.getLookupName obj)
     :lookup-multiple? (= "LookupMulti" (.getInterpretation obj))
     :data-type        (.getDataType obj)
     :searchable?      (.getSearchable obj)
     :default          (.getDefault obj)
     :unique?          (.getUnique obj)
     :in-key-index?    (.getInKeyIndex obj)
     :units            (.getUnits obj)})

  MObject
  (->clj [obj]
    {:id            (.getId obj)
     :object-type   (.getObjectType obj)
     :mime-type     (.getMIMEType obj)
     :visible-name  (.getVisibleName obj)
     :standard-name (.getStandardName obj)
     :description   (.getDescription obj)}))


(defn- resource [metadata resource-id]
  (->> metadata p/->clj :system :resources
       (some #(when (= resource-id (:id %)) %))))

(extend-type Metadata
  p/ICommonMetadata
  (classes [obj resource-id]
    (:classes (resource obj resource-id)))

  (lookups [obj resource-id]
    (:lookups (resource obj resource-id))))

(defrecord ResourceMetadata [id lookups classes key-field]
  p/ICommonMetadata
  (classes [_ _]
    classes)

  (lookups [_ _]
    lookups))

(defn metadata->resource-metadata [metadata resource-id]
  (let [r (resource metadata resource-id)]
    (ResourceMetadata.
     resource-id
     (:lookups r)
     (:classes r)
     (:key-field r))))

;; helpers

(defn fields [metadata resource-id, class-id]
  (->> (p/classes metadata resource-id)
       (some #(when (= class-id (:id %)) %))
       :tables))

(defn field [metadata resource-id, class-id field-id]
  (->> (fields metadata resource-id class-id)
       (some #(when (= field-id (:id %)) %))))

(defn lookup [metadata resource-id lookup-name]
  (->> (p/lookups metadata resource-id)
       (some #(when (= (:id %) lookup-name) %))))

(defn resolve-lookup [metadata resource-id class-id field-id lookup-val]
  (->> (field metadata resource-id class-id field-id)
       :lookup-name
       (lookup metadata resource-id)
       :lookup-types
       (some #(when (= (:id %) lookup-val) %))
       :long-value))

(defn write-resource-metadata [path metadata resource-id]
  (spit path (with-out-str
               (-> metadata
                   (metadata->resource-metadata resource-id)
                   pprint))))

(defn read-resource-metadata [path]
  (->> path
       slurp
       edn/read-string
       map->ResourceMetadata))
