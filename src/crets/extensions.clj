(ns crets.extensions
  (:import [org.realtors.rets.common.metadata Metadata]
           [org.realtors.rets.common.metadata.types MLookup MResource MSystem MLookupType MClass MTable MObject]
           [org.realtors.rets.client SearchResult]))

;; IValues

(defprotocol IValues
  (values [obj]))

(extend-protocol IValues
  Metadata
  (values [obj]
    {:system (values (.getSystem obj))})

  MSystem
  (values [obj]
    {:resources (map values (.getMResources obj))})

  MResource
  (values [obj]
    {:id            (.getId obj)
     :resource-id   (.getResourceID obj)
     :standard-name (.getStandardName obj)
     :visible-name  (.getVisibleName obj)
     :description   (.getDescription obj)
     :key-field     (.getKeyField obj)
     :lookups       (map values (.getMLookups obj))
     :classes       (map values (.getMClasses obj))
     :objects       (map values (.getMObjects obj))})

  MLookup
  (values [obj]
    {:id           (.getId obj)
     :lookup-name  (.getLookupName obj)
     :visible-name (.getVisibleName obj)
     :lookup-types (map values (.getMLookupTypes obj))})

  MLookupType
  (values [obj]
    {:id          (.getId obj)
     :long-value  (.getLongValue obj)
     :short-value (.getShortValue obj)
     :value       (.getValue obj)})

  MClass
  (values [obj]
    {:id            (.getId obj)
     :class-name    (.getClassName obj)
     :standard-name (.getStandardName obj)
     :description   (.getDescription obj)
     :tables        (map values (.getMTables obj))})

  MTable
  (values [obj]
    {:id               (.getId obj)
     :system-name      (.getSystemName obj)
     :long-name        (.getLongName obj)
     :lookup?          (.isLookup obj)
     :lookup-name      (.getLookupName obj)
     :lookup-multiple? (= "LookupMulti" (.getInterpretation obj))
     :data-type        (.getDataType obj)})

  MObject
  (values [obj]
    {:id            (.getId obj)
     :object-type   (.getObjectType obj)
     :mime-type     (.getMIMEType obj)
     :visible-name  (.getVisibleName obj)
     :standard-name (.getStandardName obj)
     :description   (.getDescription obj)})

  SearchResult
  (values [obj]
    {:column-names (.getColumns obj)
     :rows (iterator-seq (.iterator obj))}))


;; ISchema

(defprotocol ISchema
  (classes [this resource-id])
  (lookups [this resource-id]))

(defn- resource [metadata resource-id]
  (->> metadata values :system :resources
       (some #(when (= resource-id (:id %)) %))))

(extend-type Metadata
  ISchema
  (classes [obj resource-id]
    (:classes (resource obj resource-id)))

  (lookups [obj resource-id]
    (:lookups (resource obj resource-id))))

(defrecord MinimalSchema [lookups' classes']
  ISchema
  (classes [_ _]
    classes')

  (lookups [_ _]
    lookups'))

(defn metadata->minimal-schema [metadata resource-id]
  (MinimalSchema.
    (lookups metadata resource-id)
    (classes metadata resource-id)))

;; helpers

(defn fields [schema resource-id, class-id]
  (->> (classes schema resource-id)
       (some #(when (= class-id (:id %)) %))
       :tables))

(defn field [schema resource-id, class-id field-id]
  (->> (fields schema resource-id class-id)
       (some #(when (= field-id (:id %)) %))))

(defn lookup [schema resource-id lookup-name]
  (->> (lookups schema resource-id)
       (some #(when (= (:id %) lookup-name) %))))

(defn resolve-lookup [schema resource-id class-id field-id lookup-val]
  (->> (field schema resource-id class-id field-id)
       :lookup-name
       (lookup schema resource-id)
       :lookup-types
       (some #(when (= (:id %) lookup-val) %))
       :long-value))

