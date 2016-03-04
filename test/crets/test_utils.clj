(ns crets.test-utils
  (:require [clojure.java.io :as io]
            [clojure.string :as str]
            [crets.extensions :refer [IValues]])
  (:import [org.realtors.rets.common.metadata Metadata]
           [org.realtors.rets.client RetsUtil GetMetadataResponse SearchResultHandler SearchResultImpl]))

(defn mock-metadata [path compact?]
  (with-open [stream (-> path io/resource io/input-stream)]
    (-> stream
        (GetMetadataResponse. compact? false)
        .getMetadata
        first
        Metadata.)))

(defn mock-search-result [path]
  (let [res (SearchResultImpl.)
        xml (-> path
                io/resource
                slurp
                java.io.StringReader.
                org.xml.sax.InputSource.)]
    (doto (SearchResultHandler. res)
      (.parse xml))
    res))

(def default-metadata (mock-metadata "test/metadata.xml" false))
(def compact-metadata (mock-metadata "test/compact-metadata.xml" true))
(def search-result (mock-search-result "test/compact-search.xml"))

(defn rets-search-request-parameters
 "a vector of key-value vectors representing the request params"
 [req]
 (let [pairs (-> req .getHttpParameters (str/split #"&"))]
   (->> pairs
        (map #(str/split % #"="))
        (map (fn [[k v]] [k (RetsUtil/urlDecode v)])))))

(defprotocol IMockSearchResult
  (getCount [this]))

(defprotocol IMockSession
  (login [this user-id pass])
  (getSessionId [this])
  (getMetadata [this])
  (search [this req]))

(defrecord MockSession [state]
  IMockSession
  (login [_ _ _]
    (swap! state assoc :session-id "mock-session-id"))

  (getSessionId [_]
    (:session-id @state))

  (getMetadata [_]
    compact-metadata)

  (search [_ req]
    ;; to support limit & offset, go fishing in the rets SearchRequest
    ;; then reify IValues with the respectively limited :rows of the sample result
    (let [params (->> req rets-search-request-parameters (into {}))
          {:keys [limit offset] :or {limit Integer/MAX_VALUE offset 0}}
          (->> (select-keys params ["Limit" "Offset"])
                                (map (fn [[k v]] [(-> k str/lower-case keyword)
                                                  (Integer/parseInt v)]))
                                (into {}))]
      (reify
        IMockSearchResult
        (getCount [_]
          (.getCount search-result))
        IValues
        (values [_]
          {:column-names (.getColumns search-result)
           :total-count (.getCount search-result)
           :rows (->> (.iterator search-result)
                      iterator-seq
                      (drop offset)
                      (take limit))})))))

(defn mock-session []
  (MockSession. (atom {})))


