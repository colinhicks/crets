(ns crets.test-utils
  (:require [clojure.java.io :as io]
            [clojure.string :as str])
  (:import org.realtors.rets.common.metadata.Metadata)
  (:import [org.realtors.rets.client RetsUtil GetMetadataResponse SearchResultHandler SearchResultImpl]))

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
    search-result))

(defn mock-session []
  (MockSession. (atom {})))

(defn rets-search-request-parameters
 "a vector of key-value vectors representing the request params"
 [req]
 (let [pairs (-> req .getHttpParameters (str/split #"&"))]
   (->> pairs
        (map #(str/split % #"="))
        (map (fn [[k v]] [k (RetsUtil/urlDecode v)])))))

