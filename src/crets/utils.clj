(ns crets.utils
  (:require [clojure.java.io :as io]
            [clojure.string :as str])
  (:import [org.realtors.rets.client GetMetadataResponse SearchResultHandler SearchResultImpl RetsUtil]
           org.realtors.rets.common.metadata.Metadata))

(defn rets-search-request-parameters
 "a vector of key-value vectors representing the request params"
 [req]
 (let [pairs (-> req .getHttpParameters (str/split #"&"))]
   (->> pairs
        (map #(str/split % #"="))
        (map (fn [[k v]] [k (RetsUtil/urlDecode v)])))))

(defn resource->metadata [path compact?]
  (with-open [stream (-> path io/resource io/input-stream)]
    (-> stream
        (GetMetadataResponse. compact? false)
        .getMetadata
        first
        Metadata.)))

(defn resource->search-result [path]
  (let [res (SearchResultImpl.)
        xml (-> path
                io/resource
                slurp
                java.io.StringReader.
                org.xml.sax.InputSource.)]
    (doto (SearchResultHandler. res)
      (.parse xml))
    res))
