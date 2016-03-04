(ns crets.client
  (:import [org.realtors.rets.client
            CommonsHttpClient
            RetsSession
            RetsVersion
            SearchRequest
            SearchResult])
  (:require [clojure.string :as str]
            [clojure.core.async :as async]))

;; login

(defn create-session
  ([host] (create-session host RetsVersion/DEFAULT))
  ([host version]
   (doto (RetsSession. host (CommonsHttpClient.) version)
     (.setMethod "POST"))))

(defn authenticated? [session]
  (boolean (.getSessionId session)))

(defn authorizer [user-id pass]
  (fn ensure-auth! [session]
    (if-not (authenticated? session)
      (doto session
        (.login user-id pass))
      session)))


;; metadata

(defn fetch-metadata [session]
  (.getMetadata session))


;; search

(defn search-spec->request [{:keys [resource-id
                                    class-id
                                    query
                                    include-count?
                                    count-only?
                                    limit
                                    offset
                                    fields]}]
  (let [req (SearchRequest. resource-id class-id query)]
    (if count-only?
      (.setCountOnly req)
      (do (when include-count? (.setCountFirst req))
          (when limit (.setLimit req limit))
          (when offset (.setOffset req limit))
          (when fields (.setSelect req (str/join "," fields)))))
    req))

(defn fetch-search [session search-spec]
  (->> search-spec
       search-spec->request
       (.search session)))

(defn batch-search-async [session search-spec out-ch batch-size parallelism]
  (let [fetch-batch (fn [offset batch-ch]
                          (async/go
                            (let [search-spec' (assoc search-spec
                                                      :include-count? false
                                                      :limit batch-size
                                                      :offset offset)]
                              (try
                                (async/put! batch-ch
                                       (fetch-search session search-spec'))
                                (catch Exception e
                                  (async/put! batch-ch e))
                                (finally
                                  (async/close! batch-ch))))))
        fetch-peek (fn []
                      (async/go
                             (try
                               (fetch-search session (assoc search-spec :count-only? true))
                               (catch Exception e
                                 (async/put! out-ch e)))))]
    (async/go
           (let [server-count (.getCount (async/<! (fetch-peek)))
                 limit (min server-count (or (:limit search-spec) Integer/MAX_VALUE))
                 batch-ch (async/chan)]
             (async/onto-chan batch-ch (range 0 limit batch-size))
             (async/pipeline-async parallelism out-ch fetch-batch batch-ch)))))

(defn search-spec
  [resource-id class-id query & {:as options
                                 :keys [include-count? count-only? limit offset fields]}]
  (assoc options
         :resource-id resource-id
         :class-id class-id
         :query query))




