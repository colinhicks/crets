(ns crets.client
  (:import [org.realtors.rets.client
            CommonsHttpClient
            RetsSession
            RetsVersion
            SearchRequest
            SearchResult])
  (:require [clojure.string :as str]))

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
                                    count-only?
                                    limit
                                    offset
                                    fields]}]
  (let [req (SearchRequest. resource-id class-id query)]
    (if count-only?
      (.setCountOnly req)
      (do (.setCountFirst req)
          (when limit (.setLimit req limit))
          (when offset (.setOffset req limit))
          (when fields (.setSelect req (str/join "," fields)))))
    req))

(defn fetch-search [session search-spec]
  (->> search-spec
       search-spec->request
       (.search session)))

(defn search-spec
  [resource-id class-id query & {:as options
                                 :keys [count-only? limit offset fields]
                                 :or {count-only? false limit nil offset nil fields nil}}]
  (merge options {:resource-id resource-id
                  :class-id class-id
                  :query query}))




