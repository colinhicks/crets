(ns crets.test-mocks
  (:require[clojure.string :as str]
            [crets.protocols :as p]
            [crets.utils :as utils]))

(def default-metadata (utils/resource->metadata "test/metadata.xml" false))
(def compact-metadata (utils/resource->metadata "test/compact-metadata.xml" true))
(def search-result (utils/resource->search-result "test/compact-search.xml"))

(defprotocol IMockSearchResult
  (getCount [this]))

(defprotocol IMockSession
  (login [this user-id pass])
  (getSessionId [this])
  (getMetadata [this])
  (search [this req]))

(defrecord MockSession [state throws-on]
  IMockSession
  (login [_ _ _]
    (when (some #{'login} throws-on)
      (throw (Exception. "Mock exception: login")))
    (swap! state assoc :session-id "mock-session-id"))

  (getSessionId [_]
    (:session-id @state))

  (getMetadata [_]
    compact-metadata)

  (search [_ req]
    (when (some #{'search} throws-on)
      (throw (Exception. "Mock exception: search")))
    ;; to support limit & offset, go fishing in the rets SearchRequest
    ;; then reify Interop with the respectively limited :rows of the sample result
    (let [params (->> req utils/rets-search-request-parameters (into {}))
          {:keys [limit offset] :or {limit Integer/MAX_VALUE offset 0}}
          (->> (select-keys params ["Limit" "Offset"])
                                (map (fn [[k v]] [(-> k str/lower-case keyword)
                                                  (Integer/parseInt v)]))
                                (into {}))]
      (reify
        IMockSearchResult
        (getCount [_]
          (when (some #{'getCount} throws-on)
            (throw (Exception. "Mock exception: getCount")))
          (.getCount search-result))
        p/Interop
        (->clj [_]
          (when (some #{'values} throws-on)
            (throw (Exception. "Mock exception: values")))
          {:column-names (.getColumns search-result)
           :total-count (.getCount search-result)
           :rows (->> (.iterator search-result)
                      iterator-seq
                      (drop offset)
                      (take limit))})))))

(defn mock-session [& {:keys [throws-on] :or {throws-on '()}}]
  (MockSession. (atom {}) throws-on))


