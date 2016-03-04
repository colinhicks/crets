(ns crets.client-test
  (:require [clojure.core.async :as async]
            [clojure.test :refer :all]
            [crets.client :as sut]
            [crets.extensions :refer [IValues]]
            [crets.test-utils :as utils]))

(deftest authorizer-ensures-auth
  (is (not (sut/authenticated? (utils/mock-session))))
  
  (is (sut/authenticated?
       ((sut/authorizer "username" "password") (utils/mock-session)))))


(deftest search-request-specification
  (is (= {:resource-id "Property" :class-id "Residential" :query "(Status=1)" :limit 100}
         (sut/search-spec "Property" "Residential" "(Status=1)" :limit 100)))

  (is (-> (sut/search-spec "Property" "Residential" "(Status=1)" :count-only? true)
           sut/search-spec->request
           utils/rets-search-request-parameters
           set
           (every? #{["SearchType" "Property"]
                     ["Class" "Residential"]
                     ["Query" "(Status=1)"]
                     ["Count" "2"]})))

    (is (-> (sut/search-spec "Property" "Residential" "(Status=1)" :include-count? true)
           sut/search-spec->request
           utils/rets-search-request-parameters
           set
           (every? #{["SearchType" "Property"]
                     ["Class" "Residential"]
                     ["Query" "(Status=1)"]
                     ["Count" "1"]})))
  
  (is (-> (sut/search-spec "Property" "Residential" "(Status=1)"
                            :limit 100 :offset 100)
           sut/search-spec->request
           utils/rets-search-request-parameters
           set
           (every? #{["SearchType" "Property"]
                     ["Class" "Residential"]
                     ["Query" "(Status=1)"]
                     ["Limit" "100"]
                     ["Offset" "100"]}))))


(deftest mock-session-search-behavior
  (is (= 1 (-> (utils/mock-session)
               (sut/fetch-search {:limit 1})
               .values
               :rows
               count)))

  (is (not= (-> (utils/mock-session) (sut/fetch-search {:limit 1}) .values :rows first)
            (-> (utils/mock-session) (sut/fetch-search {:limit 1 :offset 1}) .values :rows first))))

(deftest batch-search
  (is (satisfies? IValues
                  (let [out (async/chan)]
                    (-> (utils/mock-session)
                        (sut/batch-search-async {} out 100 1))
                    (async/<!! out)))
      "output channel should receive SearchResult (IValues) vals")

  (is (= 10
         (let [out (async/chan)
               batches (do (-> (utils/mock-session)
                               (sut/batch-search-async {:limit 50000} out 10 1))
                           (async/<!! (async/into [] out)))]
           (count batches))))

  (is (= 5
         (let [out (async/chan)
               batches (do (-> (utils/mock-session)
                               (sut/batch-search-async {:limit 50} out 10 1))
                           (async/<!! (async/into [] out)))]
           (count batches)))
      "Batch searches should return a value on the out chan per run, until either 
       1) the spec's limit is met, or 2) the server has no more results for the query")

  (is (instance? Exception
                 (let [out (async/chan)]
                    (-> (utils/mock-session :throws-on '(search))
                        (sut/batch-search-async {} out 100 1))
                    (async/<!! out)))
      "Exceptions should be received on the out chan"))
