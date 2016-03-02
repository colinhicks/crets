(ns crets.client-test
  (:require [crets.client :as sut]
            [clojure.test :refer :all]
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
  
  (is (-> (sut/search-spec "Property" "Residential" "(Status=1)"
                            :limit 100 :offset 100)
           sut/search-spec->request
           utils/rets-search-request-parameters
           set
           (every? #{["SearchType" "Property"]
                     ["Class" "Residential"]
                     ["Query" "(Status=1)"]
                     ["Limit" "100"]
                     ["Offset" "100"]
                     ["Count" "1"]}))))


(deftest search-results)
