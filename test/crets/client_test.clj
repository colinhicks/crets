(ns crets.client-test
  (:require [clojure.core.async :as async]
            [clojure.string :as str]
            [clojure.test :refer :all]
            [crets.client :as sut]
            [crets.test-utils :as utils]
            [crets.transform :as transform]
            [crets.type-extensions :as ext]))

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

(deftest search-query-validation
  (is (sut/valid-search? {:resource-id "Property"
                          :class-id "Residential"
                          :query "(Status=|1,2,3) AND (ListPrice=0-100000)"}
                         utils/default-metadata)
      "Query fields must exist in the metadata respective to the resource and class ids")

  (is (not (sut/valid-search? {:resource-id "Property"
                               :class-id "Residential"
                               :query "(BahnhofPlatz=1) | (Status=|1,2,3)"}
                              utils/default-metadata))
      "Unknown query fields are invalid")

  (is (= #{"BahnhofPlatz"}
         (:missing-fields (sut/validate-search {:resource-id "Property"
                                                :class-id "Residential"
                                                :query "(BahnhofPlatz=1) | (Status=|1,2,3)"}
                                               utils/default-metadata)))
      "validate-search returns map including :missing-fields set")

  (is (not (sut/valid-search? {:resource-id "Seafloor"
                               :class-id "Residential"
                               :query "(Status=1)"}
                              utils/default-metadata))
      "Unknown resources are invalid")

  (is (not (:valid-resource-id-and-class-id?
            (sut/validate-search {:resource-id "Seafloor"
                                  :class-id "Residential"
                                  :query "(Status=1)"}
                                 utils/default-metadata))))

  (is (not (sut/valid-search? {:resource-id "Property"
                               :class-id "AirbedAndBreakfast"
                               :query "(Status=1)"}
                              utils/default-metadata))
      "Unknown classes are invalid")

  (is (not (sut/valid-search? {:resource-id "Property" :class-id "Residential" :query ""}
                              utils/default-metadata))
      "Blank queries are invalid")

  (is (not (sut/valid-search? {:resource-id "Property" :class-id "Residential" :query "Status=1"}
                              utils/default-metadata))
      "Broken query syntax is invalid"))

(deftest search-with-transform
  (is (= {:brokerage "Laffalot Realty",
          :elementary-school nil,
          :exterior-features ["AGENT OWNER" "BURGLAR ALARM"],
          :high-school nil,
          :interior-features [],
          :listing-agent-id "P345",
          :listing-date #inst "2003-04-02T00:00:00.000-00:00",
          :listing-id "demo.crt.realtors.org-100",
          :listing-price 387117,
          :listing-type "RESIDENTIAL",
          :listing-url "http://demo.crt.realtors.org/retriever_po",
          :location "Aurora",
          :middle-school nil,
          :square-footage "2026",
          :status "Active",
          :street-name "Fourth St.",
          :view nil,
          :zip-code 60134}
         (let [spec {:resource-id "Property" :class-id "RES"}
               {:keys [resource-id class-id]} spec
               schema utils/compact-metadata
               convert-values (transform/field-converter spec schema)
               lookup-values (transform/field-lookup-resolver spec schema)
               use-readable-keys (map (transform/field-key-fn
                                       #(-> (ext/field schema resource-id class-id %)
                                            :long-name
                                            (str/replace #" " "_"))))]
             (->> spec
                  (sut/fetch-search (utils/mock-session))
                  (transform/search-result->fields (comp convert-values
                                                   lookup-values
                                                   use-readable-keys
                                                   transform/field-keywordize))
                  (map (partial into (sorted-map)))
                  first)))))
