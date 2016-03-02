(ns crets.core-test
  (:require [clojure.test :refer :all]
            [crets.core :as sut]
            [crets.extensions :as ext]
            [crets.test-utils :as utils]
            [clojure.string :as str]))

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

(deftest search-result-transforming
  (is (= [["BROKER" "Laffalot Realty"] ["LP" "387117"]
          ["STNAME" "Fourth St."] ["STATUS" "A"] ["SQFT" "2026"] ["VEW" "13"] ["LN" "demo.crt.realtors.org-100"]
          ["AGENT_ID" "P345"] ["URL" "http://demo.crt.realtors.org/retriever_po"] ["IF" ""] ["LTYP" "RES"] ["AR" "AUR"]
          ["ZIP_CODE" "60134"] ["LD" "2003-04-02"] ["E_SCHOOL" ""] ["M_SCHOOL" ""] ["H_SCHOOL" ""] ["EF" "AO,BA"]]
         (-> utils/search-result
             sut/search-result->fields
             first))
      "Should convert SearchResult rows into 2-vectors w/ keyword form of the column name.")

  (is (let [first-fields
            (->> utils/search-result
                 (sut/search-result->fields (comp (map (fn [[k v]]
                                                         (condp = k
                                                           "LP" [k (Integer/parseInt v)]
                                                           "SQFT" [k (Integer/parseInt v)]
                                                           [k v])))
                                                  (map (fn [[k v]] [(str/capitalize k) v]))))
                 first
                 set)]
        (every? first-fields [["Lp" 387117] ["Sqft" 2026]]))
      "The optional transducer is passed each kv pair"))

(deftest field-conversion
  (is (= [["LD" #inst "2003-04-02T00:00:00.000-00:00"]
          ["LP" 123456]] 
         (-> (sut/field-converter {:resource-id "Property" :class-id "RES"}
                                     utils/compact-metadata)
             (transduce conj [["LD" "2003-04-02"]
                              ["LP" "123456"]]))))

  (is (= [["H_SCHOOL" "300"]
          ["LP" 123456]]
         (-> (sut/field-converter {:resource-id "Property" :class-id "RES"}
                                     utils/compact-metadata)
             (transduce conj [["H_SCHOOL" "300" (comment :data-type int :lookup? true)]
                              ["LP" "123456"]])))
      "The field converter should ignore lookup fields"))

(deftest lookup-resolution
  (is (= [["STATUS" "Active"]
          ["EF" ["AGENT OWNER" "BURGLAR ALARM"]]
          ["IF" []]]
         (-> (sut/field-lookup-resolver {:resource-id "Property" :class-id "RES"}
                                           utils/compact-metadata)
             (transduce conj [["STATUS" "A"]
                              ["EF" "AO,BA"]
                              ["IF" ""]]))))

  (is (= [["H_SCHOOL" "Batavia 300"]
          ["LP" 123456]]
         (-> (comp (sut/field-lookup-resolver {:resource-id "Property" :class-id "RES"}
                                                 utils/compact-metadata)
                   (sut/field-converter {:resource-id "Property" :class-id "RES"}
                                           utils/compact-metadata))
             (transduce conj [["H_SCHOOL" "300" (comment :data-type int :lookup? true)]
                              ["LP" "123456"]])))
      "The field converter should ignore lookup fields"))

(deftest end-to-end-search-example
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
         (let [spec (sut/search-spec "Property" "RES" "(query_ignored_here=1)")
               {:keys [resource-id class-id]} spec
               schema utils/compact-metadata
               convert-values (sut/field-converter spec schema)
               lookup-values (sut/field-lookup-resolver spec schema)
               use-readable-keys (map (sut/field-key-fn
                                       #(-> (ext/field schema resource-id class-id %)
                                            :long-name
                                            (str/replace #" " "_"))))]
             (->> spec
                  (sut/fetch-search (utils/mock-session))
                  (sut/search-result->fields (comp convert-values
                                                   lookup-values
                                                   use-readable-keys
                                                   sut/field-keywordize))
                  (map (partial into (sorted-map)))
                  first)))))
