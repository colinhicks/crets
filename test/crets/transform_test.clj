(ns crets.transform-test
  (:require [clojure.string :as str]
            [clojure.test :refer :all]
            [crets.test-utils :as utils]
            [crets.transform :as sut]
            [crets.type-extensions :as ext]))

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

