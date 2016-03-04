(ns crets.extensions-test
  (:require [clojure.test :refer :all]
            [crets.extensions :as sut]
            [crets.test-utils :refer [default-metadata compact-metadata search-result]]))

(deftest rets-metadata-extended-with-protocols
  (is (satisfies? sut/IValues
                  default-metadata))
  (is (satisfies? sut/ISchema
                  default-metadata)))

(deftest classes-protocol-fn
  (is (= "Residential"
         (-> default-metadata
             (sut/classes "Property")
             first
             :id))))

(deftest lookups-protocol-fn
  (is (= "AR"
         (-> compact-metadata
             (sut/lookups "Property")
             first
             :id))))

(deftest field-helper-returns-values-for-single-class-field
  (is (sut/field default-metadata "Property" "Residential" "ListingID")))

(deftest resolve-lookup-helper
  (is (= "Batavia 300"
         (sut/resolve-lookup compact-metadata "Property" "RES" "H_SCHOOL" "300"))))

(deftest metadata-to-minimal-schema
  (is (satisfies? sut/ISchema
                  (sut/metadata->minimal-schema default-metadata "Property"))))

(deftest rets-search-result-extended-with-protocol
  (is (satisfies? sut/IValues
                  search-result)))

(deftest search-result-values 
 (is (= #{"BROKER", "LP", "STNAME", "STATUS", "SQFT", "VEW", "LN", "AGENT_ID", "URL",
          "IF", "LTYP", "AR", "ZIP_CODE", "LD", "E_SCHOOL", "M_SCHOOL", "H_SCHOOL", "EF"}
         (-> search-result
             sut/values
             :column-names
             set)))

  (is (seq?
       (:rows (sut/values search-result)))))

