(ns crets.type-extensions-test
  (:require [clojure.test :refer :all]
            [crets.test-mocks :as mocks]
            [crets.protocols :as p]
            [crets.type-extensions :as sut]))

(deftest rets-metadata-extended-with-protocols
  (is (satisfies? p/Interop
                  mocks/default-metadata))
  (is (satisfies? p/ICommonMetadata
                  mocks/default-metadata)))

(deftest classes-protocol-fn
  (is (= "Residential"
         (-> mocks/default-metadata
             (p/classes "Property")
             first
             :id))))

(deftest lookups-protocol-fn
  (is (= "AR"
         (-> mocks/compact-metadata
             (p/lookups "Property")
             first
             :id))))

(deftest field-helper-returns-values-for-single-class-field
  (is (sut/field mocks/default-metadata "Property" "Residential" "ListingID")))

(deftest resolve-lookup-helper
  (is (= "Batavia 300"
         (sut/resolve-lookup mocks/compact-metadata "Property" "RES" "H_SCHOOL" "300"))))

(deftest metadata-to-minimal-schema
  (is (satisfies? p/ICommonMetadata
                  (sut/metadata->resource-metadata mocks/default-metadata "Property"))))

(deftest rets-search-result-extended-with-protocol
  (is (satisfies? p/Interop
                  mocks/search-result)))

(deftest search-result-values 
 (is (= #{"BROKER", "LP", "STNAME", "STATUS", "SQFT", "VEW", "LN", "AGENT_ID", "URL",
          "IF", "LTYP", "AR", "ZIP_CODE", "LD", "E_SCHOOL", "M_SCHOOL", "H_SCHOOL", "EF"}
         (-> mocks/search-result
             p/->clj
             :column-names
             set)))

  (is (seq?
       (:rows (p/->clj mocks/search-result)))))

