(ns crets.gen-schema-test
  (:require [clojure.test :refer :all]
            [crets
             [gen-schema :as sut]
             [type-extensions :as ext]]
            [schema.core :as s]
            [shrubbery.core :refer [stub]]))

(def metadata-stub (stub ext/ICommonMetadata {:classes [{:id "Foo"
                                                         :tables [{:id "Bar_Baz"
                                                                   :data-type "Date"
                                                                   :lookup? false
                                                                   :lookup-multiple? false}
                                                                  {:id "Buz_Quux"
                                                                   :data-type "Int"
                                                                   :lookup? false
                                                                   :lookup-multiple? false}]}]}))

(deftest test-stype
  (is (= 's/Str (sut/stype {:data-type "Char"})))
  (is (= 's/Int (sut/stype {:data-type "Int"})))
  (is (= 's/Str (sut/stype {:data-type "Int" :lookup? true})))
  (is (= ['s/Str] (sut/stype {:data-type "Int" :lookup? true :lookup-multiple? true}))))

(deftest test-schema
  (is (= {:bar-baz 's/Inst :buz-quux 's/Int}
         (sut/schema metadata-stub '... "Foo"))))

(deftest test-record-forms
  (let [sch {:bar-baz 's/Inst :buz-quux 's/Int}]
    (is (= '(s/defrecord ASchemaRecord [bar-baz :- s/Inst buz-quux :- s/Int])
           (sut/schema-record-form 'ASchemaRecord sch)))
    (is (= '(defrecord ARecord [bar-baz buz-quux])
           (sut/record-form 'ARecord sch)))))

(deftest test-nested-record-forms
  (let [sch {:bar-baz {:bar 's/Str :baz 's/Num}}]
    (is (= '(s/defrecord ASchemaRecord [bar-baz :- {:bar s/Str :baz s/Num}])
           (sut/schema-record-form 'ASchemaRecord sch)))
    (is (= '(defrecord ARecord [bar-baz])
           (sut/record-form 'ARecord sch)))))

