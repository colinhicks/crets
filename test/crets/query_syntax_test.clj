(ns crets.query-syntax-test
  (:require [clojure.test :refer :all]
            [crets.query-syntax :as sut]))

(deftest query-basic
  (is (= [:search-condition [:field-criteria [:field "Foo"] [:number "1"]]]
         (sut/parser "(Foo=1)")))

  (is (= [:search-condition [:field-criteria [:field "Foo"] [:string-literal "bar"]]]
         (sut/parser "(Foo=bar)")))

  (is (= [:search-condition [:field-criteria [:field "Foo"]
                             [:string-list [:string-eq "bar"] [:string-eq "baz"]]]]
         (sut/parser "(Foo=bar,baz)"))))

(deftest query-lookup
  (is (= [:search-condition [:field-criteria [:field "Foo"] [:lookup-or "1" "2"]]]
         (sut/parser "(Foo=|1,2)")))

  (is (= [:search-condition [:field-criteria
                             [:field "Foo"] [:lookup-and "1" "2"]]]
         (sut/parser "(Foo=+1,2)")))

  (is (= [:search-condition [:field-criteria [:field "Foo"] [:lookup-not "1" "2"]]]
         (sut/parser "(Foo=~1,2)"))))

(deftest query-partial-string
  (is (= [:search-condition [:field-criteria
                             [:field "Foo"]
                             [:string-list [:string-start "b"]]]]
         (sut/parser "(Foo=b*)")))

  (is (= [:search-condition [:field-criteria
                             [:field "Foo"]
                             [:string-list [:string-char "b" "?" "z"]]]]
         (sut/parser "(Foo=b?z)"))))


(deftest query-dates
  (is (= [:search-condition [:field-criteria
                             [:field "Date"]
                             [:period [:full-date
                                       [:date-fullyear "2013"]
                                       [:date-month "11"]
                                       [:date-mday "02"]]]]]
         (sut/parser "(Date=2013-11-02)")))

  (is (= [:search-condition [:field-criteria
                             [:field "Date"]
                             [:period [:date-time
                                       [:full-date
                                        [:date-fullyear "2013"]
                                        [:date-month "11"]
                                        [:date-mday "02"]]
                                       [:full-time
                                        [:time-hour "00"]
                                        [:time-minute "00"]
                                        [:time-second "00"]
                                        [:time-offset "Z"]]]]]]
         (sut/parser "(Date=2013-11-02T00:00:00Z)")))

  (is (= [:search-condition [:field-criteria
                             [:field "Date"]
                             [:range-list [:range [:greater
                                                   [:period [:date-time
                                                             [:full-date
                                                              [:date-fullyear "2013"]
                                                              [:date-month "11"]
                                                              [:date-mday "02"]]
                                                             [:full-time
                                                              [:time-hour "00"]
                                                              [:time-minute "00"]
                                                              [:time-second "00"]
                                                              [:time-offset "Z"]]]]]]]]]
         (sut/parser "(Date=2013-11-02T00:00:00Z+)"))))

(deftest query-date-range-between

  (is (= [:search-condition [:field-criteria
                             [:field "Date"]
                             [:range-list [:range [:between
                                                   [:period [:date-time
                                                             [:full-date
                                                              [:date-fullyear "2013"]
                                                              [:date-month "11"]
                                                              [:date-mday "02"]]
                                                             [:full-time
                                                              [:time-hour "00"]
                                                              [:time-minute "00"]
                                                              [:time-second "00"]
                                                              [:time-offset "Z"]]]]
                                                   [:period [:date-time
                                                             [:full-date
                                                              [:date-fullyear "2013"]
                                                              [:date-month "11"]
                                                              [:date-mday "03"]]
                                                             [:full-time
                                                              [:time-hour "00"]
                                                              [:time-minute "00"]
                                                              [:time-second "00"]
                                                              [:time-offset "Z"]]]]]]]]]
         (sut/parser "(Date=2013-11-02T00:00:00Z-2013-11-03T00:00:00Z)"))))

(deftest query-logical
  (is (= [:search-condition
          [:field-criteria [:field "Foo"] [:lookup-not "1"]]
          [:and]
          [:field-criteria [:field "Bar"] [:string-literal "baz"]]]
         (sut/parser "(Foo=~1) AND (Bar=baz)")))

  (is (= [:search-condition
          [:field-criteria [:field "Foo"] [:number "1"]]
          [:or]
          [:field-criteria [:field "Bar"] [:number "1"]]]
         (sut/parser "(Foo=1)|(Bar=1)"))))

(deftest query-nesting
  (is (= [:search-condition
          [:field-criteria [:field "Foo"] [:number "1"]]
          [:and]
          [:search-condition
           [:field-criteria [:field "Bar"] [:number "1"]]
           [:or]
           [:field-criteria [:field "Baz"] [:number "1"]]]]
         (sut/parser "(Foo=1),((Bar=1)|(Baz=1))"))))

(deftest required-criteria
  (is (= '({:field "Foo" :args ([:number "1"])}
           {:field "Bar" :args ([:number "2"])}
           {:field "Baz" :args ([:number "3"])})
         (sut/required-criteria "(Foo=1),((Bar=2)|(Baz=3))"))))

(deftest required-fields
  (is (= '("Foo" "Bar" "Baz")
         (sut/required-fields "(Foo=|0,1),((Bar=2)|(Baz=3))"))))



