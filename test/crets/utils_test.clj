(ns crets.utils-test
  (:require [crets.utils :as sut]
            [clojure.test :refer :all]))

(deftest grouping-fields
  (let [fields {:foo 1 :bar 2 :baz 3}]
    (is (= '([:quux {:foo 1 :bar 2}] [:baz 3])
           (sut/group-fields {:quux #{:foo :bar}} fields)))

    (is (= #{[:quux {:foo 1 :bar 2}] [:norf {:bar 2 :baz 3}]}
           (-> {:quux #{:foo :bar}
                :norf #(when (re-find #"^b.*" (name %)) %)}
               (sut/group-fields fields)
               set)))

    (is (= '([:quux [[:ar 2] [:az 3]]] [:foo 1])
           (sut/group-fields {:quux #(->> %
                                          name
                                          (re-matches #"^b(.*)")
                                          second
                                          keyword)}
                fields
                [])))))

