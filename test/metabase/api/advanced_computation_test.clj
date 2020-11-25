(ns metabase.api.advanced-computation-test
  "Unit tests for /api/advanced_computation endpoints."
  (:require [clojure.test :refer :all]
            [metabase
             [query-processor-test :as qp.test]
             [test :as mt]]
            [metabase.test
             [fixtures :as fixtures]
             [util :as tu]]))

(use-fixtures :once (fixtures/initialize :db))

(def ^:private query-defaults
  {:middleware {:add-default-userland-constraints? true
                :js-int-to-string?                 true}})

(deftest pivot-dataset-test
  (mt/dataset sample-dataset
    (testing "POST /api/advanced_computation/pivot/dataset"
      (testing "Run a pivot table"
        (let [result  ((mt/user->client :rasta) :post 200 "advanced_computation/pivot/dataset"
                                                (mt/mbql-query orders
                                                               {:aggregation [[:count] [:sum $orders.quantity]]
                                                                :breakout    [[:fk-> $orders.user_id $people.state]
                                                                              [:fk-> $orders.user_id $people.source]
                                                                              [:fk-> $orders.product_id $products.category]]}))]
          (println (clojure.pprint/pprint result))
          ;; This resultset is going to be entirely too large to write a full equality assertion for.
          (is (= 4 (count result)))

          (let [entry (nth result 0)]
            (is (= [{:description     nil
                     :table_id        (mt/id :people)
                     :special_type    "type/State"
                     :name            "STATE"
                     :settings        nil
                     :source          "breakout"
                     :fk_field_id     (mt/id :orders :user_id)
                     :field_ref       ["fk->" ["field-id" (mt/id :orders :user_id)] ["field-id" (mt/id :people :state)]]
                     :parent_id       nil
                     :id              (mt/id :people :state)
                     :visibility_type "normal"
                     :display_name    "User → State"
                     :fingerprint     {:global {:distinct-count 49
                                                :nil%           0.0}
                                       :type   #:type{:Text {:percent-json   0.0
                                                             :percent-url    0.0
                                                             :percent-email  0.0
                                                             :percent-state  1.0
                                                             :average-length 2.0}}}
                     :base_type       "type/Text"}
                    {:base_type    "type/BigInteger"
                     :special_type "type/Number"
                     :name         "count"
                     :display_name "Count"
                     :source       "aggregation"
                     :field_ref    ["aggregation" 0]}
                    {:base_type    "type/BigInteger"
                     :special_type "type/Quantity"
                     :settings     nil
                     :name         "sum"
                     :display_name "Sum of Quantity"
                     :source       "aggregation"
                     :field_ref    ["aggregation" 1]}] (:cols (:data entry))))
            (is (= 48 (count (:rows (:data entry))))))

          (let [entry (nth result 1)]
            (is (= [{:description     nil
                     :table_id        (mt/id :people)
                     :special_type    "type/State"
                     :name            "STATE"
                     :settings        nil
                     :source          "breakout"
                     :fk_field_id     (mt/id :orders :user_id)
                     :field_ref       ["fk->" ["field-id" (mt/id :orders :user_id)] ["field-id" (mt/id :people :state)]]
                     :parent_id       nil
                     :id              (mt/id :people :state)
                     :visibility_type "normal"
                     :display_name    "User → State"
                     :fingerprint     {:global {:distinct-count 49
                                                :nil%           0.0}
                                       :type   #:type{:Text {:percent-json   0.0
                                                             :percent-url    0.0
                                                             :percent-email  0.0
                                                             :percent-state  1.0
                                                             :average-length 2.0}}}
                     :base_type       "type/Text"}
                    {:description     nil
                     :table_id        (mt/id :people)
                     :special_type    "type/Source"
                     :name            "SOURCE"
                     :settings        nil
                     :source          "breakout"
                     :fk_field_id     (mt/id :orders :user_id)
                     :field_ref       ["fk->" ["field-id" (mt/id :orders :user_id)] ["field-id" (mt/id :people :source)]]
                     :parent_id       nil
                     :id              (mt/id :people :source)
                     :visibility_type "normal"
                     :display_name    "User → Source"
                     :fingerprint     {:global {:distinct-count 5
                                                :nil%           0.0}
                                       :type   #:type{:Text {:percent-json   0.0
                                                             :percent-url    0.0
                                                             :percent-email  0.0
                                                             :percent-state  0.0
                                                             :average-length 7.4084}}}
                     :base_type       "type/Text"}
                    {:base_type    "type/BigInteger"
                     :special_type "type/Number"
                     :name         "count"
                     :display_name "Count"
                     :source       "aggregation"
                     :field_ref    ["aggregation" 0]}
                    {:base_type    "type/BigInteger"
                     :special_type "type/Quantity"
                     :settings     nil
                     :name         "sum"
                     :display_name "Sum of Quantity"
                     :source       "aggregation"
                     :field_ref    ["aggregation" 1]}] (:cols (:data entry))))
            (is (= 226 (count (:rows (:data entry))))))

          (let [entry (nth result 2)]
            (is (= [{:description     nil
                     :table_id        (mt/id :people)
                     :special_type    "type/Source"
                     :name            "SOURCE"
                     :settings        nil
                     :source          "breakout"
                     :fk_field_id     (mt/id :orders :user_id)
                     :field_ref       ["fk->" ["field-id" (mt/id :orders :user_id)] ["field-id" (mt/id :people :source)]]
                     :parent_id       nil
                     :id              (mt/id :people :source)
                     :visibility_type "normal"
                     :display_name    "User → Source"
                     :fingerprint     {:global {:distinct-count 5
                                                :nil%           0.0}
                                       :type   #:type{:Text {:percent-json   0.0
                                                             :percent-url    0.0
                                                             :percent-email  0.0
                                                             :percent-state  0.0
                                                             :average-length 7.4084}}}
                     :base_type       "type/Text"}
                    {:description     nil
                     :table_id        (mt/id :products)
                     :special_type    "type/Category"
                     :name            "CATEGORY"
                     :settings        nil
                     :source          "breakout"
                     :fk_field_id     (mt/id :orders :product_id)
                     :field_ref       ["fk->" ["field-id" (mt/id :orders :product_id)] ["field-id" (mt/id :products :category)]]
                     :parent_id       nil
                     :id              (mt/id :products :category)
                     :visibility_type "normal"
                     :display_name    "Product → Category"
                     :fingerprint     {:global {:distinct-count 4
                                                :nil%           0.0}
                                       :type   #:type{:Text {:percent-json   0.0
                                                             :percent-url    0.0
                                                             :percent-email  0.0
                                                             :percent-state  0.0
                                                             :average-length 6.375}}}
                     :base_type       "type/Text"}
                    {:base_type    "type/BigInteger"
                     :special_type "type/Number"
                     :name         "count"
                     :display_name "Count"
                     :source       "aggregation"
                     :field_ref    ["aggregation" 0]}
                    {:base_type    "type/BigInteger"
                     :special_type "type/Quantity"
                     :settings     nil
                     :name         "sum"
                     :display_name "Sum of Quantity"
                     :source       "aggregation"
                     :field_ref    ["aggregation" 1]}] (:cols (:data entry))))
            (is (= 20 (count (:rows (:data entry))))))

          (let [entry (nth result 3)]
            (is (= [{:base_type    "type/BigInteger"
                     :special_type "type/Number"
                     :name         "count"
                     :display_name "Count"
                     :source       "aggregation"
                     :field_ref    ["aggregation" 0]}
                    {:base_type    "type/BigInteger"
                     :special_type "type/Quantity"
                     :settings     nil
                     :name         "sum"
                     :display_name "Sum of Quantity"
                     :source       "aggregation"
                     :field_ref    ["aggregation" 1]}] (:cols (:data entry))))
            (is (= 1 (count (:rows (:data entry)))))
            (is (= [[18760 69540]] (:rows (:data entry))))))))))
