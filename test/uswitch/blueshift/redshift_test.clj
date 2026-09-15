(ns uswitch.blueshift.redshift-test
  (:require [clojure.test :refer (deftest testing is are)]
            [clojure.string :as s]
            [uswitch.blueshift.redshift :as redshift]
            [uswitch.blueshift.s3 :as s3]))

(def ^:private target "tradeswell_data_warehouse_dev.tw_marketing_partner_campaign_day")
(def ^:private staging "tw_marketing_partner_campaign_day_staging")

(deftest delete-null-marketplace-query-is-gated-on-the-source-list
  (testing "no armed source produces no delete at all"
    (are [sources] (nil? (redshift/delete-null-marketplace-query target staging sources))
      nil
      []
      '()
      #{}))

  (testing "an absent manifest key reaches the query as nil, so it is also no delete"
    (let [manifest (s3/map->Manifest {:table "tw_marketing_partner_campaign_day"})]
      (is (contains? manifest :delete-null-marketplace-data-sources))
      (is (nil? (:delete-null-marketplace-data-sources manifest)))
      (is (nil? (redshift/delete-null-marketplace-query
                 target staging (:delete-null-marketplace-data-sources manifest))))))

  (testing "one armed source filters to that source"
    (let [query (redshift/delete-null-marketplace-query target staging ["facebook_ads"])]
      (is (s/includes? query (str "and " target ".partner_marketplace_id is null")))
      (is (s/includes? query (str "and " target ".data_source in ('facebook_ads')")))))

  (testing "several armed sources are quoted individually"
    (is (s/includes? (redshift/delete-null-marketplace-query target staging ["facebook_ads" "google_ads"])
                     (str "and " target ".data_source in ('facebook_ads', 'google_ads')")))))

(deftest delete-null-marketplace-query-joins-staging-on-the-partition-columns
  (let [query (redshift/delete-null-marketplace-query target staging ["facebook_ads"])]
    (is (s/starts-with? query (format "delete from %s using " target)))
    (is (s/includes? query (format "(select report_date, data_source, data_type, partner_company_id from %s group by 1,2,3,4) staging" staging)))
    (are [column] (s/includes? query (str "and " target "." column " = staging." column))
      "data_source"
      "data_type"
      "partner_company_id")
    (is (s/includes? query (str "where " target ".report_date = staging.report_date")))))

(deftest delete-null-marketplace-customer-query-keys-on-partner-order-id
  (testing "the customer variant carries the same gate"
    (are [sources] (nil? (redshift/delete-null-marketplace-customer-query target staging sources))
      nil
      []))

  (let [query (redshift/delete-null-marketplace-customer-query target staging ["facebook_ads"])]
    (testing "tw_retail_partner_customer_orders has no report_date"
      (is (not (s/includes? query "report_date"))))
    (is (s/includes? query (format "(select partner_order_id, data_source, data_type, partner_company_id from %s group by 1,2,3,4) staging" staging)))
    (is (s/includes? query (str "where " target ".partner_order_id = staging.partner_order_id")))
    (is (s/includes? query (str "and " target ".partner_marketplace_id is null")))
    (is (s/includes? query (str "and " target ".data_source in ('facebook_ads')")))))

(deftest manifests-validate-with-and-without-the-new-key
  (let [base {:table          "tw_marketing_partner_campaign_day"
              :schema         "tradeswell_data_warehouse_dev"
              :pk-columns     ["partner_company_id"]
              :pk-nulls       ["partner_marketplace_id"]
              :columns        ["partner_company_id"]
              :full-columns   ["partner_company_id"]
              :jdbc-url       "jdbc:postgresql://localhost:5439/tradeswell"
              :username       "tradeswell"
              :password       ""
              :add-status     nil
              :strategy       "delete-null-marketplace-merge"
              :options        []
              :staging-select nil
              :data-pattern   #".*\.gz"}]
    (testing "the key is optional"
      (is (nil? (s3/validate (s3/map->Manifest base)))))
    (testing "and an armed list is accepted"
      (is (nil? (s3/validate (s3/map->Manifest (assoc base :delete-null-marketplace-data-sources ["facebook_ads"]))))))))

(deftest load-table-dispatches-the-new-strategies
  (with-redefs [redshift/delete-null-marketplace-merge-table (fn [_ _] :merge-table)
                redshift/delete-null-marketplace-merge-customer-table (fn [_ _] :customer-table)]
    (are [strategy expected] (= expected (redshift/load-table "s3://bucket/x.manifest"
                                                              {:table "t" :strategy strategy}))
      "delete-null-marketplace-merge"          :merge-table
      "delete-null-marketplace-merge-customer" :customer-table)))

(def ^:private stub-connection
  (reify java.sql.Connection
    (commit [_] nil)
    (isClosed [_] true)))

;; Shaped like partner_campaign_psus: a mix of plain and :pk-nulls key columns,
;; and a data_source column so the source list can filter.
(def ^:private pk-metadata
  {"marketing_partner_id" "character varying(256)"
   "retail_partner_company_id" "character varying(256)"
   "currency_code" "character(3)"
   "presence_index" "integer"
   "collection_timestamp" "timestamp without time zone"
   "data_source" "character varying(64)"
   "partner_marketplace_id" "character varying(256)"})

(def ^:private pk-columns
  ["marketing_partner_id" "retail_partner_company_id" "currency_code"
   "data_source" "partner_marketplace_id"])

(def ^:private pk-nulls ["retail_partner_company_id" "currency_code" "partner_marketplace_id"])

(defn- statements-for
  "Runs table-fn with every statement builder stubbed and returns the statement
   list execute was handed."
  [table-fn table-manifest]
  (let [executed (atom nil)]
    (with-redefs [redshift/connection                (fn [_ _ _] stub-connection)
                  redshift/get-table-metadata        (fn [_ _ _ _ _] pk-metadata)
                  redshift/prepare-statement         (fn ([sql] sql) ([sql _] sql))
                  redshift/create-staging-table-stmt (fn [_ _] :create-staging)
                  redshift/copy-from-s3-stmt         (fn [_ _ _] :copy)
                  redshift/create-row-nums-table-stmt (fn [_ _] :create-row-nums)
                  redshift/delete-from-row-nums-stmt (fn [_ _] :delete-row-nums)
                  redshift/drop-row-nums-column-stmt (fn [_] :drop-row-num-column)
                  redshift/merge-from-staging-stmt   (fn [_ _ _ _ _ _] :merge)
                  redshift/drop-table-stmt           (fn [table] [:drop table])
                  redshift/execute                   (fn [_ & statements] (reset! executed (vec statements)))]
      (table-fn "s3://bucket/x.manifest" table-manifest))
    @executed))

(deftest an-unarmed-load-still-runs-every-other-statement
  ;; execute stops at the first nil statement, so dropping the remove nil? would
  ;; silently skip the merge and both drops on every unarmed table.
  (let [without-delete [:create-staging :copy :create-row-nums :delete-row-nums
                        :drop-row-num-column :merge [:drop "t_staging"] [:drop "t_rnums"]]]
    (testing "no armed source: the delete is absent and nothing after it is lost"
      (are [table-fn] (= without-delete (statements-for table-fn {:table "t" :pk-nulls []}))
        redshift/delete-null-marketplace-merge-table
        redshift/delete-null-marketplace-merge-customer-table))

    ;; The marker and the index both matter: both builders emit
    ;; "partner_marketplace_id is null", and a delete after the merge would undo it.
    (testing "one armed source: the delete is inserted at index 2, from that fn's own builder"
      (are [table-fn marker]
           (let [statements (statements-for table-fn
                                            {:table "t" :pk-nulls []
                                             :delete-null-marketplace-data-sources ["facebook_ads"]})
                 delete     (nth statements 2)]
             (and (= 9 (count statements))
                  (string? delete)
                  (s/includes? delete "partner_marketplace_id is null")
                  (s/includes? delete marker)
                  (= without-delete (concat (take 2 statements) (drop 3 statements)))))
        redshift/delete-null-marketplace-merge-table          "t.report_date = staging.report_date"
        redshift/delete-null-marketplace-merge-customer-table "t.partner_order_id = staging.partner_order_id"))))

(deftest delete-null-marketplace-pk-query-keys-on-the-manifests-own-columns
  (testing "same gate as the other two variants"
    (are [sources] (nil? (redshift/delete-null-marketplace-pk-query
                          target staging pk-columns pk-nulls pk-metadata sources))
      nil
      []
      '()))

  (let [query (redshift/delete-null-marketplace-pk-query
               target staging pk-columns pk-nulls pk-metadata ["facebook_ads"])]
    (testing "partner_marketplace_id is forced IS NULL on the target and never joined"
      (is (s/includes? query (str "and " target ".partner_marketplace_id is null")))
      (is (not (s/includes? query (str target ".partner_marketplace_id = staging."))))
      (is (not (s/includes? query "COALESCE(staging.partner_marketplace_id"))))

    (testing "a plain key column compares with equality"
      (is (s/includes? query (str target ".marketing_partner_id = staging.marketing_partner_id"))))

    (testing "a :pk-nulls column mirrors the merge's COALESCE on both sides"
      (is (s/includes? query (str "COALESCE(" target ".currency_code, '') = COALESCE(staging.currency_code, '')")))
      (is (s/includes? query (str "COALESCE(" target ".retail_partner_company_id, '') = COALESCE(staging.retail_partner_company_id, '')")))
      (is (not (s/includes? query (str target ".currency_code = staging.currency_code")))))

    (testing "the staging subquery selects the key columns without partner_marketplace_id"
      (is (s/includes? query "(select marketing_partner_id, retail_partner_company_id, currency_code, data_source from")))

    (testing "the source list filters when the table has a data_source column"
      (is (s/includes? query (str "and " target ".data_source in ('facebook_ads')"))))))

(deftest delete-null-marketplace-pk-query-without-a-data-source-column
  ;; digital_shelf_keyword_product_history has no data_source column, so the list
  ;; can only arm the table -- there is no per-source distinction to draw.
  (let [metadata (dissoc pk-metadata "data_source")
        columns  (remove #{"data_source"} pk-columns)
        query    (redshift/delete-null-marketplace-pk-query
                  target staging columns pk-nulls metadata ["facebook_ads"])]
    (is (some? query))
    (is (not (s/includes? query "data_source")))
    (is (s/includes? query (str "and " target ".partner_marketplace_id is null")))))

(deftest load-table-dispatches-the-pk-strategy
  (with-redefs [redshift/delete-null-marketplace-merge-pk-table (fn [_ _] :pk-table)]
    (is (= :pk-table (redshift/load-table "s3://bucket/x.manifest"
                                          {:table "t" :strategy "delete-null-marketplace-merge-pk"})))))

(deftest pk-table-fn-puts-its-delete-at-index-2
  (let [without-delete [:create-staging :copy :create-row-nums :delete-row-nums
                        :drop-row-num-column :merge [:drop "t_staging"] [:drop "t_rnums"]]
        armed (statements-for redshift/delete-null-marketplace-merge-pk-table
                              {:table "t" :pk-columns pk-columns :pk-nulls pk-nulls
                               :delete-null-marketplace-data-sources ["facebook_ads"]})]
    (testing "unarmed emits no delete and loses nothing after it"
      (is (= without-delete (statements-for redshift/delete-null-marketplace-merge-pk-table
                                            {:table "t" :pk-columns pk-columns :pk-nulls pk-nulls}))))
    (testing "armed inserts the pk-keyed delete at index 2, before the merge"
      (is (= 9 (count armed)))
      (is (string? (nth armed 2)))
      (is (s/includes? (nth armed 2) "partner_marketplace_id is null"))
      (is (s/includes? (nth armed 2) "COALESCE(t.currency_code, '')"))
      (is (= without-delete (concat (take 2 armed) (drop 3 armed)))))))
