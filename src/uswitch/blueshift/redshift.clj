(ns uswitch.blueshift.redshift
  (:require [amazonica.aws.s3 :refer (put-object)]
            [cheshire.core :as json]
            [clojure.tools.logging :refer (info error debug)]
            [clojure.string :as s]
            [clojure.core.async :refer (chan <!! >!! close! thread timeout alts!!)]
            [metrics.meters :refer (mark! meter)]
            [metrics.counters :refer (inc! dec! counter)]
            [uswitch.blueshift.util :as util])
  (:import [java.util UUID]
           [java.io ByteArrayInputStream]
           [com.amazonaws.auth DefaultAWSCredentialsProviderChain]
           [java.sql DriverManager SQLException]))

(defn manifest [bucket files]
  {:entries (for [f files] {:url (str "s3://" bucket "/" f)
                            :mandatory true})})

(defn put-manifest
  "Uploads the manifest to S3 as JSON, returns the URL to the uploaded object.
   Manifest should be generated with uswitch.blueshift.redshift/manifest."
  [bucket manifest]
  (let [file-name      (str (UUID/randomUUID) ".manifest")
        s3-url         (str "s3://" bucket "/" file-name)
        manifest-bytes (.getBytes (json/generate-string manifest))]
    (put-object :bucket-name bucket :key file-name :input-stream (ByteArrayInputStream. manifest-bytes))
    {:key file-name
     :url s3-url}))

(def redshift-imports (meter [(str *ns*) "redshift-imports" "imports"]))
(def redshift-import-rollbacks (meter [(str *ns*) "redshift-imports" "rollbacks"]))
(def redshift-import-commits (meter [(str *ns*) "redshift-imports" "commits"]))

;; pgsql driver isn't loaded automatically from classpath
(Class/forName "org.postgresql.Driver")

(defn connection [jdbc-url username password]
  (doto (DriverManager/getConnection jdbc-url username password)
    (.setAutoCommit false)))

(def ^{:dynamic true} *current-connection* nil)

(defn prepare-statement
  ([sql] (prepare-statement sql *current-connection*))
  ([sql conn]
   (.prepareStatement conn sql)))

(def open-connections (counter [(str *ns*) "redshift-connections" "open-connections"]))

(defmacro with-connection [jdbc-url username password & body]
  `(binding [*current-connection* (connection ~jdbc-url ~username ~password)]
     (inc! open-connections)
     (try (let [res# ~@body]
            (debug "COMMIT")
            (.commit *current-connection*)
            (mark! redshift-import-commits)
            res#)
          (catch SQLException e#
            (error e# "ROLLBACK")
            (mark! redshift-import-rollbacks)
            (.rollback *current-connection*)
            (throw e#))
          (finally
            (dec! open-connections)
            (when-not (.isClosed *current-connection*)
              (.close *current-connection*))))))

(defn create-staging-table-stmt [target-table staging-table]
  (prepare-statement (format "CREATE TEMPORARY TABLE %s (LIKE %s INCLUDING DEFAULTS)"
                             staging-table
                             target-table)))

(def credentials-chain (DefaultAWSCredentialsProviderChain.))

(defn copy-from-s3-stmt [table manifest-url {:keys [columns options] :as table-manifest}]
  (let [permission (if-let [iam-role (System/getenv "BLUESHIFT_S3_IAM_ROLE")]
                     (format "IAM_ROLE '%s'" iam-role)
                     (let [credentials (.getCredentials credentials-chain)
                           access-key  (.getAWSAccessKeyId credentials)
                           secret-key  (.getAWSSecretKey credentials)]
                       (format "CREDENTIALS 'aws_access_key_id=%s;aws_secret_access_key=%s'" access-key secret-key)))]
    (prepare-statement (format "COPY %s (%s) FROM '%s' %s %s manifest"
                               table
                               (s/join "," columns)
                               manifest-url
                               permission
                               (s/join " " options)))))

(defn truncate-table-stmt [target-table]
  (prepare-statement (format "truncate table %s" target-table)))

(defn delete-in-query [target-table staging-table key]
  (format "DELETE FROM %s WHERE %s IN (SELECT %s FROM %s)" target-table key key staging-table))

(defn delete-join-query
  [target-table staging-table keys]
  (let [where (s/join " AND " (for [pk keys] (str target-table "." pk "=" staging-table "." pk)))]
    (format "DELETE FROM %s USING %s WHERE %s" target-table staging-table where)))

(defn delete-target-query
  "Attempts to optimise delete strategy based on keys arity. With single primary keys
   its significantly faster to delete."
  [target-table staging-table keys]
  (cond (= 1 (count keys)) (delete-in-query target-table staging-table (first keys))
        :default           (delete-join-query target-table staging-table keys)))

(defn delete-target-stmt
  "Deletes rows, with the same primary key value(s), from target-table that will be
   overwritten by values in staging-table."
  [target-table staging-table keys]
  (prepare-statement (delete-target-query target-table staging-table keys)))

(defn staging-select-statement [{:keys [columns staging-select]} staging-table]
  (cond
    (string? staging-select)          (s/replace staging-select #"\{\{table\}\}" staging-table)
    (= :distinct staging-select)      (format "SELECT DISTINCT * FROM %s" staging-table)
    (= :distinct-hash staging-select) (let [cols (->> columns
                                                      (remove #{"hash"})
                                                      (s/join ","))]
                                        (format "SELECT %s, max(hash) FROM %s group by %s" cols staging-table cols))
    :else                              (format "SELECT * FROM %s" staging-table)))

(defn insert-from-staging-stmt [target-table staging-table table-manifest]
  (let [select-statement (staging-select-statement table-manifest staging-table)]
    (prepare-statement (format "INSERT INTO %s %s" target-table select-statement))))

(defn append-from-staging-stmt [target-table staging-table keys]
  (let [join-columns (s/join " AND " (map #(str "s." % " = t." %) keys))
        where-clauses (s/join " AND " (map #(str "t." % " IS NULL") keys))]
    (prepare-statement (format "INSERT INTO %s SELECT s.* FROM %s s LEFT JOIN %s t ON %s WHERE %s"
                               target-table staging-table target-table join-columns where-clauses))))

(defn add-from-staging-stmt [target-table staging-table]
  (prepare-statement (format "INSERT INTO %s SELECT s.* FROM %s s"
                             target-table staging-table)))

(defn drop-table-stmt [table]
  (prepare-statement (format "DROP TABLE %s" table)))

(defn select-stl-load-errors-stmt [files]
  (let [file-str (s/join "' ,'" files)]
    (format "select * from stl_load_errors where query in (select query from (select max(query) as query, filename from stl_load_errors where filename in ('%s') group by filename));" file-str)))

(defn get-stl-load-errors [conn files]
  (let [ps (.prepareStatement conn (select-stl-load-errors-stmt files))
        rs (.executeQuery ps)
        results (loop [results []]
                  (if (.next rs)
                    (recur (conj results {:filename (s/trim (.getString rs "filename"))
                                          :line-number (.getInt rs "line_number")
                                          :colname (s/trim (.getString rs "colname"))
                                          :err-reason (s/trim (.getString rs "err_reason"))}))
                    results))]
    (.close rs)
    (.close ps)
    results))

(defn- aws-censor
  [s]
  (-> s
      (clojure.string/replace #"aws_access_key_id=[^;]*" "aws_access_key_id=***")
      (clojure.string/replace #"aws_secret_access_key=[^;]*" "aws_secret_access_key=***")))

(def executing-statements (counter [(str *ns*) "redshift-connections" "executing-statements"]))

(defn- execute*
  "Will return a map with error details if the statement fails"
  [statement millis]
  (try
    (inc! executing-statements)
    (.execute statement)
    (dec! executing-statements)
    nil
    (catch SQLException e
      (dec! executing-statements)
      (error "error executing statement: " (.toString statement))
      {:cause     :sql-exception
       :statement (.toString statement)
       :message   (.getMessage e)})))

(def timeouts (meter [(str *ns*) "redshift-connections" "statement-timeouts"]))

(defn execute
  "Executes statements in the order specified. Will throw an exception if the statement
   fails or the timeout is triggered."
  [{:keys [timeout-millis] :or {timeout-millis (* 1000 60 60)}} & statements]
  (loop [statements statements]
    (when-let [statement (first statements)]
      (let [result-ch (thread (execute* statement timeout-millis))
            timeout-ch (timeout timeout-millis)
            [v ch] (alts!! [result-ch timeout-ch])]
        (cond (and (= ch result-ch)
                   (not (nil? v)))  (throw (ex-info "error during execute" v))
              (= ch timeout-ch) (do (println "timeout during statement, canceling")
                                    (mark! timeouts)
                                    (.cancel statement)
                                    (throw (ex-info "timeout during execution"
                                                    {:cause     :timeout
                                                     :statement (.toString statement)
                                                     :millis    timeout-millis})))
              :else (recur (rest statements)))))))

(defn merge-table [redshift-manifest-url {:keys [table schema jdbc-url username password pk-columns strategy execute-opts] :as table-manifest}]
  (let [target-table (if (s/blank? schema) table (str schema "." table))
        staging-table (str table "_staging")]
    (mark! redshift-imports)
    (with-connection jdbc-url username password
      (execute execute-opts
               (create-staging-table-stmt target-table staging-table)
               (copy-from-s3-stmt staging-table redshift-manifest-url table-manifest)
               (delete-target-stmt target-table staging-table pk-columns)
               (insert-from-staging-stmt target-table staging-table table-manifest)
               (drop-table-stmt staging-table)))))

(defn replace-table [redshift-manifest-url {:keys [table schema jdbc-url username password pk-columns strategy execute-opts] :as table-manifest}]
  (let [target-table (if (s/blank? schema) table (str schema "." table))]
    (mark! redshift-imports)
    (with-connection jdbc-url username password
      (execute execute-opts
               (truncate-table-stmt target-table)
               (copy-from-s3-stmt target-table redshift-manifest-url table-manifest)))))

(defn append-table [redshift-manifest-url {:keys [table schema jdbc-url username password pk-columns strategy execute-opts] :as table-manifest}]
  (let [target-table (if (s/blank? schema) table (str schema "." table))
        staging-table (str table "_staging")]
    (mark! redshift-imports)
    (with-connection jdbc-url username password
      (execute execute-opts
               (create-staging-table-stmt target-table staging-table)
               (copy-from-s3-stmt staging-table redshift-manifest-url table-manifest)
               (append-from-staging-stmt target-table staging-table pk-columns)
               (drop-table-stmt staging-table)))))

(defn add-table [redshift-manifest-url {:keys [table schema jdbc-url username password pk-columns strategy execute-opts] :as table-manifest}]
  (let [target-table (if (s/blank? schema) table (str schema "." table))
        staging-table (str table "_staging")]
    (mark! redshift-imports)
    (with-connection jdbc-url username password
      (execute execute-opts
               (create-staging-table-stmt target-table staging-table)
               (copy-from-s3-stmt staging-table redshift-manifest-url table-manifest)
               (add-from-staging-stmt target-table staging-table)
               (drop-table-stmt staging-table)))))

(defn load-table
  "kicks off any loads based on :strategy, also will replace any {{ENV_VAR}}s found in :table :schema or :jdbc-url with the values from those env vars"
  [redshift-manifest-url {strategy :strategy :as table-manifest}]
  (let [env-table-manifest (reduce (fn [m k] (update m k (fn [v] (util/replace-with-env-vars v)))) table-manifest [:table :schema :jdbc-url :username :password])]
    (case (keyword strategy)
      :merge (merge-table redshift-manifest-url env-table-manifest)
      :replace (replace-table redshift-manifest-url env-table-manifest)
      :add (add-table redshift-manifest-url env-table-manifest)
      :append (append-table redshift-manifest-url env-table-manifest))))

(defn get-stl-errors
  [table-manifest files]
  (let [env-table-manifest (reduce (fn [m k] (update m k (fn [v] (util/replace-with-env-vars v)))) table-manifest [:table :schema :jdbc-url :username :password])
        {:keys [jdbc-url username password]} env-table-manifest
        conn (connection jdbc-url username password)
        results (get-stl-load-errors conn files)]
    (when-not (.isClosed conn)
      (.close conn))
    results))
