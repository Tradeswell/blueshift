(ns uswitch.blueshift.sql
  (:refer-clojure :exclude [hash])
  (:require
   [clojure.tools.logging :refer (info error warn debug errorf)]
   [next.jdbc.sql :as sql]
   [next.jdbc :as jdbc]
   [next.jdbc.types :as jdbc.types]))

(defonce cfg (atom {}))

(defn sql-client []
  (-> @cfg
      :status-db
      (assoc :dbtype "postgresql")
      jdbc/get-datasource))

(defn get-processed-file [path]
  (sql/get-by-id (sql-client) (keyword (str (-> @cfg :status-db :schema) "." (-> @cfg :status-db :table))) path :path {}))

(defn update-processed-file-status [path status]
  {:pre  [(contains?  #{"pending" "transferred" "processing" "upserted" "failed"} status)]
   :post [(= 1 (count %))]}
  (sql/update! (sql-client) (keyword (str (-> @cfg :status-db :schema) "." (-> @cfg :status-db :table)))
               {:status (jdbc.types/as-other status)}
               {:path path}))

(defn update-files-status
  [files status]
  (when (:status-db @cfg)
    (doseq [f files]
      (info "Setting file: " f " to status: " status)
      (update-processed-file-status f status))))
