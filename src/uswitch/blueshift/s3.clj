(ns uswitch.blueshift.s3
  (:require [cheshire.core :as json]
            [cheshire.generate :refer [add-encoder encode-str]]
            [com.stuartsierra.component :refer (Lifecycle system-map using start stop)]
            [clojure.string :as str]
            [clojure.tools.logging :refer (info error warn debug)]
            [amazonica.aws.s3 :as s3]
            [clojure.set :refer (difference)]
            [clojure.core.async :refer (go-loop thread put! chan >!! <!! >! <! alts!! timeout close!)]
            [clojure.edn :as edn]
            [uswitch.blueshift.util :refer (close-channels clear-keys log-error)]
            [schema.core :as s]
            [metrics.counters :refer (counter inc! dec!)]
            [metrics.timers :refer (timer time!)]
            [uswitch.blueshift.redshift :as redshift]
            [uswitch.blueshift.sql :as sql])
  (:import [java.io PushbackReader InputStreamReader]
           [org.apache.http.conn ConnectionPoolTimeoutException]
           (java.time Instant)
           (java.time.format DateTimeFormatter)))

(add-encoder java.time.Instant encode-str)

(defrecord Manifest [table pk-columns columns jdbc-url username password add-status options data-pattern strategy staging-select pk-nulls data-sources])

(defn delete-object
  [bucket key]
  (s3/delete-object bucket key))

(defn copy-object
  [src-bucket src-key dest-bucket dest-key]
  (s3/copy-object src-bucket src-key dest-bucket dest-key))

(defn move-object
  [src-bucket src-key dest-bucket dest-key]
  (s3/copy-object src-bucket src-key dest-bucket dest-key)
  (delete-object src-bucket src-key))

(def ManifestSchema {:table          s/Str
                     :schema         (s/maybe s/Str)
                     :pk-columns     [s/Str]
                     :pk-nulls       (s/maybe [s/Str])
                     :columns        [s/Str]
                     :jdbc-url       s/Str
                     :username       s/Str
                     :password       s/Str
                     :add-status     (s/maybe s/Bool)
                     :strategy       s/Str
                     :data-sources   (s/maybe [s/Str])
                     :options        s/Any
                     :staging-select (s/maybe (s/either s/Str s/Keyword))
                     :data-pattern   s/Regex})

(defn validate [manifest]
  (when-let [error-info (s/check ManifestSchema manifest)]
    (let [ex (ex-info "Invalid manifest. Check map for more details." error-info)]
      (log-error (str "Error validating manifest" ex))
      (throw ex))))

(defn list-all-objects
  [request]
  (let [response (s3/list-objects request)
        next-request (assoc request :marker (:next-marker response))]
    (concat (:object-summaries response) (if (:truncated? response)
                                           (lazy-seq (list-all-objects next-request))
                                           []))))

(defn- directories
  ([bucket]
   (:common-prefixes (s3/list-objects {:bucket-name bucket :delimiter "/"})))
  ([bucket directory]
   {:pre [(.endsWith directory "/")]}
   (:common-prefixes (s3/list-objects {:bucket-name bucket :delimiter "/" :prefix directory}))))

(defn leaf-directories
  [bucket]
  (loop [work (directories bucket)
         result nil]
    (if (seq work)
      (let [sub-dirs (directories bucket (first work))]
        (recur (concat (rest work) sub-dirs)
               (if (seq sub-dirs)
                 result
                 (cons (first work) result))))
      result)))

(defn read-edn [stream]
  (edn/read (PushbackReader. (InputStreamReader. stream))))

(defn assoc-if-nil [record key value]
  (if (nil? (key record))
    (assoc record key value)
    record))

(defn manifest [bucket files]
  (letfn [(manifest? [{:keys [key]}]
            (re-matches #".*manifest\.edn$" key))]
    (when-let [manifest-file-key (:key (first (filter manifest? files)))]
      (with-open [content (:input-stream (s3/get-object bucket manifest-file-key))]
        (-> (read-edn content)
            (map->Manifest)
            (assoc-if-nil :strategy "merge")
            (update-in [:data-pattern] re-pattern))))))

(defn- step-scan
  [bucket directory]
  (try
    (let [fs (list-all-objects {:bucket-name bucket :prefix directory})]
      (if-let [manifest (manifest bucket fs)]
        (do
          (validate manifest)
          (let [data-files  (filter (fn [{:keys [key]}]
                                      (re-matches (:data-pattern manifest) key))
                                    fs)]
            (if (seq data-files)
              (do
                (info "Watcher triggering import" (:table manifest))
                (debug "Triggering load:" load)
                (let [all-files (map :key data-files)
                      files (if (= "merge" (:strategy manifest))
                              [(first all-files)]
                              all-files)]
                  {:state :load, :table-manifest manifest, :files files}))
              {:state :scan, :pause? true})))
        {:state :scan, :pause? true}))
    (catch clojure.lang.ExceptionInfo e
      (log-error (str "Error with manifest file: "  e))
      {:state :scan, :pause? true})
    (catch ConnectionPoolTimeoutException e
      (warn e "Connection timed out. Will re-try.")
      {:state :scan, :pause? true})
    (catch Exception e
      (log-error (str "Failed reading content of: " (str bucket "/" directory) " Exception: " e))
      {:state :scan, :pause? true})))

(def importing-files (counter [(str *ns*) "importing-files" "files"]))
(def import-timer (timer [(str *ns*) "importing-files" "time"]))

(defn- step-load
  [bucket table-manifest files]
  (let [redshift-manifest  (redshift/manifest bucket files)
        {:keys [key url]}  (redshift/put-manifest bucket redshift-manifest)]
    (info "Importing" (count files) "data files to table" (:table table-manifest) "from manifest" url)
    (when (:add-status table-manifest)
      (sql/update-files-status files "processing"))
    (debug "Importing Redshift Manifest" redshift-manifest)
    (inc! importing-files (count files))
    (try (time! import-timer
                (redshift/load-table url table-manifest))
         (info "Successfully imported" (count files) "files")
         (delete-object bucket key)
         (when (:add-status table-manifest)
           (sql/update-files-status files "upserted"))
         (dec! importing-files (count files))
         {:state :delete
          :files files}
         (catch java.sql.SQLException e
           (log-error (str "Error loading into: " (:table table-manifest) " Exception: " e))
           (log-error (str (:table table-manifest) "Redshift manifest content for previous error: " redshift-manifest))
           (delete-object bucket key)
           (when (:add-status table-manifest)
             (sql/update-files-status files "failed"))
           (dec! importing-files (count files))
           {:state :scan
            :pause? true})
         (catch Exception e
           (if-let [m (ex-data e)]
             (log-error (str "Failed to load files to table" (:table table-manifest) m))
             (log-error (str "Failed to load files to table" (:table table-manifest) "from manifest: " url " Exception: " e)))
           (delete-object bucket key)
           (when (:add-status table-manifest)
             (sql/update-files-status files "failed"))
           (dec! importing-files (count files))
           (if (and (ex-data e) (str/includes? (ex-data e) "stl_load_errors"))
             {:state :stl-load-error :table-manifest table-manifest :files files}
             {:state :scan :pause? true})))))

(defn- step-delete
  [bucket files]
  (doseq [key files]
    (info "Deleting" (str "s3://" bucket "/" key))
    (try
      (delete-object bucket key)
      (catch Exception e
        (warn "Couldn't delete" key "  - ignoring"))))
  {:state :scan, :pause? true})

(defn- create-stl-load-error-msg
  [{:keys [err-reason colname filename line-number]} dest-bucket-key]
  (-> {:log
       {:level "ERROR"
        :timestamp (.format DateTimeFormatter/ISO_INSTANT (Instant/now))
        :module "data-sisyphus"
        :source "uswitch.blueshift.s3"
        :category "stl-load-error"
        :message err-reason}
       :custom-fields
       {:source-data-filename filename
        :line-number line-number
        :column-name colname
        :errors-filename dest-bucket-key}}
      json/generate-string))

(defn- step-stl-load-error
  [bucket table-manifest files]
  (info "Processing stl-load-error")
  (let [bucket-files (mapv #(str "s3://" bucket "/" %) files)
        stl-errors (redshift/get-stl-errors table-manifest bucket-files)]
    (doseq [err stl-errors]
      (let [file (:filename err)
            key (str/replace-first file (str "s3://" bucket "/") "")
            date-str (.format (java.text.SimpleDateFormat. "yyyy-MM-dd") (new java.util.Date))
            dest-key (str "errors/" date-str (subs file (str/last-index-of file "/")))
            dest-bucket-key (str "s3://" bucket "/" dest-key)
            existing-files (list-all-objects {:bucket-name bucket :prefix key})]
        (if (seq existing-files)
          (do (error "Found stl-load-error:")
              (error (create-stl-load-error-msg err dest-bucket-key))
              (info "Moving error file to " dest-bucket-key)
              (try
                (move-object bucket key bucket dest-key)
                (catch Exception e
                  (warn e "Couldn't move" key "  - ignoring"))))
          (info "Did not find an s3 file for " file "so we are skipping this.")))))
  {:state :scan, :pause? true})

(defn- progress
  [{:keys [state] :as world}
   {:keys [bucket directory] :as configuration}]
  (case state
    :scan   (step-scan   bucket directory)
    :load   (step-load   bucket (:table-manifest world) (:files world))
    :delete (step-delete bucket (:files world))
    :stl-load-error (step-stl-load-error bucket (:table-manifest world) (:files world))))

(defrecord KeyWatcher [bucket directory
                       poll-interval-seconds
                       poll-interval-random-seconds]
  Lifecycle
  (start [this]
    (info "Starting KeyWatcher for" (str bucket "/" directory) "polling every" poll-interval-seconds "seconds")
    (let [control-ch    (chan)
          configuration {:bucket bucket :directory directory}]
      (thread
        (loop [timer (timeout (*
                               (+ poll-interval-seconds
                                  (int (* (rand) (float poll-interval-random-seconds))))
                               1000))
               world {:state :scan}]
          (let [next-world (progress world configuration)]
            (if (:pause? next-world)
              (let [[_ c] (alts!! [control-ch timer])]
                (when (not= c control-ch)
                  (let [t (*
                           (+ poll-interval-seconds
                              (int (* (rand) (float poll-interval-random-seconds))))
                           1000)]
                    (recur (timeout t) next-world))))
              (recur timer next-world)))))
      (assoc this :watcher-control-ch control-ch)))
  (stop [this]
    (info "Stopping KeyWatcher for" (str bucket "/" directory))
    (close-channels this :watcher-control-ch)))

(defn spawn-key-watcher! [bucket directory
                          poll-interval-seconds poll-interval-random-seconds]
  (start (KeyWatcher. bucket directory
                      poll-interval-seconds
                      poll-interval-random-seconds)))

(def directories-watched (counter [(str *ns*) "directories-watched" "directories"]))

(defrecord KeyWatcherSpawner [bucket-watcher
                              poll-interval-seconds
                              poll-interval-random-seconds]
  Lifecycle
  (start [this]
    (info "Starting KeyWatcherSpawner")
    (let [{:keys [new-directories-ch bucket]} bucket-watcher
          watchers (atom nil)]
      (go-loop [dirs (<! new-directories-ch)]
        (when dirs
          (doseq [dir dirs]
            (swap! watchers conj (spawn-key-watcher!
                                  bucket dir
                                  poll-interval-seconds
                                  poll-interval-random-seconds))
            (inc! directories-watched))
          (recur (<! new-directories-ch))))
      (assoc this :watchers watchers)))
  (stop [this]
    (info "Stopping KeyWatcherSpawner")
    (when-let [watchers (:watchers this)]
      (info "Stopping" (count @watchers) "watchers")
      (doseq [watcher @watchers]
        (stop watcher)
        (dec! directories-watched)))
    (clear-keys this :watchers)))

(defn key-watcher-spawner [config]
  (map->KeyWatcherSpawner
   {:poll-interval-seconds (-> config :s3 :poll-interval :seconds)
    :poll-interval-random-seconds (or (-> config :s3 :poll-interval :random-seconds) 0)}))

(defn matching-directories [bucket key-pattern]
  (try (->> (leaf-directories bucket)
            (filter #(re-matches key-pattern %))
            (set))
       (catch Exception e
         (log-error (str "Error checking for matching object keys in: " bucket " Exception: " e))
         #{})))

(defrecord BucketWatcher [bucket key-pattern poll-interval-seconds]
  Lifecycle
  (start [this]
    (info "Starting BucketWatcher. Polling" bucket "every" poll-interval-seconds "seconds for keys matching" key-pattern)
    (let [new-directories-ch (chan)
          control-ch         (chan)]
      (thread
        (loop [dirs nil]
          (let [available-dirs (matching-directories bucket key-pattern)
                new-dirs       (difference available-dirs dirs)]
            (when (seq new-dirs)
              (info "New directories:" new-dirs "spawning" (count new-dirs) "watchers")
              (>!! new-directories-ch new-dirs))
            (let [[v c] (alts!! [(timeout (* 1000 poll-interval-seconds)) control-ch])]
              (when-not (= c control-ch)
                (recur available-dirs))))))
      (assoc this :control-ch control-ch :new-directories-ch new-directories-ch)))
  (stop [this]
    (info "Stopping BucketWatcher")
    (close-channels this :control-ch :new-directories-ch)))

(defn bucket-watcher
  "Creates a process watching for objects in S3 buckets."
  [config]
  (map->BucketWatcher {:bucket (-> config :s3 :bucket)
                       :poll-interval-seconds (-> config :s3 :poll-interval :seconds)
                       :key-pattern (or (re-pattern (-> config :s3 :key-pattern))
                                        #".*")}))

(defrecord PrintSink [prefix chan-k component]
  Lifecycle
  (start [this]
    (let [ch (get component chan-k)]
      (go-loop [msg (<! ch)]
        (when msg
          (info prefix msg)
          (recur (<! ch)))))
    this)
  (stop [this]
    this))

(defn print-sink
  [prefix chan-k]
  (map->PrintSink {:prefix prefix :chan-k chan-k}))

(defn s3-system [config]
  (system-map :bucket-watcher (bucket-watcher config)
              :key-watcher-spawner (using (key-watcher-spawner config)
                                          [:bucket-watcher])))
