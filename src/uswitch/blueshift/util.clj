(ns uswitch.blueshift.util
  (:require [cheshire.core :as json]
            [clojure.core.async :refer (close!)]
            [clostache.parser :as clo]
            [clojure.tools.logging :refer (error)])
  (:import
   (java.time Instant)
   (java.time.format DateTimeFormatter)))

(defn clear-keys
  "dissoc for components records. assoc's nil for the specified keys"
  [m & ks]
  (apply assoc m (interleave ks (repeat (count ks) nil))))

(defn close-channels [state & ks]
  (doseq [k ks]
    (when-let [ch (get state k)]
      (close! ch)))
  (apply clear-keys state ks))

(defn replace-with-env-vars
  "Replaces any string with {{ENV_VARIABLE_NAME}}s in them with the value from the environment"
  [input-str]
  (let [env-map (into {} (map (fn [[k v]] {(keyword k) v}) (System/getenv)))]
    (clo/render input-str env-map)))

(defn create-error-log-msg
  [err-reason custom-fields]
  {:log
   {:level "ERROR"
    :timestamp (.format DateTimeFormatter/ISO_INSTANT (Instant/now))
    :module "blueshift"
    :source (str *ns*)
    :category "error"
    :message err-reason}
   :custom-fields (or custom-fields {})})

(defn log-error
  ([msg]
   (log-error msg nil))
  ([msg data]
   (error msg (json/generate-string (create-error-log-msg msg data)))))
