(ns uswitch.blueshift.util
  (:require [clojure.core.async :refer (close!)]
            [clostache.parser :as clo]))

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
