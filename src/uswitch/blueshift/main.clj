(ns uswitch.blueshift.main
  (:require [clojure.tools.logging :refer (info)]
            [clojure.tools.cli :refer (parse-opts)]
            [uswitch.blueshift.system :refer (build-system)]
            [uswitch.blueshift.sql :as sql]
            [com.stuartsierra.component :refer (start stop)]
            [uswitch.blueshift.util :as util]
            [cider.nrepl.middleware :as mid]
            [nrepl.server :as nrepl-server])
  (:gen-class))

(def cli-options
  [["-c" "--config CONFIG" "Path to EDN configuration file"
    :default "./etc/config.edn"
    :validate [string?]]
   ["-h" "--help"]])

(defn wait! []
  (let [s (java.util.concurrent.Semaphore. 0)]
    (.acquire s)))

(def custom-nrepl-handler
  "We build our own custom nrepl handler, mimicking CIDER's."
  (apply nrepl-server/default-handler
         (conj mid/cider-middleware 'refactor-nrepl.middleware/wrap-refactor)))

(defn start-nrepl-server []
  (info "Starting NREPL server....")
  (nrepl-server/start-server :port 3178 :handler custom-nrepl-handler))

(defn -main [& args]
  (let [{:keys [options summary]} (parse-opts args cli-options)]
    (when (:help options)
      (println summary)
      (System/exit 0))
    (let [{:keys [config]} options]
      (when (not= "prod" (System/getenv "STAGE"))
        (start-nrepl-server))
      (info "Starting Blueshift with configuration" config)
      (let [cfg-str (slurp config)
            env-str (util/replace-with-env-vars cfg-str)
            cfg-map (read-string env-str)
            _  (reset! sql/cfg cfg-map)
            system (build-system cfg-map)]
        (start system)
        (wait!)))))

(comment
  
  (def system
    (atom
      (build-system {:s3 {:bucket        "uswitch-blueshift"
                          :key-pattern   "production/insight-load/.*"
                          :poll-interval {:seconds 10}}})))

  (prn system)
  (swap! system start)


  
  
  )
