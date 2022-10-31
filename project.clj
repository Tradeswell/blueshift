(defproject blueshift "0.1.8-SNAPSHOT"
  :description "Automate importing S3 data into Amazon Redshift"
  :url "https://github.com/uswitch/blueshift"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.10.1"]
                 [org.clojure/tools.logging "1.2.4"]
                 [com.stuartsierra/component "1.1.0"]
                 [org.clojure/core.async "1.5.648"]
                 [org.clojure/tools.cli "1.0.214"]
                 [amazonica "0.3.162"]
                 [joda-time "2.12.0"]
                 [commons-codec "1.15"]
                 [org.slf4j/jcl-over-slf4j "2.0.3"]
                 [cheshire "5.11.0"]
                 [prismatic/schema "1.4.1"]
                 [metrics-clojure "2.10.0"]
                 [io.dropwizard.metrics/metrics-jvm "4.2.12"]
                 ;; java 11 FIX
                 ;; in 11 the java xml stuff is no longer included
                 ;; https://stackoverflow.com/questions/43574426/how-to-resolve-java-lang-noclassdeffounderror-javax-xml-bind-jaxbexception-in-j
                 [javax.xml.bind/jaxb-api "2.4.0-b180830.0359"]
                 [com.sun.xml.bind/jaxb-core "4.0.1"]
                 [com.sun.xml.bind/jaxb-impl "4.0.1"]
                 [javax.activation/activation "1.1.1"]
                 [com.tradeswell/clostache "1.6.0-SNAPSHOT"]
                 [com.github.seancorfield/next.jdbc "1.2.737"]
                 [com.github.seancorfield/honeysql "2.1.818"]
                 [org.postgresql/postgresql "42.5.0"]
                 [nrepl/nrepl "1.0.0"]
                 [refactor-nrepl/refactor-nrepl "3.5.2"]
                 [cider/cider-nrepl "0.28.5"]
                 ]
  ;; deal with java 11
  ;; https://www.deps.co/blog/how-to-upgrade-clojure-projects-to-use-java-11/
  :managed-dependencies [[org.clojure/core.rrb-vector "0.1.2"]
                         [org.flatland/ordered "1.15.10"]]

  :release-tasks [["vcs" "assert-committed"]
                  ["change" "version"
                   "leiningen.release/bump-version" "release"]
                  ["vcs" "commit"]
                  ["vcs" "tag" "--no-sign"]
                  ["deploy"]]

  :repositories ^:replace [["snapshots" {:url "https://tradeswell.jfrog.io/artifactory/tw-maven-snapshots-only"
                                         :username [:env/jfrog_user :gpg]
                                         :password [:env/jfrog_access_token :gpg]}]
                           ["releases" {:url "https://tradeswell.jfrog.io/artifactory/tw-maven"
                                        :username [:env/jfrog_user :gpg]
                                        :password [:env/jfrog_access_token :gpg]
                                        :sign-releases false}]]

  :profiles {:dev {:dependencies [[org.slf4j/slf4j-simple "2.0.3"]
                                  [org.clojure/tools.namespace "1.3.0"]]
                   :source-paths ["./dev"]
                   :jvm-opts ["-Dorg.slf4j.simpleLogger.defaultLogLevel=debug"
                              "-Dorg.slf4j.simpleLogger.log.org.apache.http=info"
                              "-Dorg.slf4j.simpleLogger.log.com.amazonaws=info"
                              "-Dorg.slf4j.simpleLogger.log.com.codahale=debug"]}
             :uberjar {:aot [uswitch.blueshift.main]
                       :dependencies [[ch.qos.logback/logback-classic "1.4.4"]]}}
  :main uswitch.blueshift.main)
