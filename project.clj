(def tk-version "1.4.1")
(def ks-version "1.3.1")

(defproject puppetlabs/trapperkeeper-filesystem-watcher "0.1.0-SNAPSHOT"
  :description "Trapperkeeper filesystem watcher service"
  :url "https://github.com/puppetlabs/trapperkeeper-filesystem-watcher"
  :license {:name "Apache License, Version 2.0"
            :url "http://www.apache.org/licenses/LICENSE-2.0"}

  :pedantic? :abort

  :exclusions [org.clojure/clojure]

  :dependencies [[org.clojure/clojure "1.7.0"]
                 [org.clojure/tools.logging "0.3.1"]
                 [prismatic/schema "1.1.1"]
                 [me.raynes/fs "1.4.6"]
                 [puppetlabs/trapperkeeper ~tk-version]
                 [puppetlabs/kitchensink ~ks-version]
                 [puppetlabs/trapperkeeper-scheduler "0.0.1"]
                 [puppetlabs/i18n "0.4.1"]]

  :plugins [[lein-release "1.0.5"]]

  :lein-release {:scm :git
                 :deploy-via :lein-deploy}

  :deploy-repositories [["releases" {:url "https://clojars.org/repo"
                                     :username :env/clojars_jenkins_username
                                     :password :env/clojars_jenkins_password
                                     :sign-releases false}]]

  :source-paths ["src/clj"]
  :java-source-paths ["src/java"]

  :profiles {:dev {:dependencies [[puppetlabs/trapperkeeper ~tk-version
                                   :classifier "test"
                                   :scope "test"]
                                  [puppetlabs/kitchensink ~ks-version
                                   :classifier "test"
                                   :scope "test"]]}}

  :main puppetlabs.trapperkeeper.main
)
