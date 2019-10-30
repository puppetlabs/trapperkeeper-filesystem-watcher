(defproject puppetlabs/trapperkeeper-filesystem-watcher "1.1.2-SNAPSHOT"
  :description "Trapperkeeper filesystem watcher service"
  :url "https://github.com/puppetlabs/trapperkeeper-filesystem-watcher"
  :license {:name "Apache License, Version 2.0"
            :url "http://www.apache.org/licenses/LICENSE-2.0"}

  :min-lein-version "2.7.1"

  :parent-project  {:coords [puppetlabs/clj-parent "0.4.3"]
                    :inherit [:managed-dependencies]}

  :pedantic? :abort

  :exclusions [org.clojure/clojure]

  :dependencies [[org.clojure/clojure]
                 [org.clojure/tools.logging]
                 [prismatic/schema]
                 [me.raynes/fs]
                 [puppetlabs/trapperkeeper]
                 [puppetlabs/kitchensink]
                 [puppetlabs/i18n]]

  :deploy-repositories [["releases" {:url "https://clojars.org/repo"
                                     :username :env/clojars_jenkins_username
                                     :password :env/clojars_jenkins_password
                                     :sign-releases false}]]

  :source-paths ["src/clj"]
  :java-source-paths ["src/java"]

  :profiles {:dev {:dependencies [[puppetlabs/trapperkeeper
                                   :classifier "test"
                                   :scope "test"]
                                  [puppetlabs/kitchensink
                                   :classifier "test"
                                   :scope "test"]]}}

  :plugins  [[lein-parent "0.3.1"]
             [puppetlabs/i18n "0.7.1"]]

  :main puppetlabs.trapperkeeper.main
)
