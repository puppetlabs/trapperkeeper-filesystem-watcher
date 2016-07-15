(ns puppetlabs.enterprise.services.watcher.filesystem-watch-service
  (:require [me.raynes.fs :as fs]
            [puppetlabs.trapperkeeper.services :as tk]
            [puppetlabs.enterprise.services.protocols.filesystem-watch-service :refer :all]
            [puppetlabs.enterprise.services.watcher.filesystem-watch-core :as watch-core])
  (:import (java.nio.file FileSystems)
           (com.puppetlabs.enterprise DirWatchUtils)
           (java.util HashMap)))

(tk/defservice filesystem-watch-service
  FilesystemWatchService
  [[:SchedulerService after]
   [:ShutdownService shutdown-on-error]]

  (init
    [this context]
    {:watched-paths (atom {})
     :watch-service (.newWatchService (FileSystems/getDefault))
     :watch-keys (HashMap.) ;; Must be a mutable map because Java :(
     :stopped? (atom false)})

  (start
    [this context]
    (let [{:keys [watched-paths watch-service watch-keys stopped?]} context]
      (watch-core/schedule-watching! watched-paths
                                     watch-service
                                     watch-keys
                                     after
                                     stopped?
                                     (partial shutdown-on-error (tk/service-id this))))
    context)

  (stop
    [this context]
    ;; This signals to the background thread that it should stop polling
    ;; the filesystem for changes and terminate.
    (reset! (:stopped? context) true)
    ;; Shut down the WatchService
    (.close (:watch-service context))
    context)

  (watch-dir!
    [this path callback]
    (let [normalized-path (watch-core/normalized-path-str path)
          {:keys [watched-paths watch-service watch-keys]} (tk/service-context this)]
      (watch-core/validate-path! (keys @watched-paths) normalized-path)
      (DirWatchUtils/registerRecursive watch-service
                                       [(.toPath (fs/file normalized-path))]
                                       watch-keys)
      (swap! watched-paths assoc normalized-path callback))))
