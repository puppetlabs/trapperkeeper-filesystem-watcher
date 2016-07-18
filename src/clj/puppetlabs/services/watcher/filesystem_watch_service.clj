(ns puppetlabs.services.watcher.filesystem-watch-service
  (:require [clojure.tools.logging :as log]
            [me.raynes.fs :as fs]
            [puppetlabs.trapperkeeper.services :as tk]
            [puppetlabs.services.protocols.filesystem-watch-service :refer :all]
            [puppetlabs.services.watcher.filesystem-watch-core :as watch-core])
  (:import (java.io IOException)
           (java.nio.file FileSystems)
           (com.puppetlabs DirWatchUtils)))

(tk/defservice filesystem-watch-service
  FilesystemWatchService
  [[:SchedulerService after]
   [:ShutdownService shutdown-on-error]]

  (init
    [this context]
    {:watchers (atom [])
     :stopped? (atom false)})

  (start
    [this context]
    (let [{:keys [watchers stopped?]} context]
      (watch-core/schedule-watching! watchers
                                     after
                                     stopped?
                                     (partial shutdown-on-error (tk/service-id this))))
    context)

  (stop
    [this context]
    ;; This signals to the background thread that it should stop polling
    ;; the filesystem for changes and terminate.
    (reset! (:stopped? context) true)
    ;; Shut down the WatchServices
    (doseq [watcher @(:watchers context)]
      (try
        (.close (:watch-service watcher))
        (catch IOException e
          (log/warn e "Exception while closing watch service"))))
    context)

  (create-watcher!
   [this]
   (let [{:keys [watchers]} (tk/service-context this)
         watch-service (.newWatchService (FileSystems/getDefault))
         watcher {:watch-service watch-service
                  :callbacks (atom [])}]
     (swap! watchers conj watcher)
     watcher))

  (add-watch-dir!
   [this watcher dir options]
   (watch-core/validate-watch-options! options)
   (DirWatchUtils/registerRecursive
    (:watch-service watcher)
    [(.toPath (fs/file dir))]))

  (add-callback!
   [this watcher callback]
   (swap! (:callbacks watcher) conj callback)))



