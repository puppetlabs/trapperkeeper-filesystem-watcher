(ns puppetlabs.trapperkeeper.services.watcher.filesystem-watch-service
  (:require [clojure.tools.logging :as log]
            [puppetlabs.trapperkeeper.services :as tk]
            [puppetlabs.trapperkeeper.services.protocols.filesystem-watch-service :refer :all]
            [puppetlabs.trapperkeeper.services.watcher.filesystem-watch-core :as watch-core])
  (:import (java.io IOException)))

(tk/defservice filesystem-watch-service
  FilesystemWatchService
  [[:ShutdownService shutdown-on-error]]

  (init
    [this context]
    {:watchers (atom {})})

  (stop
    [this context]
    ;; Shut down the WatchServices
    (doseq [[watcher _] @(:watchers context)]
      (try
        (.close (:watch-service watcher))
        (catch IOException e
          (log/warn e "Exception while closing watch service"))))
    context)

  (create-watcher
   [this]
   (let [{:keys [watchers]} (tk/service-context this)
         watcher (watch-core/create-watcher)]
     (swap!
       watchers
       merge
       {watcher
        (watch-core/watch!
          watcher
          (partial shutdown-on-error (tk/service-id this)))})
     watcher)))
