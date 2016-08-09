(ns puppetlabs.trapperkeeper.services.watcher.filesystem-watch-service
  (:require [clojure.tools.logging :as log]
            [puppetlabs.i18n.core :refer [trs]]
            [puppetlabs.trapperkeeper.services :as tk]
            [puppetlabs.trapperkeeper.services.protocols.filesystem-watch-service :refer :all]
            [puppetlabs.trapperkeeper.services.watcher.filesystem-watch-core :as watch-core])
  (:import (java.io IOException)))

(tk/defservice filesystem-watch-service
  FilesystemWatchService
  [[:ShutdownService shutdown-on-error]]

  (init
    [this context]
    (assoc context :watchers (atom [])))

  (stop
    [this context]
    ;; Shut down the WatchServices
    (doseq [watcher @(:watchers context)]
      (try
        (.close (:watch-service watcher))
        (catch IOException e
          (log/warn e (trs "Exception while closing watch service")))))
    context)

  (create-watcher
   [this]
   (let [{:keys [watchers]} (tk/service-context this)
         watcher (watch-core/create-watcher)
         shutdown-fn (partial shutdown-on-error (tk/service-id this))]
     (watch-core/watch! watcher shutdown-fn)
     (swap! watchers conj watcher)
     watcher)))
