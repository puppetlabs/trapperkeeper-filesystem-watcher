(ns puppetlabs.trapperkeeper.services.watcher.filesystem-watch-service
  (:require [clojure.tools.logging :as log]
            [puppetlabs.i18n.core :refer [trs]]
            [puppetlabs.trapperkeeper.services :as tk]
            [puppetlabs.trapperkeeper.services.protocols.filesystem-watch-service :refer [FilesystemWatchService create-watcher]]
            [puppetlabs.trapperkeeper.services.watcher.filesystem-watch-core :as watch-core])
  (:import (java.nio.file WatchService)))

(def max-future-cancel-wait-tries 100)
(def future-cancel-wait-sleep-ms 10)

(tk/defservice filesystem-watch-service
  FilesystemWatchService
  [[:ShutdownService shutdown-on-error]]

  (init
    [this context]
    (assoc context :watchers (atom [])
                   :watchers-futures (atom [])
                   :stopping? (atom false)))

  (stop
    [this context]
    (log/info (trs "Shutting down watcher service"))
    (reset! (:stopping? context) true)
    ;; Shut down the WatchServices
    (doseq [watcher @(:watchers context)]
      (try
        (.close ^WatchService (:watch-service watcher))
        (catch Throwable e
          (log/warn e (trs "Exception while closing watch service")))))

    (doseq [watchers-future @(:watchers-futures context)]
      (try
        (future-cancel watchers-future)
        (loop [tries max-future-cancel-wait-tries]
          (if (and (pos? tries) (not (future-done? watchers-future)))
            (do
              (Thread/sleep future-cancel-wait-sleep-ms)
              (recur (dec tries)))
            (log/debug (trs "Future completed after {0} tries" (- max-future-cancel-wait-tries tries)))))
        (catch Throwable e
          (log/warn e (trs "Exception while closing watch service")))))

    (log/info (trs "Done shutting down watcher service"))
    context)

  (create-watcher
    [this]
    (create-watcher this {:recursive true}))

  (create-watcher
    [this options]
    (let [{:keys [watchers watchers-futures stopping?]} (tk/service-context this)
          watcher (watch-core/create-watcher options)
          shutdown-fn (partial shutdown-on-error (tk/service-id this))
          watch-future (watch-core/watch! watcher shutdown-fn stopping?)]
      (swap! watchers conj watcher)
      (swap! watchers-futures conj watch-future)
      watcher)))
