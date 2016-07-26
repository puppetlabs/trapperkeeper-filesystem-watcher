(ns puppetlabs.trapperkeeper.services.watcher.filesystem-watch-core
  (:require [clojure.tools.logging :as log]
            [me.raynes.fs :as fs]
            [schema.core :as schema]
            [puppetlabs.i18n.core :refer [trs]]
            [puppetlabs.kitchensink.core :as ks]
            [puppetlabs.trapperkeeper.services.protocols.filesystem-watch-service :refer [Event Watcher]])
  (:import (clojure.lang Atom IFn)
           (java.nio.file StandardWatchEventKinds Path WatchEvent FileSystems ClosedWatchServiceException)
           (com.puppetlabs DirWatchUtils)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Constants
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(def event-type-mappings
  {StandardWatchEventKinds/ENTRY_CREATE :create
   StandardWatchEventKinds/ENTRY_MODIFY :modify
   StandardWatchEventKinds/ENTRY_DELETE :delete
   StandardWatchEventKinds/OVERFLOW :unknown})

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Functions
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(schema/defn clojurize :- Event
  [event :- WatchEvent
   watch-path :- Path]
  {:type (get event-type-mappings (.kind event))
   :path (-> watch-path
             (.resolve (.context event))
             fs/file)})

;; This is quite similar to the function above but it is more a direct
;; conversion of the exact data available on a specific WatchEvent instance,
;; only used for debugging.
(defn clojurize-for-logging
  [e]
  {:context (.context e)
   :count (.count e)
   :kind (.kind e)})

(defn pprint-events
  [events]
  (->> events
       (map #(update % :path str))
       ks/pprint-to-string))

(defn validate-watch-options!
  [options]
  (when-not (= true (:recursive options))
    (throw (IllegalArgumentException. "Support for non-recursive directory watching not yet implemented"))))

(defrecord WatcherImpl
  [watch-service callbacks]
  Watcher
  (add-watch-dir!
    [this dir options]
    (validate-watch-options! options)
    (DirWatchUtils/registerRecursive watch-service
                                     [(.toPath (fs/file dir))]))
  (add-callback!
    [this callback]
    (swap! callbacks conj callback)))

(defn create-watcher
  []
  (map->WatcherImpl {:watch-service (.newWatchService (FileSystems/getDefault))
                     :callbacks (atom [])}))

(schema/defn watch-new-directories!
  [events :- [Event]
   watcher :- (schema/protocol Watcher)]
  (let [dir-create? (fn [event]
                      (and (= :create (:type event))
                           (fs/directory? (:path event))))]
    (DirWatchUtils/registerRecursive (:watch-service watcher)
                                     (->> events
                                          (filter dir-create?)
                                          (map #(.toPath (:path %)))))))

(schema/defn watch!
  "Returns a future representing processing events for the passed in watcher.
  The future will swallow errors, loop producing side-effects until stopped,
  and is expected to coordinate with other functions via the stopped? atom."
  [watcher :- (schema/protocol Watcher)
   stopped? :- Atom
   shutdown-on-error :- IFn]
  (future
    (while (not @stopped?)
      ;; `.take` here will block until there has been an event for this key
      ;;
      ;; If `.close` is called on the `WatchService` while blocking on
      ;; `.take` a `ClosedWatchServiceException` will be raised.
      ;; The `WatchKey` will then be invalidated, so `.reset` will return
      ;; false, but enqueued events will still be available via `.pollEvents`
      ;; and `.watchable` should continue to function as desired.
      (let [watch-key (try
                        (.take (:watch-service watcher))
                        (catch ClosedWatchServiceException e
                          (log/info (trs "Closing watcher {0}" watcher))))
            orig-events (.pollEvents watch-key)
            events (map #(clojurize % (.watchable watch-key)) orig-events)
            callbacks @(:callbacks watcher)]
        (when-not (empty? events)
          (log/info (trs "Got {0} event(s) for watched-path {1}"
                         (count orig-events) (.watchable watch-key)))
          (log/debug (trs "Events:\n{0}"
                          (pprint-events events)))
          (log/trace (trs "orig-events:\n{0}"
                          (ks/pprint-to-string (map clojurize-for-logging orig-events))))
          (shutdown-on-error #(doseq [callback callbacks]
                               (callback events)))
          (watch-new-directories! events watcher)
          (.reset watch-key))))))

