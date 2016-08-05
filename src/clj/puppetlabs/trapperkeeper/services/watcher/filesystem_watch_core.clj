(ns puppetlabs.trapperkeeper.services.watcher.filesystem-watch-core
  (:require [clojure.tools.logging :as log]
            [me.raynes.fs :as fs]
            [schema.core :as schema]
            [puppetlabs.i18n.core :refer [trs]]
            [puppetlabs.kitchensink.core :as ks]
            [puppetlabs.trapperkeeper.services.protocols.filesystem-watch-service :refer [Event Watcher]])
  (:import (clojure.lang Atom IFn)
           (java.nio.file StandardWatchEventKinds Path WatchEvent WatchKey FileSystems ClosedWatchServiceException)
           (com.puppetlabs DirWatchUtils)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Constants
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(def event-type-mappings
  {StandardWatchEventKinds/ENTRY_CREATE :create
   StandardWatchEventKinds/ENTRY_MODIFY :modify
   StandardWatchEventKinds/ENTRY_DELETE :delete
   StandardWatchEventKinds/OVERFLOW :unknown})

(def window-min 100)

(def window-max 2000)

(def window-units java.util.concurrent.TimeUnit/MILLISECONDS)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Functions
;;;
;;; Helper functions in this namespace are heavily influenced by WatchDir.java
;;; from Java's official documentation:
;;; https://docs.oracle.com/javase/tutorial/essential/io/notification.html
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(schema/defn clojurize :- Event
  [event :- WatchEvent
   watch-path :- Path]
  {:type (get event-type-mappings (.kind event))
   :path (when-not (= StandardWatchEventKinds/OVERFLOW (.kind event))
           (-> watch-path
               (.resolve (.context event))
               fs/file))})

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
    (throw
      (IllegalArgumentException.
        (trs "Support for non-recursive directory watching not yet implemented")))))

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
  (map->WatcherImpl
    {:watch-service (.newWatchService (FileSystems/getDefault))
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

(schema/defn retrieve-events :- {WatchKey [WatchEvent]}
  "Blocks until an event the watcher is concerned with has occured. Will then
  poll for a new event, watiting at least `window-min` for a new event to
  occur. Will continue polling for as long as there are new events that occur
  within `window-min`, or the `window-max` time limit has been exceeded."
  [watcher :- (schema/protocol Watcher)]
  (let [watch-key (.take (:watch-service watcher))
        events (vec (.pollEvents watch-key))
        time-limit (+ (System/currentTimeMillis) window-max)
        events-by-key {watch-key events}]
    (.reset watch-key)
    (watch-new-directories! (map #(clojurize % (.watchable watch-key)) events) watcher)
      (loop [keys-events events-by-key]
        (if-let [waiting-key (.poll (:watch-service watcher) window-min window-units)]
          (let [waiting-events (vec (.pollEvents waiting-key))]
            (watch-new-directories! (map #(clojurize % (.watchable waiting-key)) waiting-events) watcher)
            (.reset waiting-key)
            (if (< (System/currentTimeMillis) time-limit)
              (recur (update keys-events waiting-key into waiting-events))
              (update keys-events waiting-key waiting-events)))
          keys-events))))


(schema/defn process-events!
  "Process for side-effects any events that occured for watcher's watch-key"
  [watcher :- (schema/protocol Watcher)
   events-by-key :- {WatchKey [WatchEvent]}]
  (let [orig-events (flatten (vals events-by-key))
        clojure-events (mapcat
                         (fn [kv] (map #(clojurize % (.watchable (first kv))) (second kv)))
                         events-by-key)
        callbacks @(:callbacks watcher)]
    (log/info (trs "Got {0} event(s) on path(s) {1}"
                   (count orig-events) (map #(.watchable %) (keys events-by-key))))
    (log/debugf "%s\n%s"
                (trs "Events:")
                (pprint-events clojure-events))
    (log/tracef "%s\n%s"
                (trs "orig-events:")
                (ks/pprint-to-string
                  (map clojurize-for-logging orig-events)))
    (doseq [callback callbacks]
      (callback clojure-events))))

(schema/defn watch!
  "Creates a future and processes events for the passed in watcher.
  The future will continue until the underlying WatchService is closed."
  [watcher :- (schema/protocol Watcher)
   shutdown-fn :- IFn]
  (future
    (let [stopped? (atom false)]
      (shutdown-fn #(while (not @stopped?)
                     (try
                       (let [events-by-key (retrieve-events watcher)]
                         (when-not (empty? (vals events-by-key))
                           (process-events! watcher events-by-key)))
                      (catch ClosedWatchServiceException e
                        (reset! stopped? true)
                        (log/info (trs "Closing watcher {0}" watcher)))))))))

