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
  "Takes the Java WatchEvent and the watchable Java Path that the event
  occurred within and creates a Clojure map to represent the Event
  throughout the system. The watched-path key is the registered watchable,
  the changed-path the relative path to what changed, and the full path is
  the absolute path to the changed event (watched-path + changed-path)."
  [event :- WatchEvent
   watched-path :- Path]
  (let [kind (get event-type-mappings (.kind event))
        count (.count event)]
    (if (= :unknown kind)
      {:type kind
       :count count
       :watched-path (.toFile watched-path)}
      {:type kind
       :count count
       :watched-path (.toFile watched-path)
       :changed-path (.. watched-path (resolve (.context event)) (toFile))})))

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
                           (fs/directory? (:changed-path event))))]
    (DirWatchUtils/registerRecursive (:watch-service watcher)
                                     (->> events
                                          (filter dir-create?)
                                          (map #(.toPath (:changed-path %)))))))

(schema/defn watch-key->events :- [Event]
  [watch-key :- WatchKey]
  (let [events (.pollEvents watch-key)]
    (map #(clojurize % (.watchable watch-key)) events)))

(schema/defn retrieve-events :- [Event]
  "Blocks until an event the watcher is concerned with has occured. Will then
  poll for a new event, waiting at least `window-min` for a new event to
  occur. Will continue polling for as long as there are new events that occur
  within `window-min`, or the `window-max` time limit has been exceeded."
  [watcher :- (schema/protocol Watcher)]
  (let [watch-key (.take (:watch-service watcher))
        initial-events (watch-key->events watch-key)
        time-limit (+ (System/currentTimeMillis) window-max)]
    (watch-new-directories! initial-events watcher)
    (.reset watch-key)
    (if-not (empty? initial-events)
      (loop [events initial-events]
        (if-let [waiting-key (.poll (:watch-service watcher) window-min window-units)]
          (let [waiting-events (watch-key->events waiting-key)]
            (watch-new-directories! waiting-events watcher)
            (.reset waiting-key)
            (if (< (System/currentTimeMillis) time-limit)
              (recur (concat events waiting-events))
              (concat events waiting-events)))
          events))
      initial-events)))


(schema/defn process-events!
  "Process for side-effects any events that occured for watcher's watch-key"
  [watcher :- (schema/protocol Watcher)
   events :- [Event]]
  (let [callbacks @(:callbacks watcher)
        events-by-dir (group-by :watched-path events)]
    (doseq [[dir events'] events-by-dir]
      (log/debug (trs "Got {0} event(s) in directory {1}"
                   (count events') dir)))
    (log/tracef "%s\n%s"
                (trs "Events:")
                (ks/pprint-to-string events))
    (doseq [callback callbacks]
      (callback events))))

(schema/defn watch!
  "Creates a future and processes events for the passed in watcher.
  The future will continue until the underlying WatchService is closed."
  [watcher :- (schema/protocol Watcher)
   shutdown-fn :- IFn]
  (future
    (let [stopped? (atom false)]
      (shutdown-fn #(while (not @stopped?)
                     (try
                       (let [events (retrieve-events watcher)]
                         (when-not (empty? events)
                           (process-events! watcher events)))
                      (catch ClosedWatchServiceException e
                        (reset! stopped? true)
                        (log/info (trs "Closing watcher {0}" watcher)))))))))

