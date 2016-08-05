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
   watched-path :- Path]
  (let [kind (get event-type-mappings (.kind event))
        changed-path (when-not (= :unknown kind)
                       (.context event))
        full-path (when-not (= :unknown kind)
                    (-> watched-path (.resolve changed-path) (.toFile)))]
    {:type kind
     :count (.count event)
     :watched-path (.toFile watched-path)
     :changed-path (when changed-path (.toFile changed-path))
     :full-path full-path}))

(schema/defn format-for-debugging
  [{:keys [changed-path count type]} :- Event]
  {:context changed-path
   :count count
   :kind type})

(schema/defn format-for-consumers
  [{:keys [full-path type]} :- Event]
  {:path full-path
   :type type})

(schema/defn format-for-users
  [event :- Event]
  (update (format-for-consumers event) :path str))

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
                           (fs/directory? (:full-path event))))]
    (DirWatchUtils/registerRecursive (:watch-service watcher)
                                     (->> events
                                          (filter dir-create?)
                                          (map #(.toPath (:full-path %)))))))

(defn get-event-maps-from-key
  [watch-key]
  (let [events (.pollEvents watch-key)]
    (map #(clojurize % (.watchable watch-key)) events)))

(schema/defn retrieve-events :- [Event]
  "Blocks until an event the watcher is concerned with has occured. Will then
  poll for a new event, watiting at least `window-min` for a new event to
  occur. Will continue polling for as long as there are new events that occur
  within `window-min`, or the `window-max` time limit has been exceeded."
  [watcher :- (schema/protocol Watcher)]
  (let [watch-key (.take (:watch-service watcher))
        events (get-event-maps-from-key watch-key)
        time-limit (+ (System/currentTimeMillis) window-max)]
    (.reset watch-key)
    (watch-new-directories! events watcher)
      (loop [events' events]
        (if-let [waiting-key (.poll (:watch-service watcher) window-min window-units)]
          (let [waiting-events (get-event-maps-from-key waiting-key)]
            (watch-new-directories! waiting-events watcher)
            (.reset waiting-key)
            (if (< (System/currentTimeMillis) time-limit)
              (recur (into events' waiting-events))
              (into events' waiting-events)))
          events'))))


(schema/defn process-events!
  "Process for side-effects any events that occured for watcher's watch-key"
  [watcher :- (schema/protocol Watcher)
   events :- [Event]]
  (let [callbacks @(:callbacks watcher)]
    (log/info (trs "Got {0} event(s) on path(s) {1}"
                   (count events) (distinct (map #(:full-path %) events))))
    (log/debugf "%s\n%s"
                (trs "Events:")
                (ks/pprint-to-string
                  (map format-for-users events)))
    (log/tracef "%s\n%s"
                (trs "orig-events:")
                (ks/pprint-to-string
                  (map format-for-debugging events)))
    (doseq [callback callbacks]
      (callback (map format-for-consumers events)))))

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

