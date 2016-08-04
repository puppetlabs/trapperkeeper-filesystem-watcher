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

(schema/defn retrieve-events
  :- [(schema/one WatchKey "key") (schema/one [WatchEvent] "events")]
  "Blocks until an event the watcher is concerned with has occured.
  Returns the native WatchKey and WatchEvents"
  [watcher :- (schema/protocol Watcher)]
  (let [watch-key (.take (:watch-service watcher))
        events (.pollEvents watch-key)]
    (.reset watch-key)
    (watch-new-directories! (map #(clojurize % (.watchable watch-key)) events) watcher)
    [watch-key events]))

(schema/defn process-events!
  "Process for side-effects any events that occured for watcher's watch-key"
  [watcher :- (schema/protocol Watcher)
   watch-key :- WatchKey
   orig-events :- [WatchEvent]]
  (let [clojure-events (map #(clojurize % (.watchable watch-key)) orig-events)
        callbacks @(:callbacks watcher)]
    (log/info (trs "Got {0} event(s) for watched-path {1}"
                   (count orig-events) (.watchable watch-key)))
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
                       (let [[watch-key events] (retrieve-events watcher)]
                         (when-not (empty? events)
                           (process-events! watcher watch-key events)))
                      (catch ClosedWatchServiceException e
                        (reset! stopped? true)
                        (log/info (trs "Closing watcher {0}" watcher)))))))))

