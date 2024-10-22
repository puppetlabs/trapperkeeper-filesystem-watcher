(ns puppetlabs.trapperkeeper.services.watcher.filesystem-watch-core
  (:require [clojure.set :as set]
            [clojure.tools.logging :as log]
            [me.raynes.fs :as fs]
            [schema.core :as schema]
            [puppetlabs.i18n.core :refer [trs]]
            [puppetlabs.kitchensink.core :as ks]
            [puppetlabs.trapperkeeper.services.protocols.filesystem-watch-service :refer [Event Watcher] :as watch-protocol])
  (:import (clojure.lang IFn)
           (java.io File)
           (java.nio.file StandardWatchEventKinds Path WatchEvent WatchKey FileSystems ClosedWatchServiceException WatchService)
           (com.puppetlabs DirWatchUtils)
           (java.util.concurrent Future)))

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
       :changed-path (.. watched-path (resolve ^Path (.context event)) (toFile))})))

(defn validate-watch-options!
  "Validate that the options supplied include a valid Boolean value for key :recursive."
  [options]
  (when-not (instance? Boolean (:recursive options))
    (throw
      (IllegalArgumentException.
        (trs "Must pass a boolean value for :recursive (directory watching) option")))))

(def all-types #{:create :modify :delete :unknown})

(defrecord WatcherImpl
  [watch-service callbacks recursive]
  Watcher
  (add-watch-dir!
    [_this dir]
    (let [watched-path (.toPath (fs/file dir))]
      (if @recursive
        (DirWatchUtils/registerRecursive watch-service [watched-path])
        (DirWatchUtils/register watch-service watched-path))))

  (add-watch-dir!
    [this dir options]
    (validate-watch-options! options)
    ;; The value of recursive was already set by `create-watcher`, possibly by specifying an option.
    ;; We validate that the option supplied to `add-watch-dir!` has the same value.
    (log/debug
      (trs "Passing options to `add-watch-dir!` is deprecated. Pass options to `create-watcher` instead"))
    (when-not (= @recursive (:recursive options))
      (throw
        (IllegalArgumentException.
          (trs "Watcher already set to :recursive {0}, cannot change to :recursive {1}"
               @recursive (:recursive options)))))
    (watch-protocol/add-watch-dir! this dir))

  (add-file-callback!
    [this path-to-file callback]
    (watch-protocol/add-file-callback! this path-to-file callback all-types))

  (add-file-callback!
    [_this path-to-file callback types]
    (swap! callbacks conj {:callback callback :file path-to-file :types (set types)}))

  (add-callback!
    [this callback]
    (watch-protocol/add-callback! this callback all-types))

  (add-callback!
    [_this callback types]
    (swap! callbacks conj {:callback callback :types (set types)})))

(defn create-watcher
  ([]
   (create-watcher {:recursive true}))
  ([{:keys [recursive] :as options}]
   (validate-watch-options! options)
   (map->WatcherImpl
     {:watch-service (.newWatchService (FileSystems/getDefault))
      :callbacks (atom [])
      :recursive (atom recursive)})))

(schema/defn watch-new-directories!
  "Given an initial set of events and a watcher, identify any events that
  represent the creation of a directory and register them with the watch service.
  If no events are directory creations, nothing is registered."
  [events :- [Event]
   watcher :- (schema/protocol Watcher)]
  (let [dir-create? (fn [event]
                      (and (= :create (:type event))
                           (fs/directory? (:changed-path event))))]
    (DirWatchUtils/registerRecursive ^WatchService (:watch-service watcher)
                                     (->> events
                                          (filter dir-create?)
                                          (map #(.toPath ^File (:changed-path %)))))))

(schema/defn watch-key->events :- [Event]
  [watch-key :- WatchKey]
  (let [events (.pollEvents watch-key)]
    (map #(clojurize % (.watchable watch-key)) events)))

(schema/defn retrieve-events :- [Event]
  "Blocks until an event the watcher is concerned with has occurred. Will then
  poll for a new event, waiting at least `window-min` for a new event to
  occur. Will continue polling for as long as there are new events that occur
  within `window-min`, or the `window-max` time limit has been exceeded."
  [watcher :- (schema/protocol Watcher)]
  (let [^WatchService watch-service (:watch-service watcher)
        watch-key (.take watch-service)
        ;; use `vec` to ensure the events sequence is correctly concatenated below.
        ;; Without it, the events will be out of order.
        initial-events  (vec (watch-key->events watch-key))
        time-limit (+ (System/currentTimeMillis) window-max)
        recursive @(:recursive watcher)]
    (when recursive
      (watch-new-directories! initial-events watcher))
    (.reset watch-key)
    (if-not (empty? initial-events)
      (loop [events initial-events]
        (if-let [waiting-key (.poll watch-service window-min window-units)]
          (let [waiting-events (watch-key->events waiting-key)]
            (when recursive
              (watch-new-directories! waiting-events watcher))
            (.reset waiting-key)
            (if (< (System/currentTimeMillis) time-limit)
              (recur (into events waiting-events))
              (into events waiting-events)))
          events))
      initial-events)))

(defn filter-events
  [callback-entry events]
  (let [allowed-types (:types callback-entry)
        event-filter (fn [event] (contains? allowed-types (:type event)))
        ;; construct an optimal filter function based on the type of filtering needed
        filter-fn (if (nil? (:file callback-entry))
                    ;; check the type match only
                    event-filter
                    ;; check the file as well as the type match
                    (fn [event] (and (event-filter event)
                                     (= (.toPath ^File (:changed-path event)) (:file callback-entry)))))]
    (filter filter-fn events)))

(defn ->tuple
  "Create a tuple of the two fields we care about in an Event.
  This simple element querying our sets simple."
  [event]
  [(:changed-path event) (:type event)])

(defn already-seen?
  "Expects a set of event tuples (see ->tuple) and a full Event.
  Is this a modify event for a changed-path where we've already seen a create or modify event?"
  [events {:keys [changed-path type] :as _event}]
  (when (= :modify type)
     (or (set/subset? #{[changed-path :create]} events)
         (set/subset? #{[changed-path :modify]} events))))

(defn should-overwrite?
  "Expects a set of event tuples (see ->tuple) and a full Event.
  Is this a create event that should supersede a previously found modify or create event?"
  [events {:keys [changed-path type] :as _event}]
  (when (= :create type)
     (or (set/subset? #{[changed-path :modify]} events)
         (set/subset? #{[changed-path :create]} events))))

(defn reducible-remove
  [{:keys [tuples] :as memo} item]
  (cond
    ;; Ignore the new item if we've already seen a value representing it
    (already-seen? tuples item)
      memo
    ;; Remove old value and replace it with the new value
    (should-overwrite? tuples item)
      (-> memo
          (update :tuples set/difference #{[(:changed-path item) :modify]})
          (update :tuples set/union #{(->tuple item)})
          (update :events (fn [es] (filter #(and (= (:changed-path item) (:changed-path %))
                                                (or
                                                  (= :modify (:type %))
                                                  (= :create (:type %))))
                                           es)))
          (update :events conj item))
    ;; default add the event to our tracker
    :else
      (-> memo
          (update :tuples set/union #{(->tuple item)})
          (update :events conj item))))

(schema/defn process-events!
  "Process for side effects any events that occurred for watcher's watch-key"
  [watcher :- (schema/protocol Watcher)
   events :- [Event]]
  (let [callbacks @(:callbacks watcher)
        compressed-events (->> events (reduce reducible-remove {:tuples #{} :events []}) :events)]
    ;; avoid doing a potentially expensive walk when we aren't logging at :debug
    (when (log/enabled? :debug)
      (let [events-by-dir (group-by :watched-path compressed-events)]
        (doseq [[dir events'] events-by-dir]
          (log/debug (trs "Got {0} event(s) in directory {1}"
                       (count events') dir)))))

    ;; avoid doing a potentially expensive print-to-string when we aren't logging at :trace
    (when (log/enabled? :trace)
      (log/tracef "%s\n%s"
                  (trs "Events:")
                  (ks/pprint-to-string compressed-events)))

    (doseq [callback-entry callbacks]
      ((:callback callback-entry) (filter-events callback-entry compressed-events)))))

(schema/defn watch! :- Future
  "Creates and returns a future. Processes events for the passed in watcher within the context of that future.
  The future will continue until the underlying WatchService is closed, or the future is interrupted."
  [watcher :- (schema/protocol Watcher)
   shutdown-fn :- IFn
   stopped? :- (schema/atom schema/Bool)]
  (future
    (shutdown-fn
      #(while (not @stopped?)
         (try
           (let [events (retrieve-events watcher)]
             (when-not (empty? events)
               (process-events! watcher events)))
          (catch ClosedWatchServiceException _e
            (log/info (trs "Closing watcher {0}" watcher)))
          ;; it is possible for `retrieve-events` to generate an InterruptedException if the `.take` occurs when an
          ;; interrupt is requested. This is explicitly handled to prevent shutting down the whole application.
          (catch InterruptedException _e
            (log/info (trs "Watching for events interrupted by thread shutdown")))
          (catch Throwable e
            (log/error e (trs "Fatal error while watching for events"))
            (throw e)))))))

