(ns puppetlabs.enterprise.services.watcher.filesystem-watch-core
  (:require [clojure.tools.logging :as log]
            [me.raynes.fs :as fs]
            [schema.core :as schema]
            [puppetlabs.i18n.core :refer [trs]]
            [puppetlabs.kitchensink.core :as ks]
            [puppetlabs.enterprise.services.protocols.filesystem-watch-service :as watch-protocol])
  (:import (clojure.lang Atom IFn)
           (java.nio.file StandardWatchEventKinds WatchService Path WatchEvent)
           (java.util Map)
           (com.puppetlabs.enterprise DirWatchUtils)))

(def poll-interval-ms
  1000) ;; 1 second

(defn normalized-path-str
  [path]
  (str (fs/normalized path)))

(defn validate-path!
  [existing-paths new-path]
  (when (contains? (set existing-paths) new-path)
    (throw (IllegalArgumentException. (trs "Path {0} is already watched." new-path))))
  (when-not (fs/directory? new-path)
    (throw (IllegalArgumentException. (trs "Path {0} is not a directory." new-path))))
  (doseq [path existing-paths]
    (when (fs/child-of? path new-path)
      (throw (IllegalArgumentException.
              (trs "Path {0} may not be a subdirectory of existing path {1}." new-path path))))
    (when (fs/child-of? new-path path)
      (throw (IllegalArgumentException.
              (trs "Path {0} may not contain existing path {1}." path new-path))))))

(def event-type-mappings
  {StandardWatchEventKinds/ENTRY_CREATE :create
   StandardWatchEventKinds/ENTRY_MODIFY :modify
   StandardWatchEventKinds/ENTRY_DELETE :delete})

(schema/defn clojurize :- watch-protocol/Event
  [event :- WatchEvent
   watch-path :- Path]
  {:type (get event-type-mappings (.kind event))
   :path (-> watch-path
             (.resolve (.context event))
             fs/normalized)})

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

(defn get-callback
  [path-callbacks changed-path]
  (let [result (reduce
                (fn [acc [root callback]]
                  (if (or (= root changed-path)
                          (fs/child-of? root changed-path)
                          (fs/child-of? changed-path root))
                    (conj acc callback)
                    acc))
                []
                path-callbacks)]
    (if (= 1 (count result))
      (first result)
      (throw (IllegalStateException.
              (trs "did not find unique watched path for changed path: {0}" result))))))

(defn handle-overflow-events!
  [events]
  (when (some #(= (.kind %) StandardWatchEventKinds/OVERFLOW) events)
    (throw (IllegalStateException. "don't know how to handle OVERFLOW event kind"))))

(defn watch-new-directories!
  [events watch-service watch-keys]
  (let [dir-create? (fn [event]
                      (and (= :create (:type event))
                           (fs/directory? (:path event))))]
    (DirWatchUtils/registerRecursive watch-service
                                     (filter dir-create? events)
                                     watch-keys)))

(defn reset-key-and-unwatch-deleted!
  [watched-path watch-keys watch-key]
  ;; reset key and remove from set if directory no longer accessible
  (let [valid? (.reset watch-key)]
    (when (not valid?)
      (log/info (trs "Removing watched directory {0} because it was deleted" watched-path))
      (.remove watch-keys watch-key)
      (when (.isEmpty watch-keys)
        ;; all directories are inaccessible
        (log/warn
         (trs "Filesystem polling will stop because all watched directories have been deleted"))))))

;; This function (and other helper functions in this namespace)
;; are heavily influenced by WatchDir.java from Java's official documentation:
;; https://docs.oracle.com/javase/tutorial/essential/io/notification.html
(schema/defn handle-watch-events!
  [path-callbacks :- Atom
   watch-service :- WatchService
   watch-keys :- Map
   stopped? :- Atom
   shutdown-on-error :- IFn]

  ;; If we were going to add any additional "debouncing" here I think it
  ;; would look something like this:
  ;;
  ;(let [mas? (atom true)
  ;      watch-keys (atom [])]
  ;  (while @mas?
  ;    (if-let [watch-key (.poll watch-service)]
  ;      (swap! watch-keys conj watch-key)
  ;      (reset! mas? false))))
  ;;
  ;; (yes, I know that code is ugly as sin, and actually, as Chris points out,
  ;; perhaps what we'd want to do is something like write a small implementation
  ;; of Java's Iterator interface that wrapped the call to .poll,
  ;; and then you could use Clojure's iterator-seq.)
  ;;
  ;; ... The idea being that we'd collect all of the available watch keys
  ;; first, and then process them as a batch.  I think this could result
  ;; in fewer callback invocations for nested watch keys (i.e. files
  ;; nested under the root watched path) but the extent to which that is
  ;; true is likely platform-dependent.
  ;;
  ;; https://tickets.puppetlabs.com/browse/PE-15621

  (when-let [watch-key (.poll watch-service)]
    (if-let [watch-path (get watch-keys watch-key)]
      (let [path (normalized-path-str watch-path)
            orig-events (.pollEvents watch-key)
            events (map #(clojurize % watch-path) orig-events)
            callback (get-callback @path-callbacks path)]
        ;; Sometimes, .pollEvents returns an empty list
        (when-not (empty? events)
          (log/info (trs "Got {0} event(s) for watched-path {1}"
                         (count orig-events) path))
          (log/debug (trs "Events:\n{0}"
                          (pprint-events events)))
          (log/trace (trs "orig-events:\n{0}"
                          (ks/pprint-to-string (map clojurize-for-logging orig-events))))
          (handle-overflow-events! orig-events)
          (shutdown-on-error #(callback events))
          (watch-new-directories! events watch-service watch-keys)
          (reset-key-and-unwatch-deleted! watch-path watch-keys watch-key)))
      (throw (IllegalStateException. "WatchKey not recognized!!")))))

(defn handle-events-and-reschedule!
  [path-callbacks watch-service watch-keys after stopped? shutdown-on-error]
  (when-not @stopped?
    (handle-watch-events! path-callbacks
                          watch-service
                          watch-keys
                          stopped?
                          shutdown-on-error)
    (after poll-interval-ms
     #(handle-events-and-reschedule! path-callbacks
                                     watch-service
                                     watch-keys
                                     after
                                     stopped?
                                     shutdown-on-error))))

(schema/defn ^:always-validate schedule-watching!
  [path-callbacks :- Atom
   watch-service :- WatchService
   watch-keys :- Map
   after :- IFn
   stopped? :- Atom
   shutdown-on-error :- IFn]
  (after poll-interval-ms
   #(handle-events-and-reschedule! path-callbacks
                                   watch-service
                                   watch-keys
                                   after
                                   stopped?
                                   shutdown-on-error)))
