(ns puppetlabs.services.watcher.filesystem-watch-core
  (:require [clojure.tools.logging :as log]
            [me.raynes.fs :as fs]
            [schema.core :as schema]
            [puppetlabs.i18n.core :refer [trs]]
            [puppetlabs.kitchensink.core :as ks]
            [puppetlabs.services.protocols.filesystem-watch-service :as watch-protocol])
  (:import (clojure.lang Atom IFn)
           (java.nio.file StandardWatchEventKinds Path WatchEvent WatchService)
           (com.puppetlabs DirWatchUtils)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Constants
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(def poll-interval-ms
  1000) ;; 1 second

(def event-type-mappings
  {StandardWatchEventKinds/ENTRY_CREATE :create
   StandardWatchEventKinds/ENTRY_MODIFY :modify
   StandardWatchEventKinds/ENTRY_DELETE :delete
   StandardWatchEventKinds/OVERFLOW :unknown})

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Schemas
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(def Watcher
  {:watch-service WatchService
   :callbacks (schema/atom [IFn])})

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Functions
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(schema/defn clojurize :- watch-protocol/Event
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

(defn watch-new-directories!
  [events watcher]
  (let [dir-create? (fn [event]
                      (and (= :create (:type event))
                           (fs/directory? (:path event))))]
    (DirWatchUtils/registerRecursive (:watch-service watcher)
                                     (filter dir-create? events))))

(defn reset-key-and-unwatch-deleted!
  [watch-key]
  ;; reset key and remove from set if directory no longer accessible
  (let [valid? (.reset watch-key)]
    (when (not valid?)
      ;; TODO: not sure if we need to do anything else here now that we're not retaining a map of
      ;; these keys.  The Oracle example code makes it seem like maybe you do, but I can't tell what.
      (log/info (trs "Removing watched directory {0} because it was deleted" (.watchable watch-key))))))

;; This function (and other helper functions in this namespace)
;; are heavily influenced by WatchDir.java from Java's official documentation:
;; https://docs.oracle.com/javase/tutorial/essential/io/notification.html
(schema/defn handle-watch-events!
  [watchers :- [Watcher]
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

  (doseq [watcher watchers]
    (when-let [watch-key (.poll (:watch-service watcher))]
      (let [orig-events (.pollEvents watch-key)
            events (map #(clojurize % (.watchable watch-key)) orig-events)
            callbacks @(:callbacks watcher)]
        ;; Sometimes, .pollEvents returns an empty list
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
          (reset-key-and-unwatch-deleted! watch-key))))))

(defn handle-events-and-reschedule!
  [watchers after stopped? shutdown-on-error]
  (when-not @stopped?
    (handle-watch-events! @watchers shutdown-on-error)
    (after poll-interval-ms
     #(handle-events-and-reschedule! watchers
                                     after
                                     stopped?
                                     shutdown-on-error))))

(schema/defn ^:always-validate schedule-watching!
  [watchers :- (schema/atom [Watcher])
   after :- IFn
   stopped? :- Atom
   shutdown-on-error :- IFn]
  (after poll-interval-ms
   #(handle-events-and-reschedule! watchers
                                   after
                                   stopped?
                                   shutdown-on-error)))
