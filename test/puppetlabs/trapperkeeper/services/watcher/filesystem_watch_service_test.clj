(ns puppetlabs.trapperkeeper.services.watcher.filesystem-watch-service-test
  (:require [clojure.test :refer [deftest testing is use-fixtures]]
            [clojure.set :as set]
            [schema.test :as schema-test]
            [me.raynes.fs :as fs]
            [puppetlabs.trapperkeeper.services.protocols.filesystem-watch-service :refer :all]
            [puppetlabs.trapperkeeper.services.watcher.filesystem-watch-service :refer [filesystem-watch-service]]
            [puppetlabs.trapperkeeper.services.watcher.filesystem-watch-core :as watch-core]
            [puppetlabs.trapperkeeper.testutils.bootstrap :refer [with-app-with-config]]
            [puppetlabs.trapperkeeper.testutils.logging :refer [with-test-logging]]
            [puppetlabs.trapperkeeper.core :as tk]
            [puppetlabs.trapperkeeper.app :as tk-app]
            [puppetlabs.trapperkeeper.internal :as tk-internal])
  (:import (com.puppetlabs DirWatchUtils)
           (java.net URI)
           (java.nio.file FileSystems FileSystem Paths)))

(use-fixtures :once schema-test/validate-schemas)

(defn make-callback
  [dest]
  (fn [events]
    (swap! dest concat events)))

(defn contains-events?
  [dest events]
  (let [select-keys-set (set (map #(select-keys % [:changed-path :type]) @dest))]
     (set/subset? events select-keys-set)))

(defn exactly-matches-event?
  [dest expected-event]
  (let [select-keys-set (set (map #(select-keys % [:changed-path :type]) @dest))]
    (= expected-event select-keys-set)))


(def wait-time
  "Number of milliseconds wait-for-events! should wait."
  (* 10 1000)) ;; 10 seconds

(defn wait-for-events
  "Waits for events to land in dest.  If they do, events is returned.  Gives up
  and returns the contents of dest after wait-time."
  [dest events]
  (let [start-time (System/currentTimeMillis)]
    (loop []
      (let [elapsed-time (- (System/currentTimeMillis) start-time)]
        (if (contains-events? dest events)
          events
          (if (< elapsed-time wait-time)
            (recur)
            (do
              (println "timed-out waiting for events")
              @dest)))))))

(defn wait-for-exactly-event
  "Waits for event to land in dest. On first registered event or events in dest,
  determines whether the registered event(s) match the supplied expected
  event. If so, returns the expected event. Otherwise returns contents of dest."
  [dest expected-event]
  (let [start-time (System/currentTimeMillis)]
    (loop [elapsed-time 0]
      (if-not (empty? @dest)
        (if (exactly-matches-event? dest expected-event)
          expected-event
          @dest)
        (if (< elapsed-time wait-time)
          (recur (- (System/currentTimeMillis) start-time))
          (do
            (println "timed-out waiting for events")
            @dest))))))

(defn watch!*
  [watcher root callback]
  (add-watch-dir! watcher root {:recursive true})
  (add-callback! watcher callback))

;; TODO perhaps move this (or something similar) up to the TK protocol
(defn watch!
  [service root callback]
  (watch!* (create-watcher service) root callback))

(deftest ^:integration single-path-test
  (let [root (fs/temp-dir "single-path-test")
        first-file (fs/file root "first-file")
        second-file (fs/file root "second-file")
        results (atom [])
        callback (make-callback results)]
    (with-app-with-config
     app [filesystem-watch-service] {}
     (let [service (tk-app/get-service app :FilesystemWatchService)]
       (watch! service root callback))
     (testing "callback not invoked until directory changes"
       (is (= @results [])))
     (testing "callback is invoked when a new file is created"
       (spit first-file "foo")
       (let [events #{{:changed-path first-file
                       :type :create}}]
         ;; This is the first of many weird assertions like this, but it's done
         ;; this way on purpose to get decent reporting from failed assertions.
         ;; See above the docstring on wait-for-events.
         (is (= events (wait-for-events results events)))))
     (testing "callback invoked again when another new file is created"
       (reset! results [])
       (spit second-file "bar")
       (let [events #{{:changed-path second-file
                       :type :create}}]
         (is (= events (wait-for-events results events)))))
     (testing "watch-dir! also reports file modifications"
       (testing "of a single file"
         (reset! results [])
         (spit first-file "something different")
         (let [events #{{:changed-path first-file
                  :type :modify}}]
           (is (= events (wait-for-events results events)))))
       (testing "of multiple files"
         (reset! results [])
         (spit first-file "still not the same as before")
         (spit second-file "still not the same as before")
         (let [events #{{:changed-path first-file
                         :type :modify}
                        {:changed-path second-file
                         :type :modify}}]
           (is (= events (wait-for-events results events))))))
     (testing "watch-dir! also reports file deletions"
       (testing "of multiple files"
         (reset! results [])
         (is (fs/delete first-file))
         (is (fs/delete second-file))
         (let [events #{{:changed-path first-file
                         :type :delete}
                        {:changed-path second-file
                         :type :delete}}]
           (is (= events (wait-for-events results events))))))
     (testing "re-creation of a deleted directory"
       (reset! results [])
       (let [sub-dir (fs/file root "sub-dir")]
         (testing "Initial directory creation and deletion"
           (is (fs/mkdir sub-dir))
           (let [events #{{:changed-path sub-dir
                           :type :create}}]
             (is (= events (wait-for-events results events))))
           (is (fs/delete sub-dir))
           (let [events #{{:changed-path sub-dir
                           :type :delete}}]
             (is (= events (wait-for-events results events)))))
         (testing "Re-creating the directory fires an event as expected"
           (reset! results [])
           (is (fs/mkdir sub-dir))
           (let [events #{{:changed-path sub-dir
                           :type :create}}]
             (is (= events (wait-for-events results events))))))))))

(deftest ^:integration multi-callbacks-test
  (let [root-1 (fs/temp-dir "multi-root-test-1")
        root-2 (fs/temp-dir "multi-root-test-2")
        root-1-file (fs/file root-1 "test-file")
        root-2-file (fs/file root-2 "test-file")
        results-1 (atom [])
        results-2 (atom [])
        callback-1 (make-callback results-1)
        callback-2 (make-callback results-2)]
    (with-app-with-config
     app [filesystem-watch-service] {}
     (let [service (tk-app/get-service app :FilesystemWatchService)]
       (watch! service root-1 callback-1)
       (watch! service root-2 callback-2))
     (testing "callback-1 is invoked when root-1 changes"
       (spit root-1-file "foo")
       (let [events #{{:changed-path root-1-file
                       :type :create}}]
         (is (= events (wait-for-events results-1 events))))
       (testing "but not callback-2"
         (is (= @results-2 []))))
     (testing "callback-2 is invoked when root-2 changes"
       (reset! results-1 [])
       (spit root-2-file "foo")
       (let [events #{{:changed-path root-2-file
                       :type :create}}]
         (is (= events (wait-for-events results-2 events))))
       (testing "but not callback-1"
         (is (= @results-1 []))))
     (testing "both callbacks invoked when both roots change"
       (reset! results-1 [])
       (reset! results-2 [])
       (spit root-1-file "bar")
       (spit root-2-file "bar")
       (let [events-1 #{{:changed-path root-1-file
                         :type :modify}}
             events-2 #{{:changed-path root-2-file
                         :type :modify}}]
         (is (= events-1 (wait-for-events results-1 events-1)))
         (is (= events-2 (wait-for-events results-2 events-2))))))))

(deftest ^:integration nested-files-test
  (let [root-dir (fs/temp-dir "root-dir")
        intermediate-dir (fs/file root-dir "intermediate-dir")
        nested-dir (fs/file intermediate-dir "nested-dir")
        results (atom [])
        callback (make-callback results)]
    (is (fs/mkdirs nested-dir))
    (with-app-with-config
     app [filesystem-watch-service] {}
     (let [service (tk-app/get-service app :FilesystemWatchService)]
       (watch! service root-dir callback))
     (testing "file creation at root dir"
       (let [test-file (fs/file root-dir "foo")]
         (spit test-file "foo")
         (let [events #{{:changed-path test-file
                         :type :create}}]
           (is (= events (wait-for-events results events))))))
     (testing "file creation at one level down"
       (let [test-file (fs/file intermediate-dir "foo")]
         (reset! results [])
         (spit test-file "foo")
         (let [events #{{:changed-path test-file
                         :type :create}}]
           (is (= events (wait-for-events results events))))))
     (testing "file creation two levels down"
       (let [test-file (fs/file nested-dir "foo")]
         (reset! results [])
         (spit test-file "foo")
         (let [events #{{:changed-path test-file
                         :type :create}}]
           (is (= events (wait-for-events results events))))))
     (let [root-file (fs/file root-dir "bar")
           intermediate-file (fs/file intermediate-dir "bar")
           nested-file (fs/file nested-dir "bar")]
       (testing "file creation at all three levels"
         (reset! results [])
         (spit root-file "bar")
         (spit nested-file "bar")
         (spit intermediate-file "bar")
         (let [events #{{:changed-path root-file
                         :type :create}
                        {:changed-path nested-file
                         :type :create}
                        {:changed-path intermediate-file
                         :type :create}}]
           (is (= events (wait-for-events results events)))))
       (testing "modifying a nested file"
         (reset! results [])
         (spit nested-file "something new")
         (let [events #{{:changed-path nested-file
                         :type :modify}}]
           (is (= events (wait-for-events results events)))))
       (testing "deletion of a nested file"
         (reset! results [])
         (is (fs/delete nested-file))
         (let [events #{{:changed-path nested-file
                         :type :delete}}]
           (is (= events (wait-for-events results events)))))
       (testing "burn it all down"
         (reset! results [])
         (is (fs/delete-dir intermediate-dir))
         (let [events #{{:changed-path intermediate-dir
                         :type :delete}}]
           (is (= events (wait-for-events results events)))))
       (let [another-nested-dir (fs/file root-dir "another-nested-dir")
             new-nested-file (fs/file another-nested-dir "new-nested-file")]
         (testing "new nested directory"
           (testing "creation"
             (reset! results [])
             (is (fs/mkdir another-nested-dir))
             (let [events #{{:changed-path another-nested-dir
                             :type :create}}]
               (is (= events (wait-for-events results events)))))
           (testing "creation of a file within"
             (reset! results [])
             (spit new-nested-file "new nested file in nested dir")
             (let [events #{{:changed-path new-nested-file
                             :type :create}}]
               (is (= events (wait-for-events results events)))))
           (testing "deletion"
             (reset! results [])
             (is (fs/delete-dir another-nested-dir))
             (let [events #{{:changed-path another-nested-dir
                             :type :delete}}]
               (is (= events (wait-for-events results events)))))))))))

(deftest ^:integration recurse-false-test
  (testing "Watching of nested directories with recursive disabled"
    (with-app-with-config
      app [filesystem-watch-service] {}
      (let [service (tk-app/get-service app :FilesystemWatchService)
           watcher (create-watcher service {:recursive false})
           results (atom [])
           callback (make-callback results)
           root-dir (fs/temp-dir "root-dir")
           intermediate-dir (fs/file root-dir "intermediate-dir")
           nested-dir (fs/file intermediate-dir "nested-dir")
           canary-file (fs/file root-dir "canary")]

      (add-watch-dir! watcher root-dir)
      (add-callback! watcher callback)
       ;; basic smoke test - ensure with recursive off we haven't broken expected event watching
      (testing "expect events from"
        (testing "creating an intermediate directory"
          (fs/mkdirs intermediate-dir)
          ;; also make the nested-directory we'll use for testing later
          (fs/mkdirs nested-dir)
          (let [events #{{:changed-path intermediate-dir
                          :type :create}}]
            (is (= events (wait-for-events results events)))))
        (reset! results [])
        (testing "creating a file in root directory"
          (let [events #{{:changed-path canary-file
                          :type :create}}]
            (spit canary-file "foo")
            (is (= events (wait-for-events results events)))))

        (reset! results [])
        (testing "modifying a file in root directory"
          (let [events #{{:changed-path canary-file
                         :type :modify}}]
            (spit canary-file "bar")
            (is (= events (wait-for-events results events)))))

        (reset! results [])
        (testing "deleting a file in root directory"
          (let [events #{{:changed-path canary-file
                         :type :delete}}]
            (fs/delete canary-file)
            (is (= events (wait-for-events results events))))))

        ;; In these tests we expect events --not-- to occur. In order to (loosely)
        ;; validate that events are firing as expected and our test isn't giving us
        ;; false negatives, we also modify a canary file as a "control".
      (testing "expect no events from nested directories"
        (let [nested-file (fs/file nested-dir "nested file")]
          (reset! results [])
          (testing "creating a file in a nested directory"
            (let [event #{{:changed-path canary-file
                            :type :create}}]
              (spit nested-file "foo") ;; expect no events from
              (fs/touch canary-file) ;; control
              (is (= event (wait-for-exactly-event results event)))))

          (reset! results [])
          (testing "modifying a file in nested directory"
            (let [event #{{:changed-path canary-file
                            :type :modify}}]
              (spit nested-file "foo")
              (spit canary-file "bar")
              (is (= event (wait-for-exactly-event results event)))))

          (reset! results [])
          (testing "deleting a file in nested directory"
            (let [event #{{:changed-path canary-file
                            :type :modify}}]
              (fs/delete nested-file)
              (spit canary-file "baz")
              (is (= event (wait-for-exactly-event results event)))))

          (reset! results [])
          (testing "creating a directory in a nested directory"
            (let [event #{{:changed-path canary-file
                            :type :modify}}]
              (fs/mkdirs (fs/file nested-dir "foo"))
              (spit canary-file "qux")
              (is (= event (wait-for-exactly-event results event)))))

          (reset! results [])
          (testing "deleting a directory in a nested directory"
            (let [event #{{:changed-path canary-file
                            :type :modify}}]
              (fs/delete-dir (fs/file nested-dir "foo"))
              (spit canary-file "quux")
              (is (= event (wait-for-exactly-event results event)))))))))))

(deftest ^:integration multiple-watch-dirs-test
  (testing "Watching of multiple directories using a single watcher"
    (with-app-with-config
     app [filesystem-watch-service] {}
     (let [service (tk-app/get-service app :FilesystemWatchService)
           watcher (create-watcher service)
           results (atom [])
           callback (make-callback results)
           dir-1 (fs/temp-dir "dir-1")
           dir-2 (fs/temp-dir "dir-2")
           test-file-1 (fs/file dir-1 "test-file")
           test-file-2 (fs/file dir-2 "test-file")]
       (add-watch-dir! watcher dir-1 {:recursive true})
       (add-watch-dir! watcher dir-2 {:recursive true})
       (add-callback! watcher callback)
       (testing "Events in dir-1"
         (spit test-file-1 "foo")
         (let [events #{{:changed-path test-file-1
                         :type :create}}]
           (is (= events (wait-for-events results events)))))
       (reset! results [])
       (testing "Events in dir-2"
         (spit test-file-2 "foo")
         (let [events #{{:changed-path test-file-2
                         :type :create}}]
           (is (= events (wait-for-events results events)))))
       (reset! results [])
       (testing "Events in both dirs"
         (spit test-file-1 "bar")
         (spit test-file-2 "bar")
         (let [events #{{:changed-path test-file-1
                         :type :modify}
                        {:changed-path test-file-2
                         :type :modify}}]
           (is (= events (wait-for-events results events)))))
       (reset! results [])
       (testing "Nested watch directories"
         (let [nested-dir (fs/file dir-1 "nested-dir")
               test-file-nested (fs/file nested-dir "test-file")]
           (is (fs/mkdir nested-dir))
           (add-watch-dir! watcher nested-dir {:recursive true})
           ;; There is a bug in the JDK in which we misreport the changed
           ;; path if we are trying to register a new watch path when an event
           ;; comes in. This sleep allows us to finish registering the
           ;; directory before the CREATE event will come in. We are ignoring
           ;; this for the time being as the current consumer will take the
           ;; same action regardless of the path. See
           ;; https://tickets.puppetlabs.com/browse/TK-387 for more details.
           (Thread/sleep 100)
           (spit test-file-nested "foo")
           (let [events #{{:changed-path nested-dir
                           :type :create}
                          {:changed-path test-file-nested
                           :type :create}}]
             (is (= events (wait-for-events results events))))
           (reset! results [])
           (testing "Events in all three directories"
             (spit test-file-1 "baz")
             (spit test-file-2 "baz")
             (spit test-file-nested "baz")
             (let [events #{{:changed-path test-file-1
                             :type :modify}
                            {:changed-path test-file-2
                             :type :modify}
                            {:changed-path test-file-nested
                             :type :modify}}]
               (is (= events (wait-for-events results events)))))
           (reset! results [])
           (testing "Deletion thereof"
             (is (fs/delete-dir nested-dir))
             (let [events #{{:changed-path nested-dir
                             :type :delete}}]
               (is (= events (wait-for-events results events))))
             (reset! results [])
             (testing "Leaves the parent watched dir unaffected"
               (spit test-file-1 "bamboozle")
               (let [events #{{:changed-path test-file-1
                               :type :modify}}]
                 (is (= events (wait-for-events results events))))))))))))

(deftest ^:integration multiple-watcher-test
  (testing "Multiple watchers"
    (with-app-with-config
     app [filesystem-watch-service] {}
     (let [service (tk-app/get-service app :FilesystemWatchService)
           watcher-1 (create-watcher service)
           watcher-2 (create-watcher service)
           results-1 (atom [])
           callback-1 (make-callback results-1)
           results-2 (atom [])
           callback-2 (make-callback results-2)
           dir-1 (fs/temp-dir "dir-1")
           dir-2 (fs/temp-dir "dir-2")
           test-file-1 (fs/file dir-1 "test-file")
           test-file-2 (fs/file dir-2 "test-file")]
       (testing "Watching separate directories"
         (watch!* watcher-1 dir-1 callback-1)
         (watch!* watcher-2 dir-2 callback-2)
         (testing "Events do not bleed over between watchers"
           (spit test-file-1 "foo")
           (spit test-file-2 "foo")
           (let [events-1 #{{:changed-path test-file-1
                             :type :create}}
                 events-2 #{{:changed-path test-file-2
                             :type :create}}]
             (is (= events-1 (wait-for-events results-1 events-1)))
             (is (= events-2 (wait-for-events results-2 events-2))))))
       (testing "Watching the same directory"
         (reset! results-1 [])
         (reset! results-2 [])
         ;; Create a third watcher
         (let [watcher-3 (create-watcher service)
               results-3 (atom [])
               callback-3 (make-callback results-3)]
           ;; ... and tell it to watch the same directory as the first one.
           (watch!* watcher-3 dir-1 callback-3)
           (spit test-file-1 "bar")
           (spit test-file-2 "bar")
           (let [events-1 #{{:changed-path test-file-1
                             :type :modify}}
                 events-2 #{{:changed-path test-file-2
                             :type :modify}}]
             (is (= events-1 (wait-for-events results-1 events-1)))
             (is (= events-1 (wait-for-events results-3 events-1)))
             (is (= events-2 (wait-for-events results-2 events-2))))))))))

(deftest ^:integration callback-exception-shutdown-test
  (let [root-dir (fs/temp-dir "root-dir")
        error (Exception. "boom")
        callback (fn [& _] (throw error))]
    (let [app (tk/boot-services-with-config [filesystem-watch-service] {})
          service (tk-app/get-service app :FilesystemWatchService)]
      (watch! service root-dir callback)
      (with-test-logging
       (spit (fs/file root-dir "test-file") "foo")
       (let [reason (tk-internal/wait-for-app-shutdown app)]
         (is (= (:cause reason) :service-error))
         (is (= (:error reason) error)))
       (is (logged?
            #"shutdown-on-error triggered because of exception"
            :error))))))

(deftest ^:integration debouncing-test
  (testing "debouncing"
    (let [root-dir (fs/temp-dir "debounce")
          actual-events (atom [])
          callback-invocations (atom 0)
          callback (fn [e]
                     (swap! actual-events concat e)
                     (swap! callback-invocations inc))
          app (tk/boot-services-with-config [filesystem-watch-service] {})
          svc (tk-app/get-service app :FilesystemWatchService)
          event-cadence (quot watch-core/window-min 2)]
      ;; We trigger half the minimum number of events that could independently
      ;; occur without exceeding the window-max, given the fixed event-cadence
      ;; set above, we set the event-cadence to half of window-min to increase
      ;; the likelihood that they will debounced into a single callback
      ;; invocation. This requires that window-max be at least four times the
      ;; duration of window-min.
      (testing "multiple events trigger one callback when within window-min and window-max"
        (let [test-dir (fs/file root-dir "window-min")
              num-events (quot (quot watch-core/window-max watch-core/window-min) 2)
              files (for [n (range num-events)]
                      (fs/file test-dir (str n ".txt")))
              expected-events (set (for [f files] {:changed-path f
                                                   :type :create}))]
          (is (> num-events 1))
          (is (fs/mkdirs test-dir))
          (watch! svc test-dir callback)
          (doseq [f files]
            (fs/touch f)
            (Thread/sleep event-cadence))
          (is (= expected-events (wait-for-events actual-events expected-events)))
          (is (= @callback-invocations 1))))
      ;; We trigger events at the same cadence as above to ensure that we will
      ;; be causing events within window-min and continuing the polling loop.
      ;; We trigger them for what should be twice the duration of window-max
      ;; however to see the loop properly short circuited. The callback should be
      ;; invoked at least twice.
      (testing "multiple callbacks are triggered if polling exceeds `window-max`"
        (let [test-dir (fs/file root-dir "window-max")
              event-nums (* (quot watch-core/window-max watch-core/window-min) 4)
              files (map #(fs/file test-dir (str % ".txt")) (range 0 event-nums))
              expected-events (set (map (fn [f] {:changed-path f :type :create}) files))]
          (is (>= event-nums 4))
          (is (fs/mkdirs test-dir))
          (watch! svc test-dir callback)
          (reset! actual-events [])
          (reset! callback-invocations 0)
          (doseq [f files] (fs/touch f) (Thread/sleep event-cadence))
          (is (= expected-events (wait-for-events actual-events expected-events)))
          (is (>= @callback-invocations 2)))))))

(deftest ^:integration cannot-watch-recursive-and-not-recursive
  (testing "Cannot watch directories recursively and non-recursively with same watcher"
    (with-app-with-config
     app [filesystem-watch-service] {}
     (let [service (tk-app/get-service app :FilesystemWatchService)
           watcher (create-watcher service {:recursive false})
           results (atom [])
           callback (make-callback results)
           first-dir (fs/temp-dir "first")
           second-dir (fs/temp-dir "second")]
      ;; adding another directory with the same recursive value is OK
      (add-watch-dir! watcher first-dir {:recursive false})
      ;; but adding another directory with a different recursive value should fail
      (is (thrown-with-msg?
            IllegalArgumentException
            #"cannot change to :recursive true"
            (add-watch-dir! watcher second-dir {:recursive true})))))))

;; Here we create a stub object that implements the WatchEvent interface as
;; the concrete class is a private inner class. See:
;; https://github.com/openjdk-mirror/jdk7u-jdk/blob/f4d80957e89a19a29bb9f9807d2a28351ed7f7df/src/share/classes/sun/nio/fs/AbstractWatchKey.java#L190-L222
;; We test only the OVERFLOW event because it is special case and will not be
;; triggered in the normal integration tests above.
(deftest clojurize-overflows
  (testing "clojurize"
    (let [watch-path (.toPath (fs/temp-dir "clojurize-overflows"))
          overflow-event (reify
                          java.nio.file.WatchEvent
                          (kind [this] java.nio.file.StandardWatchEventKinds/OVERFLOW)
                          (count [this] 1)
                          (context [this] nil))
          expected {:type :unknown :count 1 :watched-path (.toFile watch-path)}]
      (testing "overflow events are handled normally"
        (is (= expected (watch-core/clojurize overflow-event watch-path)))))))

(deftest gracefully-handles-tmpdirs
  (testing "Does not fail if directory disappears when attempting to register"
    (with-test-logging
      (DirWatchUtils/register
                 (.newWatchService (FileSystems/getDefault))
                 (Paths/get (URI. "file:///etc/foobar/baznoid")))
      (is (logged? #"Failed to register.*/etc/foobar/baznoid")))))
