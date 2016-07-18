(ns puppetlabs.services.watcher.filesystem-watch-service-test
  (:require [clojure.test :refer [deftest testing is use-fixtures]]
            [clojure.set :as set]
            [schema.test :as schema-test]
            [me.raynes.fs :as fs]
            [puppetlabs.services.protocols.filesystem-watch-service :refer :all]
            [puppetlabs.services.watcher.filesystem-watch-service :refer [filesystem-watch-service]]
            [puppetlabs.trapperkeeper.services.scheduler.scheduler-service :refer [scheduler-service]]
            [puppetlabs.trapperkeeper.testutils.bootstrap :refer [with-app-with-config]]
            [puppetlabs.trapperkeeper.testutils.logging :refer [with-test-logging]]
            [puppetlabs.trapperkeeper.core :as tk]
            [puppetlabs.trapperkeeper.app :as tk-app]
            [puppetlabs.trapperkeeper.internal :as tk-internal]))

(use-fixtures :once schema-test/validate-schemas)

(def watch-service-and-deps
  [filesystem-watch-service scheduler-service])

(defn make-callback
  [dest]
  (fn [events]
    (swap! dest concat events)))

(defn contains-events?
  [dest events]
  (set/subset? events (set @dest)))

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

;; TODO perhaps move this (or something similar) up to the TK protocol
(defn watch!
  [service root callback]
  (let [watcher (create-watcher service)]
    (add-watch-dir! watcher root {:recursive true})
    (add-callback! watcher callback)))

(deftest ^:integration single-path-test
  (let [root (fs/temp-dir "single-path-test")
        first-file (fs/file root "first-file")
        second-file (fs/file root "second-file")
        results (atom [])
        callback (make-callback results)]
    (with-app-with-config
     app watch-service-and-deps {}
     (let [service (tk-app/get-service app :FilesystemWatchService)]
       (watch! service root callback))
     (testing "callback not invoked until directory changes"
       (is (= @results [])))
     (testing "callback is invoked when a new file is created"
       (spit first-file "foo")
       (let [events #{{:path first-file
                       :type :create}}]
         ;; This is the first of many weird assertions like this, but it's done
         ;; this way on purpose to get decent reporting from failed assertions.
         ;; See above the docstring on wait-for-events.
         (is (= events (wait-for-events results events)))))
     (testing "callback invoked again when another new file is created"
       (reset! results [])
       (spit second-file "bar")
       (let [events #{{:path second-file
                       :type :create}}]
         (is (= events (wait-for-events results events)))))
     (testing "watch-dir! also reports file modifications"
       (testing "of a single file"
         (reset! results [])
         (spit first-file "something different")
         (let [events #{{:path first-file
                  :type :modify}}]
           (is (= events (wait-for-events results events)))))
       (testing "of multiple files"
         (reset! results [])
         (spit first-file "still not the same as before")
         (spit second-file "still not the same as before")
         (let [events #{{:path first-file
                         :type :modify}
                        {:path second-file
                         :type :modify}}]
           (is (= events (wait-for-events results events))))))
     (testing "watch-dir! also reports file deletions"
       (testing "of multiple files"
         (reset! results [])
         (is (fs/delete first-file))
         (is (fs/delete second-file))
         (let [events #{{:path first-file
                         :type :delete}
                        {:path second-file
                         :type :delete}}]
           (is (= events (wait-for-events results events)))))))))

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
     app watch-service-and-deps {}
     (let [service (tk-app/get-service app :FilesystemWatchService)]
       (watch! service root-1 callback-1)
       (watch! service root-2 callback-2))
     (testing "callback-1 is invoked when root-1 changes"
       (spit root-1-file "foo")
       (let [events #{{:path root-1-file
                       :type :create}}]
         (is (= events (wait-for-events results-1 events))))
       (testing "but not callback-2"
         (is (= @results-2 []))))
     (testing "callback-2 is invoked when root-2 changes"
       (reset! results-1 [])
       (spit root-2-file "foo")
       (let [events #{{:path root-2-file
                       :type :create}}]
         (is (= events (wait-for-events results-2 events))))
       (testing "but not callback-1"
         (is (= @results-1 []))))
     (testing "both callbacks invoked when both roots change"
       (reset! results-1 [])
       (reset! results-2 [])
       (spit root-1-file "bar")
       (spit root-2-file "bar")
       (let [events-1 #{{:path root-1-file
                         :type :modify}}
             events-2 #{{:path root-2-file
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
     app watch-service-and-deps {}
     (let [service (tk-app/get-service app :FilesystemWatchService)]
       (watch! service root-dir callback))
     (testing "file creation at root dir"
       (let [test-file (fs/file root-dir "foo")]
         (spit test-file "foo")
         (let [events #{{:path test-file
                         :type :create}}]
           (is (= events (wait-for-events results events))))))
     (testing "file creation at one level down"
       (let [test-file (fs/file intermediate-dir "foo")]
         (reset! results [])
         (spit test-file "foo")
         (let [events #{{:path test-file
                         :type :create}}]
           (is (= events (wait-for-events results events))))))
     (testing "file creation two levels down"
       (let [test-file (fs/file nested-dir "foo")]
         (reset! results [])
         (spit test-file "foo")
         (let [events #{{:path test-file
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
         (let [events #{{:path root-file
                         :type :create}
                        {:path nested-file
                         :type :create}
                        {:path intermediate-file
                         :type :create}}]
           (is (= events (wait-for-events results events)))))
       (testing "modifying a nested file"
         (reset! results [])
         (spit nested-file "something new")
         (let [events #{{:path nested-file
                         :type :modify}}]
           (is (= events (wait-for-events results events)))))
       (testing "deletion of a nested file"
         (reset! results [])
         (is (fs/delete nested-file))
         (let [events #{{:path nested-file
                         :type :delete}}]
           (is (= events (wait-for-events results events)))))
       (testing "burn it all down"
         (reset! results [])
         (is (fs/delete-dir intermediate-dir))
         (let [events #{{:path intermediate-dir
                         :type :delete}}]
           (is (= events (wait-for-events results events)))))
       (let [another-nested-dir (fs/file root-dir "another-nested-dir")
             new-nested-file (fs/file another-nested-dir "new-nested-file")]
         (testing "new nested directory"
           (testing "creation"
             (reset! results [])
             (is (fs/mkdir another-nested-dir))
             (let [events #{{:path another-nested-dir
                             :type :create}}]
               (is (= events (wait-for-events results events)))))
           (testing "creation of a file within"
             (reset! results [])
             (spit new-nested-file "new nested file in nested dir")
             (let [events #{{:path new-nested-file
                             :type :create}}]
               (is (= events (wait-for-events results events)))))
           (testing "deletion"
             (reset! results [])
             (is (fs/delete-dir another-nested-dir))
             (let [events #{{:path another-nested-dir
                             :type :delete}}]
               (is (= events (wait-for-events results events)))))))))))

(deftest callback-exception-shutdown
  (let [root-dir (fs/temp-dir "root-dir")
        error (Exception. "boom")
        callback (fn [& _] (throw error))]
    (let [app (tk/boot-services-with-config watch-service-and-deps {})
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
