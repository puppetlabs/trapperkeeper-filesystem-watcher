(ns puppetlabs.enterprise.services.protocols.filesystem-watch-service
  (:require [schema.core :as schema])
  (:import (java.io File)))

(def Event
  "Schema for an event on a file watched by this service."
  {:type (schema/enum :create :modify :delete)
   :path File})

(defprotocol FilesystemWatchService
  (watch-dir!
    [this path callback]
    "Begins (recursive) monitoring of a directory on the filesystem.  callback
    will be invoked when path, or any files underneath it, change.  The callback
    will be passed a sequence of Events as its only argument.  The exact events
    are unspecified and possibly platform-dependent; however, the following
    events are guaranteed to be passed to the callback

     * an event of :type :create with :path p, when a file is created at path p
     * an event of :type :modify with :path p, when the contents of a file at path p are modified
     * an event of :type :delete with :path p, when a file is deleted at path p

    Note that, for any of those particular changes, there may also be additional
    events passed to the callback, such as events on a parent directory of a
    changed file."))
