(ns puppetlabs.services.protocols.filesystem-watch-service
  (:require [schema.core :as schema])
  (:import (java.io File)))

(def Event
  "Schema for an event on a file watched by this service."
  {:type (schema/enum :create :modify :delete :unknown)
   :path File})

;; TODO: add schema for Watcher {:watch-service WatchService :callbacks (schema/atom [IFn])}

(defprotocol FilesystemWatchService

  (create-watcher! [this]
    "Returns a Watcher which can be used to initiate watching of a directory on
    the filesystem.  The Watcher is an opaque object of unspecified type.  A
    Watcher must be passed in as an argument to subsequent calls on this
    service but nothing else can be done with it.")

  (add-watch-dir! [this watcher dir options]
    "Given a Watcher and a directory on the filesystem, initiate watching of
    dir.  The watcher's callbacks will be invoked when dir changes.  Available
    options are:
      * :recursive true   - If true, callbacks will be invoked when dir or any
                            file underneath dir changes.")

  (add-callback! [this watcher callback]
    "Adds a callback to a Watcher.  The callback will be invoked when any
    watched directories change.  The callback will be passed a sequence of
    Events as its only argument.  The exact events are unspecified and possibly
    platform-dependent; however, the following events are guaranteed to be
    passed to the callback

     * an event of :type :create with :path p, when a file is created at path p
     * an event of :type :modify with :path p, when the contents of a file at path p are modified
     * an event of :type :delete with :path p, when a file is deleted at path p

    Note that, for any of those particular changes, there may also be additional
    events passed to the callback, such as events on a parent directory of a
    changed file."))
