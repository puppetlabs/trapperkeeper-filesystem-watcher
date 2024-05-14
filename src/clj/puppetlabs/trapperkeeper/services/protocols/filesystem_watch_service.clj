(ns puppetlabs.trapperkeeper.services.protocols.filesystem-watch-service
  (:require [schema.core :as schema])
  (:import (java.io File)))

(def Event
  "Schema for an event on a file watched by this service."
  (schema/if #(= (:type %) :unknown)
    {:type (schema/eq :unknown)
     :count schema/Int
     :watched-path File}
    {:type (schema/enum :create :modify :delete)
     :count schema/Int
     :watched-path File
     :changed-path File}))

(defprotocol Watcher
  (add-watch-dir! [this dir] [this dir options]
    "Given a directory on the filesystem, initiate watching of dir.  The
    watcher's callbacks will be invoked when dir changes.  Available options are:
      * :recursive true   - [deprecated] If true, callbacks will be invoked when dir or any file
                            underneath dir changes. Passing options to this function is deprecated -
                            pass `recursive` option to create-watcher function of the
                            FilesystemWatchService protocol instead.

    When dir is deleted, the behavior is unspecified, left up to the
    implementation, and may be platform-specific.")

  (add-file-callback! [this path-to-file callback] [this path-to-file callback types]
    "Adds a callback to a Watcher tha will be invoked when the watched file changes.
    The parent directory containing the file must be added via `add-watch-dir!` for the callbacks to be triggered.
    The callback will be passed a sequence of Events as its only argument.
    The exact events passed to the callback are unspecified, left up to the implementation,
    and possibly platform-dependent; however, the following events are guaranteed to be passed to the callback

     * an event of :type :create with :path p, when a file is created at path p
     * an event of :type :modify with :path p, when the contents of a file at path p are modified
     * an event of :type :delete with :path p, when a file is deleted at path p

     The types of events where the callback is triggered can be limited with the optional fourth argument. The fourth
     argument should be the `set` of types of interest from: `create`, `modify`, `delete`, `unknown`.
     The default is to specify all types.")

  (add-callback! [this callback] [this callback types]
    "Adds a callback to a Watcher.  The callback will be invoked when any
    watched directories change.  The callback will be passed a sequence of
    Events as its only argument.  The exact events passed to the callback are
    unspecified, left up to the implementation, and possibly platform-dependent;
    however, the following events are guaranteed to be passed to the callback

     * an event of :type :create with :path p, when a file is created at path p
     * an event of :type :modify with :path p, when the contents of a file at path p are modified
     * an event of :type :delete with :path p, when a file is deleted at path p

    Note that, for any of those particular changes, there may also be additional
    events passed to the callback, such as events on a parent directory of a
    changed file.

    The types of events where the callback is triggered can be limited with the optional third argument.
    The types argument should be the `set` of types of interest from: `create`, `modify`, `delete`, `unknown`.
    The default is to specify all types."))

(defprotocol FilesystemWatchService
  (create-watcher [this] [this options]
    "Returns a Watcher which can be used to initiate watching of a directory on
    the filesystem. Available options are:
      * :recursive (true | false) - If true, callbacks will be invoked when dir or any file
                                    underneath dir, including files within nested directories of
                                    dir, changes. If false, callbacks will be invoked when any
                                    file inside of dir changes. Note that on some implementations,
                                    modifying the contents of a directory is considered a change
                                    to the directory itself (platform-specific)".))
