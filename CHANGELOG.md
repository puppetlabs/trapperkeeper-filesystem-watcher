# 1.1.0

- (TK-448) (Feature release) Add support for non-recursive directory watching.
  Adds the ability to watch only the contents of a given directory and not the
  contents of sub-directories. Deprecates specifying this option on the
  `add-watch-dir!` function, and adds support for it to the `create-watcher`
  function.

# 1.0.1

 - (maint) Fixes a long running, subtle bug in which we discard the trapperkeeper context on init

# 1.0.0

 - [TK-385](https://tickets.puppetlabs.com/browse/TK-385) Return events as soon as possible rather than polling
 - [TK-389](https://tickets.puppetlabs.com/browse/TK-389) Provide basic debouncing facilities
 - [TK-391](https://tickets.puppetlabs.com/browse/TK-391) Handle overflow events

# 0.1.0

Initial release.  Includes Trapperkeeper service protocol and an implementation
based on `java.nio.file.WatchService`.  Only recursive directory watching is
supported.
