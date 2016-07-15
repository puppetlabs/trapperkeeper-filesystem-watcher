package com.puppetlabs.enterprise;

import com.sun.nio.file.SensitivityWatchEventModifier;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.List;
import java.util.Map;

import static java.nio.file.StandardWatchEventKinds.ENTRY_CREATE;
import static java.nio.file.StandardWatchEventKinds.ENTRY_DELETE;
import static java.nio.file.StandardWatchEventKinds.ENTRY_MODIFY;

/**
 * This class contains some helpers adopted from Java's official examples/documentation:
 * https://docs.oracle.com/javase/tutorial/essential/io/notification.html
 */
public class DirWatchUtils {

    private static final org.slf4j.Logger log = LoggerFactory.getLogger(DirWatchUtils.class);

    /**
     * Register the given directory with the WatchService
     */
    public static void register(final WatchService watcher,
                                final Map<WatchKey, Path> keys,
                                final Path dir) throws IOException {
        WatchKey key =  dir.register(watcher,  new WatchEvent.Kind[]{
                StandardWatchEventKinds.ENTRY_MODIFY,
                StandardWatchEventKinds.ENTRY_CREATE,
                StandardWatchEventKinds.ENTRY_DELETE},
                SensitivityWatchEventModifier.HIGH);
        Path prev = keys.get(key);
        if (prev == null) {
            log.debug("Registering watched path: ", dir);
        } else {
            if (!dir.equals(prev)) {
                log.debug(String.format("Update watched path: %s -> %s", prev, dir));
            }
        }
        keys.put(key, dir);
    }

    /**
     * Register the given directory, and all its sub-directories, with the WatchService.
     * Populates the given 'keys' map with WatchKeys -> Paths.
     */
    public static void registerRecursive(
            final WatchService watcher,
            final List<Path> startingPaths,
            final Map<WatchKey, Path> keys) throws IOException {
        for (Path start : startingPaths) {
            // register directory and sub-directories
            Files.walkFileTree(start, new SimpleFileVisitor<Path>() {
                @Override
                public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs)
                        throws IOException {
                    register(watcher, keys, dir);
                    return FileVisitResult.CONTINUE;
                }
            });
        }
    }

}
