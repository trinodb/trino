/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.util;

import io.airlift.log.Logger;

import java.io.File;
import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardWatchEventKinds;
import java.nio.file.WatchEvent;
import java.nio.file.WatchKey;
import java.nio.file.WatchService;
import java.util.function.Consumer;

public final class FileWatcher
{
    private static final Logger log = Logger.get(FileWatcher.class);

    private Thread thread;
    private WatchService watchService;

    private FileWatcher()
    {
        // hide for prevent instantiation outside of this class
    }

    /**
     * Starts watching a file and the given path and calls the callback when it is changed.
     * A shutdown hook is registered to stop watching. To control this yourself, create an
     * instance and use the start/stop methods.
     */
    public static void onFileChange(Path file, Consumer<File> callback) throws IOException
    {
        FileWatcher fileWatcher = new FileWatcher();
        fileWatcher.start(file, callback);
        Runtime.getRuntime().addShutdownHook(new Thread(fileWatcher::stop));
    }

    public void start(Path file, Consumer<File> callback) throws IOException
    {
        watchService = FileSystems.getDefault().newWatchService();
        Path parent = file.getParent();
        parent.register(watchService, StandardWatchEventKinds.ENTRY_MODIFY, StandardWatchEventKinds.ENTRY_CREATE, StandardWatchEventKinds.ENTRY_DELETE);
        log.info("Going to watch " + file);

        thread = new Thread(() -> {
            while (true) {
                WatchKey wk = null;
                try {
                    wk = watchService.take();
                    Thread.sleep(500); // give a chance for duplicate events to pile up
                    for (WatchEvent<?> event : wk.pollEvents()) {
                        Path changed = parent.resolve((Path) event.context());
                        if (Files.exists(changed) && Files.isSameFile(changed, file)) {
                            log.info("File change event: " + changed);
                            callback.accept(changed.toFile());
                            break;
                        }
                    }
                }
                catch (InterruptedException e) {
                    log.info("Ending my watch");
                    Thread.currentThread().interrupt();
                    break;
                }
                catch (Exception e) {
                    log.error("Error while loading file %s", file.toAbsolutePath(), e);
                }
                finally {
                    if (wk != null) {
                        wk.reset();
                    }
                }
            }
        });
        thread.start();
    }

    public void stop()
    {
        thread.interrupt();
        try {
            watchService.close();
        }
        catch (IOException e) {
            log.info("Error closing watch service", e);
        }
    }
}
