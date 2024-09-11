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
package io.trino.spooling.filesystem;

import com.google.common.annotations.VisibleForTesting;
import com.google.inject.Inject;
import io.airlift.log.Logger;
import io.airlift.units.Duration;
import io.trino.filesystem.FileEntry;
import io.trino.filesystem.FileIterator;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.spi.security.ConnectorIdentity;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;

import java.io.IOException;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ScheduledExecutorService;

import static io.trino.spooling.filesystem.FileSystemSpooledSegmentHandle.getExpirationFromLocation;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

public class FileSystemSegmentPruner
{
    private final Logger log = Logger.get(FileSystemSegmentPruner.class);

    private final TrinoFileSystem fileSystem;
    private final ScheduledExecutorService executor;
    private final boolean enabled;
    private final Duration interval;
    private final Location location;
    private final long batchSize;
    private boolean closed;

    private boolean filesAreOrdered = true;

    @Inject
    public FileSystemSegmentPruner(FileSystemSpoolingConfig config, TrinoFileSystemFactory fileSystemFactory, @ForSegmentPruner ScheduledExecutorService executor)
    {
        this.fileSystem = requireNonNull(fileSystemFactory, "fileSystemFactory is null")
                .create(ConnectorIdentity.ofUser("ignored"));
        this.executor = requireNonNull(executor, "executor is null");
        this.enabled = config.isPruningEnabled();
        this.interval = config.getPruningInterval();
        this.batchSize = config.getPruningBatchSize();
        this.location = Location.of(config.getLocation());
    }

    @PostConstruct
    public void start()
    {
        if (!enabled) {
            return;
        }

        log.info("Started expired segment pruning with interval %s and batch size %d", interval, batchSize);
        executor.scheduleAtFixedRate(this::prune, 0, interval.toMillis(), MILLISECONDS);
    }

    @PreDestroy
    public void shutdown()
    {
        if (!closed) {
            closed = true;
            executor.shutdownNow();
        }
    }

    private void prune()
    {
        pruneExpiredBefore(Instant.now().truncatedTo(ChronoUnit.SECONDS));
    }

    @VisibleForTesting
    void pruneExpiredBefore(Instant expiredBefore)
    {
        if (closed) {
            return;
        }
        try {
            List<Location> expiredSegments = new ArrayList<>();
            FileIterator iterator = orderDetectingIterator(fileSystem.listFiles(location));
            while (iterator.hasNext()) {
                FileEntry file = iterator.next();
                Optional<Instant> handle = getExpirationFromLocation(file.location());
                // Not a spooled segment
                if (handle.isEmpty()) {
                    continue;
                }

                if (handle.get().isBefore(expiredBefore)) {
                    expiredSegments.add(file.location());
                    if (expiredSegments.size() >= batchSize) {
                        pruneExpiredSegments(expiredBefore, expiredSegments);
                    }
                }
                else if (filesAreOrdered) {
                    // First non expired segment was found, no need to check the rest
                    // since we know that files are lexicographically ordered.
                    pruneExpiredSegments(expiredBefore, expiredSegments);
                    return;
                }
            }
            pruneExpiredSegments(expiredBefore, expiredSegments);
        }
        catch (IOException e) {
            log.error(e, "Failed to prune segments");
        }
    }

    private void pruneExpiredSegments(Instant expiredBefore, List<Location> expiredSegments)
    {
        if (expiredSegments.isEmpty()) {
            return;
        }

        try {
            int batchSize = expiredSegments.size();

            Instant oldest = getExpirationFromLocation(expiredSegments.getFirst())
                    .orElseThrow(() -> new IllegalStateException("No expiration time found for " + expiredSegments.getFirst()));
            Instant newest = getExpirationFromLocation(expiredSegments.getLast())
                    .orElseThrow(() -> new IllegalStateException("No expiration time found for " + expiredSegments.getLast()));
            fileSystem.deleteFiles(expiredSegments);
            log.info("Pruned %d segments expired before %s [oldest: %s, newest: %s]", batchSize, expiredBefore, oldest, newest);
        }
        catch (IOException e) {
            log.warn(e, "Failed to delete %d expired segments", expiredSegments.size());
        }
        catch (Exception e) {
            log.error(e, "Unexpected error while pruning expired segments");
        }
    }

    private FileIterator orderDetectingIterator(FileIterator delegate)
    {
        return new FileIterator()
        {
            private FileEntry last;

            @Override
            public boolean hasNext()
                    throws IOException
            {
                return delegate.hasNext();
            }

            @Override
            public FileEntry next()
                    throws IOException
            {
                FileEntry next = delegate.next();
                // Switch the filesAreOrdered if the files listing order is broken
                if (filesAreOrdered && last != null && last.location().fileName().compareTo(next.location().fileName()) > 0) {
                    filesAreOrdered = false;
                    last = next;
                }
                return next;
            }
        };
    }
}
