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
package io.trino.filesystem.cache.local;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.hash.BloomFilter;
import com.google.common.hash.Funnel;
import com.google.common.hash.Funnels;
import com.google.errorprone.annotations.concurrent.GuardedBy;
import io.airlift.log.Logger;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Clock;
import java.util.ArrayDeque;
import java.util.Comparator;
import java.util.Deque;
import java.util.Iterator;
import java.util.List;
import java.util.OptionalLong;
import java.util.concurrent.TimeUnit;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.nio.file.StandardCopyOption.ATOMIC_MOVE;
import static java.nio.file.StandardOpenOption.CREATE_NEW;
import static java.nio.file.StandardOpenOption.WRITE;
import static java.util.Objects.requireNonNull;
import static java.util.UUID.randomUUID;

final class BloomFilterAccessTracker
{
    private static final Logger log = Logger.get(BloomFilterAccessTracker.class);

    private static final String ACCESS_DIRECTORY = "access";

    private static final Funnel<CharSequence> FUNNEL = Funnels.stringFunnel(UTF_8);
    private static final double FALSE_POSITIVE_PROBABILITY = 0.03;

    private final Path accessDirectory;
    private final Clock clock;
    private final long bucketMillis;
    private final long historyMillis;
    private final int bucketCount;
    private final long expectedInsertionsPerBucket;
    private final NativeFileSystemCacheStats stats;
    @GuardedBy("this")
    private final Deque<Bucket> history = new ArrayDeque<>();
    @GuardedBy("this")
    private Bucket current;

    BloomFilterAccessTracker(Path root, Duration historyDuration, Duration bucketDuration, DataSize memory, NativeFileSystemCacheStats stats)
            throws IOException
    {
        this(root, historyDuration, bucketDuration, memory, Clock.systemUTC(), stats);
    }

    @VisibleForTesting
    BloomFilterAccessTracker(Path root, Duration historyDuration, Duration bucketDuration, DataSize memory, Clock clock, NativeFileSystemCacheStats stats)
            throws IOException
    {
        this.accessDirectory = root.resolve(CacheFileLayout.VERSION_DIRECTORY).resolve(ACCESS_DIRECTORY);
        this.clock = requireNonNull(clock, "clock is null");
        this.bucketMillis = Math.max(1, bucketDuration.roundTo(TimeUnit.MILLISECONDS));
        this.historyMillis = Math.max(bucketMillis, historyDuration.roundTo(TimeUnit.MILLISECONDS));
        this.bucketCount = Math.max(1, (int) Math.ceil(historyMillis / (double) bucketMillis));
        long bucketBytes = Math.max(1024, memory.toBytes() / bucketCount);
        this.expectedInsertionsPerBucket = bucketBytes;
        this.stats = requireNonNull(stats, "stats is null");

        Files.createDirectories(accessDirectory);
        loadPersistedBuckets();
        current = new Bucket(bucketStart(clock.millis()), newFilter());
    }

    synchronized void record(String fileHash)
    {
        rotateIfNeeded();
        current.filter().put(fileHash);
        stats.recordAccess();
    }

    synchronized boolean mightContain(String fileHash)
    {
        return lastAccessTime(fileHash).isPresent();
    }

    synchronized OptionalLong lastAccessTime(String fileHash)
    {
        rotateIfNeeded();
        if (current.filter().mightContain(fileHash)) {
            return OptionalLong.of(current.startMillis());
        }
        for (Iterator<Bucket> iterator = history.descendingIterator(); iterator.hasNext(); ) {
            Bucket bucket = iterator.next();
            if (bucket.filter().mightContain(fileHash)) {
                return OptionalLong.of(bucket.startMillis());
            }
        }
        return OptionalLong.empty();
    }

    @GuardedBy("this")
    private void rotateIfNeeded()
    {
        long now = clock.millis();
        if (now < current.startMillis() + bucketMillis) {
            return;
        }
        persist(current);
        history.addLast(current);
        trimHistory(now);
        current = new Bucket(bucketStart(now), newFilter());
    }

    @GuardedBy("this")
    private void loadPersistedBuckets()
            throws IOException
    {
        long now = clock.millis();
        if (!Files.exists(accessDirectory)) {
            return;
        }
        List<Path> bucketFiles;
        try (var stream = Files.list(accessDirectory)) {
            bucketFiles = stream
                    .filter(path -> path.getFileName().toString().endsWith(".bloom"))
                    .sorted(Comparator.comparing(Path::getFileName))
                    .collect(toImmutableList());
        }
        for (Path bucketFile : bucketFiles) {
            long startMillis;
            try {
                startMillis = bucketStart(bucketFile);
            }
            catch (RuntimeException _) {
                deleteIfExists(bucketFile);
                continue;
            }
            if (now - startMillis > historyMillis) {
                deleteIfExists(bucketFile);
                continue;
            }
            try (InputStream input = Files.newInputStream(bucketFile)) {
                history.addLast(new Bucket(startMillis, BloomFilter.readFrom(input, FUNNEL)));
            }
            catch (IOException | RuntimeException _) {
                deleteIfExists(bucketFile);
            }
        }
        trimHistory(now);
    }

    private void persist(Bucket bucket)
    {
        Path path = bucketPath(bucket.startMillis());
        Path temporaryPath = accessDirectory.resolve(path.getFileName() + ".tmp." + randomUUID());
        try {
            try (OutputStream output = Files.newOutputStream(temporaryPath, CREATE_NEW, WRITE)) {
                bucket.filter().writeTo(output);
            }
            Files.move(temporaryPath, path, ATOMIC_MOVE);
        }
        catch (IOException | RuntimeException e) {
            log.warn(e, "Failed to persist native file system cache access bucket: %s", path);
            deleteIfExists(temporaryPath);
        }
    }

    @GuardedBy("this")
    private void trimHistory(long now)
    {
        while (!history.isEmpty() && now - history.peekFirst().startMillis() > historyMillis) {
            deleteIfExists(bucketPath(history.removeFirst().startMillis()));
        }
        while (history.size() >= bucketCount) {
            deleteIfExists(bucketPath(history.removeFirst().startMillis()));
        }
    }

    private long bucketStart(long millis)
    {
        return millis - (millis % bucketMillis);
    }

    private Path bucketPath(long startMillis)
    {
        return accessDirectory.resolve(startMillis + ".bloom");
    }

    private static long bucketStart(Path path)
    {
        String fileName = path.getFileName().toString();
        return Long.parseLong(fileName.substring(0, fileName.length() - ".bloom".length()));
    }

    private BloomFilter<CharSequence> newFilter()
    {
        return BloomFilter.create(FUNNEL, expectedInsertionsPerBucket, FALSE_POSITIVE_PROBABILITY);
    }

    private static void deleteIfExists(Path path)
    {
        try {
            Files.deleteIfExists(path);
        }
        catch (IOException _) {
        }
    }

    @Override
    public synchronized String toString()
    {
        return toStringHelper(this)
                .add("accessDirectory", accessDirectory)
                .add("bucketMillis", bucketMillis)
                .add("bucketCount", bucketCount)
                .add("expectedInsertionsPerBucket", expectedInsertionsPerBucket)
                .toString();
    }

    private record Bucket(long startMillis, BloomFilter<CharSequence> filter)
    {
        private Bucket
        {
            requireNonNull(filter, "filter is null");
        }
    }
}
