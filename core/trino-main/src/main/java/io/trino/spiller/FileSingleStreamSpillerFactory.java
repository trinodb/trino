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
package io.trino.spiller;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.inject.Inject;
import io.airlift.log.Logger;
import io.trino.FeaturesConfig;
import io.trino.cache.NonKeyEvictableLoadingCache;
import io.trino.execution.buffer.CompressionCodec;
import io.trino.execution.buffer.PagesSerdeFactory;
import io.trino.memory.context.LocalMemoryContext;
import io.trino.operator.SpillContext;
import io.trino.spi.TrinoException;
import io.trino.spi.block.BlockEncodingSerde;
import io.trino.spi.type.Type;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;

import javax.crypto.SecretKey;

import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.FileStore;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Optional;

import static com.google.common.util.concurrent.MoreExecutors.listeningDecorator;
import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.trino.FeaturesConfig.SPILLER_SPILL_PATH;
import static io.trino.cache.SafeCaches.buildNonEvictableCacheWithWeakInvalidateAll;
import static io.trino.execution.buffer.PagesSerdes.createSpillingPagesSerdeFactory;
import static io.trino.spi.StandardErrorCode.OUT_OF_SPILL_SPACE;
import static io.trino.util.Ciphers.createRandomAesEncryptionKey;
import static java.lang.String.format;
import static java.nio.file.Files.createDirectories;
import static java.nio.file.Files.createTempFile;
import static java.nio.file.Files.delete;
import static java.nio.file.Files.deleteIfExists;
import static java.nio.file.Files.getFileStore;
import static java.nio.file.Files.isExecutable;
import static java.nio.file.Files.isReadable;
import static java.nio.file.Files.isWritable;
import static java.nio.file.Files.newDirectoryStream;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.Executors.newFixedThreadPool;

public class FileSingleStreamSpillerFactory
        implements SingleStreamSpillerFactory
{
    private static final Logger log = Logger.get(FileSingleStreamSpillerFactory.class);

    @VisibleForTesting
    static final String SPILL_FILE_PREFIX = "spill";
    @VisibleForTesting
    static final String SPILL_FILE_SUFFIX = ".bin";
    private static final String SPILL_FILE_GLOB = "spill*.bin";
    private static final Duration SPILL_PATH_HEALTH_EXPIRY_INTERVAL = Duration.ofMinutes(5);

    private final ListeningExecutorService executor;
    private final PagesSerdeFactory serdeFactory;
    private final List<Path> spillPaths;
    private final SpillerStats spillerStats;
    private final double maxUsedSpaceThreshold;
    private final boolean spillEncryptionEnabled;
    private int roundRobinIndex;
    private final NonKeyEvictableLoadingCache<Path, Boolean> spillPathHealthCache;

    @Inject
    public FileSingleStreamSpillerFactory(BlockEncodingSerde blockEncodingSerde, SpillerStats spillerStats, FeaturesConfig featuresConfig, NodeSpillConfig nodeSpillConfig)
    {
        this(
                listeningDecorator(newFixedThreadPool(
                        featuresConfig.getSpillerThreads(),
                        daemonThreadsNamed("binary-spiller-%s"))),
                requireNonNull(blockEncodingSerde, "blockEncodingSerde is null"),
                spillerStats,
                featuresConfig.getSpillerSpillPaths(),
                featuresConfig.getSpillMaxUsedSpaceThreshold(),
                nodeSpillConfig.getSpillCompressionCodec(),
                nodeSpillConfig.isSpillEncryptionEnabled());
    }

    @VisibleForTesting
    public FileSingleStreamSpillerFactory(
            ListeningExecutorService executor,
            BlockEncodingSerde blockEncodingSerde,
            SpillerStats spillerStats,
            List<Path> spillPaths,
            double maxUsedSpaceThreshold,
            CompressionCodec compressionCodec,
            boolean spillEncryptionEnabled)
    {
        this.serdeFactory = createSpillingPagesSerdeFactory(blockEncodingSerde, compressionCodec);
        this.executor = requireNonNull(executor, "executor is null");
        this.spillerStats = requireNonNull(spillerStats, "spillerStats cannot be null");
        requireNonNull(spillPaths, "spillPaths is null");
        this.spillPaths = ImmutableList.copyOf(spillPaths);
        spillPaths.forEach(path -> {
            try {
                createDirectories(path);
            }
            catch (IOException e) {
                throw new IllegalArgumentException(format("could not create spill path %s; adjust %s config property or filesystem permissions", path, SPILLER_SPILL_PATH), e);
            }
            if (!isAccessible(path)) {
                throw new IllegalArgumentException(format("spill path %s is not accessible, it must be +rwx; adjust %s config property or filesystem permissions", path, SPILLER_SPILL_PATH));
            }
        });
        this.maxUsedSpaceThreshold = maxUsedSpaceThreshold;
        this.spillEncryptionEnabled = spillEncryptionEnabled;
        this.roundRobinIndex = 0;

        this.spillPathHealthCache = buildNonEvictableCacheWithWeakInvalidateAll(
                CacheBuilder.newBuilder()
                        .expireAfterWrite(SPILL_PATH_HEALTH_EXPIRY_INTERVAL),
                CacheLoader.from(path -> isAccessible(path) && isSeeminglyHealthy(path)));
    }

    @PostConstruct
    public void cleanupOldSpillFiles()
    {
        spillPaths.forEach(FileSingleStreamSpillerFactory::cleanupOldSpillFiles);
    }

    @PreDestroy
    public void destroy()
    {
        executor.shutdownNow();
    }

    private static void cleanupOldSpillFiles(Path path)
    {
        try (DirectoryStream<Path> stream = newDirectoryStream(path, SPILL_FILE_GLOB)) {
            stream.forEach(spillFile -> {
                try {
                    log.info("Deleting old spill file: %s", spillFile);
                    delete(spillFile);
                }
                catch (Exception e) {
                    log.warn("Could not cleanup old spill file: %s", spillFile);
                }
            });
        }
        catch (IOException e) {
            log.warn(e, "Error cleaning spill files");
        }
    }

    @Override
    public SingleStreamSpiller create(List<Type> types, SpillContext spillContext, LocalMemoryContext memoryContext)
    {
        Optional<SecretKey> encryptionKey = spillEncryptionEnabled ? Optional.of(createRandomAesEncryptionKey()) : Optional.empty();
        return new FileSingleStreamSpiller(
                serdeFactory,
                encryptionKey,
                executor,
                getNextSpillPath(),
                spillerStats,
                spillContext,
                memoryContext,
                spillPathHealthCache::invalidateAll);
    }

    private synchronized Path getNextSpillPath()
    {
        int spillPathsCount = spillPaths.size();
        for (int i = 0; i < spillPathsCount; ++i) {
            int pathIndex = (roundRobinIndex + i) % spillPathsCount;
            Path path = spillPaths.get(pathIndex);
            if (hasEnoughDiskSpace(path) && spillPathHealthCache.getUnchecked(path)) {
                roundRobinIndex = (roundRobinIndex + i + 1) % spillPathsCount;
                return path;
            }
        }
        if (spillPaths.isEmpty()) {
            throw new TrinoException(OUT_OF_SPILL_SPACE, "No spill paths configured");
        }
        throw new TrinoException(OUT_OF_SPILL_SPACE, "No free or healthy space available for spill");
    }

    private boolean hasEnoughDiskSpace(Path path)
    {
        try {
            FileStore fileStore = getFileStore(path);
            return fileStore.getUsableSpace() > fileStore.getTotalSpace() * (1.0 - maxUsedSpaceThreshold);
        }
        catch (IOException e) {
            throw new TrinoException(OUT_OF_SPILL_SPACE, "Cannot determine free space for spill", e);
        }
    }

    private boolean isAccessible(Path path)
    {
        return isReadable(path) && isWritable(path) && isExecutable(path);
    }

    private boolean isSeeminglyHealthy(Path path)
    {
        try {
            Path healthTemp = createTempFile(path, "spill", "healthcheck");
            return deleteIfExists(healthTemp);
        }
        catch (IOException e) {
            log.warn(e, "Health check failed for spill %s", path);
            return false;
        }
    }

    @VisibleForTesting
    long getSpillPathCacheSize()
    {
        return spillPathHealthCache.size();
    }
}
