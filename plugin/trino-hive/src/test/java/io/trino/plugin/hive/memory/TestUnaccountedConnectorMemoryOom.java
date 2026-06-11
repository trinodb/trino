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
package io.trino.plugin.hive.memory;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.log.Logger;
import io.airlift.slice.Slices;
import io.airlift.units.DataSize;
import io.trino.execution.TaskId;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.memory.MemoryPool;
import io.trino.orc.OrcWriteValidation.OrcWriteValidationMode;
import io.trino.orc.OrcWriter;
import io.trino.orc.OrcWriterOptions;
import io.trino.orc.OrcWriterStats;
import io.trino.orc.OutputStreamOrcDataSink;
import io.trino.orc.metadata.ColumnMetadata;
import io.trino.orc.metadata.CompressionKind;
import io.trino.orc.metadata.OrcType;
import io.trino.plugin.hive.HiveQueryRunner;
import io.trino.plugin.hive.orc.OrcPageSource;
import io.trino.spi.Page;
import io.trino.spi.block.VariableWidthBlockBuilder;
import io.trino.spi.security.ConnectorIdentity;
import io.trino.spi.type.Type;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.MaterializedResult;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import static io.airlift.units.DataSize.Unit.GIGABYTE;
import static io.airlift.units.DataSize.succinctBytes;
import static io.trino.plugin.hive.TestingHiveUtils.getConnectorService;
import static io.trino.spi.type.VarbinaryType.VARBINARY;
import static java.lang.Math.toIntExact;
import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * Reproduces a JVM-level OutOfMemoryError caused by connector page-source memory that the engine
 * cannot see in time.
 *
 * <p>Background. A {@code ConnectorPageSource} reports its memory only through
 * {@code getMemoryUsage()}, which {@code TableScanOperator} polls AFTER each {@code getNextSourcePage()}
 * returns. The page that a call builds (and any allocation made during the call) is therefore
 * invisible to the engine until the call returns. The connectors also account their internal memory
 * on a disconnected root ({@code newSimpleAggregatedMemoryContext()} in
 * {@code OrcPageSourceFactory}/{@code ParquetPageSourceFactory}/{@code IcebergPageSourceProvider}),
 * so nothing trips the per-query memory limit mid-call.
 *
 * <p>Scenario. {@code -Xmx16g} with default {@code memory.heap-headroom-per-node} (30% ~= 4.8 GB),
 * leaving a ~11.2 GB pool. We pre-fill the pool with ~9 GB of real {@code byte[]} reserved as if
 * another query were running, so the relevant question becomes "can untracked connector memory
 * overflow the ~5 GB headroom", not "can it overflow the whole heap". Then a scan over a handful of
 * ORC files, each holding a single ~1 GB highly-compressible value (tiny on disk), builds a ~1 GB
 * block per split. A test-only barrier in {@link OrcPageSource} holds every split right after the
 * block is built but before the engine polls, so the blocks coexist. Their combined ~6 GB overflows
 * the headroom and the JVM dies — even though the accounted pool usage never approaches its limit,
 * so the engine never aborts any query.
 *
 * <p>Run with: {@code mvnd -pl plugin/trino-hive test -Dtest=TestUnaccountedConnectorMemoryOom
 * -Dair.test.jvmsize=16g -DargLine=-XX:HeapDumpPath=/tmp/repro_heap.hprof}. Expected outcome is a
 * forked-JVM crash with {@code OutOfMemoryError} (the airbase surefire config sets
 * {@code -XX:+ExitOnOutOfMemoryError}).
 */
public class TestUnaccountedConnectorMemoryOom
{
    private static final Logger log = Logger.get(TestUnaccountedConnectorMemoryOom.class);

    // Each ORC file holds one row with a single value of this size. A single value cannot be split
    // across pages, so reading one row forces a block of (at least) this size in one getNextSourcePage.
    // ORC's SliceDirectColumnReader rejects a batch whose values total more than 1 GB, so a single
    // value must stay just under 1 GB. Each split therefore materializes a ~1 GB block.
    private static final long VALUE_BYTES = DataSize.of(1000, DataSize.Unit.MEGABYTE).toBytes();
    // Number of ORC files. Each becomes one split; the engine runs up to task.concurrency at once.
    private static final int SPLITS = 8;
    private static final int TASK_CONCURRENCY = 8;
    // Pre-reserved "other query" memory that fills most of the pool, leaving only the ~5 GB headroom.
    private static final long FILLER_BYTES = DataSize.of(10, GIGABYTE).toBytes();

    // Keep the filler byte[] strongly reachable for the life of the JVM.
    @SuppressWarnings("MismatchedQueryAndUpdateOfCollection")
    private static final List<byte[]> POOL_FILLER_HOLD = new ArrayList<>();

    @Test
    public void reproduceOom()
            throws Exception
    {
        DistributedQueryRunner queryRunner = HiveQueryRunner.builder()
                .setWorkerCount(0) // single node: coordinator runs the tasks
                .setSkipTimezoneSetup(true)
                // run the scan splits concurrently so their (unaccounted) blocks coexist
                .amendSession(session -> session.setSystemProperty("task_concurrency", Integer.toString(TASK_CONCURRENCY)))
                .build();

        MemoryPool pool = queryRunner.getCoordinator().getLocalMemoryManager().getMemoryPool();
        log.info("Memory pool max = %s, -Xmx = %s", succinctBytes(pool.getMaxBytes()), succinctBytes(Runtime.getRuntime().maxMemory()));

        startHeapWatcher(pool);

        // 1. Generate ORC files with a single huge, highly-compressible value each.
        TrinoFileSystem fileSystem = getConnectorService(queryRunner, TrinoFileSystemFactory.class)
                .create(ConnectorIdentity.ofUser("test"));
        Location dir = Location.of("local:///orc_oom_" + UUID.randomUUID());
        fileSystem.createDirectory(dir);
        writeOrcFiles(fileSystem, dir);

        // 2. External table over those files.
        queryRunner.execute("CREATE SCHEMA IF NOT EXISTS hive.repro");
        queryRunner.execute("CREATE TABLE hive.repro.big (c VARBINARY) WITH (format = 'ORC', external_location = '%s')".formatted(dir));

        // 3. Fill the pool with real bytes, reserved as if another query held them.
        fillPool(pool);

        // 4. Hold every split right after it builds its (still unaccounted) block.
        Object materializeLock = new Object();
        OrcPageSource.afterPageBuiltHookForTesting = page -> {
            // ORC returns blocks lazily per channel; getPage() forces every block to be read, so the
            // big value's slice is materialized and resident in heap while we hold the page (modelling
            // a page source that has actually read the data into memory before the engine gets a
            // chance to poll getMemoryUsage / account the page). Materialize one split at a time so the
            // heap climbs visibly one ~1 GB block per line until an allocation finally OOMs.
            synchronized (materializeLock) {
                io.trino.spi.Page loaded = page.getPage();
                long held = loaded.getSizeInBytes();
                // Use flushed System.out so this survives a hard -XX:+ExitOnOutOfMemoryError exit.
                System.out.printf("[split] read %s into heap; heap used=%s, pool reserved=%s (accounted), pool free=%s%n",
                        succinctBytes(held), succinctBytes(usedHeap()),
                        succinctBytes(pool.getReservedBytes()), succinctBytes(pool.getFreeBytes()));
                System.out.flush();
            }
            try {
                // Keep this split's ~1 GB block alive while more splits pile theirs up. The JVM should
                // OOM here (or in a sibling's getPage above) once the unaccounted blocks overflow the
                // headroom on top of the 10 GB accounted filler.
                Thread.sleep(SECONDS.toMillis(120));
            }
            catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        };

        // 5. Force materialization of every value. length() must read the whole block.
        log.info("starting scan; expecting JVM OutOfMemoryError before this returns");
        MaterializedResult result = queryRunner.execute("SELECT max(length(c)) FROM hive.repro.big");
        log.error("UNEXPECTED: query completed without OOM, result = %s", result.getOnlyValue());
    }

    private static void writeOrcFiles(TrinoFileSystem fileSystem, Location dir)
            throws Exception
    {
        List<String> columnNames = ImmutableList.of("c");
        List<Type> types = ImmutableList.of(VARBINARY);
        ColumnMetadata<OrcType> orcType = OrcType.createRootOrcType(columnNames, types);

        // Build one page with a single ~1 GB value of zeros (compresses to ~nothing on disk).
        VariableWidthBlockBuilder blockBuilder = (VariableWidthBlockBuilder) VARBINARY.createBlockBuilder(null, 1, toIntExact(VALUE_BYTES));
        blockBuilder.writeEntry(Slices.allocate(toIntExact(VALUE_BYTES)));
        Page page = new Page(blockBuilder.build());

        for (int i = 0; i < SPLITS; i++) {
            Location file = dir.appendPath("data_" + i + ".orc");
            try (OrcWriter writer = new OrcWriter(
                    OutputStreamOrcDataSink.create(fileSystem.newOutputFile(file)),
                    columnNames,
                    types,
                    orcType,
                    CompressionKind.ZSTD,
                    new OrcWriterOptions(),
                    ImmutableMap.of(),
                    false,
                    OrcWriteValidationMode.BOTH,
                    new OrcWriterStats())) {
                writer.write(page);
            }
            log.info("wrote %s (%s logical)", file, succinctBytes(VALUE_BYTES));
        }
    }

    private static void fillPool(MemoryPool pool)
    {
        long chunk = DataSize.of(64, DataSize.Unit.MEGABYTE).toBytes();
        long remaining = FILLER_BYTES;
        while (remaining > 0) {
            int size = toIntExact(Math.min(chunk, remaining));
            POOL_FILLER_HOLD.add(new byte[size]);
            remaining -= size;
        }
        // Reserve in the pool under a fake query/task, as if another query had allocated this memory.
        pool.reserve(TaskId.valueOf("filler_query.0.0.0"), "filler", FILLER_BYTES);
        log.info("filled pool: reserved = %s of max %s (free = %s)",
                succinctBytes(pool.getReservedBytes()), succinctBytes(pool.getMaxBytes()), succinctBytes(pool.getFreeBytes()));
    }

    private static void startHeapWatcher(MemoryPool pool)
    {
        Thread watcher = new Thread(() -> {
            while (true) {
                System.out.printf("[watch] heap used=%s  pool reserved=%s (accounted)  pool free=%s%n",
                        succinctBytes(usedHeap()), succinctBytes(pool.getReservedBytes()), succinctBytes(pool.getFreeBytes()));
                System.out.flush();
                try {
                    Thread.sleep(200);
                }
                catch (InterruptedException e) {
                    return;
                }
            }
        }, "heap-watcher");
        watcher.setDaemon(true);
        watcher.start();
    }

    private static long usedHeap()
    {
        Runtime runtime = Runtime.getRuntime();
        return runtime.totalMemory() - runtime.freeMemory();
    }
}
