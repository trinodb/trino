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
package io.trino.plugin.iceberg;

import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.metastore.HiveMetastore;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.QueryRunner;
import io.trino.testing.sql.TestTable;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.FileFormat;
import org.intellij.lang.annotations.Language;
import org.junit.jupiter.api.Test;

import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import static io.trino.plugin.iceberg.IcebergTestUtils.getFileSystemFactory;
import static io.trino.plugin.iceberg.IcebergTestUtils.getHiveMetastore;
import static java.util.Locale.ENGLISH;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression guard for the {@code $all_entries} metadata-table OOM (https://github.com/trinodb/trino/issues/29701).
 * <p>
 * Iceberg's {@code BaseAllMetadataTableScan.reachableManifests} materializes the union of every snapshot's
 * manifest list while planning {@code $all_entries}, holding O(snapshots^2) {@code ManifestFile} objects live
 * on a single (coordinator) node. {@code $entries} streams the current snapshot only and stays bounded.
 * <p>
 * This measures the peak <em>live</em> heap (sampled with a forced GC so transient garbage is excluded -
 * the key point is that the regression is in the live set, not total allocation) of {@code count(*)} over
 * each table. The unfixed connector's peak grows as O(snapshots^2); the split-per-manifest fix streams
 * manifests through a bounded {@code ParallelIterable} queue, so its peak is governed by the queue size
 * rather than the snapshot count. Measured at 2000 single-manifest snapshots: unfixed {@code $all_entries}
 * peaks at ~889 MB, the fix at ~67 MB (a ~13x reduction), while {@code $entries} stays single-digit MB.
 */
final class TestIcebergAllEntriesTableMemory
        extends AbstractTestQueryFramework
{
    // At this many single-manifest snapshots the unfixed reachableManifests union is O(snapshots^2) and peaks
    // at ~889 MB, while the streaming fix stays queue-bounded at ~67 MB. The large gap lets the absolute
    // ceiling sit comfortably above the fixed peak (noise margin) yet far below the regression.
    private static final int SNAPSHOTS = 2000;
    // The fixed implementation's peak is bounded by the ParallelIterable in-flight queue (not by N), measured
    // at ~67 MB. The ceiling gives ~2x margin for GC/sampling noise while staying ~6x below the
    // O(snapshots^2) regression (~889 MB at this snapshot count).
    private static final long PEAK_LIVE_HEAP_CEILING = 150L * 1024 * 1024;

    private HiveMetastore metastore;
    private TrinoFileSystemFactory fileSystemFactory;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        QueryRunner queryRunner = IcebergQueryRunner.builder().build();
        metastore = getHiveMetastore(queryRunner);
        fileSystemFactory = getFileSystemFactory(queryRunner);
        return queryRunner;
    }

    @Test
    void testAllEntriesPeakLiveHeapIsBounded()
    {
        try (TestTable table = newTrinoTable("test_all_entries_memory", "AS SELECT 1 id")) {
            BaseTable icebergTable = loadTable(table.getName());
            // Mint many append snapshots, one manifest each (fast-append never merges manifests) - the
            // "append-only writer that never compacts" shape that triggers the O(snapshots^2) planning blow-up.
            for (int i = 0; i < SNAPSHOTS; i++) {
                DataFile dataFile = DataFiles.builder(icebergTable.spec())
                        .withPath(icebergTable.location() + "/data/synthetic-" + i + "." + FileFormat.PARQUET.name().toLowerCase(ENGLISH))
                        .withFormat(FileFormat.PARQUET)
                        .withFileSizeInBytes(1)
                        .withRecordCount(1)
                        .build();
                icebergTable.newFastAppend().appendFile(dataFile).commit();
            }

            String entriesSql = "SELECT count(*) FROM \"" + table.getName() + "$entries\"";
            String allEntriesSql = "SELECT count(*) FROM \"" + table.getName() + "$all_entries\"";

            // Warm up classes/codegen so the measurement reflects steady state, not one-time initialization.
            computeActual(entriesSql);
            computeActual(allEntriesSql);

            long entriesPeakLive = measurePeakLiveHeap(entriesSql);
            long allEntriesPeakLive = measurePeakLiveHeap(allEntriesSql);

            assertThat(allEntriesPeakLive)
                    .as("$all_entries peak live heap (%s bytes) must stay bounded like $entries (%s bytes) for %s snapshots; "
                            + "an O(snapshots^2) reachableManifests retention regressed", allEntriesPeakLive, entriesPeakLive, SNAPSHOTS)
                    .isLessThan(PEAK_LIVE_HEAP_CEILING);
        }
    }

    /**
     * Runs {@code sql} while a watchdog forces a GC before each heap read, so the maximum it observes is the
     * peak <em>live</em> set (transient garbage is collected away). Returns peak-used minus the post-GC baseline.
     */
    private long measurePeakLiveHeap(@Language("SQL") String sql)
    {
        MemoryMXBean memoryMxBean = ManagementFactory.getMemoryMXBean();
        long baseline = forceGcAndRead(memoryMxBean);

        AtomicLong peakLive = new AtomicLong(baseline);
        AtomicBoolean running = new AtomicBoolean(true);
        Thread sampler = new Thread(() -> {
            while (running.get()) {
                System.gc();
                peakLive.accumulateAndGet(memoryMxBean.getHeapMemoryUsage().getUsed(), Math::max);
                try {
                    Thread.sleep(20);
                }
                catch (InterruptedException e) {
                    return;
                }
            }
        });
        sampler.setDaemon(true);
        sampler.start();
        try {
            computeActual(sql);
        }
        finally {
            running.set(false);
            try {
                sampler.join();
            }
            catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        return peakLive.get() - baseline;
    }

    private static long forceGcAndRead(MemoryMXBean memoryMxBean)
    {
        for (int i = 0; i < 3; i++) {
            System.gc();
            try {
                Thread.sleep(100);
            }
            catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        return memoryMxBean.getHeapMemoryUsage().getUsed();
    }

    private BaseTable loadTable(String tableName)
    {
        return IcebergTestUtils.loadTable(tableName, metastore, fileSystemFactory, "hive", "tpch");
    }
}
