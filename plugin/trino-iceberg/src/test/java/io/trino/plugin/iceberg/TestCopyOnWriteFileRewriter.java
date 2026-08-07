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

import io.trino.plugin.hive.RollbackAction;
import io.trino.spi.Page;
import io.trino.testing.connector.TestingConnectorSession;
import org.apache.iceberg.Metrics;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.io.LocationProvider;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

final class TestCopyOnWriteFileRewriter
{
    private static final PartitionSpec UNPARTITIONED = PartitionSpec.unpartitioned();
    private static final String ORIGINAL_PATH = "s3://bucket/data/original.parquet";
    private static final String NEW_PATH = "s3://bucket/data/new.parquet";

    @Test
    void computeNewFilePathEmbedsQueryIdAndUsesLocationProviderForUnpartitionedTable()
    {
        CapturingLocationProvider locationProvider = new CapturingLocationProvider();

        String path = CopyOnWriteFileRewriter.computeNewFilePath(
                TestingConnectorSession.SESSION,
                IcebergFileFormat.PARQUET,
                locationProvider,
                UNPARTITIONED,
                Optional.empty());

        assertThat(path).startsWith("data-location/");
        assertThat(path).contains(TestingConnectorSession.SESSION.getQueryId());
        assertThat(path).endsWith(".parquet");
        assertThat(locationProvider.unpartitionedCalls).isEqualTo(1);
        assertThat(locationProvider.partitionedCalls).isZero();
    }

    @Test
    void computeNewFilePathDelegatesToPartitionedLocationWhenPartitionDataPresent()
    {
        CapturingLocationProvider locationProvider = new CapturingLocationProvider();

        String path = CopyOnWriteFileRewriter.computeNewFilePath(
                TestingConnectorSession.SESSION,
                IcebergFileFormat.ORC,
                locationProvider,
                UNPARTITIONED,
                Optional.of(new PartitionData(new Object[0])));

        assertThat(path).startsWith("partitioned-location/");
        assertThat(path).endsWith(".orc");
        assertThat(locationProvider.partitionedCalls).isEqualTo(1);
        assertThat(locationProvider.unpartitionedCalls).isZero();
    }

    @Test
    void computeNewFilePathProducesUniquePathsAcrossInvocations()
    {
        CapturingLocationProvider locationProvider = new CapturingLocationProvider();

        String path1 = CopyOnWriteFileRewriter.computeNewFilePath(
                TestingConnectorSession.SESSION, IcebergFileFormat.PARQUET, locationProvider, UNPARTITIONED, Optional.empty());
        String path2 = CopyOnWriteFileRewriter.computeNewFilePath(
                TestingConnectorSession.SESSION, IcebergFileFormat.PARQUET, locationProvider, UNPARTITIONED, Optional.empty());

        // Random UUID suffix guarantees collision-free paths even under identical inputs and
        // the same query id, so two concurrent rewrites in the same query never overwrite each other.
        assertThat(path1).isNotEqualTo(path2);
    }

    @Test
    void returnsNewFileAndPreservesRollbackActionWhenRewriteProducesSurvivingRows()
    {
        FakeWriter writer = FakeWriter.withRecordCount(100);

        CopyOnWriteFileRewriter.RewriteResult result = CopyOnWriteFileRewriter.finalizeRewrite(
                writer,
                ORIGINAL_PATH,
                NEW_PATH,
                UNPARTITIONED,
                Optional.empty(),
                IcebergFileFormat.PARQUET, /* originalRecordCount */ 150, /* originalFileSize */ 1000,
                System.currentTimeMillis());

        assertThat(result.newFile()).isPresent();
        assertThat(writer.calls).containsExactly("commit");
        // Rollback action intact so caller can invoke it if downstream commit fails.
        assertRollbackActionDeletesCommittedFile(result.rollbackAction(), writer);
    }

    @Test
    void deletesEmptyOutputEagerlyAndReturnsNoOpRollbackWhenAllRowsAreDeleted()
    {
        FakeWriter writer = FakeWriter.withRecordCount(0);

        CopyOnWriteFileRewriter.RewriteResult result = CopyOnWriteFileRewriter.finalizeRewrite(
                writer,
                ORIGINAL_PATH,
                NEW_PATH,
                UNPARTITIONED,
                Optional.empty(),
                IcebergFileFormat.PARQUET, /* originalRecordCount */ 150, /* originalFileSize */ 1000,
                System.currentTimeMillis());

        assertThat(result.newFile()).isEmpty();
        // Empty file cleaned up at the sink: rollback action was consumed, now no-op.
        assertThat(writer.rollbackActionRunCount).isEqualTo(1);
        runUnchecked(result.rollbackAction());
        assertThat(writer.rollbackActionRunCount).isEqualTo(1); // still one: returned rollback is no-op
    }

    @Test
    void rollsBackPartialFileAndRethrowsWhenWriterCommitFails()
    {
        FakeWriter writer = FakeWriter.withRecordCount(100);
        writer.commitException = new RuntimeException("commit exploded");

        assertThatThrownBy(() -> CopyOnWriteFileRewriter.finalizeRewrite(
                writer,
                ORIGINAL_PATH,
                NEW_PATH,
                UNPARTITIONED,
                Optional.empty(),
                IcebergFileFormat.PARQUET,
                150,
                1000,
                System.currentTimeMillis()))
                .isInstanceOf(RuntimeException.class)
                .hasMessage("commit exploded");

        // Rewriter must have attempted writer.rollback() to clean partial file.
        assertThat(writer.calls).containsExactly("commit", "rollback");
    }

    @Test
    void deletesCommittedFileAndRethrowsWhenPostCommitMetricsRetrievalFails()
    {
        FakeWriter writer = FakeWriter.withRecordCount(100);
        writer.fileMetricsException = new RuntimeException("metrics exploded");

        assertThatThrownBy(() -> CopyOnWriteFileRewriter.finalizeRewrite(
                writer,
                ORIGINAL_PATH,
                NEW_PATH,
                UNPARTITIONED,
                Optional.empty(),
                IcebergFileFormat.PARQUET,
                150,
                1000,
                System.currentTimeMillis()))
                .isInstanceOf(RuntimeException.class)
                .hasMessage("metrics exploded");

        // Post-commit failure: committed file must be deleted via rollbackAction.
        assertThat(writer.calls).containsExactly("commit");
        assertThat(writer.rollbackActionRunCount).isEqualTo(1);
    }

    @Test
    void throwsUncheckedIOExceptionWhenEmptyOutputCleanupFailsToDelete()
    {
        FakeWriter writer = FakeWriter.withRecordCount(0);
        writer.rollbackActionRunException = new IOException("fs refused delete");

        assertThatThrownBy(() -> CopyOnWriteFileRewriter.finalizeRewrite(
                writer,
                ORIGINAL_PATH,
                NEW_PATH,
                UNPARTITIONED,
                Optional.empty(),
                IcebergFileFormat.PARQUET,
                150,
                1000,
                System.currentTimeMillis()))
                .isInstanceOf(UncheckedIOException.class)
                .hasMessageContaining(NEW_PATH)
                .hasRootCauseMessage("fs refused delete");
    }

    private static void assertRollbackActionDeletesCommittedFile(RollbackAction rollbackAction, FakeWriter writer)
    {
        int before = writer.rollbackActionRunCount;
        runUnchecked(rollbackAction);
        assertThat(writer.rollbackActionRunCount).isEqualTo(before + 1);
    }

    private static void runUnchecked(RollbackAction rollbackAction)
    {
        try {
            rollbackAction.run();
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /**
     * Minimal fake that exercises commit/rollback semantics. Not thread-safe; tests are single-threaded.
     */
    private static final class FakeWriter
            implements IcebergFileWriter
    {
        final List<String> calls = new ArrayList<>();
        RuntimeException commitException;
        RuntimeException fileMetricsException;
        IOException rollbackActionRunException;
        int rollbackActionRunCount;
        private final long recordCount;

        private FakeWriter(long recordCount)
        {
            this.recordCount = recordCount;
        }

        static FakeWriter withRecordCount(long recordCount)
        {
            return new FakeWriter(recordCount);
        }

        @Override
        public RollbackAction commit()
        {
            calls.add("commit");
            if (commitException != null) {
                throw commitException;
            }
            return () -> {
                rollbackActionRunCount++;
                if (rollbackActionRunException != null) {
                    throw rollbackActionRunException;
                }
            };
        }

        @Override
        public void rollback()
        {
            calls.add("rollback");
        }

        @Override
        public FileMetrics getFileMetrics()
        {
            if (fileMetricsException != null) {
                throw fileMetricsException;
            }
            return new FileMetrics(new Metrics(recordCount), Optional.empty());
        }

        @Override
        public long getWrittenBytes()
        {
            return 10L;
        }

        @Override
        public long getMemoryUsage()
        {
            return 0;
        }

        @Override
        public void appendRows(Page dataPage)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public long getValidationCpuNanos()
        {
            return 0;
        }
    }

    private static final class CapturingLocationProvider
            implements LocationProvider
    {
        int unpartitionedCalls;
        int partitionedCalls;

        @Override
        public String newDataLocation(String filename)
        {
            unpartitionedCalls++;
            return "data-location/" + filename;
        }

        @Override
        public String newDataLocation(PartitionSpec spec, StructLike partitionData, String filename)
        {
            partitionedCalls++;
            return "partitioned-location/" + filename;
        }
    }
}
