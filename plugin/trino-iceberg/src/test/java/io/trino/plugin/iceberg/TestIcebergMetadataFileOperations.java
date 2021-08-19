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

import io.trino.Session;
import io.trino.plugin.hive.metastore.HiveMetastore;
import io.trino.plugin.iceberg.testing.TrackingFileIoProvider;
import io.trino.plugin.iceberg.testing.TrackingFileIoProvider.OperationContext;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.DistributedQueryRunner;
import org.testng.annotations.Test;

import javax.management.MBeanServer;
import javax.management.ObjectName;

import java.io.File;
import java.lang.management.ManagementFactory;
import java.util.Map;
import java.util.Optional;
import java.util.function.Predicate;

import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static io.trino.plugin.hive.metastore.file.FileHiveMetastore.createTestingFileHiveMetastore;
import static io.trino.plugin.iceberg.testing.TrackingFileIoProvider.OperationType.INPUT_FILE_EXISTS;
import static io.trino.plugin.iceberg.testing.TrackingFileIoProvider.OperationType.INPUT_FILE_GET_LENGTH;
import static io.trino.plugin.iceberg.testing.TrackingFileIoProvider.OperationType.INPUT_FILE_NEW_STREAM;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static java.util.Objects.requireNonNull;
import static org.testng.Assert.assertEquals;

public class TestIcebergMetadataFileOperations
        extends AbstractTestQueryFramework
{
    private static final MBeanServer BEAN_SERVER = ManagementFactory.getPlatformMBeanServer();
    private static final String BEAN_NAME = "trino.plugin.iceberg.testing:type=TrackingFileIoProvider,name=iceberg";
    private static final String OPERATION_COUNTS_ATTRIBUTE = "OperationCounts";
    private static final Session TEST_SESSION = testSessionBuilder()
            .setCatalog("iceberg")
            .setSchema("test_schema")
            .build();

    @Override
    protected DistributedQueryRunner createQueryRunner()
            throws Exception
    {
        Session session = testSessionBuilder()
                .setCatalog("iceberg")
                .setSchema("test_schema")
                .build();

        DistributedQueryRunner queryRunner = DistributedQueryRunner.builder(session)
                // Tests that inspect MBean attributes need to run with just one node, otherwise
                // the attributes may come from the bound class instance in non-coordinator node
                .setNodeCount(1)
                .build();

        File baseDir = queryRunner.getCoordinator().getBaseDataDir().resolve("iceberg_data").toFile();
        HiveMetastore metastore = createTestingFileHiveMetastore(baseDir);
        queryRunner.installPlugin(new TestingIcebergPlugin(metastore, true));
        queryRunner.createCatalog("iceberg", "iceberg");

        queryRunner.execute("CREATE SCHEMA test_schema");
        return queryRunner;
    }

    @Test
    public void testOperations()
            throws Exception
    {
        assertUpdate("CREATE TABLE test_select_from AS SELECT 1 col0", 1);
        String queryId = runAndGetId("SELECT * FROM test_select_from");

        OperationCounts counts = getCounts().forQueryId(queryId);

        // The table contains exactly one metadata, snapshot and manifest files, so file-naming based filters are sufficient
        // for assertions.

        String metadataFileSuffix = "metadata.json";
        assertEquals(counts.forPathContaining(metadataFileSuffix).forOperation(INPUT_FILE_NEW_STREAM).sum(), 1);
        // getLength is cached, so only assert number of different InputFile instances for a file
        assertEquals(counts.forPathContaining(metadataFileSuffix).forOperation(INPUT_FILE_GET_LENGTH).get().size(), 0);
        assertEquals(counts.forPathContaining(metadataFileSuffix).forOperation(INPUT_FILE_EXISTS).sum(), 0);

        String snapshotFilePrefix = "/snap-";
        assertEquals(counts.forPathContaining(snapshotFilePrefix).forOperation(INPUT_FILE_NEW_STREAM).sum(), 1);
        // getLength is cached, so only assert number of different InputFile instances for a file
        assertEquals(counts.forPathContaining(snapshotFilePrefix).forOperation(INPUT_FILE_GET_LENGTH).get().size(), 1);
        assertEquals(counts.forPathContaining(snapshotFilePrefix).forOperation(INPUT_FILE_EXISTS).sum(), 0);

        String manifestFileSuffix = "-m0.avro";
        // Iceberg seems to read manifest files twice during Streams.stream(combinedScanIterable) call in IcebergSplitSource
        assertEquals(counts.forPathContaining(manifestFileSuffix).forOperation(INPUT_FILE_NEW_STREAM).sum(), 2);
        // getLength is cached, so only assert number of different InputFile instances for a file
        assertEquals(counts.forPathContaining(manifestFileSuffix).forOperation(INPUT_FILE_GET_LENGTH).get().size(), 1);
        assertEquals(counts.forPathContaining(manifestFileSuffix).forOperation(INPUT_FILE_EXISTS).sum(), 0);
    }

    private String runAndGetId(String query)
    {
        return getDistributedQueryRunner().executeWithQueryId(TEST_SESSION, query).getQueryId().getId();
    }

    private static OperationCounts getCounts()
            throws Exception
    {
        return new OperationCounts((Map<OperationContext, Integer>) BEAN_SERVER.getAttribute(new ObjectName(BEAN_NAME), OPERATION_COUNTS_ATTRIBUTE));
    }

    private static class OperationCounts
    {
        private final Map<OperationContext, Integer> allCounts;
        private Optional<String> queryId = Optional.empty();
        private Predicate<String> pathPredicate = path -> true;
        private Optional<TrackingFileIoProvider.OperationType> type = Optional.empty();

        public OperationCounts(Map<OperationContext, Integer> allCounts)
        {
            this.allCounts = requireNonNull(allCounts, "allCounts is null");
        }

        public OperationCounts forQueryId(String queryId)
        {
            this.queryId = Optional.of(queryId);
            return this;
        }

        public OperationCounts forPathContaining(String value)
        {
            this.pathPredicate = path -> path.contains(value);
            return this;
        }

        public OperationCounts forOperation(TrackingFileIoProvider.OperationType type)
        {
            this.type = Optional.of(type);
            return this;
        }

        public Map<OperationContext, Integer> get()
        {
            return allCounts.entrySet().stream()
                    .filter(entry -> queryId.map(id -> entry.getKey().getQueryId().equals(id)).orElse(true))
                    .filter(entry -> pathPredicate.test(entry.getKey().getFilePath()))
                    .filter(entry -> type.map(value -> value == entry.getKey().getOperationType()).orElse(true))
                    .collect(toImmutableMap(Map.Entry::getKey, Map.Entry::getValue));
        }

        public int sum()
        {
            return get().values().stream().mapToInt(Integer::intValue).sum();
        }
    }
}
