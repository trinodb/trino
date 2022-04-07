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
package io.trino.sql.planner;

import com.google.common.collect.ImmutableMap;
import io.trino.Session;
import io.trino.plugin.hive.HivePlugin;
import io.trino.testing.DistributedQueryRunner;

import java.nio.file.Path;
import java.util.List;
import java.util.Map;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.airlift.configuration.ConfigurationLoader.loadPropertiesFrom;
import static io.airlift.testing.Closeables.closeAllRuntimeException;
import static io.trino.sql.planner.TestHivePartitionedTpcdsCostBasedPlan.PARTITIONED_TPCDS_METADATA_DIR;
import static io.trino.sql.planner.TestHivePartitionedTpchCostBasedPlan.PARTITIONED_TPCH_METADATA_DIR;
import static io.trino.sql.planner.TestHiveTpcdsCostBasedPlan.TPCDS_METADATA_DIR;
import static io.trino.sql.planner.TestHiveTpcdsCostBasedPlan.TPCDS_SQL_FILES;
import static io.trino.sql.planner.TestHiveTpchCostBasedPlan.TPCH_METADATA_DIR;
import static io.trino.sql.planner.TestHiveTpchCostBasedPlan.TPCH_SQL_FILES;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

/**
 * This class helps to generate the gzip metadata files for TPCH/TPCDS partitioned and unpartitioned data.
 * These metadata files contains statistics which is being used by cost based plan tests for plan generation.
 * To generate these gzip metadata files, this class uses RecordingHiveMetastore which records the hive metastore
 * requests that can be used later, instead of live metastore.
 */
public class HiveMetadataRecorder
        implements AutoCloseable
{
    private DistributedQueryRunner queryRunner;

    private HiveMetadataRecorder(String schema, String configPath, String recordingDir)
            throws Exception
    {
        requireNonNull(schema, "schema is null");
        requireNonNull(configPath, "configPath is null");
        requireNonNull(recordingDir, "recordingDir is null");

        queryRunner = createQueryRunner(schema, configPath, recordingDir);
    }

    private void record(List<String> queries)
    {
        queries.forEach(query -> queryRunner.execute("EXPLAIN " + query));
        // Save the recording cache
        queryRunner.execute("CALL system.write_hive_metastore_recording()");
    }

    private static DistributedQueryRunner createQueryRunner(String schema, String configPath, String recordingDir)
            throws Exception
    {
        ImmutableMap.Builder<String, String> configBuilder = ImmutableMap.builder();
        Session session = testSessionBuilder()
                .setCatalog("hive")
                .setSchema(schema)
                .build();
        DistributedQueryRunner queryRunner = DistributedQueryRunner.builder(session)
                .build();

        String recordingPath = getResourcePath(format("%s/%s.json.gz", recordingDir, schema));
        Path.of(recordingPath).toFile().getParentFile().mkdirs();

        queryRunner.installPlugin(new HivePlugin());
        queryRunner.createCatalog("hive", "hive", configBuilder
                .putAll(loadPropertiesFrom(configPath))
                .put("hive.metastore-recording-path", recordingPath)
                .put("hive.metastore-recording-duration", "2h")
                .buildOrThrow());
        return queryRunner;
    }

    private static String getResourcePath(String relativePath)
    {
        return Path.of("testing/trino-benchto-benchmarks/src/test/resources", relativePath).toString();
    }

    @Override
    public void close()
    {
        closeAllRuntimeException(queryRunner);
        queryRunner = null;
    }

    public static void main(String[] args)
            throws Exception
    {
        recordMetadata(
                ImmutableMap.of(
                        TPCH_METADATA_DIR, "tpch_sf1000_orc",
                        PARTITIONED_TPCH_METADATA_DIR, "tpch_sf1000_orc_part"),
                TPCH_SQL_FILES.stream()
                        .map(AbstractHiveCostBasedPlanTest::readQuery)
                        .collect(toImmutableList()));
        recordMetadata(
                ImmutableMap.of(
                        TPCDS_METADATA_DIR, "tpcds_sf1000_orc",
                        PARTITIONED_TPCDS_METADATA_DIR, "tpcds_sf1000_orc_part"),
                TPCDS_SQL_FILES.stream()
                        .map(AbstractHiveCostBasedPlanTest::readQuery)
                        .collect(toImmutableList()));
    }

    private static void recordMetadata(Map<String, String> recordingDirToSchema, List<String> queries)
            throws Exception
    {
        String configPath = getResourcePath("hive.properties");
        for (String recordingDir : recordingDirToSchema.keySet()) {
            String schema = recordingDirToSchema.get(recordingDir);
            try (HiveMetadataRecorder recorder = new HiveMetadataRecorder(schema, configPath, recordingDir)) {
                recorder.record(queries);
            }
        }
    }
}
