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
import io.trino.plugin.tpch.TpchPlugin;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.DistributedQueryRunner;
import io.trino.tpch.TpchTable;
import org.testng.annotations.Test;

import java.io.File;
import java.util.Optional;

import static com.google.inject.util.Modules.EMPTY_MODULE;
import static io.trino.plugin.hive.HiveTestUtils.HDFS_ENVIRONMENT;
import static io.trino.plugin.hive.metastore.file.FileHiveMetastore.createTestingFileHiveMetastore;
import static io.trino.plugin.tpch.TpchMetadata.TINY_SCHEMA_NAME;
import static io.trino.testing.QueryAssertions.copyTpchTables;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static org.assertj.core.api.Assertions.assertThat;

@Test(singleThreaded = true) // e.g. trackingFileIoProvider is shared mutable state
public class TestIcebergDeleteFilter
        extends AbstractTestQueryFramework
{
    private TrackingFileIoProvider trackingFileIoProvider;

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

        trackingFileIoProvider = new TrackingFileIoProvider(new HdfsFileIoProvider(HDFS_ENVIRONMENT));
        queryRunner.installPlugin(new TestingIcebergPlugin(Optional.of(metastore), Optional.of(trackingFileIoProvider), EMPTY_MODULE));
        queryRunner.createCatalog("iceberg", "iceberg");
        queryRunner.installPlugin(new TpchPlugin());
        queryRunner.createCatalog("tpch", "tpch");

        queryRunner.execute("CREATE SCHEMA test_schema");

        copyTpchTables(queryRunner, "tpch", TINY_SCHEMA_NAME, session, TpchTable.getTables());
        return queryRunner;
    }

    @Test
    public void testDeleteFilterCaching()
    {
        assertUpdate("DELETE FROM lineitem WHERE extendedprice > 1000", 60048);
        trackingFileIoProvider.reset();

        assertQuerySucceeds("SELECT COUNT(*) FROM lineitem");

        long deleteFilterCount = trackingFileIoProvider.getOperationCounts()
                .entrySet().stream()
                .filter(entry -> entry.getKey().getFilePath().contains("/data/") && entry.getKey().getFilePath().endsWith(".orc"))
                .count();
        assertThat(deleteFilterCount).isEqualTo(25);
    }
}
