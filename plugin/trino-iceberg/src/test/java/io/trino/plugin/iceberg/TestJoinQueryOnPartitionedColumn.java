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

import com.google.common.collect.ImmutableSet;
import io.trino.Session;
import io.trino.plugin.hive.HdfsConfig;
import io.trino.plugin.hive.HdfsConfiguration;
import io.trino.plugin.hive.HdfsConfigurationInitializer;
import io.trino.plugin.hive.HdfsEnvironment;
import io.trino.plugin.hive.HiveHdfsConfiguration;
import io.trino.plugin.hive.NodeVersion;
import io.trino.plugin.hive.authentication.NoHdfsAuthentication;
import io.trino.plugin.hive.metastore.HiveMetastore;
import io.trino.plugin.hive.metastore.MetastoreConfig;
import io.trino.plugin.hive.metastore.file.FileHiveMetastore;
import io.trino.plugin.hive.metastore.file.FileHiveMetastoreConfig;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.MaterializedResult;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.File;

import static io.trino.testing.TestingSession.testSessionBuilder;

public class TestJoinQueryOnPartitionedColumn
        extends AbstractTestQueryFramework
{
    @Override
    protected DistributedQueryRunner createQueryRunner()
            throws Exception
    {
        Session session = testSessionBuilder()
                .setCatalog("iceberg")
                .setSystemProperty("optimize_metadata_queries", "true")
                .build();
        DistributedQueryRunner queryRunner = DistributedQueryRunner.builder(session).build();

        File baseDir = queryRunner.getCoordinator().getBaseDataDir().resolve("iceberg_data").toFile();

        HdfsConfig hdfsConfig = new HdfsConfig();
        HdfsConfiguration hdfsConfiguration = new HiveHdfsConfiguration(new HdfsConfigurationInitializer(hdfsConfig), ImmutableSet.of());
        HdfsEnvironment hdfsEnvironment = new HdfsEnvironment(hdfsConfiguration, hdfsConfig, new NoHdfsAuthentication());

        HiveMetastore metastore = new FileHiveMetastore(
                new NodeVersion("test_version"),
                hdfsEnvironment,
                new MetastoreConfig(),
                new FileHiveMetastoreConfig()
                        .setCatalogDirectory(baseDir.toURI().toString())
                        .setMetastoreUser("test"));

        queryRunner.installPlugin(new TestingIcebergPlugin(metastore, false));
        queryRunner.createCatalog("iceberg", "iceberg");

        return queryRunner;
    }

    @BeforeClass
    public void setUp()
    {
        assertUpdate("CREATE SCHEMA test_schema");
        assertUpdate("CREATE TABLE test_schema.test_table (a BIGINT, b TIMESTAMP(6) with time zone, c row(d BIGINT, e BIGINT)) WITH (partitioning = ARRAY['a', 'day(b)'])");
        assertUpdate("create table test_schema.test_table_2 (foo BIGINT, bar BIGINT)");

        assertUpdate("INSERT INTO test_schema.test_table VALUES (0, CAST('2019-09-08' AS TIMESTAMP(6) with time zone), (1, 1)), (1, CAST('2020-09-09' AS TIMESTAMP(6) with time zone), (1, 1)), (2, CAST('2021-09-09' AS TIMESTAMP(6) with time zone), (1, 1))", 3);
        assertUpdate("INSERT INTO test_schema.test_table VALUES (15, CAST('2019-09-08' AS TIMESTAMP(6) with time zone), (1, 1)), (20, CAST('2020-09-09' AS TIMESTAMP(6) with time zone), (1, 1)), (50, CAST('2021-09-09' AS TIMESTAMP(6) with time zone), (1, 1))", 3);
        assertUpdate("INSERT INTO test_schema.test_table_2 VALUES (0,100), (1,101), (15,102), (16,103)", 4);
    }

    @Test
    public void testPartitionTable()
    {
        assertQuery("SELECT count(*) FROM test_schema.test_table join test_schema.test_table_2 on test_schema.test_table.a = test_schema.test_table_2.foo", "VALUES 3");
        assertQuery("SELECT count(*) FROM test_schema.test_table join test_schema.test_table_2 on test_schema.test_table.a = test_schema.test_table_2.bar", "VALUES 0");
    }
}
