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
package io.prestosql.plugin.hive;

import alluxio.client.table.TableMasterClient;
import io.airlift.json.JsonCodec;
import io.prestosql.plugin.hive.authentication.HiveIdentity;
import io.prestosql.plugin.hive.metastore.HiveMetastore;
import io.prestosql.plugin.hive.metastore.alluxio.AlluxioHiveMetastore;
import io.prestosql.plugin.hive.metastore.alluxio.AlluxioHiveMetastoreConfig;
import io.prestosql.plugin.hive.metastore.alluxio.AlluxioMetastoreModule;
import io.prestosql.plugin.hive.security.SqlStandardAccessControlMetadata;
import io.prestosql.spi.connector.ConnectorMetadata;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.connector.SchemaTableName;
import org.joda.time.DateTimeZone;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.TimeZone;
import java.util.concurrent.ScheduledExecutorService;

import static io.prestosql.plugin.hive.AbstractTestHive.TEST_SERVER_VERSION;
import static io.prestosql.plugin.hive.HiveTestUtils.HDFS_ENVIRONMENT;
import static io.prestosql.plugin.hive.HiveTestUtils.TYPE_MANAGER;
import static io.prestosql.plugin.hive.HiveTestUtils.getHiveSession;
import static io.prestosql.plugin.hive.HiveTestUtils.newSession;
import static java.util.concurrent.Executors.newFixedThreadPool;
import static java.util.concurrent.Executors.newScheduledThreadPool;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class TestHiveAlluxioMetastore
{
    private HiveMetastore metastore;
    private String alluxioAddress;

    private static final String SCHEMA = "default";
    private static final Set<SchemaTableName> TABLES = new HashSet<>();
    private static final ConnectorSession SESSION = getHiveSession(new HiveConfig());
    private static final HiveIdentity IDENTITY = new HiveIdentity(SESSION);

    static
    {
        TABLES.add(new SchemaTableName(SCHEMA, "presto_test_bucketed_by_bigint_boolean"));
        TABLES.add(new SchemaTableName(SCHEMA, "presto_test_bucketed_by_double_float"));
        TABLES.add(new SchemaTableName(SCHEMA, "presto_test_bucketed_by_string_int"));
        TABLES.add(new SchemaTableName(SCHEMA, "presto_test_offline_partition"));
        TABLES.add(new SchemaTableName(SCHEMA, "presto_test_partition_format"));
        TABLES.add(new SchemaTableName(SCHEMA, "presto_test_sequence"));
        TABLES.add(new SchemaTableName(SCHEMA, "presto_test_types_orc"));
        TABLES.add(new SchemaTableName(SCHEMA, "presto_test_types_parquet"));
        TABLES.add(new SchemaTableName(SCHEMA, "presto_test_types_rcbinary"));
        TABLES.add(new SchemaTableName(SCHEMA, "presto_test_types_rctext"));
        TABLES.add(new SchemaTableName(SCHEMA, "presto_test_types_sequencefile"));
        TABLES.add(new SchemaTableName(SCHEMA, "presto_test_types_textfile"));
        TABLES.add(new SchemaTableName(SCHEMA, "presto_test_unpartitioned"));
        TABLES.add(new SchemaTableName(SCHEMA, "tmp_presto_test"));
    }

    private HdfsEnvironment hdfsEnvironment;
    private ScheduledExecutorService heartbeatService;
    private HiveConfig hiveConfig;
    protected LocationService locationService;
    private HiveMetadataFactory metadataFactory;
    private HiveTransactionManager transactionManager;

    @Parameters({
            "hive.hadoop2.alluxio.host",
            "hive.hadoop2.alluxio.port"
    })
    @BeforeClass
    public void setup(String host, String port)
    {
        this.hdfsEnvironment = HDFS_ENVIRONMENT;
        this.hiveConfig = new HiveConfig();
        this.hiveConfig.setTimeZone("America/Los_Angeles");
        this.alluxioAddress = host + ":" + port;
        this.metastore = createMetastore();
        this.transactionManager = new HiveTransactionManager();
        this.heartbeatService = newScheduledThreadPool(1);
        locationService = new HiveLocationService(hdfsEnvironment);

        HivePartitionManager partitionManager = new HivePartitionManager(hiveConfig);
        JsonCodec<PartitionUpdate> partitionUpdateCodec = JsonCodec.jsonCodec(PartitionUpdate.class);

        this.metadataFactory = new HiveMetadataFactory(
                this.metastore,
                this.hdfsEnvironment,
                partitionManager,
                DateTimeZone.forTimeZone(TimeZone.getTimeZone(this.hiveConfig.getTimeZone())),
                10,
                true,
                false,
                false,
                false,
                true,
                1000,
                Optional.empty(),
                TYPE_MANAGER,
                locationService,
                partitionUpdateCodec,
                newFixedThreadPool(2),
                heartbeatService,
                new HiveTypeTranslator(),
                TEST_SERVER_VERSION,
                SqlStandardAccessControlMetadata::new);
    }

    HiveMetastore createMetastore()
    {
        AlluxioHiveMetastoreConfig alluxioConfig = new AlluxioHiveMetastoreConfig();
        alluxioConfig.setMasterAddress(this.alluxioAddress);
        TableMasterClient client = AlluxioMetastoreModule.createCatalogMasterClient(alluxioConfig);
        return new AlluxioHiveMetastore(client);
    }

    @Test
    public void testGetAllDatabases()
    {
        List<String> databases = this.metastore.getAllDatabases();
        assertEquals(databases.size(), 1);
    }

    @Test
    public void testGetSchema()
    {
        try (AbstractTestHive.Transaction transaction = newTransaction()) {
            ConnectorMetadata metadata = transaction.getMetadata();
            List<SchemaTableName> tables = metadata.listTables(newSession(), Optional.of(SCHEMA));
            tables.forEach(tbl -> assertTrue(TABLES.contains(tbl)));
        }
    }

    private AbstractTestHive.HiveTransaction newTransaction()
    {
        return new AbstractTestHive.HiveTransaction(transactionManager, metadataFactory.get());
    }
}
