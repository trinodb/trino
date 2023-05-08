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
package io.trino.plugin.iceberg.catalog.hms;

import com.google.common.collect.ImmutableSet;
import io.airlift.units.Duration;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.filesystem.hdfs.HdfsFileSystemFactory;
import io.trino.hdfs.DynamicHdfsConfiguration;
import io.trino.hdfs.HdfsConfig;
import io.trino.hdfs.HdfsConfigurationInitializer;
import io.trino.hdfs.HdfsEnvironment;
import io.trino.hdfs.TrinoHdfsFileSystemStats;
import io.trino.hdfs.authentication.NoHdfsAuthentication;
import io.trino.hdfs.s3.HiveS3Config;
import io.trino.hdfs.s3.TrinoS3ConfigurationInitializer;
import io.trino.plugin.base.CatalogName;
import io.trino.plugin.hive.TrinoViewHiveMetastore;
import io.trino.plugin.hive.containers.HiveMinioDataLake;
import io.trino.plugin.hive.metastore.cache.CachingHiveMetastore;
import io.trino.plugin.hive.metastore.thrift.BridgingHiveMetastore;
import io.trino.plugin.hive.metastore.thrift.ThriftMetastore;
import io.trino.plugin.hive.metastore.thrift.ThriftMetastoreConfig;
import io.trino.plugin.hive.metastore.thrift.ThriftMetastoreFactory;
import io.trino.plugin.iceberg.IcebergSchemaProperties;
import io.trino.plugin.iceberg.catalog.BaseTrinoCatalogTest;
import io.trino.plugin.iceberg.catalog.TrinoCatalog;
import io.trino.spi.security.ConnectorIdentity;
import io.trino.spi.type.TestingTypeManager;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;

import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.google.common.base.Verify.verify;
import static io.trino.plugin.hive.TestingThriftHiveMetastoreBuilder.testingThriftHiveMetastoreBuilder;
import static io.trino.plugin.hive.containers.HiveHadoop.HIVE3_IMAGE;
import static io.trino.plugin.hive.metastore.cache.CachingHiveMetastore.memoizeMetastore;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static io.trino.testing.containers.Minio.MINIO_ACCESS_KEY;
import static io.trino.testing.containers.Minio.MINIO_SECRET_KEY;
import static java.util.concurrent.TimeUnit.MINUTES;

public class TestTrinoHiveCatalogWithHiveMetastore
        extends BaseTrinoCatalogTest
{
    private static final String bucketName = "test-hive-catalog-with-hms-" + randomNameSuffix();

    // Use MinIO for storage, since HDFS is hard to get working in a unit test
    private HiveMinioDataLake dataLake;

    @BeforeClass
    public void setUp()
    {
        dataLake = new HiveMinioDataLake(bucketName, HIVE3_IMAGE);
        dataLake.start();
    }

    @AfterClass(alwaysRun = true)
    public void tearDown()
            throws Exception
    {
        if (dataLake != null) {
            dataLake.stop();
            dataLake = null;
        }
    }

    @Override
    protected TrinoCatalog createTrinoCatalog(boolean useUniqueTableLocations)
    {
        TrinoFileSystemFactory fileSystemFactory = new HdfsFileSystemFactory(new HdfsEnvironment(
                new DynamicHdfsConfiguration(
                        new HdfsConfigurationInitializer(
                                new HdfsConfig(),
                                Set.of(new TrinoS3ConfigurationInitializer(new HiveS3Config()
                                        .setS3Endpoint(dataLake.getMinio().getMinioAddress())
                                        .setS3SslEnabled(false)
                                        .setS3AwsAccessKey(MINIO_ACCESS_KEY)
                                        .setS3AwsSecretKey(MINIO_SECRET_KEY)
                                        .setS3PathStyleAccess(true)))),
                        ImmutableSet.of()),
                new HdfsConfig(),
                new NoHdfsAuthentication()),
                new TrinoHdfsFileSystemStats());
        ThriftMetastore thriftMetastore = testingThriftHiveMetastoreBuilder()
                .thriftMetastoreConfig(new ThriftMetastoreConfig()
                        // Read timed out sometimes happens with the default timeout
                        .setMetastoreTimeout(new Duration(1, MINUTES)))
                .metastoreClient(dataLake.getHiveHadoop().getHiveMetastoreEndpoint())
                .build();
        CachingHiveMetastore metastore = memoizeMetastore(new BridgingHiveMetastore(thriftMetastore), 1000);
        return new TrinoHiveCatalog(
                new CatalogName("catalog"),
                metastore,
                new TrinoViewHiveMetastore(metastore, false, "trino-version", "Test"),
                fileSystemFactory,
                new TestingTypeManager(),
                new HiveMetastoreTableOperationsProvider(fileSystemFactory, new ThriftMetastoreFactory()
                {
                    @Override
                    public boolean isImpersonationEnabled()
                    {
                        verify(new ThriftMetastoreConfig().isImpersonationEnabled(), "This test wants to test the default behavior and assumes it's off");
                        return false;
                    }

                    @Override
                    public ThriftMetastore createMetastore(Optional<ConnectorIdentity> identity)
                    {
                        return thriftMetastore;
                    }
                }),
                useUniqueTableLocations,
                false,
                false);
    }

    @Override
    protected Map<String, Object> defaultNamespaceProperties(String newNamespaceName)
    {
        return Map.of(IcebergSchemaProperties.LOCATION_PROPERTY, "s3://%s/%s".formatted(bucketName, newNamespaceName));
    }
}
