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
package io.trino.plugin.iceberg.catalog.rest;

import com.google.common.collect.ImmutableMap;
import io.trino.cache.EvictableCacheBuilder;
import io.trino.plugin.hive.orc.OrcReaderConfig;
import io.trino.plugin.hive.orc.OrcWriterConfig;
import io.trino.plugin.hive.parquet.ParquetReaderConfig;
import io.trino.plugin.hive.parquet.ParquetWriterConfig;
import io.trino.plugin.iceberg.CommitTaskData;
import io.trino.plugin.iceberg.DefaultIcebergFileSystemFactory;
import io.trino.plugin.iceberg.IcebergConfig;
import io.trino.plugin.iceberg.IcebergMetadata;
import io.trino.plugin.iceberg.IcebergSessionProperties;
import io.trino.plugin.iceberg.IcebergTableCredentials;
import io.trino.plugin.iceberg.TableStatisticsWriter;
import io.trino.plugin.iceberg.catalog.rest.IcebergRestCatalogConfig.Security;
import io.trino.spi.NodeVersion;
import io.trino.spi.catalog.CatalogName;
import io.trino.spi.connector.ConnectorMetadata;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableCredentials;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.security.PrincipalType;
import io.trino.spi.security.TrinoPrincipal;
import io.trino.testing.TestingConnectorSession;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.rest.CredentialVendingRESTCatalogAdapter;
import org.apache.iceberg.rest.CredentialVendingRESTCatalogAdapter.Bundle;
import org.apache.iceberg.rest.DelegatingRestSessionCatalog;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.Optional;

import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static com.google.common.util.concurrent.MoreExecutors.newDirectExecutorService;
import static io.airlift.json.JsonCodec.jsonCodec;
import static io.airlift.units.Duration.ZERO;
import static io.trino.hdfs.HdfsTestUtils.HDFS_FILE_SYSTEM_FACTORY;
import static io.trino.plugin.iceberg.IcebergTestUtils.TABLE_STATISTICS_READER;
import static io.trino.plugin.iceberg.catalog.rest.IcebergRestCatalogConfig.SessionType.NONE;
import static io.trino.plugin.iceberg.catalog.rest.RestCatalogTestUtils.backendCatalog;
import static io.trino.plugin.iceberg.delete.DeletionVectorWriter.UNSUPPORTED_DELETION_VECTOR_WRITER;
import static io.trino.sql.planner.TestingPlannerContext.PLANNER_CONTEXT;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static io.trino.type.InternalTypeManager.TESTING_TYPE_MANAGER;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;

/**
 * End-to-end test for the coordinator's evictable {@code tableCredentialsCache} in {@link IcebergMetadata},
 * using a {@link CredentialVendingRESTCatalogAdapter} that injects fake S3 vended credentials into
 * every {@code loadTable} REST response. Each call returns a unique access key, so the test can
 * verify that cache eviction triggers a genuine re-fetch from the REST catalog.
 * <p>
 * This validates the full credential flow:
 * REST catalog (RESTCatalogAdapter) → TrinoRestCatalog → IcebergMetadata.getTableCredentials()
 * <p>
 * The evictable coordinator cache is the primary mechanism for credential refresh: when the cache
 * TTL expires, the next {@code getTableCredentials()} call transparently re-fetches fresh credentials.
 * The coordinator uses {@code Supplier}-based credential resolution (via {@code SqlStage}) so that
 * every task update receives the latest credentials.
 */
@TestInstance(PER_CLASS)
class TestIcebergMetadataCredentialCacheWithRestCatalog
{
    private static final String NAMESPACE = "test_ns_" + randomNameSuffix();
    private static final String TABLE_NAME = "test_table_" + randomNameSuffix();
    private static final SchemaTableName SCHEMA_TABLE = new SchemaTableName(NAMESPACE, TABLE_NAME);
    private static final ConnectorSession SESSION = TestingConnectorSession.builder()
            .setPropertyMetadata(new IcebergSessionProperties(
                    new IcebergConfig(),
                    new OrcReaderConfig(),
                    new OrcWriterConfig(),
                    new ParquetReaderConfig(),
                    new ParquetWriterConfig())
                    .getSessionProperties())
            .build();

    private Path warehouseLocation;
    private DelegatingRestSessionCatalog restSessionCatalog;
    private CredentialVendingRESTCatalogAdapter vendingAdapter;
    private TrinoRestCatalog trinoCatalog;

    @BeforeAll
    void setUp()
            throws IOException
    {
        warehouseLocation = Files.createTempDirectory("iceberg-credential-cache-test");
        warehouseLocation.toFile().deleteOnExit();

        Catalog backendCatalog = backendCatalog(warehouseLocation);
        Bundle bundle = CredentialVendingRESTCatalogAdapter.createBundle(backendCatalog);
        restSessionCatalog = bundle.catalog();
        vendingAdapter = bundle.adapter();
        restSessionCatalog.initialize("test_catalog", ImmutableMap.of());

        trinoCatalog = new TrinoRestCatalog(
                new DefaultIcebergFileSystemFactory(HDFS_FILE_SYSTEM_FACTORY),
                restSessionCatalog,
                new CatalogName("test_catalog"),
                Security.NONE,
                NONE,
                ImmutableMap.of(),
                false,
                "test",
                TESTING_TYPE_MANAGER,
                false,
                false,
                EvictableCacheBuilder.newBuilder().expireAfterWrite(1000, MILLISECONDS).shareNothingWhenDisabled().build(),
                EvictableCacheBuilder.newBuilder().expireAfterWrite(1000, MILLISECONDS).shareNothingWhenDisabled().build(),
                true);

        trinoCatalog.createNamespace(SESSION, NAMESPACE, ImmutableMap.of(), new TrinoPrincipal(PrincipalType.USER, SESSION.getUser()));
        String tableLocation = warehouseLocation.resolve("iceberg_data").resolve(NAMESPACE).resolve(TABLE_NAME).toFile().getAbsolutePath();
        trinoCatalog.newCreateTableTransaction(
                        SESSION,
                        SCHEMA_TABLE,
                        new Schema(Types.NestedField.required(1, "id", Types.LongType.get())),
                        PartitionSpec.unpartitioned(),
                        SortOrder.unsorted(),
                        Optional.of(tableLocation),
                        ImmutableMap.of())
                .commitTransaction();
    }

    @AfterAll
    void tearDown()
            throws Exception
    {
        if (restSessionCatalog != null) {
            restSessionCatalog.close();
        }
    }

    @Test
    void testVendedCredentialsFlowThroughRestCatalog()
    {
        ConnectorMetadata metadata = createIcebergMetadata();
        ConnectorTableHandle tableHandle = metadata.getTableHandle(SESSION, SCHEMA_TABLE, Optional.empty(), Optional.empty());
        assertThat(tableHandle).isNotNull();

        Optional<ConnectorTableCredentials> credentials = metadata.getTableCredentials(SESSION, tableHandle);
        assertThat(credentials).isPresent();
        assertThat(credentials.get()).isInstanceOf(IcebergTableCredentials.class);

        IcebergTableCredentials icebergCredentials = (IcebergTableCredentials) credentials.get();
        // Verify vended S3 credentials flowed through the REST → Trino pipeline
        assertThat(icebergCredentials.fileIoProperties()).containsKey("s3.access-key-id");
        assertThat(icebergCredentials.fileIoProperties().get("s3.access-key-id")).startsWith("FAKE_ACCESS_KEY_");
        assertThat(icebergCredentials.fileIoProperties()).containsKey("s3.secret-access-key");
        assertThat(icebergCredentials.fileIoProperties()).containsKey("s3.session-token");
        // Because S3 credentials are present, expiresAt should be present (not empty)
        assertThat(icebergCredentials.expiresAt()).isPresent();
        // The adapter was called at least once (during setUp or this test)
        assertThat(vendingAdapter.getLoadTableCount()).isGreaterThan(0);
    }

    @Test
    void testCredentialsAreCachedAcrossMultipleCalls()
    {
        ConnectorMetadata metadata = createIcebergMetadata();
        ConnectorTableHandle tableHandle = metadata.getTableHandle(SESSION, SCHEMA_TABLE, Optional.empty(), Optional.empty());
        assertThat(tableHandle).isNotNull();

        Optional<ConnectorTableCredentials> first = metadata.getTableCredentials(SESSION, tableHandle);
        Optional<ConnectorTableCredentials> second = metadata.getTableCredentials(SESSION, tableHandle);

        assertThat(first).isPresent();
        assertThat(second).isPresent();
        // Same cached object from IcebergMetadata.tableCredentialsCache — no re-fetch
        assertThat(first.get()).isSameAs(second.get());
    }

    @Test
    void testExpiredEntryTriggersRefreshWithNewCredentials()
    {
        IcebergMetadata metadata = createIcebergMetadata();
        ConnectorTableHandle tableHandle = metadata.getTableHandle(SESSION, SCHEMA_TABLE, Optional.empty(), Optional.empty());
        assertThat(tableHandle).isNotNull();

        Optional<ConnectorTableCredentials> first = metadata.getTableCredentials(SESSION, tableHandle);
        assertThat(first).isPresent();
        IcebergTableCredentials firstCredentials = (IcebergTableCredentials) first.get();
        String firstAccessKey = firstCredentials.fileIoProperties().get("s3.access-key-id");
        int loadsAfterFirst = vendingAdapter.getLoadTableCount();

        // Mark all cached credentials as expired to simulate token expiry.
        // getOrLoadTableCredentials() detects cached != null with shouldRefresh() == true,
        // invalidates TrinoRestCatalog.tableCache, and re-fetches via a fresh REST call.
        metadata.invalidateTableCredentialsCache();

        Optional<ConnectorTableCredentials> second = metadata.getTableCredentials(SESSION, tableHandle);
        assertThat(second).isPresent();
        IcebergTableCredentials secondCredentials = (IcebergTableCredentials) second.get();
        String secondAccessKey = secondCredentials.fileIoProperties().get("s3.access-key-id");

        // After per-entry expiry: different credential instance with a new access key from a fresh REST call
        assertThat(first.get()).isNotSameAs(second.get());
        assertThat(secondAccessKey).isNotEqualTo(firstAccessKey);
        assertThat(vendingAdapter.getLoadTableCount()).isGreaterThan(loadsAfterFirst);
    }

    @Test
    void testShouldRefreshEnabledForVendedCredentials()
    {
        ConnectorMetadata metadata = createIcebergMetadata();
        ConnectorTableHandle tableHandle = metadata.getTableHandle(SESSION, SCHEMA_TABLE, Optional.empty(), Optional.empty());
        assertThat(tableHandle).isNotNull();

        Optional<ConnectorTableCredentials> credentials = metadata.getTableCredentials(SESSION, tableHandle);
        assertThat(credentials).isPresent();

        IcebergTableCredentials icebergCredentials = (IcebergTableCredentials) credentials.get();
        // Freshly fetched credentials should NOT need refresh yet (they were just created)
        assertThat(icebergCredentials.shouldRefresh()).isFalse();
        // expiresAt is present because S3 credentials are vended
        assertThat(icebergCredentials.expiresAt()).isPresent();
        assertThat(icebergCredentials.expiresAt().get()).isAfter(Instant.now());
    }

    private IcebergMetadata createIcebergMetadata()
    {
        return new IcebergMetadata(
                PLANNER_CONTEXT.getTypeManager(),
                jsonCodec(CommitTaskData.class),
                trinoCatalog,
                (connectorIdentity, fileIoProperties) -> {
                    throw new UnsupportedOperationException();
                },
                TABLE_STATISTICS_READER,
                new TableStatisticsWriter(new NodeVersion("test-version")),
                UNSUPPORTED_DELETION_VECTOR_WRITER,
                Optional.empty(),
                false,
                _ -> false,
                newDirectExecutorService(),
                directExecutor(),
                newDirectExecutorService(),
                newDirectExecutorService(),
                0,
                ZERO);
    }
}
