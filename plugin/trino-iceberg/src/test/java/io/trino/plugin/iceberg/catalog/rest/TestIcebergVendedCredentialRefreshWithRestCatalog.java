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

import com.google.common.cache.Cache;
import com.google.common.collect.ImmutableMap;
import io.trino.cache.EvictableCacheBuilder;
import io.trino.plugin.iceberg.IcebergTableCredentials;
import io.trino.spi.connector.ConnectorTableCredentials;
import io.trino.spi.connector.SchemaTableName;
import io.trino.testing.containers.IcebergRestCatalogBackendContainer;
import io.trino.testing.containers.Minio;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.Schema;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.SessionCatalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.rest.RESTSessionCatalog;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.testcontainers.containers.Network;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sts.StsClient;
import software.amazon.awssdk.services.sts.model.AssumeRoleRequest;
import software.amazon.awssdk.services.sts.model.AssumeRoleResponse;

import java.net.URI;
import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static io.trino.plugin.iceberg.IcebergUtil.CREDENTIAL_CACHE_TTL;
import static io.trino.plugin.iceberg.IcebergUtil.WORKER_CREDENTIAL_CACHE_TTL;
import static io.trino.plugin.iceberg.IcebergUtil.maybeRefreshVendedCredentials;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static io.trino.testing.containers.Minio.MINIO_REGION;
import static io.trino.testing.containers.Minio.MINIO_ROOT_PASSWORD;
import static io.trino.testing.containers.Minio.MINIO_ROOT_USER;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;

@TestInstance(PER_CLASS)
public class TestIcebergVendedCredentialRefreshWithRestCatalog
{
    private static final String BUCKET = "test-credential-refresh-" + randomNameSuffix();
    private static final String NAMESPACE = "default";
    private static final String TABLE_NAME = "credential_refresh_test_" + randomNameSuffix();
    private static final SchemaTableName SCHEMA_TABLE = new SchemaTableName(NAMESPACE, TABLE_NAME);

    private Minio minio;
    private IcebergRestCatalogBackendContainer restCatalogContainer;
    private RESTSessionCatalog restCatalog;
    private SessionCatalog.SessionContext sessionContext;

    @BeforeAll
    void startContainers()
    {
        Network network = Network.newNetwork();
        minio = Minio.builder().withNetwork(network).build();
        minio.start();
        minio.createBucket(BUCKET);

        AssumeRoleResponse sts = StsClient.builder()
                .endpointOverride(URI.create(minio.getMinioAddress()))
                .credentialsProvider(StaticCredentialsProvider.create(
                        AwsBasicCredentials.create(MINIO_ROOT_USER, MINIO_ROOT_PASSWORD)))
                .region(Region.of(MINIO_REGION))
                .build()
                .assumeRole(AssumeRoleRequest.builder().build());

        restCatalogContainer = new IcebergRestCatalogBackendContainer(
                Optional.of(network),
                "s3://%s/warehouse/".formatted(BUCKET),
                sts.credentials().accessKeyId(),
                sts.credentials().secretAccessKey(),
                sts.credentials().sessionToken());
        restCatalogContainer.start();

        restCatalog = new RESTSessionCatalog();
        restCatalog.initialize("test", ImmutableMap.of(
                CatalogProperties.URI, "http://" + restCatalogContainer.getRestCatalogEndpoint()));

        sessionContext = SessionCatalog.SessionContext.createEmpty();
        restCatalog.createNamespace(sessionContext, Namespace.of(NAMESPACE), ImmutableMap.of());
        restCatalog.buildTable(
                        sessionContext,
                        TableIdentifier.of(NAMESPACE, TABLE_NAME),
                        new Schema(Types.NestedField.required(1, "id", Types.LongType.get())))
                .create();
    }

    @AfterAll
    void stopContainers()
            throws Exception
    {
        if (restCatalog != null) {
            restCatalog.close();
        }
        if (restCatalogContainer != null) {
            restCatalogContainer.stop();
        }
        if (minio != null) {
            minio.stop();
        }
    }

    @Test
    void testStaleCredentialsRefreshedFromLiveRestCatalog()
    {
        IcebergTableCredentials stale = staleCredentials();
        AtomicInteger callCount = new AtomicInteger();

        Optional<ConnectorTableCredentials> result = maybeRefreshVendedCredentials(
                newWorkerCache(),
                SCHEMA_TABLE,
                Optional.of(stale),
                () -> {
                    callCount.incrementAndGet();
                    return loadFreshCredentials();
                });

        assertThat(callCount.get()).isEqualTo(1);
        assertThat(result).isPresent();
        IcebergTableCredentials refreshed = (IcebergTableCredentials) result.get();
        assertThat(refreshed.credentialsFetchedAtMs()).isGreaterThan(0L);
        assertThat(refreshed.fileIoProperties()).containsKey("s3.access-key-id");
        assertThat(refreshed.fileIoProperties()).containsKey("s3.secret-access-key");
        assertThat(refreshed.fileIoProperties()).containsKey("s3.session-token");
    }

    @Test
    void testFreshCredentialsNotRefreshed()
    {
        IcebergTableCredentials fresh = loadFreshCredentials();
        AtomicInteger callCount = new AtomicInteger();

        Optional<ConnectorTableCredentials> result = maybeRefreshVendedCredentials(
                newWorkerCache(),
                SCHEMA_TABLE,
                Optional.of(fresh),
                () -> {
                    callCount.incrementAndGet();
                    return loadFreshCredentials();
                });

        assertThat(callCount.get()).isEqualTo(0);
        assertThat(result).isPresent();
        assertThat(((IcebergTableCredentials) result.get()).credentialsFetchedAtMs())
                .isEqualTo(fresh.credentialsFetchedAtMs());
    }

    @Test
    void testConcurrentRefreshRequestsHitRestCatalogOnlyOnce()
            throws InterruptedException
    {
        int threads = 10;
        AtomicInteger catalogCallCount = new AtomicInteger();
        Cache<SchemaTableName, IcebergTableCredentials> sharedCache = newWorkerCache();
        CountDownLatch ready = new CountDownLatch(threads);
        CountDownLatch start = new CountDownLatch(1);
        ExecutorService pool = Executors.newFixedThreadPool(threads);

        for (int i = 0; i < threads; i++) {
            pool.submit(() -> {
                ready.countDown();
                try {
                    start.await();
                }
                catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
                maybeRefreshVendedCredentials(
                        sharedCache,
                        SCHEMA_TABLE,
                        Optional.of(staleCredentials()),
                        () -> {
                            catalogCallCount.incrementAndGet();
                            return loadFreshCredentials();
                        });
            });
        }

        ready.await();
        start.countDown();
        pool.shutdown();
        pool.awaitTermination(30, TimeUnit.SECONDS);

        assertThat(catalogCallCount.get())
                .as("REST catalog should be called only once despite %d concurrent threads", threads)
                .isEqualTo(1);
    }

    @Test
    void testRefreshedCredentialsContainValidS3Properties()
    {
        Optional<ConnectorTableCredentials> result = maybeRefreshVendedCredentials(
                newWorkerCache(),
                SCHEMA_TABLE,
                Optional.of(staleCredentials()),
                this::loadFreshCredentials);

        assertThat(result).isPresent();
        IcebergTableCredentials refreshed = (IcebergTableCredentials) result.get();

        assertThat(refreshed.fileIoProperties().get("s3.access-key-id")).isNotBlank();
        assertThat(refreshed.fileIoProperties().get("s3.secret-access-key")).isNotBlank();
        assertThat(refreshed.fileIoProperties().get("s3.session-token")).isNotBlank();
        assertThat(refreshed.credentialsFetchedAtMs())
                .isBetween(System.currentTimeMillis() - 5_000, System.currentTimeMillis() + 1_000);
    }

    @Test
    void testWorkerCacheExpiryTriggersSecondRefresh()
            throws InterruptedException
    {
        Cache<SchemaTableName, IcebergTableCredentials> shortLivedCache = EvictableCacheBuilder.newBuilder()
                .maximumSize(1_000)
                .expireAfterWrite(Duration.ofMillis(100))
                .shareNothingWhenDisabled()
                .build();
        AtomicInteger callCount = new AtomicInteger();

        maybeRefreshVendedCredentials(
                shortLivedCache,
                SCHEMA_TABLE,
                Optional.of(staleCredentials()),
                () -> {
                    callCount.incrementAndGet();
                    return loadFreshCredentials();
                });

        Thread.sleep(200);

        maybeRefreshVendedCredentials(
                shortLivedCache,
                SCHEMA_TABLE,
                Optional.of(staleCredentials()),
                () -> {
                    callCount.incrementAndGet();
                    return loadFreshCredentials();
                });

        assertThat(callCount.get())
                .as("REST catalog should be called twice after worker cache expiry")
                .isEqualTo(2);
    }

    private IcebergTableCredentials loadFreshCredentials()
    {
        return IcebergTableCredentials.forFileIO(
                restCatalog.loadTable(sessionContext, TableIdentifier.of(NAMESPACE, TABLE_NAME)).io());
    }

    private static IcebergTableCredentials staleCredentials()
    {
        long staleTimestamp = System.currentTimeMillis() - CREDENTIAL_CACHE_TTL.toMillis() - 60_000;
        return new IcebergTableCredentials(
                ImmutableMap.of("s3.access-key-id", "expired-key", "s3.session-token", "expired-token"),
                staleTimestamp);
    }

    private static Cache<SchemaTableName, IcebergTableCredentials> newWorkerCache()
    {
        return EvictableCacheBuilder.newBuilder()
                .maximumSize(1_000)
                .expireAfterWrite(WORKER_CREDENTIAL_CACHE_TTL)
                .shareNothingWhenDisabled()
                .build();
    }
}
