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
package io.trino.plugin.hive.fs;

import com.google.common.collect.ImmutableList;
import eu.rekawek.toxiproxy.Proxy;
import eu.rekawek.toxiproxy.ToxiproxyClient;
import eu.rekawek.toxiproxy.model.Toxic;
import eu.rekawek.toxiproxy.model.ToxicDirection;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import io.opentelemetry.api.OpenTelemetry;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.filesystem.s3.S3FileSystemConfig;
import io.trino.filesystem.s3.S3FileSystemFactory;
import io.trino.filesystem.s3.S3FileSystemStats;
import io.trino.metastore.Column;
import io.trino.metastore.Table;
import io.trino.plugin.base.util.AutoCloseableCloser;
import io.trino.spi.TrinoException;
import io.trino.spi.security.ConnectorIdentity;
import io.trino.testing.containers.Floci;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.testcontainers.containers.Network;
import org.testcontainers.toxiproxy.ToxiproxyContainer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static io.trino.metastore.HiveType.HIVE_STRING;
import static io.trino.plugin.hive.HiveStorageFormat.ORC;
import static io.trino.plugin.hive.TableType.MANAGED_TABLE;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static io.trino.testing.containers.Floci.FLOCI_ACCESS_KEY;
import static io.trino.testing.containers.Floci.FLOCI_PORT;
import static io.trino.testing.containers.Floci.FLOCI_REGION;
import static io.trino.testing.containers.Floci.FLOCI_SECRET_KEY;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_METHOD;

/**
 * Checks the cached directory listing timeout and retries on a real (Floci) S3 endpoint. We break its
 * network with Toxiproxy. The {@code timeout} toxic makes the S3 connection hang forever and the
 * {@code latency} toxic makes it slow, so the listing can never finish in time, and it does not matter
 * how fast the host is - only our timeout stops it. We use small listing timeouts (hundreds of ms) and
 * a big S3 socket timeout, so our timeout happens first and we get the expected {@link TrinoException}.
 */
@TestInstance(PER_METHOD)
final class TestCachingDirectoryListerS3Timeout
{
    private static final int TOXIPROXY_CONTROL_PORT = 8474;
    private static final int S3_PROXY_PORT = 1234;

    private AutoCloseableCloser closer = AutoCloseableCloser.create();
    private String bucketName;
    private Floci floci;
    private Proxy toxiProxy;
    private TrinoFileSystem fileSystem;

    @BeforeEach
    void init()
            throws Exception
    {
        closer.close();
        closer = AutoCloseableCloser.create();

        Network network = closer.register(Network.newNetwork());
        floci = closer.register(new Floci()
                .withNetwork(network)
                .withNetworkAliases("floci"));
        floci.start();
        bucketName = "bucket-" + randomNameSuffix();
        floci.createBucket(bucketName);

        ToxiproxyContainer toxiproxy = closer.register(new ToxiproxyContainer("ghcr.io/shopify/toxiproxy:2.12.0")
                .withExposedPorts(TOXIPROXY_CONTROL_PORT, S3_PROXY_PORT)
                .withNetwork(network));
        toxiproxy.start();

        ToxiproxyClient toxiproxyClient = new ToxiproxyClient(toxiproxy.getHost(), toxiproxy.getControlPort());
        toxiProxy = toxiproxyClient.createProxy("floci", "0.0.0.0:" + S3_PROXY_PORT, "floci:" + FLOCI_PORT);

        String proxiedEndpoint = "http://" + toxiproxy.getHost() + ":" + toxiproxy.getMappedPort(S3_PROXY_PORT);
        S3FileSystemFactory fileSystemFactory = new S3FileSystemFactory(
                OpenTelemetry.noop(),
                new S3FileSystemConfig()
                        .setEndpoint(proxiedEndpoint)
                        .setRegion(FLOCI_REGION)
                        .setPathStyleAccess(true)
                        .setAwsAccessKey(FLOCI_ACCESS_KEY)
                        .setAwsSecretKey(FLOCI_SECRET_KEY)
                        // much bigger than the listing timeout, so our timeout happens first, not the SDK one
                        .setSocketTimeout(new Duration(30, SECONDS))
                        .setSocketConnectTimeout(new Duration(30, SECONDS))
                        // do not reuse a connection that was good before we added or removed the toxic
                        .setConnectionTtl(new Duration(10, MILLISECONDS)),
                new S3FileSystemStats());
        closer.register(fileSystemFactory::destroy);
        fileSystem = fileSystemFactory.create(ConnectorIdentity.ofUser("test"));
    }

    @AfterEach
    void cleanup()
            throws Exception
    {
        closer.close();
    }

    @Test
    void testStalledListingTimesOutAndRecovers()
            throws Exception
    {
        Location location = createTable(3);
        Table table = table(location);

        // per-element timeout only, infinite stall. the result is the same on fast and slow machines
        CachingDirectoryLister lister = newLister(new Duration(0, MILLISECONDS), new Duration(500, MILLISECONDS), 0);

        // stall all traffic from Floci, so the S3 listing hangs forever
        Toxic stall = toxiProxy.toxics().timeout("stall", ToxicDirection.DOWNSTREAM, 0);

        assertThatThrownBy(() -> drain(lister, table, location))
                .isInstanceOf(TrinoException.class)
                .hasMessageContaining("Timed out listing directory");
        // failed or partial listing must not be in cache
        assertThat(lister.isCached(location, table.getSchemaTableName())).isFalse();

        // remove the stall. a retry starts a new listing, now it works and is cached
        stall.remove();

        assertThat(drain(lister, table, location)).hasSize(3);
        assertThat(lister.isCached(location, table.getSchemaTableName())).isTrue();

        lister.shutdown();
    }

    @Test
    void testTotalTimeoutFailsWhenListingTooSlow()
            throws Exception
    {
        Location location = createTable(3);
        Table table = table(location);

        // total timeout is small, per-element timeout is large, so only the total timeout can fire
        CachingDirectoryLister lister = newLister(new Duration(1, SECONDS), new Duration(10, SECONDS), 0);

        // every S3 response is delayed much longer than the total timeout, so the listing never finishes in time
        Toxic slow = toxiProxy.toxics().latency("slow", ToxicDirection.DOWNSTREAM, 3000);

        assertThatThrownBy(() -> drain(lister, table, location))
                .isInstanceOf(TrinoException.class)
                .hasMessageContaining("Timed out listing directory");
        assertThat(lister.isCached(location, table.getSchemaTableName())).isFalse();

        slow.remove();
        lister.shutdown();
    }

    @Test
    void testRetriesRecoverWhenStallClears()
            throws Exception
    {
        Location location = createTable(3);
        Table table = table(location);

        // small per-element timeout and many retries, so the call keeps trying while the stall is up
        CachingDirectoryLister lister = newLister(new Duration(0, MILLISECONDS), new Duration(300, MILLISECONDS), 10);

        Toxic stall = toxiProxy.toxics().timeout("stall", ToxicDirection.DOWNSTREAM, 0);
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            // run the listing in background, it will fail a few attempts while the stall is up
            Future<List<TrinoFileStatus>> future = executor.submit(() -> drain(lister, table, location));
            Thread.sleep(1000);
            // clear the stall: the next retry attempt lists successfully within the same call
            stall.remove();

            assertThat(future.get(30, SECONDS)).hasSize(3);
            assertThat(lister.isCached(location, table.getSchemaTableName())).isTrue();
        }
        finally {
            executor.shutdownNow();
            lister.shutdown();
        }
    }

    @Test
    void testRetriesExhaustedFailsAndCacheStaysClean()
            throws Exception
    {
        Location location = createTable(3);
        Table table = table(location);

        // permanent stall: the initial attempt and both retries all time out
        CachingDirectoryLister lister = newLister(new Duration(0, MILLISECONDS), new Duration(300, MILLISECONDS), 2);

        toxiProxy.toxics().timeout("stall", ToxicDirection.DOWNSTREAM, 0);

        assertThatThrownBy(() -> drain(lister, table, location))
                .isInstanceOf(TrinoException.class)
                .hasMessageContaining("Timed out listing directory");
        assertThat(lister.isCached(location, table.getSchemaTableName())).isFalse();

        lister.shutdown();
    }

    private static CachingDirectoryLister newLister(Duration listingTimeout, Duration listingElementTimeout, int listingMaxRetries)
    {
        return new CachingDirectoryLister(
                new Duration(5, SECONDS),
                DataSize.of(1, DataSize.Unit.MEGABYTE),
                ImmutableList.of("*"),
                ImmutableList.of(),
                _ -> true,
                listingTimeout,
                listingElementTimeout,
                listingMaxRetries,
                2,
                1000);
    }

    private Location createTable(int fileCount)
    {
        String tablePrefix = "table-" + randomNameSuffix();
        for (int i = 0; i < fileCount; i++) {
            floci.putObject(bucketName, new byte[] {1, 2, 3}, tablePrefix + "/file" + i);
        }
        return Location.of("s3://%s/%s".formatted(bucketName, tablePrefix));
    }

    private List<TrinoFileStatus> drain(CachingDirectoryLister lister, Table table, Location location)
            throws IOException
    {
        RemoteIterator<TrinoFileStatus> iterator = lister.listFilesRecursively(fileSystem, table, location);
        List<TrinoFileStatus> files = new ArrayList<>();
        while (iterator.hasNext()) {
            files.add(iterator.next());
        }
        return files;
    }

    private static Table table(Location location)
    {
        Table.Builder builder = Table.builder();
        builder.getStorageBuilder()
                .setStorageFormat(ORC.toStorageFormat())
                .setLocation(location.toString());
        return builder
                .setDatabaseName("test_dbname")
                .setOwner(Optional.of("test_owner"))
                .setTableName("test_table")
                .setTableType(MANAGED_TABLE.name())
                .setDataColumns(List.of(new Column("col1", HIVE_STRING, Optional.empty(), Map.of())))
                .setPartitionColumns(List.of())
                .setParameters(Map.of())
                .build();
    }
}
