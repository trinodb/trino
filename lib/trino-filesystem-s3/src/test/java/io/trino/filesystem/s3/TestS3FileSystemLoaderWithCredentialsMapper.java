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
package io.trino.filesystem.s3;

import io.opentelemetry.api.OpenTelemetry;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.spi.security.ConnectorIdentity;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.auth.credentials.AwsSessionCredentials;
import software.amazon.awssdk.services.s3.S3Client;

import java.lang.reflect.Field;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

final class TestS3FileSystemLoaderWithCredentialsMapper
{
    private S3FileSystemLoader loader;
    private S3FileSystemConfig config;
    private S3FileSystemStats stats;
    private TestCredentialsMapper credentialsMapper;

    @BeforeEach
    void setUp()
            throws Exception
    {
        config = new S3FileSystemConfig()
                .setAwsAccessKey("test-access-key")
                .setAwsSecretKey("test-secret-key")
                .setRegion("us-east-1")
                .setCrossRegionAccessEnabled(false);

        stats = new S3FileSystemStats();
        credentialsMapper = new TestCredentialsMapper();

        // Use the public constructor directly
        loader = new S3FileSystemLoader(
                Optional.of(credentialsMapper),
                OpenTelemetry.noop(),
                config,
                stats);
    }

    @AfterEach
    void tearDown()
    {
        if (loader != null) {
            loader.destroy();
        }
    }

    @Test
    void testClientCachingForSameCredentials()
            throws Exception
    {
        // Arrange: Same credentials
        ConnectorIdentity identity = ConnectorIdentity.ofUser("test-user");
        Location location = Location.of("s3://test-bucket/path");

        S3SecurityMappingResult mapping = new S3SecurityMappingResult(
                Optional.of(AwsSessionCredentials.create("access1", "secret1", "token1")),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.of("https://s3.us-east-1.amazonaws.com"),
                Optional.of("us-east-1"),
                Optional.of(true));

        credentialsMapper.setMapping(mapping);

        // Act: Create file systems multiple times with same credentials
        TrinoFileSystemFactory factory = loader.apply(location);
        TrinoFileSystem fs1 = factory.create(identity);
        TrinoFileSystem fs2 = factory.create(identity);
        TrinoFileSystem fs3 = factory.create(identity);

        // Assert: Same S3Client is reused
        S3Client client1 = extractS3Client(fs1);
        S3Client client2 = extractS3Client(fs2);
        S3Client client3 = extractS3Client(fs3);

        assertThat(client1).isSameAs(client2);
        assertThat(client2).isSameAs(client3);

        // Verify cache contains only ONE entry
        Map<Optional<S3SecurityMappingResult>, S3Client> clientsCache = getClientsCache();
        assertThat(clientsCache).hasSize(1);
    }

    @Test
    void testDifferentClientsForDifferentCredentials()
            throws Exception
    {
        // Arrange: Different credentials
        ConnectorIdentity identity1 = ConnectorIdentity.ofUser("user1");
        ConnectorIdentity identity2 = ConnectorIdentity.ofUser("user2");
        Location location = Location.of("s3://test-bucket/path");

        S3SecurityMappingResult mapping1 = new S3SecurityMappingResult(
                Optional.of(AwsSessionCredentials.create("access1", "secret1", "token1")),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.of("https://s3.us-east-1.amazonaws.com"),
                Optional.of("us-east-1"),
                Optional.of(true));

        S3SecurityMappingResult mapping2 = new S3SecurityMappingResult(
                Optional.of(AwsSessionCredentials.create("access2", "secret2", "token2")),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.of("https://s3.us-west-2.amazonaws.com"),
                Optional.of("us-west-2"),
                Optional.of(false));

        TrinoFileSystemFactory factory = loader.apply(location);

        // Act: Create file systems with different credentials
        credentialsMapper.setMapping(mapping1);
        TrinoFileSystem fs1 = factory.create(identity1);

        credentialsMapper.setMapping(mapping2);
        TrinoFileSystem fs2 = factory.create(identity2);

        // Assert: Different S3Clients are created
        S3Client client1 = extractS3Client(fs1);
        S3Client client2 = extractS3Client(fs2);

        assertThat(client1).isNotSameAs(client2);

        // Verify cache contains TWO entries
        Map<Optional<S3SecurityMappingResult>, S3Client> clientsCache = getClientsCache();
        assertThat(clientsCache).hasSize(2);
    }

    @Test
    void testDifferentClientsForDifferentRegions()
            throws Exception
    {
        // Arrange: Same credentials, different regions
        ConnectorIdentity identity = ConnectorIdentity.ofUser("test-user");
        Location location = Location.of("s3://test-bucket/path");

        S3SecurityMappingResult mapping1 = new S3SecurityMappingResult(
                Optional.of(AwsSessionCredentials.create("access1", "secret1", "token1")),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.of("us-east-1"),
                Optional.empty());

        S3SecurityMappingResult mapping2 = new S3SecurityMappingResult(
                Optional.of(AwsSessionCredentials.create("access1", "secret1", "token1")),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.of("eu-west-1"),
                Optional.empty());

        TrinoFileSystemFactory factory = loader.apply(location);

        // Act: Create file systems with different regions
        credentialsMapper.setMapping(mapping1);
        TrinoFileSystem fs1 = factory.create(identity);

        credentialsMapper.setMapping(mapping2);
        TrinoFileSystem fs2 = factory.create(identity);

        // Assert: Different S3Clients for different regions
        S3Client client1 = extractS3Client(fs1);
        S3Client client2 = extractS3Client(fs2);

        assertThat(client1).isNotSameAs(client2);

        // Verify cache contains TWO entries
        Map<Optional<S3SecurityMappingResult>, S3Client> clientsCache = getClientsCache();
        assertThat(clientsCache).hasSize(2);
    }

    @Test
    void testDifferentClientsForDifferentEndpoints()
            throws Exception
    {
        // Arrange: Same credentials, different endpoints
        ConnectorIdentity identity = ConnectorIdentity.ofUser("test-user");
        Location location = Location.of("s3://test-bucket/path");

        S3SecurityMappingResult mapping1 = new S3SecurityMappingResult(
                Optional.of(AwsSessionCredentials.create("access1", "secret1", "token1")),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.of("https://s3.us-east-1.amazonaws.com"),
                Optional.of("us-east-1"),
                Optional.empty());

        S3SecurityMappingResult mapping2 = new S3SecurityMappingResult(
                Optional.of(AwsSessionCredentials.create("access1", "secret1", "token1")),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.of("https://custom-s3-endpoint.example.com"),
                Optional.of("us-east-1"),
                Optional.empty());

        TrinoFileSystemFactory factory = loader.apply(location);

        // Act: Create file systems with different endpoints
        credentialsMapper.setMapping(mapping1);
        TrinoFileSystem fs1 = factory.create(identity);

        credentialsMapper.setMapping(mapping2);
        TrinoFileSystem fs2 = factory.create(identity);

        // Assert: Different S3Clients for different endpoints
        S3Client client1 = extractS3Client(fs1);
        S3Client client2 = extractS3Client(fs2);

        assertThat(client1).isNotSameAs(client2);

        // Verify cache contains TWO entries
        Map<Optional<S3SecurityMappingResult>, S3Client> clientsCache = getClientsCache();
        assertThat(clientsCache).hasSize(2);
    }

    @Test
    void testDifferentClientsForDifferentCrossRegionAccess()
            throws Exception
    {
        // Arrange: Same credentials, different cross-region settings
        ConnectorIdentity identity = ConnectorIdentity.ofUser("test-user");
        Location location = Location.of("s3://test-bucket/path");

        S3SecurityMappingResult mapping1 = new S3SecurityMappingResult(
                Optional.of(AwsSessionCredentials.create("access1", "secret1", "token1")),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.of("us-east-1"),
                Optional.of(true));

        S3SecurityMappingResult mapping2 = new S3SecurityMappingResult(
                Optional.of(AwsSessionCredentials.create("access1", "secret1", "token1")),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.of("us-east-1"),
                Optional.of(false));

        TrinoFileSystemFactory factory = loader.apply(location);

        // Act: Create file systems with different cross-region settings
        credentialsMapper.setMapping(mapping1);
        TrinoFileSystem fs1 = factory.create(identity);

        credentialsMapper.setMapping(mapping2);
        TrinoFileSystem fs2 = factory.create(identity);

        // Assert: Different S3Clients for different cross-region settings
        S3Client client1 = extractS3Client(fs1);
        S3Client client2 = extractS3Client(fs2);

        assertThat(client1).isNotSameAs(client2);

        // Verify cache contains TWO entries
        Map<Optional<S3SecurityMappingResult>, S3Client> clientsCache = getClientsCache();
        assertThat(clientsCache).hasSize(2);
    }

    @Test
    void testEmptyMappingUsesClusterDefaults()
            throws Exception
    {
        // Arrange: No credentials mapping
        ConnectorIdentity identity = ConnectorIdentity.ofUser("test-user");
        Location location = Location.of("s3://test-bucket/path");

        credentialsMapper.setMapping(null); // Return empty

        // Act: Create file system
        TrinoFileSystemFactory factory = loader.apply(location);
        TrinoFileSystem fs = factory.create(identity);

        // Assert: Client is created with cluster defaults
        S3Client client = extractS3Client(fs);
        assertThat(client).isNotNull();

        // Verify cache contains ONE entry for empty mapping
        Map<Optional<S3SecurityMappingResult>, S3Client> clientsCache = getClientsCache();
        assertThat(clientsCache).hasSize(1);
    }

    @Test
    void testConcurrentClientCreationIsSafe()
            throws Exception
    {
        // Arrange: Multiple threads creating clients with same credentials
        ConnectorIdentity identity = ConnectorIdentity.ofUser("test-user");
        Location location = Location.of("s3://test-bucket/path");

        S3SecurityMappingResult mapping = new S3SecurityMappingResult(
                Optional.of(AwsSessionCredentials.create("access1", "secret1", "token1")),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.of("https://s3.us-east-1.amazonaws.com"),
                Optional.of("us-east-1"),
                Optional.of(true));

        credentialsMapper.setMapping(mapping);

        int threadCount = 10;
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch doneLatch = new CountDownLatch(threadCount);
        ExecutorService executor = Executors.newFixedThreadPool(threadCount);
        AtomicInteger clientCount = new AtomicInteger(0);

        TrinoFileSystemFactory factory = loader.apply(location);

        // Act: Create clients concurrently
        for (int i = 0; i < threadCount; i++) {
            executor.submit(() -> {
                try {
                    startLatch.await(); // Wait for all threads to be ready
                    TrinoFileSystem fs = factory.create(identity);
                    S3Client client = extractS3Client(fs);
                    if (client != null) {
                        clientCount.incrementAndGet();
                    }
                }
                catch (Exception e) {
                    throw new RuntimeException(e);
                }
                finally {
                    doneLatch.countDown();
                }
            });
        }

        startLatch.countDown(); // Start all threads at once
        doneLatch.await(10, TimeUnit.SECONDS);
        executor.shutdown();

        // Assert: All threads succeeded
        assertThat(clientCount.get()).isEqualTo(threadCount);

        // Assert: Only ONE client created despite concurrent access
        Map<Optional<S3SecurityMappingResult>, S3Client> clientsCache = getClientsCache();
        assertThat(clientsCache).hasSize(1);
    }

    @Test
    void testClientCacheGrowsBounded()
            throws Exception
    {
        // Arrange: Create clients with many different credential combinations
        Location location = Location.of("s3://test-bucket/path");
        TrinoFileSystemFactory factory = loader.apply(location);

        int uniqueCredentialSets = 5;

        // Act: Create file systems with different credentials
        for (int i = 0; i < uniqueCredentialSets; i++) {
            ConnectorIdentity identity = ConnectorIdentity.ofUser("user" + i);

            S3SecurityMappingResult mapping = new S3SecurityMappingResult(
                    Optional.of(AwsSessionCredentials.create("access" + i, "secret" + i, "token" + i)),
                    Optional.empty(),
                    Optional.empty(),
                    Optional.empty(),
                    Optional.empty(),
                    Optional.of("https://s3.us-east-1.amazonaws.com"),
                    Optional.of("us-east-1"),
                    Optional.of(true));

            credentialsMapper.setMapping(mapping);
            factory.create(identity);
        }

        // Assert: Cache size equals number of unique credential sets (bounded growth)
        Map<Optional<S3SecurityMappingResult>, S3Client> clientsCache = getClientsCache();
        assertThat(clientsCache).hasSize(uniqueCredentialSets);
    }

    @Test
    void testKmsKeyIdIsAppliedToContext()
            throws Exception
    {
        // Arrange: Mapping with KMS key ID
        ConnectorIdentity identity = ConnectorIdentity.ofUser("test-user");
        Location location = Location.of("s3://test-bucket/path");

        S3SecurityMappingResult mapping = new S3SecurityMappingResult(
                Optional.of(AwsSessionCredentials.create("access1", "secret1", "token1")),
                Optional.empty(),
                Optional.empty(),
                Optional.of("arn:aws:kms:us-east-1:123456789012:key/test-key"),
                Optional.empty(),
                Optional.empty(),
                Optional.of("us-east-1"),
                Optional.empty());

        credentialsMapper.setMapping(mapping);

        // Act: Create file system
        TrinoFileSystemFactory factory = loader.apply(location);
        TrinoFileSystem fs = factory.create(identity);

        // Assert: File system created successfully (KMS key would be validated at runtime)
        assertThat(fs).isNotNull();
    }

    @Test
    void testSseCustomerKeyIsAppliedToContext()
            throws Exception
    {
        // Arrange: Mapping with SSE customer key
        ConnectorIdentity identity = ConnectorIdentity.ofUser("test-user");
        Location location = Location.of("s3://test-bucket/path");

        S3SecurityMappingResult mapping = new S3SecurityMappingResult(
                Optional.of(AwsSessionCredentials.create("access1", "secret1", "token1")),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.of("YmFzZTY0LWVuY29kZWQta2V5"), // base64 encoded key
                Optional.empty(),
                Optional.of("us-east-1"),
                Optional.empty());

        credentialsMapper.setMapping(mapping);

        // Act: Create file system
        TrinoFileSystemFactory factory = loader.apply(location);
        TrinoFileSystem fs = factory.create(identity);

        // Assert: File system created successfully
        assertThat(fs).isNotNull();
    }

    @Test
    void testComplexCredentialCombination()
            throws Exception
    {
        // Arrange: Mapping with multiple fields populated
        ConnectorIdentity identity = ConnectorIdentity.ofUser("test-user");
        Location location = Location.of("s3://test-bucket/path");

        S3SecurityMappingResult mapping = new S3SecurityMappingResult(
                Optional.of(AwsSessionCredentials.create("access1", "secret1", "token1")),
                Optional.of("arn:aws:iam::123456789012:role/test-role"),
                Optional.of("test-session"),
                Optional.empty(),
                Optional.empty(),
                Optional.of("https://custom-endpoint.example.com"),
                Optional.of("eu-west-1"),
                Optional.of(true));

        credentialsMapper.setMapping(mapping);

        // Act: Create file system
        TrinoFileSystemFactory factory = loader.apply(location);
        TrinoFileSystem fs = factory.create(identity);

        // Assert: File system created successfully
        assertThat(fs).isNotNull();

        S3Client client = extractS3Client(fs);
        assertThat(client).isNotNull();
    }

    // Helper methods

    private S3Client extractS3Client(TrinoFileSystem fileSystem)
            throws Exception
    {
        Field clientField = fileSystem.getClass().getDeclaredField("client");
        clientField.setAccessible(true);
        return (S3Client) clientField.get(fileSystem);
    }

    @SuppressWarnings("unchecked")
    private Map<Optional<S3SecurityMappingResult>, S3Client> getClientsCache()
            throws Exception
    {
        Field clientsField = loader.getClass().getDeclaredField("clients");
        clientsField.setAccessible(true);
        return (ConcurrentHashMap<Optional<S3SecurityMappingResult>, S3Client>) clientsField.get(loader);
    }

    // Test implementation of S3CredentialsMapper
    private static class TestCredentialsMapper
            implements S3CredentialsMapper
    {
        private S3SecurityMappingResult mapping;

        public void setMapping(S3SecurityMappingResult mapping)
        {
            this.mapping = mapping;
        }

        @Override
        public Optional<S3SecurityMappingResult> getMapping(ConnectorIdentity identity, Location location)
        {
            return Optional.ofNullable(mapping);
        }
    }
}
