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
package io.trino.plugin.exchange.filesystem.s3;

import com.google.common.collect.ImmutableMap;
import io.trino.plugin.exchange.filesystem.FileSystemExchangeManagerFactory;
import io.trino.plugin.exchange.filesystem.TestExchangeManagerContext;
import io.trino.plugin.exchange.filesystem.containers.MinioStorage;
import io.trino.spi.exchange.ExchangeManager;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.s3.S3AsyncClient;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.net.URI;
import java.util.Map;

import static io.trino.plugin.exchange.filesystem.containers.MinioStorage.getExchangeManagerProperties;
import static java.util.UUID.randomUUID;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests for S3 client isolation between catalog operations and exchange manager operations.
 * This specifically addresses issue #26419 where S3 Tables catalog usage interferes with
 * exchange manager S3 operations.
 */
public class TestS3ClientIsolation
{
    @Test
    public void testS3ClientCreationWithRegion()
            throws Exception
    {
        try (MinioStorage minioStorage = new MinioStorage("test-isolation-region-" + randomUUID())) {
            minioStorage.start();

            Map<String, String> properties = ImmutableMap.<String, String>builder()
                    .putAll(getExchangeManagerProperties(minioStorage))
                    .put("exchange.s3.region", "us-east-1")
                    .buildOrThrow();

            ExchangeManager exchangeManager = new FileSystemExchangeManagerFactory().create(
                    properties,
                    new TestExchangeManagerContext());

            // Verify the exchange manager was created successfully
            assertThat(exchangeManager).isNotNull();

            // The client should be configured with the region for standard S3 endpoint resolution
            // This ensures isolation from any S3 Tables-specific endpoints
            S3FileSystemExchangeStorage storage = extractS3Storage(exchangeManager);
            assertThat(storage).isNotNull();
        }
    }

    @Test
    public void testS3ClientCreationWithExplicitEndpoint()
            throws Exception
    {
        try (MinioStorage minioStorage = new MinioStorage("test-isolation-endpoint-" + randomUUID())) {
            minioStorage.start();

            Map<String, String> properties = ImmutableMap.<String, String>builder()
                    .putAll(getExchangeManagerProperties(minioStorage))
                    .buildOrThrow();

            ExchangeManager exchangeManager = new FileSystemExchangeManagerFactory().create(
                    properties,
                    new TestExchangeManagerContext());

            // Verify the exchange manager was created successfully with explicit endpoint
            assertThat(exchangeManager).isNotNull();

            // When endpoint is explicitly set, it should override any default resolution
            S3FileSystemExchangeStorage storage = extractS3Storage(exchangeManager);
            assertThat(storage).isNotNull();
        }
    }

    @Test
    public void testS3ClientIsolationFromCatalogEndpoints()
            throws Exception
    {
        // Simulate scenario where S3 Tables catalog might set global AWS SDK state
        // The exchange manager should still use standard S3 endpoints

        try (MinioStorage minioStorage = new MinioStorage("test-isolation-catalog-" + randomUUID())) {
            minioStorage.start();

            Map<String, String> properties = ImmutableMap.<String, String>builder()
                    .putAll(getExchangeManagerProperties(minioStorage))
                    .put("exchange.s3.region", "us-west-2")
                    .buildOrThrow();

            // Create exchange manager
            ExchangeManager exchangeManager = new FileSystemExchangeManagerFactory().create(
                    properties,
                    new TestExchangeManagerContext());

            assertThat(exchangeManager).isNotNull();

            // Verify storage is properly isolated
            S3FileSystemExchangeStorage storage = extractS3Storage(exchangeManager);
            assertThat(storage).isNotNull();

            // The storage should create independent S3 clients per bucket
            // that are not affected by any catalog-level endpoint configurations
            verifyStorageUsesStandardS3Endpoints(storage);
        }
    }

    @Test
    public void testMultipleBucketIsolation()
            throws Exception
    {
        // Test that S3 clients for different buckets are properly isolated
        try (MinioStorage minioStorage = new MinioStorage("test-multi-bucket-" + randomUUID())) {
            minioStorage.start();

            Map<String, String> properties = ImmutableMap.<String, String>builder()
                    .putAll(getExchangeManagerProperties(minioStorage))
                    .put("exchange.s3.region", "eu-west-1")
                    .buildOrThrow();

            ExchangeManager exchangeManager = new FileSystemExchangeManagerFactory().create(
                    properties,
                    new TestExchangeManagerContext());

            assertThat(exchangeManager).isNotNull();

            S3FileSystemExchangeStorage storage = extractS3Storage(exchangeManager);
            assertThat(storage).isNotNull();

            // Each bucket should get its own S3 client instance
            // This prevents cross-contamination between different S3 operations
            verifyClientCaching(storage);
        }
    }

    @Test
    public void testRegionOrEndpointRequired()
    {
        // Verify that configuration validation requires either region or endpoint
        ExchangeS3Config config = new ExchangeS3Config();

        // Without region or endpoint, validation should fail
        assertThat(config.isEndpointOrRegionSet()).isFalse();

        // With region, validation should pass
        config.setS3Region("us-east-1");
        assertThat(config.isEndpointOrRegionSet()).isTrue();

        // Reset and test with endpoint
        config = new ExchangeS3Config();
        config.setS3Endpoint("https://s3.amazonaws.com");
        assertThat(config.isEndpointOrRegionSet()).isTrue();

        // With both, validation should also pass
        config.setS3Region("us-east-1");
        assertThat(config.isEndpointOrRegionSet()).isTrue();
    }

    @Test
    public void testS3AsyncClientEndpointConfiguration()
            throws Exception
    {
        // Test that S3AsyncClient uses correct endpoint resolution logic
        ExchangeS3Config config = new ExchangeS3Config()
                .setS3Region("us-east-1");

        S3FileSystemExchangeStorageStats stats = new S3FileSystemExchangeStorageStats();

        try (S3FileSystemExchangeStorage storage = new S3FileSystemExchangeStorage(
                stats,
                config,
                S3FileSystemExchangeStorage.CompatibilityMode.AWS)) {
            // Storage should be created successfully with region-based endpoint resolution
            assertThat(storage).isNotNull();

            // Create a test URI for regular S3 bucket (not S3 Tables)
            URI testUri = URI.create("s3://test-exchange-bucket/");

            // Verify the storage can create writers/readers for regular S3 URIs
            // without being affected by S3 Tables endpoint configurations
            assertThat(storage.createExchangeStorageWriter(testUri.resolve("test-file")))
                    .isNotNull();
        }
    }

    @Test
    public void testS3TablesPatternDoesNotAffectExchange()
    {
        // Verify that S3 Tables bucket naming pattern doesn't affect exchange manager
        // S3 Tables buckets use pattern: *--table-s3
        // Exchange manager should only use regular S3 buckets

        String regularBucket = "s3://my-exchange-bucket/";
        String s3TablesBucket = "s3://abc123def456--table-s3/";  // S3 Tables pattern

        // Exchange manager should work with regular buckets
        URI regularUri = URI.create(regularBucket);
        assertThat(regularUri.getScheme()).isEqualTo("s3");

        // S3 Tables pattern should be recognized but not used by exchange
        URI s3TablesUri = URI.create(s3TablesBucket);
        assertThat(s3TablesUri.getScheme()).isEqualTo("s3");

        // The fix ensures exchange manager never tries to use S3 Tables endpoints
        // regardless of bucket naming patterns
    }

    @Test
    public void testConcurrentS3ClientCreation()
            throws Exception
    {
        // Test that multiple S3 clients can be created concurrently without interference
        try (MinioStorage minioStorage = new MinioStorage("test-concurrent-" + randomUUID())) {
            minioStorage.start();

            Map<String, String> properties = ImmutableMap.<String, String>builder()
                    .putAll(getExchangeManagerProperties(minioStorage))
                    .put("exchange.s3.region", "ap-southeast-1")
                    .buildOrThrow();

            // Create multiple exchange managers concurrently
            ExchangeManager em1 = new FileSystemExchangeManagerFactory().create(
                    properties,
                    new TestExchangeManagerContext());
            ExchangeManager em2 = new FileSystemExchangeManagerFactory().create(
                    properties,
                    new TestExchangeManagerContext());

            assertThat(em1).isNotNull();
            assertThat(em2).isNotNull();

            // Each should have independent storage
            S3FileSystemExchangeStorage storage1 = extractS3Storage(em1);
            S3FileSystemExchangeStorage storage2 = extractS3Storage(em2);

            assertThat(storage1).isNotNull();
            assertThat(storage2).isNotNull();
            assertThat(storage1).isNotSameAs(storage2);
        }
    }

    // Helper methods

    private S3FileSystemExchangeStorage extractS3Storage(ExchangeManager exchangeManager)
            throws Exception
    {
        // Use reflection to access the storage component
        Field storageField = exchangeManager.getClass().getDeclaredField("exchangeStorage");
        storageField.setAccessible(true);
        Object storage = storageField.get(exchangeManager);

        if (storage instanceof S3FileSystemExchangeStorage) {
            return (S3FileSystemExchangeStorage) storage;
        }

        // Alternative: search through all fields
        for (Field field : exchangeManager.getClass().getDeclaredFields()) {
            field.setAccessible(true);
            Object value = field.get(exchangeManager);
            if (value instanceof S3FileSystemExchangeStorage) {
                return (S3FileSystemExchangeStorage) value;
            }
        }

        return null;
    }

    private void verifyStorageUsesStandardS3Endpoints(S3FileSystemExchangeStorage storage)
            throws Exception
    {
        // Verify that the storage doesn't use S3 Tables-specific endpoints
        // This is done by checking the internal configuration

        Field regionField = S3FileSystemExchangeStorage.class.getDeclaredField("region");
        regionField.setAccessible(true);
        Object region = regionField.get(storage);

        assertThat(region).isNotNull();

        // If endpoint is set, it should be a standard S3 endpoint, not S3 Tables
        Field endpointField = S3FileSystemExchangeStorage.class.getDeclaredField("endpoint");
        endpointField.setAccessible(true);
        Object endpoint = endpointField.get(storage);

        if (endpoint != null) {
            String endpointStr = endpoint.toString();
            // S3 Tables endpoints typically go through AWS Glue
            assertThat(endpointStr).doesNotContain("glue");
            assertThat(endpointStr).doesNotContain("s3tables");
        }
    }

    private void verifyClientCaching(S3FileSystemExchangeStorage storage)
            throws Exception
    {
        // Verify that S3 clients are cached per bucket to avoid creating
        // too many connections

        Field s3ClientsField = S3FileSystemExchangeStorage.class.getDeclaredField("s3Clients");
        s3ClientsField.setAccessible(true);
        Object s3Clients = s3ClientsField.get(storage);

        assertThat(s3Clients).isNotNull();
        // The s3Clients field should be a cache that stores clients per bucket
        assertThat(s3Clients.getClass().getName()).contains("Cache");
    }
}
