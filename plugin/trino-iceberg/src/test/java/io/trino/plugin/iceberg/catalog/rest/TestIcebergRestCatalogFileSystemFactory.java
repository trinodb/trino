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
import io.opentelemetry.api.OpenTelemetry;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.filesystem.s3.S3FileSystemConfig;
import io.trino.filesystem.s3.S3FileSystemFactory;
import io.trino.filesystem.s3.S3FileSystemStats;
import io.trino.spi.security.ConnectorIdentity;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

public class TestIcebergRestCatalogFileSystemFactory
{
    private S3FileSystemFactory s3Factory;
    private IcebergRestCatalogFileSystemFactory factory;

    @AfterEach
    public void tearDown()
    {
        if (s3Factory != null) {
            s3Factory.destroy();
        }
    }

    private void setupFactory(boolean vendedCredentialsEnabled)
    {
        S3FileSystemConfig s3Config = new S3FileSystemConfig()
                .setAwsAccessKey("static-access")
                .setAwsSecretKey("static-secret")
                .setRegion("us-east-1");

        s3Factory = new S3FileSystemFactory(OpenTelemetry.noop(), s3Config, new S3FileSystemStats());

        IcebergRestCatalogConfig config = new IcebergRestCatalogConfig()
                .setVendedCredentialsEnabled(vendedCredentialsEnabled);

        factory = new IcebergRestCatalogFileSystemFactory(s3Factory, config);
    }

    @Test
    public void testVendedCredentialsDisabled()
    {
        setupFactory(false);

        Map<String, String> fileIoProperties = ImmutableMap.<String, String>builder()
                .put("s3.access-key-id", "vended-access")
                .put("s3.secret-access-key", "vended-secret")
                .put("s3.session-token", "vended-token")
                .put("client.region", "us-west-2")
                .buildOrThrow();

        ConnectorIdentity identity = ConnectorIdentity.ofUser("test");
        TrinoFileSystem fs = factory.create(identity, fileIoProperties);

        assertThat(fs).isNotNull();
    }

    @Test
    public void testVendedCredentialsWithRegion()
    {
        setupFactory(true);

        Map<String, String> fileIoProperties = ImmutableMap.<String, String>builder()
                .put("s3.access-key-id", "vended-access")
                .put("s3.secret-access-key", "vended-secret")
                .put("s3.session-token", "vended-token")
                .put("client.region", "us-west-2")
                .buildOrThrow();

        ConnectorIdentity identity = ConnectorIdentity.ofUser("test");
        TrinoFileSystem fs = factory.create(identity, fileIoProperties);

        assertThat(fs).isNotNull();
    }

    @Test
    public void testVendedCredentialsWithEndpoint()
    {
        setupFactory(true);

        Map<String, String> fileIoProperties = ImmutableMap.<String, String>builder()
                .put("s3.access-key-id", "vended-access")
                .put("s3.secret-access-key", "vended-secret")
                .put("s3.session-token", "vended-token")
                .put("s3.endpoint", "https://minio.example.com")
                .buildOrThrow();

        ConnectorIdentity identity = ConnectorIdentity.ofUser("test");
        TrinoFileSystem fs = factory.create(identity, fileIoProperties);

        assertThat(fs).isNotNull();
    }

    @Test
    public void testVendedCredentialsWithCrossRegionAccess()
    {
        setupFactory(true);

        Map<String, String> fileIoProperties = ImmutableMap.<String, String>builder()
                .put("s3.access-key-id", "vended-access")
                .put("s3.secret-access-key", "vended-secret")
                .put("s3.session-token", "vended-token")
                .put("s3.cross-region-access-enabled", "true")
                .buildOrThrow();

        ConnectorIdentity identity = ConnectorIdentity.ofUser("test");
        TrinoFileSystem fs = factory.create(identity, fileIoProperties);

        assertThat(fs).isNotNull();
    }

    @Test
    public void testVendedCredentialsWithAllProperties()
    {
        setupFactory(true);

        Map<String, String> fileIoProperties = ImmutableMap.<String, String>builder()
                .put("s3.access-key-id", "vended-access")
                .put("s3.secret-access-key", "vended-secret")
                .put("s3.session-token", "vended-token")
                .put("client.region", "eu-west-1")
                .put("s3.endpoint", "https://minio.example.com")
                .put("s3.cross-region-access-enabled", "true")
                .buildOrThrow();

        ConnectorIdentity identity = ConnectorIdentity.ofUser("test");
        TrinoFileSystem fs = factory.create(identity, fileIoProperties);

        assertThat(fs).isNotNull();
    }

    @Test
    public void testVendedCredentialsWithoutRegion()
    {
        setupFactory(true);

        Map<String, String> fileIoProperties = ImmutableMap.<String, String>builder()
                .put("s3.access-key-id", "vended-access")
                .put("s3.secret-access-key", "vended-secret")
                .put("s3.session-token", "vended-token")
                .buildOrThrow();

        ConnectorIdentity identity = ConnectorIdentity.ofUser("test");
        TrinoFileSystem fs = factory.create(identity, fileIoProperties);

        assertThat(fs).isNotNull();
    }

    @Test
    public void testVendedCredentialsWithoutEndpoint()
    {
        setupFactory(true);

        Map<String, String> fileIoProperties = ImmutableMap.<String, String>builder()
                .put("s3.access-key-id", "vended-access")
                .put("s3.secret-access-key", "vended-secret")
                .put("s3.session-token", "vended-token")
                .put("client.region", "us-west-2")
                .buildOrThrow();

        ConnectorIdentity identity = ConnectorIdentity.ofUser("test");
        TrinoFileSystem fs = factory.create(identity, fileIoProperties);

        assertThat(fs).isNotNull();
    }

    @Test
    public void testVendedCredentialsWithoutCrossRegionAccess()
    {
        setupFactory(true);

        Map<String, String> fileIoProperties = ImmutableMap.<String, String>builder()
                .put("s3.access-key-id", "vended-access")
                .put("s3.secret-access-key", "vended-secret")
                .put("s3.session-token", "vended-token")
                .put("client.region", "us-west-2")
                .buildOrThrow();

        ConnectorIdentity identity = ConnectorIdentity.ofUser("test");
        TrinoFileSystem fs = factory.create(identity, fileIoProperties);

        assertThat(fs).isNotNull();
    }

    @Test
    public void testMissingAccessKey()
    {
        setupFactory(true);

        Map<String, String> fileIoProperties = ImmutableMap.<String, String>builder()
                .put("s3.secret-access-key", "vended-secret")
                .put("s3.session-token", "vended-token")
                .buildOrThrow();

        ConnectorIdentity identity = ConnectorIdentity.ofUser("test");
        TrinoFileSystem fs = factory.create(identity, fileIoProperties);

        assertThat(fs).isNotNull();
    }

    @Test
    public void testMissingSecretKey()
    {
        setupFactory(true);

        Map<String, String> fileIoProperties = ImmutableMap.<String, String>builder()
                .put("s3.access-key-id", "vended-access")
                .put("s3.session-token", "vended-token")
                .buildOrThrow();

        ConnectorIdentity identity = ConnectorIdentity.ofUser("test");
        TrinoFileSystem fs = factory.create(identity, fileIoProperties);

        assertThat(fs).isNotNull();
    }

    @Test
    public void testMissingSessionToken()
    {
        setupFactory(true);

        Map<String, String> fileIoProperties = ImmutableMap.<String, String>builder()
                .put("s3.access-key-id", "vended-access")
                .put("s3.secret-access-key", "vended-secret")
                .buildOrThrow();

        ConnectorIdentity identity = ConnectorIdentity.ofUser("test");
        TrinoFileSystem fs = factory.create(identity, fileIoProperties);

        assertThat(fs).isNotNull();
    }

    @Test
    public void testEmptyFileIoProperties()
    {
        setupFactory(true);

        Map<String, String> fileIoProperties = ImmutableMap.of();

        ConnectorIdentity identity = ConnectorIdentity.ofUser("test");
        TrinoFileSystem fs = factory.create(identity, fileIoProperties);

        assertThat(fs).isNotNull();
    }

    @Test
    public void testIdentityPreservation()
    {
        setupFactory(true);

        Map<String, String> fileIoProperties = ImmutableMap.<String, String>builder()
                .put("s3.access-key-id", "vended-access")
                .put("s3.secret-access-key", "vended-secret")
                .put("s3.session-token", "vended-token")
                .put("client.region", "us-west-2")
                .buildOrThrow();

        ConnectorIdentity identity = ConnectorIdentity.forUser("testuser")
                .withGroups(Set.of("group1", "group2"))
                .build();

        TrinoFileSystem fs = factory.create(identity, fileIoProperties);

        assertThat(fs).isNotNull();
    }

    @Test
    public void testMultipleCallsWithSameProperties()
    {
        setupFactory(true);

        Map<String, String> fileIoProperties = ImmutableMap.<String, String>builder()
                .put("s3.access-key-id", "vended-access")
                .put("s3.secret-access-key", "vended-secret")
                .put("s3.session-token", "vended-token")
                .put("client.region", "us-west-2")
                .buildOrThrow();

        ConnectorIdentity identity = ConnectorIdentity.ofUser("test");

        TrinoFileSystem fs1 = factory.create(identity, fileIoProperties);
        TrinoFileSystem fs2 = factory.create(identity, fileIoProperties);

        assertThat(fs1).isNotNull();
        assertThat(fs2).isNotNull();
    }

    @Test
    public void testMultipleCallsWithDifferentRegions()
    {
        setupFactory(true);

        Map<String, String> fileIoProperties1 = ImmutableMap.<String, String>builder()
                .put("s3.access-key-id", "vended-access")
                .put("s3.secret-access-key", "vended-secret")
                .put("s3.session-token", "vended-token")
                .put("client.region", "us-west-1")
                .buildOrThrow();

        Map<String, String> fileIoProperties2 = ImmutableMap.<String, String>builder()
                .put("s3.access-key-id", "vended-access")
                .put("s3.secret-access-key", "vended-secret")
                .put("s3.session-token", "vended-token")
                .put("client.region", "us-west-2")
                .buildOrThrow();

        ConnectorIdentity identity = ConnectorIdentity.ofUser("test");

        TrinoFileSystem fs1 = factory.create(identity, fileIoProperties1);
        TrinoFileSystem fs2 = factory.create(identity, fileIoProperties2);

        assertThat(fs1).isNotNull();
        assertThat(fs2).isNotNull();
    }

    @Test
    public void testVendedCredentialsWithDifferentRegions()
    {
        setupFactory(true);

        Map<String, String> fileIoProperties1 = ImmutableMap.<String, String>builder()
                .put("s3.access-key-id", "vended-access")
                .put("s3.secret-access-key", "vended-secret")
                .put("s3.session-token", "vended-token")
                .put("client.region", "us-east-1")
                .buildOrThrow();

        Map<String, String> fileIoProperties2 = ImmutableMap.<String, String>builder()
                .put("s3.access-key-id", "vended-access")
                .put("s3.secret-access-key", "vended-secret")
                .put("s3.session-token", "vended-token")
                .put("client.region", "ap-southeast-1")
                .buildOrThrow();

        ConnectorIdentity identity = ConnectorIdentity.ofUser("test");

        TrinoFileSystem fs1 = factory.create(identity, fileIoProperties1);
        TrinoFileSystem fs2 = factory.create(identity, fileIoProperties2);

        assertThat(fs1).isNotNull();
        assertThat(fs2).isNotNull();
    }

    @Test
    public void testVendedCredentialsWithHttpEndpoint()
    {
        setupFactory(true);

        Map<String, String> fileIoProperties = ImmutableMap.<String, String>builder()
                .put("s3.access-key-id", "vended-access")
                .put("s3.secret-access-key", "vended-secret")
                .put("s3.session-token", "vended-token")
                .put("s3.endpoint", "http://localhost:9000")
                .buildOrThrow();

        ConnectorIdentity identity = ConnectorIdentity.ofUser("test");
        TrinoFileSystem fs = factory.create(identity, fileIoProperties);

        assertThat(fs).isNotNull();
    }

    @Test
    public void testVendedCredentialsWithEndpointAndPort()
    {
        setupFactory(true);

        Map<String, String> fileIoProperties = ImmutableMap.<String, String>builder()
                .put("s3.access-key-id", "vended-access")
                .put("s3.secret-access-key", "vended-secret")
                .put("s3.session-token", "vended-token")
                .put("s3.endpoint", "https://s3.example.com:443")
                .buildOrThrow();

        ConnectorIdentity identity = ConnectorIdentity.ofUser("test");
        TrinoFileSystem fs = factory.create(identity, fileIoProperties);

        assertThat(fs).isNotNull();
    }

    @Test
    public void testVendedCredentialsWithCrossRegionAccessFalse()
    {
        setupFactory(true);

        Map<String, String> fileIoProperties = ImmutableMap.<String, String>builder()
                .put("s3.access-key-id", "vended-access")
                .put("s3.secret-access-key", "vended-secret")
                .put("s3.session-token", "vended-token")
                .put("s3.cross-region-access-enabled", "false")
                .buildOrThrow();

        ConnectorIdentity identity = ConnectorIdentity.ofUser("test");
        TrinoFileSystem fs = factory.create(identity, fileIoProperties);

        assertThat(fs).isNotNull();
    }

    @Test
    public void testVendedCredentialsWithRegionAndEndpoint()
    {
        setupFactory(true);

        Map<String, String> fileIoProperties = ImmutableMap.<String, String>builder()
                .put("s3.access-key-id", "vended-access")
                .put("s3.secret-access-key", "vended-secret")
                .put("s3.session-token", "vended-token")
                .put("client.region", "us-west-2")
                .put("s3.endpoint", "https://s3.us-west-2.amazonaws.com")
                .buildOrThrow();

        ConnectorIdentity identity = ConnectorIdentity.ofUser("test");
        TrinoFileSystem fs = factory.create(identity, fileIoProperties);

        assertThat(fs).isNotNull();
    }

    @Test
    public void testVendedCredentialsWithEndpointAndCrossRegionAccess()
    {
        setupFactory(true);

        Map<String, String> fileIoProperties = ImmutableMap.<String, String>builder()
                .put("s3.access-key-id", "vended-access")
                .put("s3.secret-access-key", "vended-secret")
                .put("s3.session-token", "vended-token")
                .put("s3.endpoint", "https://minio.example.com")
                .put("s3.cross-region-access-enabled", "true")
                .buildOrThrow();

        ConnectorIdentity identity = ConnectorIdentity.ofUser("test");
        TrinoFileSystem fs = factory.create(identity, fileIoProperties);

        assertThat(fs).isNotNull();
    }

    @Test
    public void testVendedCredentialsWithRegionAndCrossRegionAccess()
    {
        setupFactory(true);

        Map<String, String> fileIoProperties = ImmutableMap.<String, String>builder()
                .put("s3.access-key-id", "vended-access")
                .put("s3.secret-access-key", "vended-secret")
                .put("s3.session-token", "vended-token")
                .put("client.region", "eu-central-1")
                .put("s3.cross-region-access-enabled", "false")
                .buildOrThrow();

        ConnectorIdentity identity = ConnectorIdentity.ofUser("test");
        TrinoFileSystem fs = factory.create(identity, fileIoProperties);

        assertThat(fs).isNotNull();
    }

    @Test
    public void testVendedCredentialsWithMultipleDifferentEndpoints()
    {
        setupFactory(true);

        Map<String, String> fileIoProperties1 = ImmutableMap.<String, String>builder()
                .put("s3.access-key-id", "vended-access")
                .put("s3.secret-access-key", "vended-secret")
                .put("s3.session-token", "vended-token")
                .put("s3.endpoint", "https://s3.amazonaws.com")
                .buildOrThrow();

        Map<String, String> fileIoProperties2 = ImmutableMap.<String, String>builder()
                .put("s3.access-key-id", "vended-access")
                .put("s3.secret-access-key", "vended-secret")
                .put("s3.session-token", "vended-token")
                .put("s3.endpoint", "https://custom-s3.example.org")
                .buildOrThrow();

        ConnectorIdentity identity = ConnectorIdentity.ofUser("test");

        TrinoFileSystem fs1 = factory.create(identity, fileIoProperties1);
        TrinoFileSystem fs2 = factory.create(identity, fileIoProperties2);

        assertThat(fs1).isNotNull();
        assertThat(fs2).isNotNull();
    }
}
