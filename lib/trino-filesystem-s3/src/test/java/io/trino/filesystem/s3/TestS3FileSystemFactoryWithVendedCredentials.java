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

import com.google.common.collect.ImmutableMap;
import io.opentelemetry.api.OpenTelemetry;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.spi.security.ConnectorIdentity;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.s3.S3Client;

import java.lang.reflect.Field;
import java.util.Map;

import static io.trino.filesystem.s3.S3FileSystemConstants.EXTRA_CREDENTIALS_ACCESS_KEY_PROPERTY;
import static io.trino.filesystem.s3.S3FileSystemConstants.EXTRA_CREDENTIALS_CROSS_REGION_ACCESS_ENABLED_PROPERTY;
import static io.trino.filesystem.s3.S3FileSystemConstants.EXTRA_CREDENTIALS_ENDPOINT_PROPERTY;
import static io.trino.filesystem.s3.S3FileSystemConstants.EXTRA_CREDENTIALS_REGION_PROPERTY;
import static io.trino.filesystem.s3.S3FileSystemConstants.EXTRA_CREDENTIALS_SECRET_KEY_PROPERTY;
import static io.trino.filesystem.s3.S3FileSystemConstants.EXTRA_CREDENTIALS_SESSION_TOKEN_PROPERTY;
import static org.assertj.core.api.Assertions.assertThat;

public class TestS3FileSystemFactoryWithVendedCredentials
{
    private S3FileSystemFactory factory;

    @AfterEach
    public void tearDown()
    {
        if (factory != null) {
            factory.destroy();
        }
    }

    @Test
    public void testBackwardCompatibility()
    {
        S3FileSystemConfig config = new S3FileSystemConfig()
                .setAwsAccessKey("test-access")
                .setAwsSecretKey("test-secret")
                .setRegion("us-east-1");

        factory = new S3FileSystemFactory(OpenTelemetry.noop(), config, new S3FileSystemStats());

        ConnectorIdentity identity = ConnectorIdentity.ofUser("test");
        TrinoFileSystem fs = factory.create(identity);

        assertThat(fs).isNotNull();
    }

    @Test
    public void testVendedRegionWhenNoStaticRegion()
    {
        S3FileSystemConfig config = new S3FileSystemConfig()
                .setAwsAccessKey("test-access")
                .setAwsSecretKey("test-secret")
                .setRegion("us-east-1");

        factory = new S3FileSystemFactory(OpenTelemetry.noop(), config, new S3FileSystemStats());

        Map<String, String> extraCredentials = ImmutableMap.<String, String>builder()
                .put(EXTRA_CREDENTIALS_ACCESS_KEY_PROPERTY, "vended-access")
                .put(EXTRA_CREDENTIALS_SECRET_KEY_PROPERTY, "vended-secret")
                .put(EXTRA_CREDENTIALS_SESSION_TOKEN_PROPERTY, "vended-token")
                .put(EXTRA_CREDENTIALS_REGION_PROPERTY, "us-west-2")
                .buildOrThrow();

        ConnectorIdentity identity = ConnectorIdentity.forUser("test")
                .withExtraCredentials(extraCredentials)
                .build();

        TrinoFileSystem fs = factory.create(identity);
        assertThat(fs).isNotNull();
    }

    @Test
    public void testStaticRegionTakesPrecedenceOverVended()
    {
        S3FileSystemConfig config = new S3FileSystemConfig()
                .setAwsAccessKey("test-access")
                .setAwsSecretKey("test-secret")
                .setRegion("us-east-1");

        factory = new S3FileSystemFactory(OpenTelemetry.noop(), config, new S3FileSystemStats());

        Map<String, String> extraCredentials = ImmutableMap.<String, String>builder()
                .put(EXTRA_CREDENTIALS_ACCESS_KEY_PROPERTY, "vended-access")
                .put(EXTRA_CREDENTIALS_SECRET_KEY_PROPERTY, "vended-secret")
                .put(EXTRA_CREDENTIALS_SESSION_TOKEN_PROPERTY, "vended-token")
                .put(EXTRA_CREDENTIALS_REGION_PROPERTY, "us-west-2")
                .buildOrThrow();

        ConnectorIdentity identity = ConnectorIdentity.forUser("test")
                .withExtraCredentials(extraCredentials)
                .build();

        TrinoFileSystem fs = factory.create(identity);
        assertThat(fs).isNotNull();
    }

    @Test
    public void testVendedEndpointWhenNoStaticEndpoint()
    {
        S3FileSystemConfig config = new S3FileSystemConfig()
                .setAwsAccessKey("test-access")
                .setAwsSecretKey("test-secret")
                .setRegion("us-east-1");

        factory = new S3FileSystemFactory(OpenTelemetry.noop(), config, new S3FileSystemStats());

        Map<String, String> extraCredentials = ImmutableMap.<String, String>builder()
                .put(EXTRA_CREDENTIALS_ACCESS_KEY_PROPERTY, "vended-access")
                .put(EXTRA_CREDENTIALS_SECRET_KEY_PROPERTY, "vended-secret")
                .put(EXTRA_CREDENTIALS_SESSION_TOKEN_PROPERTY, "vended-token")
                .put(EXTRA_CREDENTIALS_ENDPOINT_PROPERTY, "https://minio.example.com")
                .buildOrThrow();

        ConnectorIdentity identity = ConnectorIdentity.forUser("test")
                .withExtraCredentials(extraCredentials)
                .build();

        TrinoFileSystem fs = factory.create(identity);
        assertThat(fs).isNotNull();
    }

    @Test
    public void testStaticEndpointTakesPrecedenceOverVended()
    {
        S3FileSystemConfig config = new S3FileSystemConfig()
                .setAwsAccessKey("test-access")
                .setAwsSecretKey("test-secret")
                .setRegion("us-east-1")
                .setEndpoint("https://s3.amazonaws.com");

        factory = new S3FileSystemFactory(OpenTelemetry.noop(), config, new S3FileSystemStats());

        Map<String, String> extraCredentials = ImmutableMap.<String, String>builder()
                .put(EXTRA_CREDENTIALS_ACCESS_KEY_PROPERTY, "vended-access")
                .put(EXTRA_CREDENTIALS_SECRET_KEY_PROPERTY, "vended-secret")
                .put(EXTRA_CREDENTIALS_SESSION_TOKEN_PROPERTY, "vended-token")
                .put(EXTRA_CREDENTIALS_ENDPOINT_PROPERTY, "https://minio.example.com")
                .buildOrThrow();

        ConnectorIdentity identity = ConnectorIdentity.forUser("test")
                .withExtraCredentials(extraCredentials)
                .build();

        TrinoFileSystem fs = factory.create(identity);
        assertThat(fs).isNotNull();
    }

    @Test
    public void testVendedCrossRegionAccessWhenStaticIsDisabled()
    {
        S3FileSystemConfig config = new S3FileSystemConfig()
                .setAwsAccessKey("test-access")
                .setAwsSecretKey("test-secret")
                .setRegion("us-east-1")
                .setCrossRegionAccessEnabled(false);

        factory = new S3FileSystemFactory(OpenTelemetry.noop(), config, new S3FileSystemStats());

        Map<String, String> extraCredentials = ImmutableMap.<String, String>builder()
                .put(EXTRA_CREDENTIALS_ACCESS_KEY_PROPERTY, "vended-access")
                .put(EXTRA_CREDENTIALS_SECRET_KEY_PROPERTY, "vended-secret")
                .put(EXTRA_CREDENTIALS_SESSION_TOKEN_PROPERTY, "vended-token")
                .put(EXTRA_CREDENTIALS_CROSS_REGION_ACCESS_ENABLED_PROPERTY, "true")
                .buildOrThrow();

        ConnectorIdentity identity = ConnectorIdentity.forUser("test")
                .withExtraCredentials(extraCredentials)
                .build();

        TrinoFileSystem fs = factory.create(identity);
        assertThat(fs).isNotNull();
    }

    @Test
    public void testStaticCrossRegionAccessTakesPrecedence()
    {
        S3FileSystemConfig config = new S3FileSystemConfig()
                .setAwsAccessKey("test-access")
                .setAwsSecretKey("test-secret")
                .setRegion("us-east-1")
                .setCrossRegionAccessEnabled(true);

        factory = new S3FileSystemFactory(OpenTelemetry.noop(), config, new S3FileSystemStats());

        Map<String, String> extraCredentials = ImmutableMap.<String, String>builder()
                .put(EXTRA_CREDENTIALS_ACCESS_KEY_PROPERTY, "vended-access")
                .put(EXTRA_CREDENTIALS_SECRET_KEY_PROPERTY, "vended-secret")
                .put(EXTRA_CREDENTIALS_SESSION_TOKEN_PROPERTY, "vended-token")
                .put(EXTRA_CREDENTIALS_CROSS_REGION_ACCESS_ENABLED_PROPERTY, "false")
                .buildOrThrow();

        ConnectorIdentity identity = ConnectorIdentity.forUser("test")
                .withExtraCredentials(extraCredentials)
                .build();

        TrinoFileSystem fs = factory.create(identity);
        assertThat(fs).isNotNull();
    }

    @Test
    public void testAllThreeOverrides()
    {
        S3FileSystemConfig config = new S3FileSystemConfig()
                .setAwsAccessKey("test-access")
                .setAwsSecretKey("test-secret")
                .setRegion("us-east-1");

        factory = new S3FileSystemFactory(OpenTelemetry.noop(), config, new S3FileSystemStats());

        Map<String, String> extraCredentials = ImmutableMap.<String, String>builder()
                .put(EXTRA_CREDENTIALS_ACCESS_KEY_PROPERTY, "vended-access")
                .put(EXTRA_CREDENTIALS_SECRET_KEY_PROPERTY, "vended-secret")
                .put(EXTRA_CREDENTIALS_SESSION_TOKEN_PROPERTY, "vended-token")
                .put(EXTRA_CREDENTIALS_REGION_PROPERTY, "eu-west-1")
                .put(EXTRA_CREDENTIALS_ENDPOINT_PROPERTY, "https://minio.example.com")
                .put(EXTRA_CREDENTIALS_CROSS_REGION_ACCESS_ENABLED_PROPERTY, "true")
                .buildOrThrow();

        ConnectorIdentity identity = ConnectorIdentity.forUser("test")
                .withExtraCredentials(extraCredentials)
                .build();

        TrinoFileSystem fs = factory.create(identity);
        assertThat(fs).isNotNull();
    }

    @Test
    public void testMultipleUsersWithSameVendedRegion()
            throws Exception
    {
        S3FileSystemConfig config = new S3FileSystemConfig()
                .setAwsAccessKey("test-access")
                .setAwsSecretKey("test-secret")
                .setRegion("us-east-1");

        factory = new S3FileSystemFactory(OpenTelemetry.noop(), config, new S3FileSystemStats());

        Map<String, String> extraCredentials = ImmutableMap.<String, String>builder()
                .put(EXTRA_CREDENTIALS_ACCESS_KEY_PROPERTY, "vended-access")
                .put(EXTRA_CREDENTIALS_SECRET_KEY_PROPERTY, "vended-secret")
                .put(EXTRA_CREDENTIALS_SESSION_TOKEN_PROPERTY, "vended-token")
                .put(EXTRA_CREDENTIALS_REGION_PROPERTY, "us-west-2")
                .buildOrThrow();

        ConnectorIdentity identity1 = ConnectorIdentity.forUser("user1")
                .withExtraCredentials(extraCredentials)
                .build();

        ConnectorIdentity identity2 = ConnectorIdentity.forUser("user2")
                .withExtraCredentials(extraCredentials)
                .build();

        TrinoFileSystem fs1 = factory.create(identity1);
        TrinoFileSystem fs2 = factory.create(identity2);

        assertThat(fs1).isNotNull();
        assertThat(fs2).isNotNull();

        S3Client client1 = getClientFromFileSystem(fs1);
        S3Client client2 = getClientFromFileSystem(fs2);

        assertThat(client1).isSameAs(client2);
    }

    @Test
    public void testDifferentVendedRegions()
    {
        S3FileSystemConfig config = new S3FileSystemConfig()
                .setAwsAccessKey("test-access")
                .setAwsSecretKey("test-secret")
                .setRegion("us-east-1");

        factory = new S3FileSystemFactory(OpenTelemetry.noop(), config, new S3FileSystemStats());

        Map<String, String> extraCredentials1 = ImmutableMap.<String, String>builder()
                .put(EXTRA_CREDENTIALS_ACCESS_KEY_PROPERTY, "vended-access")
                .put(EXTRA_CREDENTIALS_SECRET_KEY_PROPERTY, "vended-secret")
                .put(EXTRA_CREDENTIALS_SESSION_TOKEN_PROPERTY, "vended-token")
                .put(EXTRA_CREDENTIALS_REGION_PROPERTY, "us-west-1")
                .buildOrThrow();

        Map<String, String> extraCredentials2 = ImmutableMap.<String, String>builder()
                .put(EXTRA_CREDENTIALS_ACCESS_KEY_PROPERTY, "vended-access")
                .put(EXTRA_CREDENTIALS_SECRET_KEY_PROPERTY, "vended-secret")
                .put(EXTRA_CREDENTIALS_SESSION_TOKEN_PROPERTY, "vended-token")
                .put(EXTRA_CREDENTIALS_REGION_PROPERTY, "us-west-2")
                .buildOrThrow();

        ConnectorIdentity identity1 = ConnectorIdentity.forUser("test")
                .withExtraCredentials(extraCredentials1)
                .build();

        ConnectorIdentity identity2 = ConnectorIdentity.forUser("test")
                .withExtraCredentials(extraCredentials2)
                .build();

        TrinoFileSystem fs1 = factory.create(identity1);
        TrinoFileSystem fs2 = factory.create(identity2);

        assertThat(fs1).isNotNull();
        assertThat(fs2).isNotNull();
    }

    @Test
    public void testNoOverridesUsesDefaultClient()
    {
        S3FileSystemConfig config = new S3FileSystemConfig()
                .setAwsAccessKey("test-access")
                .setAwsSecretKey("test-secret")
                .setRegion("us-east-1");

        factory = new S3FileSystemFactory(OpenTelemetry.noop(), config, new S3FileSystemStats());

        ConnectorIdentity identity = ConnectorIdentity.ofUser("test");
        TrinoFileSystem fs = factory.create(identity);

        assertThat(fs).isNotNull();
    }

    @Test
    public void testOnlyCredentialsNoOverridesUsesDefaultClient()
    {
        S3FileSystemConfig config = new S3FileSystemConfig()
                .setAwsAccessKey("test-access")
                .setAwsSecretKey("test-secret")
                .setRegion("us-east-1");

        factory = new S3FileSystemFactory(OpenTelemetry.noop(), config, new S3FileSystemStats());

        Map<String, String> extraCredentials = ImmutableMap.<String, String>builder()
                .put(EXTRA_CREDENTIALS_ACCESS_KEY_PROPERTY, "vended-access")
                .put(EXTRA_CREDENTIALS_SECRET_KEY_PROPERTY, "vended-secret")
                .put(EXTRA_CREDENTIALS_SESSION_TOKEN_PROPERTY, "vended-token")
                .buildOrThrow();

        ConnectorIdentity identity = ConnectorIdentity.forUser("test")
                .withExtraCredentials(extraCredentials)
                .build();

        TrinoFileSystem fs = factory.create(identity);
        assertThat(fs).isNotNull();
    }

    @Test
    public void testExtractionMethods()
    {
        Map<String, String> extraCredentials = ImmutableMap.<String, String>builder()
                .put(EXTRA_CREDENTIALS_REGION_PROPERTY, "us-west-2")
                .put(EXTRA_CREDENTIALS_ENDPOINT_PROPERTY, "https://minio.local")
                .put(EXTRA_CREDENTIALS_CROSS_REGION_ACCESS_ENABLED_PROPERTY, "true")
                .buildOrThrow();

        ConnectorIdentity identity = ConnectorIdentity.forUser("test")
                .withExtraCredentials(extraCredentials)
                .build();

        assertThat(S3FileSystemLoader.extractRegionFromIdentity(identity))
                .isPresent()
                .hasValue("us-west-2");

        assertThat(S3FileSystemLoader.extractEndpointFromIdentity(identity))
                .isPresent()
                .hasValue("https://minio.local");

        assertThat(S3FileSystemLoader.extractCrossRegionAccessEnabledFromIdentity(identity))
                .isPresent()
                .hasValue(true);
    }

    @Test
    public void testExtractionMethodsWithMissingValues()
    {
        ConnectorIdentity identity = ConnectorIdentity.ofUser("test");

        assertThat(S3FileSystemLoader.extractRegionFromIdentity(identity))
                .isEmpty();

        assertThat(S3FileSystemLoader.extractEndpointFromIdentity(identity))
                .isEmpty();

        assertThat(S3FileSystemLoader.extractCrossRegionAccessEnabledFromIdentity(identity))
                .isEmpty();
    }

    @Test
    public void testCrossRegionAccessBooleanParsing()
    {
        Map<String, String> extraCredentialsTrue = ImmutableMap.of(
                EXTRA_CREDENTIALS_CROSS_REGION_ACCESS_ENABLED_PROPERTY, "true");

        Map<String, String> extraCredentialsFalse = ImmutableMap.of(
                EXTRA_CREDENTIALS_CROSS_REGION_ACCESS_ENABLED_PROPERTY, "false");

        ConnectorIdentity identityTrue = ConnectorIdentity.forUser("test")
                .withExtraCredentials(extraCredentialsTrue)
                .build();

        ConnectorIdentity identityFalse = ConnectorIdentity.forUser("test")
                .withExtraCredentials(extraCredentialsFalse)
                .build();

        assertThat(S3FileSystemLoader.extractCrossRegionAccessEnabledFromIdentity(identityTrue))
                .isPresent()
                .hasValue(true);

        assertThat(S3FileSystemLoader.extractCrossRegionAccessEnabledFromIdentity(identityFalse))
                .isPresent()
                .hasValue(false);
    }

    @Test
    public void testVendedCrossRegionAccessWhenStaticIsFalse()
    {
        S3FileSystemConfig config = new S3FileSystemConfig()
                .setAwsAccessKey("test-access")
                .setAwsSecretKey("test-secret")
                .setRegion("us-east-1")
                .setCrossRegionAccessEnabled(false);

        factory = new S3FileSystemFactory(OpenTelemetry.noop(), config, new S3FileSystemStats());

        Map<String, String> extraCredentials = ImmutableMap.<String, String>builder()
                .put(EXTRA_CREDENTIALS_ACCESS_KEY_PROPERTY, "vended-access")
                .put(EXTRA_CREDENTIALS_SECRET_KEY_PROPERTY, "vended-secret")
                .put(EXTRA_CREDENTIALS_SESSION_TOKEN_PROPERTY, "vended-token")
                .put(EXTRA_CREDENTIALS_CROSS_REGION_ACCESS_ENABLED_PROPERTY, "false")
                .buildOrThrow();

        ConnectorIdentity identity = ConnectorIdentity.forUser("test")
                .withExtraCredentials(extraCredentials)
                .build();

        TrinoFileSystem fs = factory.create(identity);
        assertThat(fs).isNotNull();
    }

    @Test
    public void testVendedCrossRegionAccessFalseWhenStaticIsFalse()
    {
        S3FileSystemConfig config = new S3FileSystemConfig()
                .setAwsAccessKey("test-access")
                .setAwsSecretKey("test-secret")
                .setRegion("us-east-1")
                .setCrossRegionAccessEnabled(false);

        factory = new S3FileSystemFactory(OpenTelemetry.noop(), config, new S3FileSystemStats());

        Map<String, String> extraCredentials = ImmutableMap.<String, String>builder()
                .put(EXTRA_CREDENTIALS_ACCESS_KEY_PROPERTY, "vended-access")
                .put(EXTRA_CREDENTIALS_SECRET_KEY_PROPERTY, "vended-secret")
                .put(EXTRA_CREDENTIALS_SESSION_TOKEN_PROPERTY, "vended-token")
                .put(EXTRA_CREDENTIALS_CROSS_REGION_ACCESS_ENABLED_PROPERTY, "true")
                .buildOrThrow();

        ConnectorIdentity identity = ConnectorIdentity.forUser("test")
                .withExtraCredentials(extraCredentials)
                .build();

        TrinoFileSystem fs = factory.create(identity);
        assertThat(fs).isNotNull();
    }

    @Test
    public void testClientCachingWithSameConfiguration()
            throws Exception
    {
        S3FileSystemConfig config = new S3FileSystemConfig()
                .setAwsAccessKey("test-access")
                .setAwsSecretKey("test-secret")
                .setRegion("us-east-1");

        factory = new S3FileSystemFactory(OpenTelemetry.noop(), config, new S3FileSystemStats());

        Map<String, String> extraCredentials = ImmutableMap.<String, String>builder()
                .put(EXTRA_CREDENTIALS_ACCESS_KEY_PROPERTY, "vended-access")
                .put(EXTRA_CREDENTIALS_SECRET_KEY_PROPERTY, "vended-secret")
                .put(EXTRA_CREDENTIALS_SESSION_TOKEN_PROPERTY, "vended-token")
                .put(EXTRA_CREDENTIALS_REGION_PROPERTY, "us-west-2")
                .put(EXTRA_CREDENTIALS_ENDPOINT_PROPERTY, "https://minio.example.com")
                .put(EXTRA_CREDENTIALS_CROSS_REGION_ACCESS_ENABLED_PROPERTY, "true")
                .buildOrThrow();

        ConnectorIdentity identity1 = ConnectorIdentity.forUser("user1")
                .withExtraCredentials(extraCredentials)
                .build();

        ConnectorIdentity identity2 = ConnectorIdentity.forUser("user2")
                .withExtraCredentials(extraCredentials)
                .build();

        TrinoFileSystem fs1 = factory.create(identity1);
        TrinoFileSystem fs2 = factory.create(identity2);

        assertThat(fs1).isNotNull();
        assertThat(fs2).isNotNull();

        S3Client client1 = getClientFromFileSystem(fs1);
        S3Client client2 = getClientFromFileSystem(fs2);

        assertThat(client1).isSameAs(client2);
    }

    private static S3Client getClientFromFileSystem(TrinoFileSystem fileSystem)
            throws Exception
    {
        assertThat(fileSystem).isInstanceOf(S3FileSystem.class);
        Field clientField = S3FileSystem.class.getDeclaredField("client");
        clientField.setAccessible(true);
        return (S3Client) clientField.get(fileSystem);
    }

    @Test
    public void testVendedEndpointWhenStaticEndpointIsNull()
    {
        S3FileSystemConfig config = new S3FileSystemConfig()
                .setAwsAccessKey("test-access")
                .setAwsSecretKey("test-secret")
                .setRegion("us-east-1");

        factory = new S3FileSystemFactory(OpenTelemetry.noop(), config, new S3FileSystemStats());

        Map<String, String> extraCredentials = ImmutableMap.<String, String>builder()
                .put(EXTRA_CREDENTIALS_ACCESS_KEY_PROPERTY, "vended-access")
                .put(EXTRA_CREDENTIALS_SECRET_KEY_PROPERTY, "vended-secret")
                .put(EXTRA_CREDENTIALS_SESSION_TOKEN_PROPERTY, "vended-token")
                .put(EXTRA_CREDENTIALS_REGION_PROPERTY, "us-west-2")
                .put(EXTRA_CREDENTIALS_ENDPOINT_PROPERTY, "https://minio.example.com")
                .buildOrThrow();

        ConnectorIdentity identity = ConnectorIdentity.forUser("test")
                .withExtraCredentials(extraCredentials)
                .build();

        TrinoFileSystem fs = factory.create(identity);
        assertThat(fs).isNotNull();
    }

    @Test
    public void testCombinationRegionEndpointAndCrossRegionAccess()
    {
        S3FileSystemConfig config = new S3FileSystemConfig()
                .setAwsAccessKey("test-access")
                .setAwsSecretKey("test-secret")
                .setRegion("us-east-1")
                .setCrossRegionAccessEnabled(false);

        factory = new S3FileSystemFactory(OpenTelemetry.noop(), config, new S3FileSystemStats());

        Map<String, String> extraCredentials = ImmutableMap.<String, String>builder()
                .put(EXTRA_CREDENTIALS_ACCESS_KEY_PROPERTY, "vended-access")
                .put(EXTRA_CREDENTIALS_SECRET_KEY_PROPERTY, "vended-secret")
                .put(EXTRA_CREDENTIALS_SESSION_TOKEN_PROPERTY, "vended-token")
                .put(EXTRA_CREDENTIALS_REGION_PROPERTY, "eu-central-1")
                .put(EXTRA_CREDENTIALS_ENDPOINT_PROPERTY, "https://custom-s3.example.com")
                .put(EXTRA_CREDENTIALS_CROSS_REGION_ACCESS_ENABLED_PROPERTY, "true")
                .buildOrThrow();

        ConnectorIdentity identity = ConnectorIdentity.forUser("test")
                .withExtraCredentials(extraCredentials)
                .build();

        TrinoFileSystem fs = factory.create(identity);
        assertThat(fs).isNotNull();
    }
}
