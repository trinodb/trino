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
import io.trino.filesystem.Location;
import io.trino.filesystem.s3.S3FileSystemConfig;
import io.trino.filesystem.s3.S3SecurityMappingResult;
import io.trino.spi.security.ConnectorIdentity;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.identity.spi.AwsCredentialsIdentity;

import java.util.Map;
import java.util.Optional;

import static io.trino.filesystem.s3.S3FileSystemConstants.EXTRA_CREDENTIALS_ACCESS_KEY_PROPERTY;
import static io.trino.filesystem.s3.S3FileSystemConstants.EXTRA_CREDENTIALS_CROSS_REGION_ACCESS_ENABLED_PROPERTY;
import static io.trino.filesystem.s3.S3FileSystemConstants.EXTRA_CREDENTIALS_ENDPOINT_PROPERTY;
import static io.trino.filesystem.s3.S3FileSystemConstants.EXTRA_CREDENTIALS_REGION_PROPERTY;
import static io.trino.filesystem.s3.S3FileSystemConstants.EXTRA_CREDENTIALS_SECRET_KEY_PROPERTY;
import static io.trino.filesystem.s3.S3FileSystemConstants.EXTRA_CREDENTIALS_SESSION_TOKEN_PROPERTY;
import static org.assertj.core.api.Assertions.assertThat;

final class TestIcebergVendedCredentialsMapper
{
    private static final Location DEFAULT_LOCATION = Location.of("s3://test-bucket/path");

    @Test
    void testNoVendedCredentials()
    {
        IcebergVendedCredentialsMapper mapper = new IcebergVendedCredentialsMapper(new S3FileSystemConfig());

        ConnectorIdentity identity = ConnectorIdentity.forUser("test").build();

        Optional<S3SecurityMappingResult> result = mapper.getMapping(identity, DEFAULT_LOCATION);

        assertThat(result).isEmpty();
    }

    @Test
    void testVendedCredentialsOnly()
    {
        IcebergVendedCredentialsMapper mapper = new IcebergVendedCredentialsMapper(new S3FileSystemConfig());

        Map<String, String> extraCredentials = ImmutableMap.<String, String>builder()
                .put(EXTRA_CREDENTIALS_ACCESS_KEY_PROPERTY, "vended-access")
                .put(EXTRA_CREDENTIALS_SECRET_KEY_PROPERTY, "vended-secret")
                .put(EXTRA_CREDENTIALS_SESSION_TOKEN_PROPERTY, "vended-token")
                .buildOrThrow();

        ConnectorIdentity identity = ConnectorIdentity.forUser("test")
                .withExtraCredentials(extraCredentials)
                .build();

        Optional<S3SecurityMappingResult> result = mapper.getMapping(identity, DEFAULT_LOCATION);

        assertThat(result).isPresent();
        assertThat(result.get().credentials()).isPresent();
        assertThat(result.get().credentials().get()).isInstanceOf(AwsCredentials.class);
        AwsCredentialsIdentity credentials = (AwsCredentialsIdentity) result.get().credentials().get();
        assertThat(credentials.accessKeyId()).isEqualTo("vended-access");
        assertThat(credentials.secretAccessKey()).isEqualTo("vended-secret");
    }

    @Test
    void testVendedRegionTakesPrecedenceOverStatic()
    {
        S3FileSystemConfig config = new S3FileSystemConfig()
                .setRegion("us-east-1");

        IcebergVendedCredentialsMapper mapper = new IcebergVendedCredentialsMapper(config);

        Map<String, String> extraCredentials = ImmutableMap.<String, String>builder()
                .put(EXTRA_CREDENTIALS_ACCESS_KEY_PROPERTY, "vended-access")
                .put(EXTRA_CREDENTIALS_SECRET_KEY_PROPERTY, "vended-secret")
                .put(EXTRA_CREDENTIALS_SESSION_TOKEN_PROPERTY, "vended-token")
                .put(EXTRA_CREDENTIALS_REGION_PROPERTY, "us-west-2")
                .buildOrThrow();

        ConnectorIdentity identity = ConnectorIdentity.forUser("test")
                .withExtraCredentials(extraCredentials)
                .build();

        Optional<S3SecurityMappingResult> result = mapper.getMapping(identity, DEFAULT_LOCATION);

        assertThat(result).isPresent();
        assertThat(result.get().region()).hasValue("us-west-2");
    }

    @Test
    void testVendedEndpointTakesPrecedenceOverStatic()
    {
        S3FileSystemConfig config = new S3FileSystemConfig()
                .setEndpoint("https://s3.amazonaws.com");

        IcebergVendedCredentialsMapper mapper = new IcebergVendedCredentialsMapper(config);

        Map<String, String> extraCredentials = ImmutableMap.<String, String>builder()
                .put(EXTRA_CREDENTIALS_ACCESS_KEY_PROPERTY, "vended-access")
                .put(EXTRA_CREDENTIALS_SECRET_KEY_PROPERTY, "vended-secret")
                .put(EXTRA_CREDENTIALS_SESSION_TOKEN_PROPERTY, "vended-token")
                .put(EXTRA_CREDENTIALS_ENDPOINT_PROPERTY, "https://minio.example.com")
                .buildOrThrow();

        ConnectorIdentity identity = ConnectorIdentity.forUser("test")
                .withExtraCredentials(extraCredentials)
                .build();

        Optional<S3SecurityMappingResult> result = mapper.getMapping(identity, DEFAULT_LOCATION);

        assertThat(result).isPresent();
        assertThat(result.get().endpoint()).hasValue("https://minio.example.com");
    }

    @Test
    void testStaticCrossRegionAccessTrueAlwaysWins()
    {
        S3FileSystemConfig config = new S3FileSystemConfig()
                .setCrossRegionAccessEnabled(true);

        IcebergVendedCredentialsMapper mapper = new IcebergVendedCredentialsMapper(config);

        Map<String, String> extraCredentials = ImmutableMap.<String, String>builder()
                .put(EXTRA_CREDENTIALS_ACCESS_KEY_PROPERTY, "vended-access")
                .put(EXTRA_CREDENTIALS_SECRET_KEY_PROPERTY, "vended-secret")
                .put(EXTRA_CREDENTIALS_SESSION_TOKEN_PROPERTY, "vended-token")
                .put(EXTRA_CREDENTIALS_CROSS_REGION_ACCESS_ENABLED_PROPERTY, "false")
                .buildOrThrow();

        ConnectorIdentity identity = ConnectorIdentity.forUser("test")
                .withExtraCredentials(extraCredentials)
                .build();

        Optional<S3SecurityMappingResult> result = mapper.getMapping(identity, DEFAULT_LOCATION);

        assertThat(result).isPresent();
        assertThat(result.get().crossRegionAccessEnabled()).hasValue(true);
    }

    @Test
    void testVendedCrossRegionAccessWhenStaticIsFalse()
    {
        S3FileSystemConfig config = new S3FileSystemConfig()
                .setCrossRegionAccessEnabled(false);

        IcebergVendedCredentialsMapper mapper = new IcebergVendedCredentialsMapper(config);

        Map<String, String> extraCredentials = ImmutableMap.<String, String>builder()
                .put(EXTRA_CREDENTIALS_ACCESS_KEY_PROPERTY, "vended-access")
                .put(EXTRA_CREDENTIALS_SECRET_KEY_PROPERTY, "vended-secret")
                .put(EXTRA_CREDENTIALS_SESSION_TOKEN_PROPERTY, "vended-token")
                .put(EXTRA_CREDENTIALS_CROSS_REGION_ACCESS_ENABLED_PROPERTY, "true")
                .buildOrThrow();

        ConnectorIdentity identity = ConnectorIdentity.forUser("test")
                .withExtraCredentials(extraCredentials)
                .build();

        Optional<S3SecurityMappingResult> result = mapper.getMapping(identity, DEFAULT_LOCATION);

        assertThat(result).isPresent();
        assertThat(result.get().crossRegionAccessEnabled()).hasValue(true);
    }

    @Test
    void testIncompleteCredentialsReturnsEmpty()
    {
        IcebergVendedCredentialsMapper mapper = new IcebergVendedCredentialsMapper(new S3FileSystemConfig());

        Map<String, String> extraCredentials = ImmutableMap.<String, String>builder()
                .put(EXTRA_CREDENTIALS_ACCESS_KEY_PROPERTY, "vended-access")
                .put(EXTRA_CREDENTIALS_SECRET_KEY_PROPERTY, "vended-secret")
                .buildOrThrow();

        ConnectorIdentity identity = ConnectorIdentity.forUser("test")
                .withExtraCredentials(extraCredentials)
                .build();

        Optional<S3SecurityMappingResult> result = mapper.getMapping(identity, DEFAULT_LOCATION);

        assertThat(result).isEmpty();
    }

    @Test
    void testVendedRegionUsedWhenNoStatic()
    {
        IcebergVendedCredentialsMapper mapper = new IcebergVendedCredentialsMapper(new S3FileSystemConfig());

        Map<String, String> extraCredentials = ImmutableMap.<String, String>builder()
                .put(EXTRA_CREDENTIALS_ACCESS_KEY_PROPERTY, "vended-access")
                .put(EXTRA_CREDENTIALS_SECRET_KEY_PROPERTY, "vended-secret")
                .put(EXTRA_CREDENTIALS_SESSION_TOKEN_PROPERTY, "vended-token")
                .put(EXTRA_CREDENTIALS_REGION_PROPERTY, "ap-south-1")
                .buildOrThrow();

        ConnectorIdentity identity = ConnectorIdentity.forUser("test")
                .withExtraCredentials(extraCredentials)
                .build();

        Optional<S3SecurityMappingResult> result = mapper.getMapping(identity, DEFAULT_LOCATION);

        assertThat(result).isPresent();
        assertThat(result.get().region()).hasValue("ap-south-1");
    }

    @Test
    void testVendedEndpointUsedWhenNoStatic()
    {
        IcebergVendedCredentialsMapper mapper = new IcebergVendedCredentialsMapper(new S3FileSystemConfig());

        Map<String, String> extraCredentials = ImmutableMap.<String, String>builder()
                .put(EXTRA_CREDENTIALS_ACCESS_KEY_PROPERTY, "vended-access")
                .put(EXTRA_CREDENTIALS_SECRET_KEY_PROPERTY, "vended-secret")
                .put(EXTRA_CREDENTIALS_SESSION_TOKEN_PROPERTY, "vended-token")
                .put(EXTRA_CREDENTIALS_ENDPOINT_PROPERTY, "https://custom-s3.example.com")
                .buildOrThrow();

        ConnectorIdentity identity = ConnectorIdentity.forUser("test")
                .withExtraCredentials(extraCredentials)
                .build();

        Optional<S3SecurityMappingResult> result = mapper.getMapping(identity, DEFAULT_LOCATION);

        assertThat(result).isPresent();
        assertThat(result.get().endpoint()).hasValue("https://custom-s3.example.com");
    }

    @Test
    void testStaticRegionUsedWhenVendedNotProvided()
    {
        S3FileSystemConfig config = new S3FileSystemConfig()
                .setRegion("us-east-1");

        IcebergVendedCredentialsMapper mapper = new IcebergVendedCredentialsMapper(config);

        Map<String, String> extraCredentials = ImmutableMap.<String, String>builder()
                .put(EXTRA_CREDENTIALS_ACCESS_KEY_PROPERTY, "vended-access")
                .put(EXTRA_CREDENTIALS_SECRET_KEY_PROPERTY, "vended-secret")
                .put(EXTRA_CREDENTIALS_SESSION_TOKEN_PROPERTY, "vended-token")
                .buildOrThrow();

        ConnectorIdentity identity = ConnectorIdentity.forUser("test")
                .withExtraCredentials(extraCredentials)
                .build();

        Optional<S3SecurityMappingResult> result = mapper.getMapping(identity, DEFAULT_LOCATION);

        assertThat(result).isPresent();
        assertThat(result.get().region()).hasValue("us-east-1");
    }

    @Test
    void testStaticEndpointUsedWhenVendedNotProvided()
    {
        S3FileSystemConfig config = new S3FileSystemConfig()
                .setEndpoint("https://s3.amazonaws.com");

        IcebergVendedCredentialsMapper mapper = new IcebergVendedCredentialsMapper(config);

        Map<String, String> extraCredentials = ImmutableMap.<String, String>builder()
                .put(EXTRA_CREDENTIALS_ACCESS_KEY_PROPERTY, "vended-access")
                .put(EXTRA_CREDENTIALS_SECRET_KEY_PROPERTY, "vended-secret")
                .put(EXTRA_CREDENTIALS_SESSION_TOKEN_PROPERTY, "vended-token")
                .buildOrThrow();

        ConnectorIdentity identity = ConnectorIdentity.forUser("test")
                .withExtraCredentials(extraCredentials)
                .build();

        Optional<S3SecurityMappingResult> result = mapper.getMapping(identity, DEFAULT_LOCATION);

        assertThat(result).isPresent();
        assertThat(result.get().endpoint()).hasValue("https://s3.amazonaws.com");
    }
}
