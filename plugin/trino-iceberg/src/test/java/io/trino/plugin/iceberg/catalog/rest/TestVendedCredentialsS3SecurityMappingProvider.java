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
import io.trino.filesystem.s3.S3SecurityMappingResult;
import io.trino.spi.TrinoException;
import io.trino.spi.security.ConnectorIdentity;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.identity.spi.AwsCredentialsIdentity;

import java.util.Map;
import java.util.Optional;

import static io.trino.filesystem.s3.S3FileSystemConstants.EXTRA_CREDENTIALS_ACCESS_KEY_PROPERTY;
import static io.trino.filesystem.s3.S3FileSystemConstants.EXTRA_CREDENTIALS_CROSS_REGION_ACCESS_ENABLED_PROPERTY;
import static io.trino.filesystem.s3.S3FileSystemConstants.EXTRA_CREDENTIALS_ENDPOINT_PROPERTY;
import static io.trino.filesystem.s3.S3FileSystemConstants.EXTRA_CREDENTIALS_PATH_STYLE_ACCESS_PROPERTY;
import static io.trino.filesystem.s3.S3FileSystemConstants.EXTRA_CREDENTIALS_REGION_PROPERTY;
import static io.trino.filesystem.s3.S3FileSystemConstants.EXTRA_CREDENTIALS_SECRET_KEY_PROPERTY;
import static io.trino.filesystem.s3.S3FileSystemConstants.EXTRA_CREDENTIALS_SESSION_TOKEN_PROPERTY;
import static io.trino.plugin.iceberg.IcebergErrorCode.ICEBERG_CATALOG_ERROR;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

final class TestVendedCredentialsS3SecurityMappingProvider
{
    private static final Location DEFAULT_LOCATION = Location.of("s3://test-bucket/path");

    @Test
    void testNoVendedCredentials()
    {
        VendedCredentialsS3SecurityMappingProvider provider = new VendedCredentialsS3SecurityMappingProvider();

        ConnectorIdentity identity = ConnectorIdentity.forUser("test").build();

        Optional<S3SecurityMappingResult> result = provider.getMapping(identity, DEFAULT_LOCATION);

        assertThat(result).isEmpty();
    }

    @Test
    void testVendedCredentialsOnly()
    {
        VendedCredentialsS3SecurityMappingProvider provider = new VendedCredentialsS3SecurityMappingProvider();

        Map<String, String> extraCredentials = ImmutableMap.<String, String>builder()
                .put(EXTRA_CREDENTIALS_ACCESS_KEY_PROPERTY, "vended-access")
                .put(EXTRA_CREDENTIALS_SECRET_KEY_PROPERTY, "vended-secret")
                .put(EXTRA_CREDENTIALS_SESSION_TOKEN_PROPERTY, "vended-token")
                .buildOrThrow();

        ConnectorIdentity identity = ConnectorIdentity.forUser("test")
                .withExtraCredentials(extraCredentials)
                .build();

        Optional<S3SecurityMappingResult> result = provider.getMapping(identity, DEFAULT_LOCATION);

        assertThat(result).isPresent();
        assertThat(result.get().credentials()).isPresent();
        assertThat(result.get().credentials().get()).isInstanceOf(AwsCredentials.class);
        AwsCredentialsIdentity credentials = (AwsCredentialsIdentity) result.get().credentials().get();
        assertThat(credentials.accessKeyId()).isEqualTo("vended-access");
        assertThat(credentials.secretAccessKey()).isEqualTo("vended-secret");
    }

    @Test
    void testIncompleteCredentialsReturnsEmpty()
    {
        VendedCredentialsS3SecurityMappingProvider provider = new VendedCredentialsS3SecurityMappingProvider();

        Map<String, String> extraCredentials = ImmutableMap.<String, String>builder()
                .put(EXTRA_CREDENTIALS_ACCESS_KEY_PROPERTY, "vended-access")
                .put(EXTRA_CREDENTIALS_SECRET_KEY_PROPERTY, "vended-secret")
                .buildOrThrow();

        ConnectorIdentity identity = ConnectorIdentity.forUser("test")
                .withExtraCredentials(extraCredentials)
                .build();

        Optional<S3SecurityMappingResult> result = provider.getMapping(identity, DEFAULT_LOCATION);

        assertThat(result).isEmpty();
    }

    @Test
    void testVendedRegion()
    {
        VendedCredentialsS3SecurityMappingProvider provider = new VendedCredentialsS3SecurityMappingProvider();

        Map<String, String> extraCredentials = ImmutableMap.<String, String>builder()
                .put(EXTRA_CREDENTIALS_ACCESS_KEY_PROPERTY, "vended-access")
                .put(EXTRA_CREDENTIALS_SECRET_KEY_PROPERTY, "vended-secret")
                .put(EXTRA_CREDENTIALS_SESSION_TOKEN_PROPERTY, "vended-token")
                .put(EXTRA_CREDENTIALS_REGION_PROPERTY, "ap-south-1")
                .buildOrThrow();

        ConnectorIdentity identity = ConnectorIdentity.forUser("test")
                .withExtraCredentials(extraCredentials)
                .build();

        Optional<S3SecurityMappingResult> result = provider.getMapping(identity, DEFAULT_LOCATION);

        assertThat(result).isPresent();
        assertThat(result.get().region()).hasValue("ap-south-1");
    }

    @Test
    void testVendedEndpoint()
    {
        VendedCredentialsS3SecurityMappingProvider provider = new VendedCredentialsS3SecurityMappingProvider();

        Map<String, String> extraCredentials = ImmutableMap.<String, String>builder()
                .put(EXTRA_CREDENTIALS_ACCESS_KEY_PROPERTY, "vended-access")
                .put(EXTRA_CREDENTIALS_SECRET_KEY_PROPERTY, "vended-secret")
                .put(EXTRA_CREDENTIALS_SESSION_TOKEN_PROPERTY, "vended-token")
                .put(EXTRA_CREDENTIALS_ENDPOINT_PROPERTY, "https://custom-s3.example.com")
                .buildOrThrow();

        ConnectorIdentity identity = ConnectorIdentity.forUser("test")
                .withExtraCredentials(extraCredentials)
                .build();

        Optional<S3SecurityMappingResult> result = provider.getMapping(identity, DEFAULT_LOCATION);

        assertThat(result).isPresent();
        assertThat(result.get().endpoint()).hasValue("https://custom-s3.example.com");
    }

    @Test
    void testAllOverridesTogether()
    {
        VendedCredentialsS3SecurityMappingProvider provider = new VendedCredentialsS3SecurityMappingProvider();

        Map<String, String> extraCredentials = ImmutableMap.<String, String>builder()
                .put(EXTRA_CREDENTIALS_ACCESS_KEY_PROPERTY, "vended-access")
                .put(EXTRA_CREDENTIALS_SECRET_KEY_PROPERTY, "vended-secret")
                .put(EXTRA_CREDENTIALS_SESSION_TOKEN_PROPERTY, "vended-token")
                .put(EXTRA_CREDENTIALS_REGION_PROPERTY, "eu-west-1")
                .put(EXTRA_CREDENTIALS_ENDPOINT_PROPERTY, "https://custom-endpoint.example.com")
                .put(EXTRA_CREDENTIALS_CROSS_REGION_ACCESS_ENABLED_PROPERTY, "true")
                .put(EXTRA_CREDENTIALS_PATH_STYLE_ACCESS_PROPERTY, "true")
                .buildOrThrow();

        ConnectorIdentity identity = ConnectorIdentity.forUser("test")
                .withExtraCredentials(extraCredentials)
                .build();

        Optional<S3SecurityMappingResult> result = provider.getMapping(identity, DEFAULT_LOCATION);

        assertThat(result).isPresent();
        assertThat(result.get().region()).hasValue("eu-west-1");
        assertThat(result.get().endpoint()).hasValue("https://custom-endpoint.example.com");
        assertThat(result.get().crossRegionAccessEnabled()).hasValue(true);
        assertThat(result.get().pathStyleAccess()).hasValue(true);
        assertThat(result.get().credentials()).isPresent();
    }

    @Test
    void testCrossRegionAccessBooleanParsing()
    {
        VendedCredentialsS3SecurityMappingProvider provider = new VendedCredentialsS3SecurityMappingProvider();

        // Test "true" string
        Map<String, String> extraCredentialsTrue = ImmutableMap.<String, String>builder()
                .put(EXTRA_CREDENTIALS_ACCESS_KEY_PROPERTY, "vended-access")
                .put(EXTRA_CREDENTIALS_SECRET_KEY_PROPERTY, "vended-secret")
                .put(EXTRA_CREDENTIALS_SESSION_TOKEN_PROPERTY, "vended-token")
                .put(EXTRA_CREDENTIALS_CROSS_REGION_ACCESS_ENABLED_PROPERTY, "true")
                .buildOrThrow();

        Optional<S3SecurityMappingResult> resultTrue = provider.getMapping(
                ConnectorIdentity.forUser("test").withExtraCredentials(extraCredentialsTrue).build(),
                DEFAULT_LOCATION);
        assertThat(resultTrue).isPresent();
        assertThat(resultTrue.get().crossRegionAccessEnabled()).hasValue(true);

        // Test "false" string
        Map<String, String> extraCredentialsFalse = ImmutableMap.<String, String>builder()
                .put(EXTRA_CREDENTIALS_ACCESS_KEY_PROPERTY, "vended-access")
                .put(EXTRA_CREDENTIALS_SECRET_KEY_PROPERTY, "vended-secret")
                .put(EXTRA_CREDENTIALS_SESSION_TOKEN_PROPERTY, "vended-token")
                .put(EXTRA_CREDENTIALS_CROSS_REGION_ACCESS_ENABLED_PROPERTY, "false")
                .buildOrThrow();

        Optional<S3SecurityMappingResult> resultFalse = provider.getMapping(
                ConnectorIdentity.forUser("test").withExtraCredentials(extraCredentialsFalse).build(),
                DEFAULT_LOCATION);
        assertThat(resultFalse).isPresent();
        assertThat(resultFalse.get().crossRegionAccessEnabled()).hasValue(false);
    }

    @Test
    void testPathStyleAccessBooleanParsing()
    {
        VendedCredentialsS3SecurityMappingProvider provider = new VendedCredentialsS3SecurityMappingProvider();

        Map<String, String> extraCredentialsTrue = ImmutableMap.<String, String>builder()
                .put(EXTRA_CREDENTIALS_ACCESS_KEY_PROPERTY, "vended-access")
                .put(EXTRA_CREDENTIALS_SECRET_KEY_PROPERTY, "vended-secret")
                .put(EXTRA_CREDENTIALS_SESSION_TOKEN_PROPERTY, "vended-token")
                .put(EXTRA_CREDENTIALS_PATH_STYLE_ACCESS_PROPERTY, "true")
                .buildOrThrow();

        Optional<S3SecurityMappingResult> resultTrue = provider.getMapping(
                ConnectorIdentity.forUser("test").withExtraCredentials(extraCredentialsTrue).build(),
                DEFAULT_LOCATION);
        assertThat(resultTrue).isPresent();
        assertThat(resultTrue.get().pathStyleAccess()).hasValue(true);

        Map<String, String> extraCredentialsFalse = ImmutableMap.<String, String>builder()
                .put(EXTRA_CREDENTIALS_ACCESS_KEY_PROPERTY, "vended-access")
                .put(EXTRA_CREDENTIALS_SECRET_KEY_PROPERTY, "vended-secret")
                .put(EXTRA_CREDENTIALS_SESSION_TOKEN_PROPERTY, "vended-token")
                .put(EXTRA_CREDENTIALS_PATH_STYLE_ACCESS_PROPERTY, "false")
                .buildOrThrow();

        Optional<S3SecurityMappingResult> resultFalse = provider.getMapping(
                ConnectorIdentity.forUser("test").withExtraCredentials(extraCredentialsFalse).build(),
                DEFAULT_LOCATION);
        assertThat(resultFalse).isPresent();
        assertThat(resultFalse.get().pathStyleAccess()).hasValue(false);
    }

    @Test
    void testInvalidCrossRegionAccessValueThrows()
    {
        VendedCredentialsS3SecurityMappingProvider provider = new VendedCredentialsS3SecurityMappingProvider();

        Map<String, String> extraCredentials = ImmutableMap.<String, String>builder()
                .put(EXTRA_CREDENTIALS_ACCESS_KEY_PROPERTY, "vended-access")
                .put(EXTRA_CREDENTIALS_SECRET_KEY_PROPERTY, "vended-secret")
                .put(EXTRA_CREDENTIALS_SESSION_TOKEN_PROPERTY, "vended-token")
                .put(EXTRA_CREDENTIALS_CROSS_REGION_ACCESS_ENABLED_PROPERTY, "yes")
                .buildOrThrow();

        ConnectorIdentity identity = ConnectorIdentity.forUser("test")
                .withExtraCredentials(extraCredentials)
                .build();

        assertThatThrownBy(() -> provider.getMapping(identity, DEFAULT_LOCATION))
                .isInstanceOfSatisfying(TrinoException.class, exception -> {
                    assertThat(exception.getErrorCode()).isEqualTo(ICEBERG_CATALOG_ERROR.toErrorCode());
                    assertThat(exception).hasMessageContaining(EXTRA_CREDENTIALS_CROSS_REGION_ACCESS_ENABLED_PROPERTY);
                });
    }

    @Test
    void testInvalidPathStyleAccessValueThrows()
    {
        VendedCredentialsS3SecurityMappingProvider provider = new VendedCredentialsS3SecurityMappingProvider();

        Map<String, String> extraCredentials = ImmutableMap.<String, String>builder()
                .put(EXTRA_CREDENTIALS_ACCESS_KEY_PROPERTY, "vended-access")
                .put(EXTRA_CREDENTIALS_SECRET_KEY_PROPERTY, "vended-secret")
                .put(EXTRA_CREDENTIALS_SESSION_TOKEN_PROPERTY, "vended-token")
                .put(EXTRA_CREDENTIALS_PATH_STYLE_ACCESS_PROPERTY, "yes")
                .buildOrThrow();

        ConnectorIdentity identity = ConnectorIdentity.forUser("test")
                .withExtraCredentials(extraCredentials)
                .build();

        assertThatThrownBy(() -> provider.getMapping(identity, DEFAULT_LOCATION))
                .isInstanceOfSatisfying(TrinoException.class, exception -> {
                    assertThat(exception.getErrorCode()).isEqualTo(ICEBERG_CATALOG_ERROR.toErrorCode());
                    assertThat(exception).hasMessageContaining(EXTRA_CREDENTIALS_PATH_STYLE_ACCESS_PROPERTY);
                });
    }

    @Test
    void testCredentialsOnlyNoOverrides()
    {
        VendedCredentialsS3SecurityMappingProvider provider = new VendedCredentialsS3SecurityMappingProvider();

        Map<String, String> extraCredentials = ImmutableMap.<String, String>builder()
                .put(EXTRA_CREDENTIALS_ACCESS_KEY_PROPERTY, "vended-access")
                .put(EXTRA_CREDENTIALS_SECRET_KEY_PROPERTY, "vended-secret")
                .put(EXTRA_CREDENTIALS_SESSION_TOKEN_PROPERTY, "vended-token")
                .buildOrThrow();

        ConnectorIdentity identity = ConnectorIdentity.forUser("test")
                .withExtraCredentials(extraCredentials)
                .build();

        Optional<S3SecurityMappingResult> result = provider.getMapping(identity, DEFAULT_LOCATION);

        assertThat(result).isPresent();
        assertThat(result.get().credentials()).isPresent();
        assertThat(result.get().region()).isEmpty();
        assertThat(result.get().endpoint()).isEmpty();
        assertThat(result.get().crossRegionAccessEnabled()).isEmpty();
        assertThat(result.get().pathStyleAccess()).isEmpty();
    }
}
