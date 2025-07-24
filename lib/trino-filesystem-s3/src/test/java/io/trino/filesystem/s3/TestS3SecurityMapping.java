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

import com.google.common.collect.ImmutableSet;
import io.trino.filesystem.Location;
import io.trino.spi.security.AccessDeniedException;
import io.trino.spi.security.ConnectorIdentity;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.identity.spi.AwsCredentialsIdentity;

import java.io.File;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.google.common.io.Resources.getResource;
import static io.trino.filesystem.s3.TestS3SecurityMapping.MappingResult.credentials;
import static io.trino.filesystem.s3.TestS3SecurityMapping.MappingSelector.path;
import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestS3SecurityMapping
{
    private static final String IAM_ROLE_CREDENTIAL_NAME = "IAM_ROLE_CREDENTIAL_NAME";
    private static final String KMS_KEY_ID_CREDENTIAL_NAME = "KMS_KEY_ID_CREDENTIAL_NAME";
    private static final String CUSTOMER_KEY_CREDENTIAL_NAME = "CUSTOMER_KEY_CREDENTIAL_NAME";
    private static final String DEFAULT_PATH = "s3://default/";
    private static final String DEFAULT_USER = "testuser";

    @Test
    public void testMapping()
    {
        S3SecurityMappingConfig mappingConfig = new S3SecurityMappingConfig()
                .setConfigFile(getResourceFile("security-mapping.json"))
                .setRoleCredentialName(IAM_ROLE_CREDENTIAL_NAME)
                .setKmsKeyIdCredentialName(KMS_KEY_ID_CREDENTIAL_NAME)
                .setSseCustomerKeyCredentialName(CUSTOMER_KEY_CREDENTIAL_NAME)
                .setColonReplacement("#");

        var provider = new S3SecurityMappingProvider(mappingConfig, new S3SecurityMappingsFileSource(mappingConfig));

        // matches prefix -- mapping provides credentials
        assertMapping(
                provider,
                path("s3://foo/data/test.csv"),
                credentials("AKIAxxxaccess", "iXbXxxxsecret")
                        .withKmsKeyId("kmsKey_10"));

        // matches prefix exactly -- mapping provides credentials
        assertMapping(
                provider,
                path("s3://foo/"),
                credentials("AKIAxxxaccess", "iXbXxxxsecret")
                        .withKmsKeyId("kmsKey_10"));

        // matches prefix exactly -- mapping provides credentials, kms key from extra credentials matching default
        assertMapping(
                provider,
                path("s3://foo/")
                        .withExtraCredentialKmsKeyId("kmsKey_10"),
                credentials("AKIAxxxaccess", "iXbXxxxsecret")
                        .withKmsKeyId("kmsKey_10"));

        // matches prefix exactly -- mapping provides credentials, kms key from extra credentials, allowed, different than default
        assertMapping(
                provider,
                path("s3://foo/")
                        .withExtraCredentialKmsKeyId("kmsKey_11"),
                credentials("AKIAxxxaccess", "iXbXxxxsecret")
                        .withKmsKeyId("kmsKey_11"));

        // matches prefix exactly -- mapping provides credentials, kms key from extra credentials, not allowed
        assertMappingFails(
                provider,
                path("s3://foo/")
                        .withExtraCredentialKmsKeyId("kmsKey_not_allowed"),
                "Selected KMS Key ID is not allowed");

        // matches prefix exactly -- mapping provides credentials, kms key from extra credentials, all keys are allowed, different than default
        assertMapping(
                provider,
                path("s3://foo_all_keys_allowed/")
                        .withExtraCredentialKmsKeyId("kmsKey_777"),
                credentials("AKIAxxxaccess", "iXbXxxxsecret")
                        .withKmsKeyId("kmsKey_777"));

        // matches prefix exactly -- mapping provides credentials, kms key from extra credentials, allowed, no default key
        assertMapping(
                provider,
                path("s3://foo_no_default_key/")
                        .withExtraCredentialKmsKeyId("kmsKey_12"),
                credentials("AKIAxxxaccess", "iXbXxxxsecret")
                        .withKmsKeyId("kmsKey_12"));

        // matches prefix exactly -- mapping provides credentials, customer key from extra credentials matching default
        assertMapping(
                provider,
                path("s3://baz/")
                        .withExtraCredentialCustomerKey("customerKey_10"),
                credentials("AKIAxxxaccess", "iXbXxxxsecret")
                        .withSseCustomerKey("customerKey_10"));

        // matches prefix exactly -- mapping provides credentials, customer key from extra credentials, allowed, different from default
        assertMapping(
                provider,
                path("s3://baz/")
                        .withExtraCredentialCustomerKey("customerKey_11"),
                credentials("AKIAxxxaccess", "iXbXxxxsecret")
                        .withSseCustomerKey("customerKey_11"));

        // matches prefix exactly -- mapping provides credentials, customer key from extra credentials, not allowed
        assertMappingFails(
                provider,
                path("s3://baz/")
                        .withExtraCredentialCustomerKey("customerKey_not_allowed"),
                "Provided SSE Customer Key is not allowed");

        // matches prefix exactly -- mapping provides credentials, customer key from extra credentials, all keys are allowed, different from default
        assertMapping(
                provider,
                path("s3://baz_all_customer_keys_allowed/")
                        .withExtraCredentialCustomerKey("customerKey_777"),
                credentials("AKIAxxxaccess", "iXbXxxxsecret")
                        .withSseCustomerKey("customerKey_777"));

        // matches prefix exactly -- mapping provides credentials, customer key from extra credentials, allowed, no default key
        assertMapping(
                provider,
                path("s3://baz_no_customer_default_key/")
                        .withExtraCredentialCustomerKey("customerKey_12"),
                credentials("AKIAxxxaccess", "iXbXxxxsecret")
                        .withSseCustomerKey("customerKey_12"));

        // no role selected and mapping has no default role
        assertMappingFails(
                provider,
                path("s3://bar/test"),
                "No IAM role selected and mapping has no default role");

        // matches prefix and user selected one of the allowed roles
        assertMapping(
                provider,
                path("s3://bar/test")
                        .withExtraCredentialIamRole("arn:aws:iam::123456789101:role/allow_bucket_2"),
                MappingResult.iamRole("arn:aws:iam::123456789101:role/allow_bucket_2"));

        // user selected a role not in allowed list
        assertMappingFails(
                provider,
                path("s3://bar/test")
                        .withUser("bob")
                        .withExtraCredentialIamRole("bogus"),
                "Selected IAM role is not allowed: bogus");

        // verify that colon replacement works
        String roleWithoutColon = "arn#aws#iam##123456789101#role/allow_bucket_2";
        assertThat(roleWithoutColon).doesNotContain(":");
        assertMapping(
                provider,
                path("s3://bar/test")
                        .withExtraCredentialIamRole(roleWithoutColon),
                MappingResult.iamRole("arn:aws:iam::123456789101:role/allow_bucket_2"));

        // matches prefix -- default role used
        assertMapping(
                provider,
                path("s3://bar/abc/data/test.csv"),
                MappingResult.iamRole("arn:aws:iam::123456789101:role/allow_path"));

        // matches prefix -- default role used
        assertMapping(
                provider,
                path("s3://xyz/default"),
                MappingResult.iamRole("arn:aws:iam::123456789101:role/allow_default"));

        // matches prefix and user selected one of the allowed roles
        assertMapping(
                provider,
                path("s3://xyz/foo")
                        .withExtraCredentialIamRole("arn:aws:iam::123456789101:role/allow_foo"),
                MappingResult.iamRole("arn:aws:iam::123456789101:role/allow_foo"));

        // matches prefix and user selected one of the allowed roles
        assertMapping(
                provider,
                path("s3://xyz/bar")
                        .withExtraCredentialIamRole("arn:aws:iam::123456789101:role/allow_bar"),
                MappingResult.iamRole("arn:aws:iam::123456789101:role/allow_bar"));
    }

    @Test
    public void testMappingWithEncryptionKeysAndFallbackShouldFail()
    {
        Optional<Boolean> useClusterDefault = Optional.of(true);
        Optional<String> kmsKeyId = Optional.of("CLIENT_S3CRT_KEY_ID");

        assertThatThrownBy(() ->
                new S3SecurityMapping(
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty(),
                        kmsKeyId,
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty(),
                        useClusterDefault,
                        Optional.empty(),
                        Optional.empty()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("KMS key ID cannot be provided together with useClusterDefault");
    }

    @Test
    public void testMappingWithSseCustomerKeyAndFallbackShouldFail()
    {
        Optional<Boolean> useClusterDefault = Optional.of(true);
        Optional<String> sseCustomerKey = Optional.of("CLIENT_S3CRT_CUSTOMER_KEY");

        assertThatThrownBy(() ->
                new S3SecurityMapping(
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty(),
                        sseCustomerKey,
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty(),
                        useClusterDefault,
                        Optional.empty(),
                        Optional.empty()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("SSE Customer key cannot be provided together with useClusterDefault");
    }

    @Test
    public void testMappingWithSseCustomerAndKMSKeysShouldFail()
    {
        Optional<String> kmsKeyId = Optional.of("CLIENT_S3CRT_KEY_ID");
        Optional<String> sseCustomerKey = Optional.of("CLIENT_S3CRT_CUSTOMER_KEY");

        assertThatThrownBy(() ->
                new S3SecurityMapping(
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.of("arn:aws:iam::123456789101:role/allow_path"),
                        Optional.empty(),
                        Optional.empty(),
                        kmsKeyId,
                        Optional.empty(),
                        sseCustomerKey,
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("SSE Customer key cannot be provided together with KMS key ID");
    }

    private File getResourceFile(String name)
    {
        return new File(getResource(getClass(), name).getFile());
    }

    private static void assertMapping(S3SecurityMappingProvider provider, MappingSelector selector, MappingResult expected)
    {
        Optional<S3SecurityMappingResult> mapping = getMapping(provider, selector);

        assertThat(mapping).isPresent().get().satisfies(actual -> {
            assertThat(actual.credentials().map(AwsCredentialsIdentity::accessKeyId)).isEqualTo(expected.accessKey());
            assertThat(actual.credentials().map(AwsCredentialsIdentity::secretAccessKey)).isEqualTo(expected.secretKey());
            assertThat(actual.iamRole()).isEqualTo(expected.iamRole());
            assertThat(actual.roleSessionName()).isEqualTo(expected.roleSessionName());
            assertThat(actual.kmsKeyId()).isEqualTo(expected.kmsKeyId());
            assertThat(actual.sseCustomerKey()).isEqualTo(expected.sseCustomerKey());
            assertThat(actual.endpoint()).isEqualTo(expected.endpoint());
            assertThat(actual.region()).isEqualTo(expected.region());
        });
    }

    private static void assertMappingFails(S3SecurityMappingProvider provider, MappingSelector selector, String message)
    {
        assertThatThrownBy(() -> getMapping(provider, selector))
                .isInstanceOf(AccessDeniedException.class)
                .hasMessage("Access Denied: " + message);
    }

    private static Optional<S3SecurityMappingResult> getMapping(S3SecurityMappingProvider provider, MappingSelector selector)
    {
        return provider.getMapping(selector.identity(), selector.location());
    }

    public static class MappingSelector
    {
        public static MappingSelector empty()
        {
            return path(DEFAULT_PATH);
        }

        public static MappingSelector path(String location)
        {
            return new MappingSelector(DEFAULT_USER, ImmutableSet.of(), Location.of(location), Optional.empty(), Optional.empty(), Optional.empty());
        }

        private final String user;
        private final Set<String> groups;
        private final Location location;
        private final Optional<String> extraCredentialIamRole;
        private final Optional<String> extraCredentialKmsKeyId;
        private final Optional<String> extraCredentialCustomerKey;

        private MappingSelector(String user, Set<String> groups, Location location, Optional<String> extraCredentialIamRole, Optional<String> extraCredentialKmsKeyId, Optional<String> extraCredentialCustomerKey)
        {
            this.user = requireNonNull(user, "user is null");
            this.groups = ImmutableSet.copyOf(requireNonNull(groups, "groups is null"));
            this.location = requireNonNull(location, "location is null");
            this.extraCredentialIamRole = requireNonNull(extraCredentialIamRole, "extraCredentialIamRole is null");
            this.extraCredentialKmsKeyId = requireNonNull(extraCredentialKmsKeyId, "extraCredentialKmsKeyId is null");
            this.extraCredentialCustomerKey = requireNonNull(extraCredentialCustomerKey, "extraCredentialCustomerKey is null");
        }

        public Location location()
        {
            return location;
        }

        public MappingSelector withExtraCredentialIamRole(String role)
        {
            return new MappingSelector(user, groups, location, Optional.of(role), extraCredentialKmsKeyId, extraCredentialCustomerKey);
        }

        public MappingSelector withExtraCredentialKmsKeyId(String kmsKeyId)
        {
            return new MappingSelector(user, groups, location, extraCredentialIamRole, Optional.of(kmsKeyId), Optional.empty());
        }

        public MappingSelector withExtraCredentialCustomerKey(String customerKey)
        {
            return new MappingSelector(user, groups, location, extraCredentialIamRole, Optional.empty(), Optional.of(customerKey));
        }

        public MappingSelector withUser(String user)
        {
            return new MappingSelector(user, groups, location, extraCredentialIamRole, extraCredentialKmsKeyId, extraCredentialCustomerKey);
        }

        public ConnectorIdentity identity()
        {
            Map<String, String> extraCredentials = new HashMap<>();
            extraCredentialIamRole.ifPresent(role -> extraCredentials.put(IAM_ROLE_CREDENTIAL_NAME, role));
            extraCredentialKmsKeyId.ifPresent(kmsKeyId -> extraCredentials.put(KMS_KEY_ID_CREDENTIAL_NAME, kmsKeyId));
            extraCredentialCustomerKey.ifPresent(customerKey -> extraCredentials.put(CUSTOMER_KEY_CREDENTIAL_NAME, customerKey));

            return ConnectorIdentity.forUser(user)
                    .withGroups(groups)
                    .withExtraCredentials(extraCredentials)
                    .build();
        }
    }

    public static class MappingResult
    {
        public static MappingResult credentials(String accessKey, String secretKey)
        {
            return new MappingResult(
                    Optional.of(accessKey),
                    Optional.of(secretKey),
                    Optional.empty(),
                    Optional.empty(),
                    Optional.empty(),
                    Optional.empty(),
                    Optional.empty(),
                    Optional.empty());
        }

        public static MappingResult iamRole(String role)
        {
            return new MappingResult(
                    Optional.empty(),
                    Optional.empty(),
                    Optional.of(role),
                    Optional.empty(),
                    Optional.empty(),
                    Optional.empty(),
                    Optional.empty(),
                    Optional.empty());
        }

        private final Optional<String> accessKey;
        private final Optional<String> secretKey;
        private final Optional<String> iamRole;
        private final Optional<String> roleSessionName;
        private final Optional<String> kmsKeyId;
        private final Optional<String> sseCustomerKey;
        private final Optional<String> endpoint;
        private final Optional<String> region;

        private MappingResult(
                Optional<String> accessKey,
                Optional<String> secretKey,
                Optional<String> iamRole,
                Optional<String> roleSessionName,
                Optional<String> kmsKeyId,
                Optional<String> sseCustomerKey,
                Optional<String> endpoint,
                Optional<String> region)
        {
            this.accessKey = requireNonNull(accessKey, "accessKey is null");
            this.secretKey = requireNonNull(secretKey, "secretKey is null");
            this.iamRole = requireNonNull(iamRole, "role is null");
            this.kmsKeyId = requireNonNull(kmsKeyId, "kmsKeyId is null");
            this.sseCustomerKey = requireNonNull(sseCustomerKey, "sseCustomerKey is null");
            this.endpoint = requireNonNull(endpoint, "endpoint is null");
            this.roleSessionName = requireNonNull(roleSessionName, "roleSessionName is null");
            this.region = requireNonNull(region, "region is null");
        }

        public MappingResult withKmsKeyId(String kmsKeyId)
        {
            return new MappingResult(accessKey, secretKey, iamRole, Optional.empty(), Optional.of(kmsKeyId), Optional.empty(), endpoint, region);
        }

        public MappingResult withSseCustomerKey(String customerKey)
        {
            return new MappingResult(accessKey, secretKey, iamRole, Optional.empty(), Optional.empty(), Optional.of(customerKey), endpoint, region);
        }

        public Optional<String> accessKey()
        {
            return accessKey;
        }

        public Optional<String> secretKey()
        {
            return secretKey;
        }

        public Optional<String> iamRole()
        {
            return iamRole;
        }

        public Optional<String> roleSessionName()
        {
            return roleSessionName;
        }

        public Optional<String> kmsKeyId()
        {
            return kmsKeyId;
        }

        public Optional<String> sseCustomerKey()
        {
            return sseCustomerKey;
        }

        public Optional<String> endpoint()
        {
            return endpoint;
        }

        public Optional<String> region()
        {
            return region;
        }
    }
}
