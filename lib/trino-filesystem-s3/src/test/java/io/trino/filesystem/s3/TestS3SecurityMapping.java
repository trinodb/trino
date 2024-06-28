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
    private static final String DEFAULT_PATH = "s3://default/";
    private static final String DEFAULT_USER = "testuser";

    @Test
    public void testMapping()
    {
        S3SecurityMappingConfig mappingConfig = new S3SecurityMappingConfig()
                .setConfigFile(getResourceFile("security-mapping.json"))
                .setRoleCredentialName(IAM_ROLE_CREDENTIAL_NAME)
                .setKmsKeyIdCredentialName(KMS_KEY_ID_CREDENTIAL_NAME)
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

        // no role selected and mapping has no default role
        assertMappingFails(
                provider,
                path("s3://bar/test"),
                "No S3 role selected and mapping has no default role");

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
                "Selected S3 role is not allowed: bogus");

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

        // matches empty rule at the end -- default role used
        assertMapping(
                provider,
                MappingSelector.empty(),
                MappingResult.iamRole("arn:aws:iam::123456789101:role/default"));

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

        // matches user -- default role used
        assertMapping(
                provider,
                MappingSelector.empty()
                        .withUser("alice"),
                MappingResult.iamRole("alice_role"));

        // matches user and user selected default role
        assertMapping(
                provider,
                MappingSelector.empty()
                        .withUser("alice")
                        .withExtraCredentialIamRole("alice_role"),
                MappingResult.iamRole("alice_role"));

        // matches user and selected role not allowed
        assertMappingFails(
                provider,
                MappingSelector.empty()
                        .withUser("alice")
                        .withExtraCredentialIamRole("bogus"),
                "Selected S3 role is not allowed: bogus");

        // verify that the first matching rule is used
        // matches prefix earlier in the file and selected role not allowed
        assertMappingFails(
                provider,
                path("s3://bar/test")
                        .withUser("alice")
                        .withExtraCredentialIamRole("alice_role"),
                "Selected S3 role is not allowed: alice_role");

        // matches user regex -- default role used
        assertMapping(
                provider,
                MappingSelector.empty()
                        .withUser("bob"),
                MappingResult.iamRole("bob_and_charlie_role"));

        // matches group -- default role used
        assertMapping(
                provider,
                MappingSelector.empty()
                        .withGroups("finance"),
                MappingResult.iamRole("finance_role"));

        // matches group regex -- default role used
        assertMapping(
                provider,
                MappingSelector.empty()
                        .withGroups("eng"),
                MappingResult.iamRole("hr_and_eng_group"));

        // verify that all constraints must match
        // matches user but not group -- uses empty mapping at the end
        assertMapping(
                provider,
                MappingSelector.empty()
                        .withUser("danny"),
                MappingResult.iamRole("arn:aws:iam::123456789101:role/default"));

        // matches group but not user -- uses empty mapping at the end
        assertMapping(
                provider,
                MappingSelector.empty()
                        .withGroups("hq"),
                MappingResult.iamRole("arn:aws:iam::123456789101:role/default"));

        // matches user and group
        assertMapping(
                provider,
                MappingSelector.empty()
                        .withUser("danny")
                        .withGroups("hq"),
                MappingResult.iamRole("danny_hq_role"));

        // matches prefix -- mapping provides credentials and endpoint
        assertMapping(
                provider,
                path("s3://endpointbucket/bar"),
                credentials("AKIAxxxaccess", "iXbXxxxsecret")
                        .withEndpoint("http://localhost:7753"));

        // matches prefix -- mapping provides credentials and region
        assertMapping(
                provider,
                path("s3://regionalbucket/bar"),
                credentials("AKIAxxxaccess", "iXbXxxxsecret")
                        .withRegion("us-west-2"));

        // matches role session name
        assertMapping(
                provider,
                path("s3://somebucket/"),
                MappingResult.iamRole("arn:aws:iam::1234567891012:role/default")
                        .withRoleSessionName("iam-trino-session"));
    }

    @Test
    public void testMappingWithFallbackToClusterDefault()
    {
        S3SecurityMappingConfig mappingConfig = new S3SecurityMappingConfig()
                .setConfigFile(getResourceFile("security-mapping-with-fallback-to-cluster-default.json"));

        var provider = new S3SecurityMappingProvider(mappingConfig, new S3SecurityMappingsFileSource(mappingConfig));

        // matches prefix -- uses the role from the mapping
        assertMapping(
                provider,
                path("s3://bar/abc/data/test.csv"),
                MappingResult.iamRole("arn:aws:iam::123456789101:role/allow_path"));

        // doesn't match any rule except default rule at the end
        assertThat(getMapping(provider, MappingSelector.empty())).isEmpty();
    }

    @Test
    public void testMappingWithoutFallback()
    {
        S3SecurityMappingConfig mappingConfig = new S3SecurityMappingConfig()
                .setConfigFile(getResourceFile("security-mapping-without-fallback.json"));

        var provider = new S3SecurityMappingProvider(mappingConfig, new S3SecurityMappingsFileSource(mappingConfig));

        // matches prefix - return role from the mapping
        assertMapping(
                provider,
                path("s3://bar/abc/data/test.csv"),
                MappingResult.iamRole("arn:aws:iam::123456789101:role/allow_path"));

        // doesn't match any rule
        assertMappingFails(
                provider,
                MappingSelector.empty(),
                "No matching S3 security mapping");
    }

    @Test
    public void testMappingWithoutRoleCredentialsFallbackShouldFail()
    {
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
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("must either allow useClusterDefault role or provide role and/or credentials");
    }

    @Test
    public void testMappingWithRoleAndFallbackShouldFail()
    {
        Optional<String> iamRole = Optional.of("arn:aws:iam::123456789101:role/allow_path");
        Optional<Boolean> useClusterDefault = Optional.of(true);

        assertThatThrownBy(() ->
                new S3SecurityMapping(
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty(),
                        iamRole,
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty(),
                        useClusterDefault,
                        Optional.empty(),
                        Optional.empty()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("must either allow useClusterDefault role or provide role and/or credentials");
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
                        useClusterDefault,
                        Optional.empty(),
                        Optional.empty()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("KMS key ID cannot be provided together with useClusterDefault");
    }

    @Test
    public void testMappingWithRoleSessionNameWithoutIamRoleShouldFail()
    {
        Optional<String> roleSessionName = Optional.of("iam-trino-session");

        assertThatThrownBy(() ->
                new S3SecurityMapping(
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty(),
                        roleSessionName,
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("iamRole must be provided when roleSessionName is provided");
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
            return new MappingSelector(DEFAULT_USER, ImmutableSet.of(), Location.of(location), Optional.empty(), Optional.empty());
        }

        private final String user;
        private final Set<String> groups;
        private final Location location;
        private final Optional<String> extraCredentialIamRole;
        private final Optional<String> extraCredentialKmsKeyId;

        private MappingSelector(String user, Set<String> groups, Location location, Optional<String> extraCredentialIamRole, Optional<String> extraCredentialKmsKeyId)
        {
            this.user = requireNonNull(user, "user is null");
            this.groups = ImmutableSet.copyOf(requireNonNull(groups, "groups is null"));
            this.location = requireNonNull(location, "location is null");
            this.extraCredentialIamRole = requireNonNull(extraCredentialIamRole, "extraCredentialIamRole is null");
            this.extraCredentialKmsKeyId = requireNonNull(extraCredentialKmsKeyId, "extraCredentialKmsKeyId is null");
        }

        public Location location()
        {
            return location;
        }

        public MappingSelector withExtraCredentialIamRole(String role)
        {
            return new MappingSelector(user, groups, location, Optional.of(role), extraCredentialKmsKeyId);
        }

        public MappingSelector withExtraCredentialKmsKeyId(String kmsKeyId)
        {
            return new MappingSelector(user, groups, location, extraCredentialIamRole, Optional.of(kmsKeyId));
        }

        public MappingSelector withUser(String user)
        {
            return new MappingSelector(user, groups, location, extraCredentialIamRole, extraCredentialKmsKeyId);
        }

        public MappingSelector withGroups(String... groups)
        {
            return new MappingSelector(user, ImmutableSet.copyOf(groups), location, extraCredentialIamRole, extraCredentialKmsKeyId);
        }

        public ConnectorIdentity identity()
        {
            Map<String, String> extraCredentials = new HashMap<>();
            extraCredentialIamRole.ifPresent(role -> extraCredentials.put(IAM_ROLE_CREDENTIAL_NAME, role));
            extraCredentialKmsKeyId.ifPresent(kmsKeyId -> extraCredentials.put(KMS_KEY_ID_CREDENTIAL_NAME, kmsKeyId));

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
                    Optional.empty());
        }

        private final Optional<String> accessKey;
        private final Optional<String> secretKey;
        private final Optional<String> iamRole;
        private final Optional<String> roleSessionName;
        private final Optional<String> kmsKeyId;
        private final Optional<String> endpoint;
        private final Optional<String> region;

        private MappingResult(
                Optional<String> accessKey,
                Optional<String> secretKey,
                Optional<String> iamRole,
                Optional<String> roleSessionName,
                Optional<String> kmsKeyId,
                Optional<String> endpoint,
                Optional<String> region)
        {
            this.accessKey = requireNonNull(accessKey, "accessKey is null");
            this.secretKey = requireNonNull(secretKey, "secretKey is null");
            this.iamRole = requireNonNull(iamRole, "role is null");
            this.kmsKeyId = requireNonNull(kmsKeyId, "kmsKeyId is null");
            this.endpoint = requireNonNull(endpoint, "endpoint is null");
            this.roleSessionName = requireNonNull(roleSessionName, "roleSessionName is null");
            this.region = requireNonNull(region, "region is null");
        }

        public MappingResult withEndpoint(String endpoint)
        {
            return new MappingResult(accessKey, secretKey, iamRole, Optional.empty(), kmsKeyId, Optional.of(endpoint), region);
        }

        public MappingResult withKmsKeyId(String kmsKeyId)
        {
            return new MappingResult(accessKey, secretKey, iamRole, Optional.empty(), Optional.of(kmsKeyId), endpoint, region);
        }

        public MappingResult withRegion(String region)
        {
            return new MappingResult(accessKey, secretKey, iamRole, Optional.empty(), kmsKeyId, endpoint, Optional.of(region));
        }

        public MappingResult withRoleSessionName(String roleSessionName)
        {
            return new MappingResult(accessKey, secretKey, iamRole, Optional.of(roleSessionName), kmsKeyId, Optional.empty(), region);
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
