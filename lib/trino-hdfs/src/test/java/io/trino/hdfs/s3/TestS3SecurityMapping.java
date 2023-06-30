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
package io.trino.hdfs.s3;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.trino.hdfs.DynamicConfigurationProvider;
import io.trino.hdfs.HdfsContext;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.security.AccessDeniedException;
import io.trino.spi.security.ConnectorIdentity;
import io.trino.testing.TestingConnectorSession;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.testng.annotations.Test;

import java.util.Optional;
import java.util.Set;

import static com.google.common.io.Resources.getResource;
import static io.trino.hadoop.ConfigurationInstantiator.newEmptyConfiguration;
import static io.trino.hdfs.s3.TestS3SecurityMapping.MappingResult.clusterDefaultRole;
import static io.trino.hdfs.s3.TestS3SecurityMapping.MappingResult.credentials;
import static io.trino.hdfs.s3.TestS3SecurityMapping.MappingResult.role;
import static io.trino.hdfs.s3.TestS3SecurityMapping.MappingSelector.empty;
import static io.trino.hdfs.s3.TestS3SecurityMapping.MappingSelector.path;
import static io.trino.hdfs.s3.TrinoS3FileSystem.S3_ACCESS_KEY;
import static io.trino.hdfs.s3.TrinoS3FileSystem.S3_ENDPOINT;
import static io.trino.hdfs.s3.TrinoS3FileSystem.S3_IAM_ROLE;
import static io.trino.hdfs.s3.TrinoS3FileSystem.S3_KMS_KEY_ID;
import static io.trino.hdfs.s3.TrinoS3FileSystem.S3_ROLE_SESSION_NAME;
import static io.trino.hdfs.s3.TrinoS3FileSystem.S3_SECRET_KEY;
import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;

public class TestS3SecurityMapping
{
    private static final String IAM_ROLE_CREDENTIAL_NAME = "IAM_ROLE_CREDENTIAL_NAME";
    private static final String KMS_KEY_ID_CREDENTIAL_NAME = "KMS_KEY_ID_CREDENTIAL_NAME";
    private static final String DEFAULT_PATH = "s3://default";
    private static final String DEFAULT_USER = "testuser";

    @Test
    public void testMapping()
    {
        S3SecurityMappingConfig mappingConfig = new S3SecurityMappingConfig()
                .setConfigFilePath(getResource(getClass(), "security-mapping.json").getPath())
                .setRoleCredentialName(IAM_ROLE_CREDENTIAL_NAME)
                .setKmsKeyIdCredentialName(KMS_KEY_ID_CREDENTIAL_NAME)
                .setColonReplacement("#");

        DynamicConfigurationProvider provider = new S3SecurityMappingConfigurationProvider(mappingConfig,
                new FileBasedS3SecurityMappingsProvider(mappingConfig));

        // matches prefix -- mapping provides credentials
        assertMapping(
                provider,
                path("s3://foo/data/test.csv"),
                credentials("AKIAxxxaccess", "iXbXxxxsecret")
                        .withKmsKeyId("kmsKey_10"));

        // matches prefix exactly -- mapping provides credentials
        assertMapping(
                provider,
                path("s3://foo"),
                credentials("AKIAxxxaccess", "iXbXxxxsecret")
                        .withKmsKeyId("kmsKey_10"));

        // matches prefix exactly -- mapping provides credentials, kms key from extra credentials matching default
        assertMapping(
                provider,
                path("s3://foo").withExtraCredentialKmsKeyId("kmsKey_10"),
                credentials("AKIAxxxaccess", "iXbXxxxsecret")
                        .withKmsKeyId("kmsKey_10"));

        // matches prefix exactly -- mapping provides credentials, kms key from extra credentials, allowed, different than default
        assertMapping(
                provider,
                path("s3://foo").withExtraCredentialKmsKeyId("kmsKey_11"),
                credentials("AKIAxxxaccess", "iXbXxxxsecret")
                        .withKmsKeyId("kmsKey_11"));

        // matches prefix exactly -- mapping provides credentials, kms key from extra credentials, not allowed
        assertMappingFails(
                provider,
                path("s3://foo").withExtraCredentialKmsKeyId("kmsKey_not_allowed"),
                "Selected KMS Key ID is not allowed");

        // matches prefix exactly -- mapping provides credentials, kms key from extra credentials, all keys are allowed, different than default
        assertMapping(
                provider,
                path("s3://foo_all_keys_allowed").withExtraCredentialKmsKeyId("kmsKey_777"),
                credentials("AKIAxxxaccess", "iXbXxxxsecret")
                        .withKmsKeyId("kmsKey_777"));

        // matches prefix exactly -- mapping provides credentials, kms key from extra credentials, allowed, no default key
        assertMapping(
                provider,
                path("s3://foo_no_default_key").withExtraCredentialKmsKeyId("kmsKey_12"),
                credentials("AKIAxxxaccess", "iXbXxxxsecret")
                        .withKmsKeyId("kmsKey_12"));

        // no role selected and mapping has no default role
        assertMappingFails(
                provider,
                path("s3://bar/test"),
                "No S3 role selected and mapping has no default role");

        // matches prefix and user selected one of allowed roles
        assertMapping(
                provider,
                path("s3://bar/test").withExtraCredentialIamRole("arn:aws:iam::123456789101:role/allow_bucket_2"),
                role("arn:aws:iam::123456789101:role/allow_bucket_2"));

        // user selected role not in allowed list
        assertMappingFails(
                provider,
                path("s3://bar/test").withUser("bob").withExtraCredentialIamRole("bogus"),
                "Selected S3 role is not allowed: bogus");

        // verify that colon replacement works
        String roleWithoutColon = "arn#aws#iam##123456789101#role/allow_bucket_2";
        assertThat(roleWithoutColon).doesNotContain(":");
        assertMapping(
                provider,
                path("s3://bar/test").withExtraCredentialIamRole(roleWithoutColon),
                role("arn:aws:iam::123456789101:role/allow_bucket_2"));

        // matches prefix -- default role used
        assertMapping(
                provider,
                path("s3://bar/abc/data/test.csv"),
                role("arn:aws:iam::123456789101:role/allow_path"));

        // matches empty rule at end -- default role used
        assertMapping(
                provider,
                empty(),
                role("arn:aws:iam::123456789101:role/default"));

        // matches prefix -- default role used
        assertMapping(
                provider,
                path("s3://xyz/default"),
                role("arn:aws:iam::123456789101:role/allow_default"));

        // matches prefix and user selected one of allowed roles
        assertMapping(
                provider,
                path("s3://xyz/foo").withExtraCredentialIamRole("arn:aws:iam::123456789101:role/allow_foo"),
                role("arn:aws:iam::123456789101:role/allow_foo"));

        // matches prefix and user selected one of allowed roles
        assertMapping(
                provider,
                path("s3://xyz/bar").withExtraCredentialIamRole("arn:aws:iam::123456789101:role/allow_bar"),
                role("arn:aws:iam::123456789101:role/allow_bar"));

        // matches user -- default role used
        assertMapping(
                provider,
                empty().withUser("alice"),
                role("alice_role"));

        // matches user and user selected default role
        assertMapping(
                provider,
                empty().withUser("alice").withExtraCredentialIamRole("alice_role"),
                role("alice_role"));

        // matches user and selected role not allowed
        assertMappingFails(
                provider,
                empty().withUser("alice").withExtraCredentialIamRole("bogus"),
                "Selected S3 role is not allowed: bogus");

        // verify that first matching rule is used
        // matches prefix earlier in file and selected role not allowed
        assertMappingFails(
                provider,
                path("s3://bar/test").withUser("alice").withExtraCredentialIamRole("alice_role"),
                "Selected S3 role is not allowed: alice_role");

        // matches user regex -- default role used
        assertMapping(
                provider,
                empty().withUser("bob"),
                role("bob_and_charlie_role"));

        // matches group -- default role used
        assertMapping(
                provider,
                empty().withGroups("finance"),
                role("finance_role"));

        // matches group regex -- default role used
        assertMapping(
                provider,
                empty().withGroups("eng"),
                role("hr_and_eng_group"));

        // verify that all constraints must match
        // matches user but not group -- uses empty mapping at end
        assertMapping(
                provider,
                empty().withUser("danny"),
                role("arn:aws:iam::123456789101:role/default"));

        // matches group but not user -- uses empty mapping at end
        assertMapping(
                provider,
                empty().withGroups("hq"),
                role("arn:aws:iam::123456789101:role/default"));

        // matches user and group
        assertMapping(
                provider,
                empty().withUser("danny").withGroups("hq"),
                role("danny_hq_role"));

        // matches prefix -- mapping provides credentials and endpoint
        assertMapping(
                provider,
                path("s3://endpointbucket/bar"),
                credentials("AKIAxxxaccess", "iXbXxxxsecret").withEndpoint("http://localhost:7753"));

        // matches role session name
        assertMapping(
                provider,
                path("s3://somebucket"),
                role("arn:aws:iam::1234567891012:role/default").withRoleSessionName("iam-trino-session"));
    }

    @Test
    public void testMappingWithFallbackToClusterDefault()
    {
        S3SecurityMappingConfig mappingConfig = new S3SecurityMappingConfig()
                .setConfigFilePath(getResource(getClass(), "security-mapping-with-fallback-to-cluster-default.json").getPath());

        DynamicConfigurationProvider provider = new S3SecurityMappingConfigurationProvider(mappingConfig,
                new FileBasedS3SecurityMappingsProvider(mappingConfig));

        // matches prefix - returns role from the mapping
        assertMapping(
                provider,
                path("s3://bar/abc/data/test.csv"),
                role("arn:aws:iam::123456789101:role/allow_path"));

        // doesn't match any rule except default rule at the end
        assertMapping(
                provider,
                empty(),
                clusterDefaultRole());
    }

    @Test
    public void testMappingWithoutFallback()
    {
        S3SecurityMappingConfig mappingConfig = new S3SecurityMappingConfig()
                .setConfigFilePath(getResource(getClass(), "security-mapping-without-fallback.json").getPath());

        DynamicConfigurationProvider provider = new S3SecurityMappingConfigurationProvider(mappingConfig,
                new FileBasedS3SecurityMappingsProvider(mappingConfig));

        // matches prefix - returns role from the mapping
        assertMapping(
                provider,
                path("s3://bar/abc/data/test.csv"),
                role("arn:aws:iam::123456789101:role/allow_path"));

        // doesn't match any rule
        assertMappingFails(
                provider,
                empty(),
                "No matching S3 security mapping");
    }

    @Test
    public void testMappingWithoutRoleCredentialsFallbackShouldFail()
    {
        assertThatThrownBy(() ->
                new S3SecurityMapping(Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("must either allow useClusterDefault role or provide role and/or credentials");
    }

    @Test
    public void testMappingWithRoleAndFallbackShouldFail()
    {
        Optional<String> iamRole = Optional.of("arn:aws:iam::123456789101:role/allow_path");
        Optional<Boolean> useClusterDefault = Optional.of(true);

        assertThatThrownBy(() ->
                new S3SecurityMapping(Optional.empty(), Optional.empty(), Optional.empty(), iamRole, Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty(), useClusterDefault, Optional.empty()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("must either allow useClusterDefault role or provide role and/or credentials");
    }

    @Test
    public void testMappingWithEncryptionKeysAndFallbackShouldFail()
    {
        Optional<Boolean> useClusterDefault = Optional.of(true);
        Optional<String> kmsKeyId = Optional.of("CLIENT_S3CRT_KEY_ID");

        assertThatThrownBy(() ->
                new S3SecurityMapping(Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty(), kmsKeyId, Optional.empty(), Optional.empty(), Optional.empty(), useClusterDefault, Optional.empty()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("KMS key ID cannot be provided together with useClusterDefault");
    }

    @Test
    public void testMappingWithRoleSessionNameWithoutIamRoleShouldFail()
    {
        Optional<String> roleSessionName = Optional.of("iam-trino-session");

        assertThatThrownBy(() ->
                new S3SecurityMapping(Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty(), roleSessionName, Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("iamRole must be provided when roleSessionName is provided");
    }

    private static void assertMapping(DynamicConfigurationProvider provider, MappingSelector selector, MappingResult mappingResult)
    {
        Configuration configuration = newEmptyConfiguration();

        assertNull(configuration.get(S3_ACCESS_KEY));
        assertNull(configuration.get(S3_SECRET_KEY));
        assertNull(configuration.get(S3_IAM_ROLE));
        assertNull(configuration.get(S3_KMS_KEY_ID));

        applyMapping(provider, selector, configuration);

        assertEquals(configuration.get(S3_ACCESS_KEY), mappingResult.getAccessKey().orElse(null));
        assertEquals(configuration.get(S3_SECRET_KEY), mappingResult.getSecretKey().orElse(null));
        assertEquals(configuration.get(S3_IAM_ROLE), mappingResult.getRole().orElse(null));
        assertEquals(configuration.get(S3_KMS_KEY_ID), mappingResult.getKmsKeyId().orElse(null));
        assertEquals(configuration.get(S3_ENDPOINT), mappingResult.getEndpoint().orElse(null));
        assertEquals(configuration.get(S3_ROLE_SESSION_NAME), mappingResult.getRoleSessionName().orElse(null));
    }

    private static void assertMappingFails(DynamicConfigurationProvider provider, MappingSelector selector, String message)
    {
        Configuration configuration = newEmptyConfiguration();

        assertThatThrownBy(() -> applyMapping(provider, selector, configuration))
                .isInstanceOf(AccessDeniedException.class)
                .hasMessage("Access Denied: " + message);
    }

    private static void applyMapping(DynamicConfigurationProvider provider, MappingSelector selector, Configuration configuration)
    {
        provider.updateConfiguration(configuration, selector.getHdfsContext(), selector.getPath().toUri());
    }

    public static class MappingSelector
    {
        public static MappingSelector empty()
        {
            return path(DEFAULT_PATH);
        }

        public static MappingSelector path(String path)
        {
            return new MappingSelector(DEFAULT_USER, ImmutableSet.of(), new Path(path), Optional.empty(), Optional.empty());
        }

        private final String user;
        private final Set<String> groups;
        private final Path path;
        private final Optional<String> extraCredentialIamRole;
        private final Optional<String> extraCredentialKmsKeyId;

        private MappingSelector(String user, Set<String> groups, Path path, Optional<String> extraCredentialIamRole, Optional<String> extraCredentialKmsKeyId)
        {
            this.user = requireNonNull(user, "user is null");
            this.groups = ImmutableSet.copyOf(requireNonNull(groups, "groups is null"));
            this.path = requireNonNull(path, "path is null");
            this.extraCredentialIamRole = requireNonNull(extraCredentialIamRole, "extraCredentialIamRole is null");
            this.extraCredentialKmsKeyId = requireNonNull(extraCredentialKmsKeyId, "extraCredentialKmsKeyId is null");
        }

        public Path getPath()
        {
            return path;
        }

        public MappingSelector withExtraCredentialIamRole(String role)
        {
            return new MappingSelector(user, groups, path, Optional.of(role), extraCredentialKmsKeyId);
        }

        public MappingSelector withExtraCredentialKmsKeyId(String kmsKeyId)
        {
            return new MappingSelector(user, groups, path, extraCredentialIamRole, Optional.of(kmsKeyId));
        }

        public MappingSelector withUser(String user)
        {
            return new MappingSelector(user, groups, path, extraCredentialIamRole, extraCredentialKmsKeyId);
        }

        public MappingSelector withGroups(String... groups)
        {
            return new MappingSelector(user, ImmutableSet.copyOf(groups), path, extraCredentialIamRole, extraCredentialKmsKeyId);
        }

        public HdfsContext getHdfsContext()
        {
            ImmutableMap.Builder<String, String> extraCredentials = ImmutableMap.builder();
            extraCredentialIamRole.ifPresent(role -> extraCredentials.put(IAM_ROLE_CREDENTIAL_NAME, role));
            extraCredentialKmsKeyId.ifPresent(kmsKeyId -> extraCredentials.put(KMS_KEY_ID_CREDENTIAL_NAME, kmsKeyId));

            ConnectorSession connectorSession = TestingConnectorSession.builder()
                    .setIdentity(ConnectorIdentity.forUser(user)
                            .withGroups(groups)
                            .withExtraCredentials(extraCredentials.buildOrThrow())
                            .build())
                    .build();
            return new HdfsContext(connectorSession);
        }
    }

    public static class MappingResult
    {
        public static MappingResult credentials(String accessKey, String secretKey)
        {
            return new MappingResult(Optional.of(accessKey), Optional.of(secretKey), Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty());
        }

        public static MappingResult role(String role)
        {
            return new MappingResult(Optional.empty(), Optional.empty(), Optional.of(role), Optional.empty(), Optional.empty(), Optional.empty());
        }

        public static MappingResult clusterDefaultRole()
        {
            return new MappingResult(Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty());
        }

        public static MappingResult endpoint(String endpoint)
        {
            return new MappingResult(Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty(), Optional.of(endpoint), Optional.empty());
        }

        private final Optional<String> accessKey;
        private final Optional<String> secretKey;
        private final Optional<String> role;
        private final Optional<String> kmsKeyId;
        private final Optional<String> endpoint;
        private final Optional<String> roleSessionName;

        private MappingResult(Optional<String> accessKey, Optional<String> secretKey, Optional<String> role, Optional<String> kmsKeyId, Optional<String> endpoint, Optional<String> roleSessionName)
        {
            this.accessKey = requireNonNull(accessKey, "accessKey is null");
            this.secretKey = requireNonNull(secretKey, "secretKey is null");
            this.role = requireNonNull(role, "role is null");
            this.kmsKeyId = requireNonNull(kmsKeyId, "kmsKeyId is null");
            this.endpoint = requireNonNull(endpoint, "endpoint is null");
            this.roleSessionName = requireNonNull(roleSessionName, "roleSessionName is null");
        }

        public MappingResult withEndpoint(String endpoint)
        {
            return new MappingResult(accessKey, secretKey, role, kmsKeyId, Optional.of(endpoint), Optional.empty());
        }

        public MappingResult withKmsKeyId(String kmsKeyId)
        {
            return new MappingResult(accessKey, secretKey, role, Optional.of(kmsKeyId), endpoint, Optional.empty());
        }

        public MappingResult withRoleSessionName(String roleSessionName)
        {
            return new MappingResult(accessKey, secretKey, role, kmsKeyId, Optional.empty(), Optional.of(roleSessionName));
        }

        public Optional<String> getAccessKey()
        {
            return accessKey;
        }

        public Optional<String> getSecretKey()
        {
            return secretKey;
        }

        public Optional<String> getRole()
        {
            return role;
        }

        public Optional<String> getKmsKeyId()
        {
            return kmsKeyId;
        }

        public Optional<String> getEndpoint()
        {
            return endpoint;
        }

        public Optional<String> getRoleSessionName()
        {
            return roleSessionName;
        }
    }
}
