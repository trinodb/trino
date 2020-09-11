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
package io.prestosql.plugin.hive.s3;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.prestosql.plugin.hive.DynamicConfigurationProvider;
import io.prestosql.plugin.hive.HdfsEnvironment.HdfsContext;
import io.prestosql.plugin.hive.HiveConfig;
import io.prestosql.plugin.hive.HiveSessionProperties;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.security.AccessDeniedException;
import io.prestosql.spi.security.ConnectorIdentity;
import io.prestosql.testing.TestingConnectorSession;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.testng.annotations.Test;

import java.io.File;
import java.util.Optional;
import java.util.Set;

import static com.google.common.io.Resources.getResource;
import static io.prestosql.plugin.hive.HiveTestUtils.getHiveSessionProperties;
import static io.prestosql.plugin.hive.s3.PrestoS3FileSystem.S3_ACCESS_KEY;
import static io.prestosql.plugin.hive.s3.PrestoS3FileSystem.S3_IAM_ROLE;
import static io.prestosql.plugin.hive.s3.PrestoS3FileSystem.S3_SECRET_KEY;
import static io.prestosql.plugin.hive.s3.TestS3SecurityMapping.MappingResult.clusterDefaultRole;
import static io.prestosql.plugin.hive.s3.TestS3SecurityMapping.MappingResult.credentials;
import static io.prestosql.plugin.hive.s3.TestS3SecurityMapping.MappingResult.role;
import static io.prestosql.plugin.hive.s3.TestS3SecurityMapping.MappingSelector.empty;
import static io.prestosql.plugin.hive.s3.TestS3SecurityMapping.MappingSelector.path;
import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;

public class TestS3SecurityMapping
{
    private static final HiveSessionProperties HIVE_SESSION_PROPERTIES = getHiveSessionProperties(new HiveConfig());

    private static final String IAM_ROLE_CREDENTIAL_NAME = "IAM_ROLE_CREDENTIAL_NAME";
    private static final String DEFAULT_PATH = "s3://default";
    private static final String DEFAULT_USER = "testuser";

    @Test
    public void testMapping()
    {
        S3SecurityMappingConfig mappingConfig = new S3SecurityMappingConfig()
                .setConfigFile(new File(getResource(getClass(), "security-mapping.json").getPath()))
                .setRoleCredentialName(IAM_ROLE_CREDENTIAL_NAME)
                .setColonReplacement("#");

        DynamicConfigurationProvider provider = new S3SecurityMappingConfigurationProvider(mappingConfig);

        // matches prefix -- mapping provides credentials
        assertMapping(
                provider,
                path("s3://foo/data/test.csv"),
                credentials("AKIAxxxaccess", "iXbXxxxsecret"));

        // matches prefix exactly -- mapping provides credentials
        assertMapping(
                provider,
                path("s3://foo"),
                credentials("AKIAxxxaccess", "iXbXxxxsecret"));

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
    }

    @Test
    public void testMappingWithFallbackToClusterDefault()
    {
        S3SecurityMappingConfig mappingConfig = new S3SecurityMappingConfig()
                .setConfigFile(new File(getResource(getClass(), "security-mapping-with-fallback-to-cluster-default.json").getPath()));

        DynamicConfigurationProvider provider = new S3SecurityMappingConfigurationProvider(mappingConfig);

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
                .setConfigFile(new File(getResource(getClass(), "security-mapping-without-fallback.json").getPath()));

        DynamicConfigurationProvider provider = new S3SecurityMappingConfigurationProvider(mappingConfig);

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
                new S3SecurityMapping(Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("must either allow useClusterDefault role or provide role and/or credentials");
    }

    @Test
    public void testMappingWithRoleAndFallbackShouldFail()
    {
        Optional<String> iamRole = Optional.of("arn:aws:iam::123456789101:role/allow_path");
        Optional<Boolean> useClusterDefault = Optional.of(true);

        assertThatThrownBy(() ->
                new S3SecurityMapping(Optional.empty(), Optional.empty(), Optional.empty(), iamRole, Optional.empty(), Optional.empty(), Optional.empty(), useClusterDefault))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("must either allow useClusterDefault role or provide role and/or credentials");
    }

    private static void assertMapping(DynamicConfigurationProvider provider, MappingSelector selector, MappingResult mappingResult)
    {
        Configuration configuration = new Configuration(false);

        assertNull(configuration.get(S3_ACCESS_KEY));
        assertNull(configuration.get(S3_SECRET_KEY));
        assertNull(configuration.get(S3_IAM_ROLE));

        applyMapping(provider, selector, configuration);

        assertEquals(configuration.get(S3_ACCESS_KEY), mappingResult.getAccessKey().orElse(null));
        assertEquals(configuration.get(S3_SECRET_KEY), mappingResult.getSecretKey().orElse(null));
        assertEquals(configuration.get(S3_IAM_ROLE), mappingResult.getRole().orElse(null));
    }

    private static void assertMappingFails(DynamicConfigurationProvider provider, MappingSelector selector, String message)
    {
        Configuration configuration = new Configuration(false);

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
            return new MappingSelector(DEFAULT_USER, ImmutableSet.of(), new Path(path), Optional.empty());
        }

        private final String user;
        private final Set<String> groups;
        private final Path path;
        private final Optional<String> extraCredentialIamRole;

        private MappingSelector(String user, Set<String> groups, Path path, Optional<String> extraCredentialIamRole)
        {
            this.user = requireNonNull(user, "user is null");
            this.groups = ImmutableSet.copyOf(requireNonNull(groups, "groups is null"));
            this.path = requireNonNull(path, "path is null");
            this.extraCredentialIamRole = requireNonNull(extraCredentialIamRole, "extraCredentialIamRole is null");
        }

        public Path getPath()
        {
            return path;
        }

        public MappingSelector withExtraCredentialIamRole(String role)
        {
            return new MappingSelector(user, groups, path, Optional.of(role));
        }

        public MappingSelector withUser(String user)
        {
            return new MappingSelector(user, groups, path, extraCredentialIamRole);
        }

        public MappingSelector withGroups(String... groups)
        {
            return new MappingSelector(user, ImmutableSet.copyOf(groups), path, extraCredentialIamRole);
        }

        public HdfsContext getHdfsContext()
        {
            ImmutableMap.Builder<String, String> extraCredentials = ImmutableMap.builder();
            extraCredentialIamRole.ifPresent(role -> extraCredentials.put(IAM_ROLE_CREDENTIAL_NAME, role));

            ConnectorSession connectorSession = TestingConnectorSession.builder()
                    .setIdentity(ConnectorIdentity.forUser(user)
                            .withGroups(groups)
                            .withExtraCredentials(extraCredentials.build())
                            .build())
                    .setPropertyMetadata(HIVE_SESSION_PROPERTIES.getSessionProperties())
                    .build();
            return new HdfsContext(connectorSession, "schema");
        }
    }

    public static class MappingResult
    {
        public static MappingResult credentials(String accessKey, String secretKey)
        {
            return new MappingResult(Optional.of(accessKey), Optional.of(secretKey), Optional.empty());
        }

        public static MappingResult role(String role)
        {
            return new MappingResult(Optional.empty(), Optional.empty(), Optional.of(role));
        }

        public static MappingResult clusterDefaultRole()
        {
            return new MappingResult(Optional.empty(), Optional.empty(), Optional.empty());
        }

        private final Optional<String> accessKey;
        private final Optional<String> secretKey;
        private final Optional<String> role;

        private MappingResult(Optional<String> accessKey, Optional<String> secretKey, Optional<String> role)
        {
            this.accessKey = requireNonNull(accessKey, "accessKey is null");
            this.secretKey = requireNonNull(secretKey, "secretKey is null");
            this.role = requireNonNull(role, "role is null");
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
    }
}
