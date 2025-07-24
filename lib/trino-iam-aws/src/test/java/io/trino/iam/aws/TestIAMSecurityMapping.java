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
package io.trino.iam.aws;

import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.collect.ImmutableSet;
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
import static io.trino.iam.aws.TestIAMSecurityMapping.MappingResult.credentials;
import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestIAMSecurityMapping
{
    private static final String IAM_ROLE_CREDENTIAL_NAME = "IAM_ROLE_CREDENTIAL_NAME";
    private static final String DEFAULT_USER = "testuser";

    @Test
    public void testMapping()
    {
        IAMSecurityMappingConfig mappingConfig = new IAMSecurityMappingConfig()
                .setConfigFileInternal(getResourceFile("security-mapping.json"))
                .setRoleCredentialNameInternal(IAM_ROLE_CREDENTIAL_NAME)
                .setColonReplacementInternal("#");
        TypeReference<IAMSecurityMappings<IAMSecurityMapping>> typeRef = new TypeReference<>() {};
        var provider = new IAMSecurityMappingProvider<>(mappingConfig, new IAMSecurityMappingsFileSource<>(mappingConfig, typeRef));

        // matches user - mapping provides credentials
        assertMapping(
                provider,
                MappingSelector.empty()
                        .withUser("jane"),
                credentials("AKIAxxxaccess", "iXbXxxxsecret"));

        // matches user, no role selected and mapping has no default role
        assertMappingFails(
                provider,
                MappingSelector.empty()
                        .withUser("john"),
                "No IAM role selected and mapping has no default role");

        // matches user and selected one of the allowed roles
        assertMapping(
                provider,
                MappingSelector.empty()
                        .withUser("john")
                        .withExtraCredentialIamRole("arn:aws:iam::123456789101:role/role_a"),
                MappingResult.iamRole("arn:aws:iam::123456789101:role/role_a"));

        // matches user and selected one of the allowed roles
        assertMapping(
                provider,
                MappingSelector.empty()
                        .withUser("john")
                        .withExtraCredentialIamRole("arn:aws:iam::123456789101:role/role_b"),
                MappingResult.iamRole("arn:aws:iam::123456789101:role/role_b"));

        // user selected a role not in allowed list
        assertMappingFails(
                provider,
                MappingSelector.empty()
                        .withUser("john")
                        .withExtraCredentialIamRole("bogus"),
                "Selected IAM role is not allowed: bogus");

        // verify that colon replacement works
        String roleWithoutColon = "arn#aws#iam##123456789101#role/role_b";
        assertThat(roleWithoutColon).doesNotContain(":");
        assertMapping(
                provider,
                MappingSelector.empty()
                        .withUser("john")
                        .withExtraCredentialIamRole(roleWithoutColon),
                MappingResult.iamRole("arn:aws:iam::123456789101:role/role_b"));

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

        // verify that the first matching rule is used
        // matches prefix earlier in the file and selected role not allowed
        assertMappingFails(
                provider,
                        MappingSelector.empty()
                        .withUser("alice")
                        .withExtraCredentialIamRole("alice_other_role"),
                "Selected IAM role is not allowed: alice_other_role");

        // matches empty rule at the end -- default role used
        assertMapping(
                provider,
                MappingSelector.empty(),
                MappingResult.iamRole("arn:aws:iam::123456789101:role/default"));

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

        // matches user -- mapping provides credentials and endpoint
        assertMapping(
                provider,
                MappingSelector.empty()
                        .withUser("ryan"),
                credentials("AKIAxxxaccess", "iXbXxxxsecret")
                        .withEndpoint("http://localhost:7753"));

        // matches user -- mapping provides credentials and region
        assertMapping(
                provider,
                MappingSelector.empty()
                        .withUser("emily"),
                credentials("AKIAxxxaccess", "iXbXxxxsecret")
                        .withRegion("us-west-2"));

        // matches role session name
        assertMapping(
                provider,
                MappingSelector.empty()
                        .withUser("jack"),
                MappingResult.iamRole("arn:aws:iam::1234567891012:role/default")
                        .withRoleSessionName("iam-trino-session"));
    }

    @Test
    public void testMappingWithFallbackToClusterDefault()
    {
        IAMSecurityMappingConfig mappingConfig = new IAMSecurityMappingConfig()
                .setConfigFileInternal(getResourceFile("security-mapping-with-fallback-to-cluster-default.json"));

        TypeReference<IAMSecurityMappings<IAMSecurityMapping>> typeRef = new TypeReference<>() {};
        var provider = new IAMSecurityMappingProvider<>(mappingConfig, new IAMSecurityMappingsFileSource<>(mappingConfig, typeRef));

        // matches user -- uses the role from the mapping
        assertMapping(
                provider,
                MappingSelector.empty(),
                MappingResult.iamRole("arn:aws:iam::123456789101:role/example"));

        // doesn't match any rule except default rule at the end
        assertThat(getMapping(provider, MappingSelector.empty().withUser("fake"))).isEmpty();
    }

    @Test
    public void testMappingWithoutFallback()
    {
        IAMSecurityMappingConfig mappingConfig = new IAMSecurityMappingConfig()
                .setConfigFileInternal(getResourceFile("security-mapping-without-fallback.json"));

        TypeReference<IAMSecurityMappings<IAMSecurityMapping>> typeRef = new TypeReference<>() {};
        var provider = new IAMSecurityMappingProvider<>(mappingConfig, new IAMSecurityMappingsFileSource<>(mappingConfig, typeRef));

        // matches user - return role from the mapping
        assertMapping(
                provider,
                MappingSelector.empty()
                        .withUser("non-default"),
                MappingResult.iamRole("arn:aws:iam::123456789101:role/example"));

        // doesn't match any rule
        assertMappingFails(
                provider,
                MappingSelector.empty(),
                "No matching IAM security mapping");
    }

    @Test
    public void testMappingWithoutRoleCredentialsFallbackShouldFail()
    {
        assertThatThrownBy(() ->
                new IAMSecurityMapping(
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
                new IAMSecurityMapping(
                        Optional.empty(),
                        Optional.empty(),
                        iamRole,
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
    public void testMappingWithRoleSessionNameWithoutIamRoleShouldFail()
    {
        Optional<String> roleSessionName = Optional.of("iam-trino-session");

        assertThatThrownBy(() ->
                new IAMSecurityMapping(
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty(),
                        roleSessionName,
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

    private static void assertMapping(IAMSecurityMappingProvider<IAMSecurityMappings<IAMSecurityMapping>, IAMSecurityMapping, IAMSecurityMappingConfig> provider, MappingSelector selector, MappingResult expected)
    {
        Optional<IAMSecurityMappingResult> mapping = getMapping(provider, selector);

        assertThat(mapping).isPresent().get().satisfies(actual -> {
            assertThat(actual.credentials().map(AwsCredentialsIdentity::accessKeyId)).isEqualTo(expected.accessKey());
            assertThat(actual.credentials().map(AwsCredentialsIdentity::secretAccessKey)).isEqualTo(expected.secretKey());
            assertThat(actual.iamRole()).isEqualTo(expected.iamRole());
            assertThat(actual.roleSessionName()).isEqualTo(expected.roleSessionName());
            assertThat(actual.endpoint()).isEqualTo(expected.endpoint());
            assertThat(actual.region()).isEqualTo(expected.region());
        });
    }

    private static void assertMappingFails(IAMSecurityMappingProvider<IAMSecurityMappings<IAMSecurityMapping>, IAMSecurityMapping, IAMSecurityMappingConfig> provider, MappingSelector selector, String message)
    {
        assertThatThrownBy(() -> getMapping(provider, selector))
                .isInstanceOf(AccessDeniedException.class)
                .hasMessage("Access Denied: " + message);
    }

    private static Optional<IAMSecurityMappingResult> getMapping(IAMSecurityMappingProvider<IAMSecurityMappings<IAMSecurityMapping>, IAMSecurityMapping, IAMSecurityMappingConfig> provider, MappingSelector selector)
    {
        return provider.getMapping(selector.identity());
    }

    public static class MappingSelector
    {
        public static MappingSelector empty()
        {
            return new MappingSelector(DEFAULT_USER, ImmutableSet.of(), Optional.empty());
        }

        private final String user;
        private final Set<String> groups;
        private final Optional<String> extraCredentialIamRole;

        private MappingSelector(String user, Set<String> groups, Optional<String> extraCredentialIamRole)
        {
            this.user = requireNonNull(user, "user is null");
            this.groups = ImmutableSet.copyOf(requireNonNull(groups, "groups is null"));
            this.extraCredentialIamRole = requireNonNull(extraCredentialIamRole, "extraCredentialIamRole is null");
        }

        public MappingSelector withExtraCredentialIamRole(String role)
        {
            return new MappingSelector(user, groups, Optional.of(role));
        }

        public MappingSelector withUser(String user)
        {
            return new MappingSelector(user, groups, extraCredentialIamRole);
        }

        public MappingSelector withGroups(String... groups)
        {
            return new MappingSelector(user, ImmutableSet.copyOf(groups), extraCredentialIamRole);
        }

        public ConnectorIdentity identity()
        {
            Map<String, String> extraCredentials = new HashMap<>();
            extraCredentialIamRole.ifPresent(role -> extraCredentials.put(IAM_ROLE_CREDENTIAL_NAME, role));

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

        public MappingResult withEndpoint(String endpoint)
        {
            return new MappingResult(accessKey, secretKey, iamRole, Optional.empty(), kmsKeyId, sseCustomerKey, Optional.of(endpoint), region);
        }

        public MappingResult withRegion(String region)
        {
            return new MappingResult(accessKey, secretKey, iamRole, Optional.empty(), kmsKeyId, sseCustomerKey, endpoint, Optional.of(region));
        }

        public MappingResult withRoleSessionName(String roleSessionName)
        {
            return new MappingResult(accessKey, secretKey, iamRole, Optional.of(roleSessionName), kmsKeyId, sseCustomerKey, Optional.empty(), region);
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
