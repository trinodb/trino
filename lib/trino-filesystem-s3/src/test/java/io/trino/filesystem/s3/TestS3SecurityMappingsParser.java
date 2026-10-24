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
import com.google.common.collect.ImmutableSet;
import io.airlift.configuration.secrets.SecretsResolver;
import io.trino.filesystem.Location;
import io.trino.spi.security.ConnectorIdentity;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.identity.spi.AwsCredentialsIdentity;

import static io.trino.filesystem.s3.S3SecurityMappingsParser.parseSecurityMappings;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestS3SecurityMappingsParser
{
    private static final S3Location LOCATION = new S3Location(Location.of("s3://my-bucket/test"));

    @Test
    public void testSecretWithJsonSpecialCharacters()
    {
        // Resolving this against the raw JSON text would produce invalid JSON or inject a mapping for 'bob'
        String secret = "quote\" backslash\\ newline\n }, {\"user\": \"bob\", \"iamRole\": \"arn:aws:iam::123456789101:role/injected\"";
        String json =
                """
                {"mappings": [{"user": "alice", "accessKey": "${TESTING:access}", "secretKey": "${TESTING:secret}"}]}
                """;
        SecretsResolver secretsResolver = new SecretsResolver(ImmutableMap.of("testing", key -> ImmutableMap.of("access", "AKIA", "secret", secret).get(key)));

        S3SecurityMappings mappings = parseSecurityMappings(json, "", secretsResolver);

        S3SecurityMapping mapping = mappings.getMapping(identity("alice"), LOCATION).orElseThrow();
        assertThat(mapping.credentials().map(AwsCredentialsIdentity::accessKeyId)).hasValue("AKIA");
        assertThat(mapping.credentials().map(AwsCredentialsIdentity::secretAccessKey)).hasValue(secret);
        assertThat(mappings.getMapping(identity("bob"), LOCATION)).isEmpty();
    }

    @Test
    public void testSecretsInNestedValues()
    {
        String json =
                """
                {"config": {"mappings": [{
                    "user": "alice",
                    "iamRole": "arn:aws:iam::123456789101:role/${TESTING:role}",
                    "allowedIamRoles": ["${TESTING:first}", "prefix-${TESTING:second}-suffix"],
                    "useClusterDefault": false
                }]}}
                """;
        SecretsResolver secretsResolver = new SecretsResolver(ImmutableMap.of("testing", key -> "resolved-" + key));

        S3SecurityMappings mappings = parseSecurityMappings(json, "/config", secretsResolver);

        S3SecurityMapping mapping = mappings.getMapping(identity("alice"), LOCATION).orElseThrow();
        assertThat(mapping.iamRole()).hasValue("arn:aws:iam::123456789101:role/resolved-role");
        assertThat(mapping.allowedIamRoles()).containsExactlyInAnyOrder("resolved-first", "prefix-resolved-second-suffix");
        assertThat(mapping.useClusterDefault()).isFalse();
    }

    @Test
    public void testValuesWithoutSecretsUnchanged()
    {
        String json =
                """
                {"mappings": [{"user": "alice", "iamRole": "arn:aws:iam::123456789101:role/plain"}]}
                """;

        S3SecurityMappings mappings = parseSecurityMappings(json, "", new SecretsResolver(ImmutableMap.of()));

        assertThat(mappings.getMapping(identity("alice"), LOCATION).orElseThrow().iamRole())
                .hasValue("arn:aws:iam::123456789101:role/plain");
    }

    @Test
    public void testUnknownSecretProvider()
    {
        String json =
                """
                {"mappings": [{"user": "alice", "iamRole": "${UNKNOWN:role}"}]}
                """;

        assertThatThrownBy(() -> parseSecurityMappings(json, "", new SecretsResolver(ImmutableMap.of())))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Failed to resolve secret in security mapping at '/mappings/0/iamRole': No secret provider for key 'unknown'");
    }

    private static ConnectorIdentity identity(String user)
    {
        return ConnectorIdentity.forUser(user).withGroups(ImmutableSet.of()).build();
    }
}
