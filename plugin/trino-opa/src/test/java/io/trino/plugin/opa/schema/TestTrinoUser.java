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
package io.trino.plugin.opa.schema;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.airlift.json.JsonCodec;
import io.trino.spi.security.Identity;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static io.airlift.json.JsonCodec.jsonCodec;
import static io.airlift.json.JsonCodec.mapJsonCodec;
import static org.assertj.core.api.Assertions.assertThat;

final class TestTrinoUser
{
    private static final JsonCodec<TrinoUser> USER_CODEC = jsonCodec(TrinoUser.class);
    private static final JsonCodec<Map<String, Object>> MAP_CODEC = mapJsonCodec(String.class, Object.class);

    @Test
    void testSerializeUserOnly()
    {
        assertThat(MAP_CODEC.fromJson(USER_CODEC.toJson(TrinoUser.createUser("some-user"))))
                .isEqualTo(Map.of("user", "some-user"));
    }

    @Test
    void testSerializeIdentityFlattened()
    {
        Identity identity = Identity.forUser("source-user")
                .withGroups(ImmutableSet.of("some-group"))
                .withExtraCredentials(ImmutableMap.of("allowed", "value", "ignored", "secret"))
                .build();

        assertThat(MAP_CODEC.fromJson(USER_CODEC.toJson(TrinoUser.createUser(identity, ImmutableSet.of("allowed")))))
                .isEqualTo(Map.of(
                        "user", "source-user",
                        "groups", List.of("some-group"),
                        "extraCredentials", Map.of("allowed", "value")));
    }

    @Test
    void testSerializeIdentityOmitsEmptyExtraCredentials()
    {
        Identity identity = Identity.forUser("source-user").withGroups(ImmutableSet.of()).build();

        assertThat(MAP_CODEC.fromJson(USER_CODEC.toJson(TrinoUser.createUser(identity, ImmutableSet.of()))))
                .isEqualTo(Map.of("user", "source-user", "groups", List.of()));
    }
}
