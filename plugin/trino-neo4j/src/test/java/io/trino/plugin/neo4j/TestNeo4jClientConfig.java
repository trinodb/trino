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
package io.trino.plugin.neo4j;

import com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.util.Map;

import static io.airlift.configuration.testing.ConfigAssertions.assertFullMapping;
import static io.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static io.airlift.configuration.testing.ConfigAssertions.recordDefaults;

public class TestNeo4jClientConfig
{
    @Test
    public void testDefaults()
    {
        assertRecordedDefaults(recordDefaults(Neo4jConnectorConfig.class)
                .setURI(null)
                .setAuthType("basic")
                .setBasicAuthUser("neo4j")
                .setBasicAuthPassword("")
                .setBearerAuthToken(null));
    }

    @Test
    public void testExplicitPropertyMappings()
    {
        Map<String, String> properties = ImmutableMap.<String, String>builder()
                .put("neo4j.uri", "bolt://1.2.3.4:12345")
                .put("neo4j.auth.type", "bearer")
                .put("neo4j.auth.basic.user", "foobar")
                .put("neo4j.auth.basic.password", "password")
                .put("neo4j.auth.bearer.token", "foobarbaz")
                .buildOrThrow();

        Neo4jConnectorConfig expected = new Neo4jConnectorConfig()
                .setURI(URI.create("bolt://1.2.3.4:12345"))
                .setAuthType("bearer")
                .setBasicAuthUser("foobar")
                .setBasicAuthPassword("password")
                .setBearerAuthToken("foobarbaz");

        assertFullMapping(properties, expected);
    }
}
