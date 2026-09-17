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
package io.trino.type;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.FeaturesConfig;
import io.trino.Session;
import io.trino.SystemSessionProperties;
import io.trino.execution.DynamicFilterConfig;
import io.trino.execution.QueryManagerConfig;
import io.trino.execution.TaskManagerConfig;
import io.trino.execution.scheduler.NodeSchedulerConfig;
import io.trino.memory.MemoryManagerConfig;
import io.trino.memory.NodeMemoryConfig;
import io.trino.metadata.SessionPropertyManager;
import io.trino.server.protocol.spooling.SpoolingEnabledConfig;
import io.trino.sql.planner.OptimizerConfig;
import io.trino.sql.query.QueryAssertions;
import org.junit.jupiter.api.Test;

import static io.airlift.json.JsonCodec.jsonCodec;
import static io.trino.SystemSessionProperties.LEGACY_TYPE_RESOLVER;
import static io.trino.SystemSessionProperties.LEGACY_VARCHAR_TO_CHAR_COERCION;
import static io.trino.SystemSessionProperties.getTypeResolutionPolicy;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static org.assertj.core.api.Assertions.assertThat;

class TestTypeResolutionPolicy
{
    @Test
    void testWorkerPolicyRoundTrip()
    {
        var codec = jsonCodec(TypeResolutionPolicy.class);
        for (CharVarcharCoercion charCoercion : CharVarcharCoercion.values()) {
            for (boolean legacyResolver : new boolean[] {false, true}) {
                TypeResolutionPolicy policy = new TypeResolutionPolicy(charCoercion, legacyResolver);
                assertThat(codec.fromJson(codec.toJson(policy))).isEqualTo(policy);
            }
        }
    }

    @Test
    void testDefaultAndSessionOverrides()
    {
        assertThat(getTypeResolutionPolicy(testSessionBuilder().build()).legacyTypeResolver()).isFalse();
        for (boolean configuredLegacy : new boolean[] {false, true}) {
            SessionPropertyManager properties = new SessionPropertyManager(new SystemSessionProperties(
                    new QueryManagerConfig(),
                    new SpoolingEnabledConfig(),
                    new TaskManagerConfig(),
                    new MemoryManagerConfig(),
                    new FeaturesConfig().setLegacyTypeResolver(configuredLegacy),
                    new OptimizerConfig(),
                    new NodeMemoryConfig(),
                    new DynamicFilterConfig(),
                    new NodeSchedulerConfig()));
            Session defaults = testSessionBuilder(properties).build();
            assertThat(getTypeResolutionPolicy(defaults).legacyTypeResolver()).isEqualTo(configuredLegacy);
            for (boolean sessionLegacy : new boolean[] {false, true}) {
                for (boolean legacyChar : new boolean[] {false, true}) {
                    Session session = Session.builder(defaults)
                            .setSystemProperty(LEGACY_TYPE_RESOLVER, Boolean.toString(sessionLegacy))
                            .setSystemProperty(LEGACY_VARCHAR_TO_CHAR_COERCION, Boolean.toString(legacyChar))
                            .build();
                    assertThat(getTypeResolutionPolicy(session).legacyTypeResolver()).isEqualTo(sessionLegacy);
                    assertThat(getTypeResolutionPolicy(session).legacyCharCoercion()).isEqualTo(legacyChar);
                }
            }
        }
    }

    @Test
    void testIntegralAndDecimalCommonType()
    {
        try (QueryAssertions assertions = new QueryAssertions()) {
            for (boolean legacy : new boolean[] {false, true}) {
                Session session = assertions.sessionBuilder()
                        .setSystemProperty(LEGACY_TYPE_RESOLVER, Boolean.toString(legacy))
                        .build();
                for (String integral : ImmutableList.of("tinyint", "smallint", "integer", "bigint")) {
                    for (var wrapper : ImmutableMap.of(
                            "%s", "decimal(38,38)",
                            "ARRAY[%s]", "array(decimal(38,38))",
                            "ROW(%s)", "row(decimal(38,38))",
                            "ARRAY[ROW(%s)]", "array(row(decimal(38,38)))").entrySet()) {
                        String first = wrapper.getKey().formatted("CAST(0 AS " + integral + ")");
                        String second = wrapper.getKey().formatted("CAST(0.1 AS decimal(38,38))");
                        String expected = wrapper.getValue();
                        assertThat(assertions.query(session, "SELECT typeof(greatest(%s, %s)), typeof(greatest(%s, %s))".formatted(first, second, second, first)))
                                .matches("VALUES (VARCHAR '%s', VARCHAR '%s')".formatted(expected, expected));
                    }
                }
            }
        }
    }

    @Test
    void testQueriesWithBothResolvers()
    {
        try (QueryAssertions assertions = new QueryAssertions()) {
            // Reuse the runner and its caches while alternating session policies.
            for (boolean legacy : new boolean[] {false, true, false, true}) {
                Session session = assertions.sessionBuilder()
                        .setSystemProperty(LEGACY_TYPE_RESOLVER, Boolean.toString(legacy))
                        .build();
                assertThat(assertions.query(session, "SELECT transform(ARRAY[1, 2], x -> CAST(x AS bigint) + 1)"))
                        .matches("VALUES ARRAY[BIGINT '2', BIGINT '3']");
                assertThat(assertions.query(session, "SELECT DECIMAL '12.3' + DECIMAL '0.45'"))
                        .matches("VALUES CAST(DECIMAL '12.75' AS decimal(5, 2))");
                assertThat(assertions.query(session, "SELECT CAST(CAST('42' AS varchar) AS integer)"))
                        .matches("VALUES 42");
                assertThat(assertions.query(session, "SELECT coalesce(NULL, ARRAY[ROW(1, 'a')], ARRAY[ROW(BIGINT '2', 'bb')])"))
                        .matches("VALUES ARRAY[ROW(BIGINT '1', CAST('a' AS varchar(2)))]");
                for (boolean legacyChar : new boolean[] {false, true}) {
                    Session charSession = Session.builder(session)
                            .setSystemProperty(LEGACY_VARCHAR_TO_CHAR_COERCION, Boolean.toString(legacyChar))
                            .build();
                    assertThat(assertions.query(charSession, "SELECT CAST(CAST('bar' AS char(5)) AS varchar(10))"))
                            .matches(legacyChar ? "VALUES CAST('bar  ' AS varchar(10))" : "VALUES CAST('bar' AS varchar(10))");
                }
            }
        }
    }
}
