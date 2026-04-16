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
package io.trino.plugin.doris;

import jakarta.validation.constraints.Max;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static io.airlift.configuration.testing.ConfigAssertions.assertFullMapping;
import static io.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static io.airlift.configuration.testing.ConfigAssertions.recordDefaults;
import static io.airlift.testing.ValidationAssertions.assertFailsValidation;

final class TestDorisConfig
{
    @Test
    void testDefaults()
    {
        assertRecordedDefaults(recordDefaults(DorisConfig.class)
                .setFenodes(null)
                .setJdbcUrl(null)
                .setUsername(null)
                .setPassword(null)
                .setFlightSqlPort(0)
                .setLargeintMapping(DorisLargeintMapping.VARCHAR)
                .setMaxSplitsPerQuery(64)
                .setMinTabletsPerSplit(1));
    }

    @Test
    void testExplicitPropertyMappings()
    {
        Map<String, String> properties = Map.ofEntries(
                Map.entry("doris.fenodes", "https://fe1:8030,fe2:8030"),
                Map.entry("doris.jdbc-url", "jdbc:mysql://fe1:9030"),
                Map.entry("doris.username", "trino"),
                Map.entry("doris.password", "secret"),
                Map.entry("doris.flight-sql-port", "9090"),
                Map.entry("doris.largeint-mapping", "DECIMAL"),
                Map.entry("doris.max-splits-per-query", "32"),
                Map.entry("doris.min-tablets-per-split", "5"));

        DorisConfig expected = new DorisConfig()
                .setFenodes("https://fe1:8030,fe2:8030")
                .setJdbcUrl("jdbc:mysql://fe1:9030")
                .setUsername("trino")
                .setPassword("secret")
                .setFlightSqlPort(9090)
                .setLargeintMapping(DorisLargeintMapping.DECIMAL)
                .setMaxSplitsPerQuery(32)
                .setMinTabletsPerSplit(5);

        assertFullMapping(properties, expected);
    }

    @Test
    void testFlightSqlPortValidation()
    {
        assertFailsValidation(
                new DorisConfig().setFlightSqlPort(65_536),
                "flightSqlPort",
                "must be less than or equal to 65535",
                Max.class);
    }
}
