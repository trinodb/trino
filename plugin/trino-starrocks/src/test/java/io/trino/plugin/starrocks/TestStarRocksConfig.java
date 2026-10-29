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
package io.trino.plugin.starrocks;

import io.airlift.units.DataSize;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.util.Map;

import static io.airlift.configuration.testing.ConfigAssertions.assertFullMapping;
import static io.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static io.airlift.configuration.testing.ConfigAssertions.recordDefaults;
import static io.airlift.units.DataSize.Unit.MEGABYTE;

final class TestStarRocksConfig
{
    @Test
    void testDefaults()
    {
        assertRecordedDefaults(recordDefaults(StarRocksConfig.class)
                .setJdbcUrl(null)
                .setCatalogName(null)
                .setUsername(null)
                .setPassword(null)
                .setFlightSqlHost(null)
                .setFlightSqlPort(9408)
                .setFlightSqlTlsEnabled(false)
                .setFlightSqlTlsRootCertificate(null)
                .setFlightSqlTlsSkipVerify(false)
                .setFlightSqlTlsOverrideHostname(null)
                .setFlightSqlMaxAllocation(DataSize.of(256, MEGABYTE)));
    }

    @Test
    void testExplicitPropertyMappings()
    {
        Map<String, String> properties = Map.ofEntries(
                Map.entry("starrocks.jdbc-url", "jdbc:starrocks://fe.example.net:9030"),
                Map.entry("starrocks.catalog-name", "external_catalog"),
                Map.entry("starrocks.username", "trino"),
                Map.entry("starrocks.password", "secret"),
                Map.entry("starrocks.flight-sql-host", "flight.example.net"),
                Map.entry("starrocks.flight-sql-port", "9500"),
                Map.entry("starrocks.flight-sql.tls.enabled", "true"),
                Map.entry("starrocks.flight-sql.tls.root-certificate", "/etc/trino/starrocks-ca.pem"),
                Map.entry("starrocks.flight-sql.tls.skip-verify", "true"),
                Map.entry("starrocks.flight-sql.tls.override-hostname", "fe.internal.example.net"),
                Map.entry("starrocks.flight-sql.max-allocation", "512MB"));

        StarRocksConfig expected = new StarRocksConfig()
                .setJdbcUrl("jdbc:starrocks://fe.example.net:9030")
                .setCatalogName("external_catalog")
                .setUsername("trino")
                .setPassword("secret")
                .setFlightSqlHost("flight.example.net")
                .setFlightSqlPort(9500)
                .setFlightSqlTlsEnabled(true)
                .setFlightSqlTlsRootCertificate(new File("/etc/trino/starrocks-ca.pem"))
                .setFlightSqlTlsSkipVerify(true)
                .setFlightSqlTlsOverrideHostname("fe.internal.example.net")
                .setFlightSqlMaxAllocation(DataSize.of(512, MEGABYTE));

        assertFullMapping(properties, expected);
    }
}
