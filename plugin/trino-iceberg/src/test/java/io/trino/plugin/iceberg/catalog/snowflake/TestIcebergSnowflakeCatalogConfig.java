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
package io.trino.plugin.iceberg.catalog.snowflake;

import com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.sql.SQLException;
import java.util.Map;

import static io.airlift.configuration.testing.ConfigAssertions.assertFullMapping;
import static io.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static io.airlift.configuration.testing.ConfigAssertions.recordDefaults;
import static org.assertj.core.api.Assertions.assertThat;

public class TestIcebergSnowflakeCatalogConfig
{
    @Test
    public void testDefaults()
    {
        assertRecordedDefaults(recordDefaults(IcebergSnowflakeCatalogConfig.class)
                .setUser(null)
                .setPassword(null)
                .setDatabase(null)
                .setUri(null)
                .setRole(null));
    }

    @Test
    public void testExplicitPropertyMapping()
    {
        Map<String, String> properties = ImmutableMap.<String, String>builder()
                .put("iceberg.snowflake-catalog.password", "password")
                .put("iceberg.snowflake-catalog.user", "user")
                .put("iceberg.snowflake-catalog.role", "role")
                .put("iceberg.snowflake-catalog.account-uri", "jdbc:snowflake://sample.url")
                .put("iceberg.snowflake-catalog.database", "database")
                .buildOrThrow();

        IcebergSnowflakeCatalogConfig expected = new IcebergSnowflakeCatalogConfig()
                .setPassword("password")
                .setUser("user")
                .setRole("role")
                .setUri(URI.create("jdbc:snowflake://sample.url"))
                .setDatabase("database");

        assertFullMapping(properties, expected);
    }

    @Test
    public void testInvalidSnowflakeUrl()
            throws SQLException
    {
        IcebergSnowflakeCatalogConfig config = new IcebergSnowflakeCatalogConfig()
                .setPassword("password")
                .setUser("user")
                .setRole("role")
                .setUri(URI.create("foobar"))
                .setDatabase("database");
        assertThat(config.isUrlValid()).isFalse();
    }
}
