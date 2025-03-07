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
package org.apache.iceberg.snowflake;

import io.trino.plugin.iceberg.catalog.snowflake.IcebergSnowflakeCatalogConfig;

import java.net.URI;
import java.util.Map;
import java.util.Optional;

import static io.trino.plugin.iceberg.catalog.snowflake.TestingSnowflakeServer.SNOWFLAKE_JDBC_URI;
import static io.trino.plugin.iceberg.catalog.snowflake.TestingSnowflakeServer.SNOWFLAKE_KEY;
import static io.trino.plugin.iceberg.catalog.snowflake.TestingSnowflakeServer.SNOWFLAKE_ROLE;
import static io.trino.plugin.iceberg.catalog.snowflake.TestingSnowflakeServer.SNOWFLAKE_TEST_DATABASE;
import static io.trino.plugin.iceberg.catalog.snowflake.TestingSnowflakeServer.SNOWFLAKE_USER;
import static org.apache.iceberg.snowflake.TrinoIcebergSnowflakeCatalogFactory.getSnowflakeDriverProperties;

public class TestTrinoSnowflakeCatalogWithKeyPair
        extends TestTrinoSnowflakeCatalog
{
    public static final IcebergSnowflakeCatalogConfig CATALOG_CONFIG =
            new IcebergSnowflakeCatalogConfig()
                    .setDatabase(SNOWFLAKE_TEST_DATABASE)
                    .setUri(URI.create(SNOWFLAKE_JDBC_URI))
                    .setRole(SNOWFLAKE_ROLE)
                    .setUser(SNOWFLAKE_USER)
                    .setKey(SNOWFLAKE_KEY);

    @Override
    protected Map<String, String> createSnowflakeDriverProperties()
    {
        return getSnowflakeDriverProperties(
                CATALOG_CONFIG.getUri(),
                CATALOG_CONFIG.getUser(),
                Optional.empty(),
                CATALOG_CONFIG.getKey(),
                CATALOG_CONFIG.getRole());
    }

    @Override
    protected IcebergSnowflakeCatalogConfig getCatalogConfig()
    {
        return CATALOG_CONFIG;
    }
}
