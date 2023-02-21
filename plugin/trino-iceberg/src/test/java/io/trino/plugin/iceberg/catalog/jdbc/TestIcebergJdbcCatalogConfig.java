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
package io.trino.plugin.iceberg.catalog.jdbc;

import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import java.util.Map;

import static io.airlift.configuration.testing.ConfigAssertions.assertFullMapping;
import static io.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static io.airlift.configuration.testing.ConfigAssertions.recordDefaults;

public class TestIcebergJdbcCatalogConfig
{
    @Test
    public void testDefaults()
    {
        assertRecordedDefaults(recordDefaults(IcebergJdbcCatalogConfig.class)
                .setDriverClass(null)
                .setConnectionUrl(null)
                .setConnectionUser(null)
                .setConnectionPassword(null)
                .setCatalogName(null)
                .setDefaultWarehouseDir(null));
    }

    @Test
    public void testExplicitPropertyMappings()
    {
        Map<String, String> properties = ImmutableMap.<String, String>builder()
                .put("iceberg.jdbc-catalog.driver-class", "org.postgresql.Driver")
                .put("iceberg.jdbc-catalog.connection-url", "jdbc:postgresql://localhost:5432/test")
                .put("iceberg.jdbc-catalog.connection-user", "foo")
                .put("iceberg.jdbc-catalog.connection-password", "bar")
                .put("iceberg.jdbc-catalog.catalog-name", "test")
                .put("iceberg.jdbc-catalog.default-warehouse-dir", "s3://bucket")
                .buildOrThrow();

        IcebergJdbcCatalogConfig expected = new IcebergJdbcCatalogConfig()
                .setDriverClass("org.postgresql.Driver")
                .setConnectionUrl("jdbc:postgresql://localhost:5432/test")
                .setConnectionUser("foo")
                .setConnectionPassword("bar")
                .setCatalogName("test")
                .setDefaultWarehouseDir("s3://bucket");

        assertFullMapping(properties, expected);
    }
}
