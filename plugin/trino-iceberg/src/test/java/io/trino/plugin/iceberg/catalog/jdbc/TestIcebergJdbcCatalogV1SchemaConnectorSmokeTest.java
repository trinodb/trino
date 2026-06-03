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

import io.trino.plugin.iceberg.catalog.jdbc.IcebergJdbcCatalogConfig.SchemaVersion;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.view.View;
import org.junit.jupiter.api.Test;

import static io.trino.testing.TestingNames.randomNameSuffix;
import static org.assertj.core.api.Assertions.assertThat;

final class TestIcebergJdbcCatalogV1SchemaConnectorSmokeTest
        extends BaseIcebergJdbcCatalogConnectorSmokeTest
{
    public TestIcebergJdbcCatalogV1SchemaConnectorSmokeTest()
    {
        super(SchemaVersion.V1);
    }

    @Test
    void testViewProperty()
    {
        String viewName = "test_view_property_" + randomNameSuffix();
        assertUpdate("CREATE VIEW " + viewName + " AS SELECT * FROM nation");
        View view = jdbcCatalog.loadView(toIdentifier(viewName));

        assertThat(computeScalar("SHOW CREATE VIEW " + viewName))
                .isEqualTo(
                        """
                        CREATE VIEW iceberg.tpch.%s SECURITY DEFINER
                        WITH (
                           location = '%s'
                        ) AS
                        SELECT *
                        FROM
                          nation""".formatted(viewName, view.location()));

        assertUpdate("DROP  VIEW " + viewName);
    }

    @Test
    void testCreateViewWithLocation()
    {
        String viewName = "test_create_view_with_location_" + randomNameSuffix();
        String namespaceLocation = jdbcCatalog.loadNamespaceMetadata(Namespace.of("tpch")).get("location");
        String viewLocation = namespaceLocation + "/" + viewName;

        assertUpdate("CREATE VIEW " + viewName + " WITH (location = '" + viewLocation + "') AS SELECT * FROM nation");
        View view = jdbcCatalog.loadView(toIdentifier(viewName));
        assertThat(view.location()).isEqualTo(viewLocation);

        assertUpdate("DROP  VIEW " + viewName);
    }
}
