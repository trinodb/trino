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
package io.trino.plugin.iceberg.catalog.rest;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.util.List;
import java.util.Map;

import static io.trino.testing.TestingNames.randomNameSuffix;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;

@TestInstance(PER_CLASS)
public class TestIcebergRestCatalogReferencedBy
        extends BaseIcebergRestCatalogReferencedByTest
{
    public TestIcebergRestCatalogReferencedBy()
    {
        super(true);
    }

    @Test
    void testDirectReadSendsNothing()
    {
        String table = "direct_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + table + " AS SELECT 1 x", 1);

        assertThat(query("SELECT * FROM " + table)).matches("VALUES 1");

        assertThat(tableReferencedByOf(table)).isNotEmpty().allSatisfy(views -> assertThat(views).isEmpty());
        assertUpdate("DROP TABLE " + table);
    }

    @Test
    void testViewIsSent()
    {
        String table = "t_" + randomNameSuffix();
        String view = "v_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + table + " AS SELECT 1 x", 1);
        assertUpdate("CREATE VIEW " + view + " AS SELECT * FROM " + table);

        assertThat(query("SELECT * FROM " + view)).matches("VALUES 1");

        assertThat(tableReferencedByOf(table)).contains(List.of(schema() + "." + view));

        assertUpdate("DROP VIEW " + view);
        assertUpdate("DROP TABLE " + table);
    }

    @Test
    void testNestedViewsAreSentOutermostFirst()
    {
        String table = "t_" + randomNameSuffix();
        String innerView = "inner_view_" + randomNameSuffix();
        String outerView = "outer_view_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + table + " AS SELECT 1 x", 1);
        assertUpdate("CREATE VIEW " + innerView + " SECURITY INVOKER AS SELECT * FROM " + table);
        assertUpdate("CREATE VIEW " + outerView + " SECURITY DEFINER AS SELECT * FROM " + innerView);

        assertThat(query("SELECT * FROM " + outerView)).matches("VALUES 1");

        assertThat(tableReferencedByOf(table)).contains(List.of(schema() + "." + outerView, schema() + "." + innerView));

        assertUpdate("DROP VIEW " + outerView);
        assertUpdate("DROP VIEW " + innerView);
        assertUpdate("DROP TABLE " + table);
    }

    @Test
    void testNestedViewLoadSendsEnclosingViews()
    {
        String table = "t_" + randomNameSuffix();
        String innerView = "inner_view_" + randomNameSuffix();
        String outerView = "outer_view_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + table + " AS SELECT 1 x", 1);
        assertUpdate("CREATE VIEW " + innerView + " SECURITY INVOKER AS SELECT * FROM " + table);
        assertUpdate("CREATE VIEW " + outerView + " SECURITY DEFINER AS SELECT * FROM " + innerView);

        assertThat(query("SELECT * FROM " + outerView)).matches("VALUES 1");

        assertThat(viewReferencedByOf(outerView)).contains(List.of());
        assertThat(viewReferencedByOf(innerView)).contains(List.of(schema() + "." + outerView));

        assertUpdate("DROP VIEW " + outerView);
        assertUpdate("DROP VIEW " + innerView);
        assertUpdate("DROP TABLE " + table);
    }

    @Test
    void testAccessThroughViewIsNotCached()
    {
        String table = "t_" + randomNameSuffix();
        String view = "v_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + table + " AS SELECT 1 x", 1);
        assertUpdate("CREATE VIEW " + view + " AS SELECT * FROM " + table);

        computeActual("SELECT * FROM " + table);
        assertThat(tableReferencedByOf(table)).isNotEmpty().allSatisfy(views -> assertThat(views).isEmpty());
        int loadsBeforeViewRead = tableReferencedByOf(table).size();

        computeActual("SELECT * FROM " + view);
        assertThat(tableReferencedByOf(table).subList(loadsBeforeViewRead, tableReferencedByOf(table).size()))
                .contains(List.of(schema() + "." + view));

        assertUpdate("DROP VIEW " + view);
        assertUpdate("DROP TABLE " + table);
    }

    @Test
    void testForeignDefinerViewFailsTheLoad()
    {
        String table = "t_" + randomNameSuffix();
        String view = "v_" + randomNameSuffix();
        createIcebergCatalog("foreign", Map.of("iceberg.rest-catalog.referenced-by-enabled", "true"));
        assertUpdate("CREATE TABLE " + table + " AS SELECT 1 x", 1);
        assertUpdate("CREATE VIEW foreign." + schema() + "." + view + " SECURITY DEFINER AS SELECT * FROM iceberg." + schema() + "." + table);

        assertThat(query("SELECT * FROM " + table)).matches("VALUES 1");
        assertThatThrownBy(() -> computeActual("SELECT * FROM foreign." + schema() + "." + view))
                .hasMessageContaining("Cannot load table")
                .hasMessageContaining("DEFINER view")
                .hasMessageContaining("iceberg.rest-catalog.referenced-by-allow-incomplete-chain");

        assertUpdate("DROP VIEW foreign." + schema() + "." + view);
        assertUpdate("DROP TABLE " + table);
    }

    @Test
    void testForeignDefinerViewIsDroppedWhenIncompleteChainAllowed()
    {
        String table = "t_" + randomNameSuffix();
        String foreignView = "foreign_v_" + randomNameSuffix();
        String localView = "local_v_" + randomNameSuffix();
        createIcebergCatalog("lenient", Map.of(
                "iceberg.rest-catalog.referenced-by-enabled", "true",
                "iceberg.rest-catalog.referenced-by-allow-incomplete-chain", "true"));
        assertUpdate("CREATE TABLE " + table + " AS SELECT 1 x", 1);
        assertUpdate("CREATE VIEW " + localView + " SECURITY INVOKER AS SELECT * FROM lenient." + schema() + "." + table);
        assertUpdate("CREATE VIEW " + foreignView + " SECURITY DEFINER AS SELECT * FROM " + localView);

        assertThat(query("SELECT * FROM " + foreignView)).matches("VALUES 1");

        assertUpdate("DROP VIEW " + foreignView);
        assertUpdate("DROP VIEW " + localView);
        assertUpdate("DROP TABLE " + table);
    }
}
