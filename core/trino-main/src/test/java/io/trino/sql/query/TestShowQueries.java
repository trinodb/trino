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
package io.trino.sql.query;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.connector.MockConnectorFactory;
import io.trino.connector.MockConnectorPlugin;
import io.trino.connector.MockConnectorTableHandle;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ConnectorViewDefinition;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.session.PropertyMetadata;
import io.trino.testing.QueryRunner;
import io.trino.testing.StandaloneQueryRunner;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.parallel.Execution;

import java.util.Optional;

import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.junit.jupiter.api.parallel.ExecutionMode.CONCURRENT;

@TestInstance(PER_CLASS)
@Execution(CONCURRENT)
public class TestShowQueries
{
    private final QueryAssertions assertions;

    public TestShowQueries()
    {
        QueryRunner queryRunner = new StandaloneQueryRunner(testSessionBuilder()
                .setCatalog("local")
                .setSchema("default")
                .build());
        queryRunner.installPlugin(new MockConnectorPlugin(MockConnectorFactory.builder()
                .withGetColumns(schemaTableName -> ImmutableList.of(
                        ColumnMetadata.builder()
                                .setName("colaa")
                                .setType(BIGINT)
                                .build(),
                        ColumnMetadata.builder()
                                .setName("cola_")
                                .setType(BIGINT)
                                .build(),
                        ColumnMetadata.builder()
                                .setName("colabc")
                                .setType(BIGINT)
                                .build()))
                .withListSchemaNames(session -> ImmutableList.of("mockschema"))
                .withListTables((session, schemaName) -> ImmutableList.of("mockTable"))
                .withGetTableHandle((session, schemaTableName) -> {
                    if (schemaTableName.getTableName().equals("mockview")) {
                        return null;
                    }
                    return new MockConnectorTableHandle(schemaTableName);
                })
                .withGetViews((session, schemaTablePrefix) -> ImmutableMap.of(
                        new SchemaTableName("mockschema", "mockview"), new ConnectorViewDefinition(
                                "SELECT cola_ AS test_column FROM mock_table",
                                Optional.empty(),
                                Optional.empty(),
                                ImmutableList.of(new ConnectorViewDefinition.ViewColumn("test_column", BIGINT.getTypeId(), Optional.empty())),
                                Optional.empty(),
                                Optional.empty(),
                                true,
                                ImmutableList.of())))
                .withGetViewProperties(() -> ImmutableList.of(PropertyMetadata.booleanProperty("boolean_property", "sample_property", true, false)))
                .build()));
        queryRunner.createCatalog("mock", "mock", ImmutableMap.of());
        queryRunner.createCatalog("testing_catalog", "mock", ImmutableMap.of());
        assertions = new QueryAssertions(queryRunner);
    }

    @AfterAll
    public void teardown()
    {
        assertions.close();
    }

    @Test
    public void testShowCatalogsLikeWithEscape()
    {
        assertThat(assertions.query("SHOW CATALOGS LIKE 't$_%' ESCAPE ''"))
                .failure().hasMessage("Escape string must be a single character");
        assertThat(assertions.query("SHOW CATALOGS LIKE 't$_%' ESCAPE '$$'"))
                .failure().hasMessage("Escape string must be a single character");
        assertThat(assertions.query("SHOW CATALOGS LIKE '%$_%' ESCAPE '$'")).matches("VALUES('testing_catalog')");
        assertThat(assertions.query("SHOW CATALOGS LIKE '$_%' ESCAPE '$'")).matches("SELECT 'testing_catalog' WHERE FALSE");
    }

    @Test
    public void testShowFunctionLike()
    {
        assertThat(assertions.query("SHOW FUNCTIONS LIKE 'split%'"))
                .skippingTypesCheck()
                .matches("VALUES " +
                        "('split', 'array(varchar(x))', 'varchar(x), varchar(y)', 'scalar', true, '')," +
                        "('split', 'array(varchar(x))', 'varchar(x), varchar(y), bigint', 'scalar', true, '')," +
                        "('split_part', 'varchar(x)', 'varchar(x), varchar(y), bigint', 'scalar', true, 'Splits a string by a delimiter and returns the specified field (counting from one)')," +
                        "('split_to_map', 'map(varchar,varchar)', 'varchar, varchar, varchar', 'scalar', true, 'Creates a map using entryDelimiter and keyValueDelimiter')," +
                        "('split_to_multimap', 'map(varchar,array(varchar))', 'varchar, varchar, varchar', 'scalar', true, 'Creates a multimap by splitting a string into key/value pairs')");
    }

    @Test
    public void testShowFunctionsLikeWithEscape()
    {
        assertThat(assertions.query("SHOW FUNCTIONS LIKE 'split$_to$_%' ESCAPE '$'"))
                .skippingTypesCheck()
                .matches("VALUES " +
                        "('split_to_map', 'map(varchar,varchar)', 'varchar, varchar, varchar', 'scalar', true, 'Creates a map using entryDelimiter and keyValueDelimiter')," +
                        "('split_to_multimap', 'map(varchar,array(varchar))', 'varchar, varchar, varchar', 'scalar', true, 'Creates a multimap by splitting a string into key/value pairs')");
    }

    @Test
    public void testShowSessionLike()
    {
        assertThat(assertions.query(
                "SHOW SESSION LIKE '%page_row_c%'"))
                .skippingTypesCheck()
                .matches("VALUES ('filter_and_project_min_output_page_row_count', '256', '256', 'integer', 'Experimental: Minimum output page row count for filter and project operators')");
    }

    @Test
    public void testShowSessionLikeWithEscape()
    {
        assertThat(assertions.query("SHOW SESSION LIKE 't$_%' ESCAPE ''"))
                .failure().hasMessage("Escape string must be a single character");
        assertThat(assertions.query("SHOW SESSION LIKE 't$_%' ESCAPE '$$'"))
                .failure().hasMessage("Escape string must be a single character");
        assertThat(assertions.query(
                "SHOW SESSION LIKE '%page$_row$_c%' ESCAPE '$'"))
                .skippingTypesCheck()
                .matches("VALUES ('filter_and_project_min_output_page_row_count', '256', '256', 'integer', 'Experimental: Minimum output page row count for filter and project operators')");
    }

    @Test
    public void testListingEmptyCatalogs()
    {
        assertions.executeExclusively(() -> {
            assertions.getQueryRunner().getAccessControl().denyCatalogs(catalog -> false);
            assertions.assertQueryReturnsEmptyResult("SHOW CATALOGS");
            assertions.getQueryRunner().getAccessControl().reset();
        });
    }

    @Test
    public void testShowColumns()
    {
        assertThat(assertions.query("SHOW COLUMNS FROM mock.mockSchema.mockTable"))
                .matches("VALUES " +
                        "(VARCHAR 'colaa', VARCHAR 'bigint' , VARCHAR '', VARCHAR '')," +
                        "(VARCHAR 'cola_', VARCHAR 'bigint' , VARCHAR '', VARCHAR '')," +
                        "(VARCHAR 'colabc', VARCHAR 'bigint' , VARCHAR '', VARCHAR '')");
    }

    @Test
    public void testShowColumnsLike()
    {
        assertThat(assertions.query("SHOW COLUMNS FROM mock.mockSchema.mockTable like 'colabc'"))
                .matches("VALUES (VARCHAR 'colabc', VARCHAR 'bigint' , VARCHAR '', VARCHAR '')");
        assertThat(assertions.query("SHOW COLUMNS FROM mock.mockSchema.mockTable like 'cola%'"))
                .matches("VALUES " +
                        "(VARCHAR 'colaa', VARCHAR 'bigint' , VARCHAR '', VARCHAR '')," +
                        "(VARCHAR 'cola_', VARCHAR 'bigint' , VARCHAR '', VARCHAR '')," +
                        "(VARCHAR 'colabc', VARCHAR 'bigint' , VARCHAR '', VARCHAR '')");
        assertThat(assertions.query("SHOW COLUMNS FROM mock.mockSchema.mockTable like 'cola_'"))
                .matches("VALUES " +
                        "(VARCHAR 'colaa', VARCHAR 'bigint' , VARCHAR '', VARCHAR '')," +
                        "(VARCHAR 'cola_', VARCHAR 'bigint' , VARCHAR '', VARCHAR '')");

        assertThat(assertions.query("SHOW COLUMNS FROM system.runtime.nodes LIKE 'node%'"))
                .matches("VALUES " +
                        "(VARCHAR 'node_id', VARCHAR 'varchar' , VARCHAR '', VARCHAR '')," +
                        "(VARCHAR 'node_version', VARCHAR 'varchar' , VARCHAR '', VARCHAR '')");
        assertThat(assertions.query("SHOW COLUMNS FROM system.runtime.nodes LIKE 'node_id'"))
                .matches("VALUES (VARCHAR 'node_id', VARCHAR 'varchar' , VARCHAR '', VARCHAR '')");
        assertEquals(0, assertions.execute("SHOW COLUMNS FROM system.runtime.nodes LIKE ''").getRowCount());
    }

    @Test
    public void testShowColumnsWithLikeWithEscape()
    {
        assertThat(assertions.query("SHOW COLUMNS FROM system.runtime.nodes LIKE 't$_%' ESCAPE ''"))
                .failure().hasMessage("Escape string must be a single character");
        assertThat(assertions.query("SHOW COLUMNS FROM system.runtime.nodes LIKE 't$_%' ESCAPE '$$'"))
                .failure().hasMessage("Escape string must be a single character");
        assertThat(assertions.query("SHOW COLUMNS FROM mock.mockSchema.mockTable LIKE 'cola$_' ESCAPE '$'"))
                .matches("VALUES (VARCHAR 'cola_', VARCHAR 'bigint' , VARCHAR '', VARCHAR '')");
    }

    @Test
    public void testShowCreateViewWithProperties()
    {
        assertThat(assertions.getQueryRunner().execute("SHOW CREATE VIEW mock.mockschema.mockview").getOnlyValue())
                .isEqualTo("""
                        CREATE VIEW mock.mockschema.mockview SECURITY INVOKER
                        WITH (
                           boolean_property = true
                        ) AS
                        SELECT cola_ test_column
                        FROM
                          mock_table""");
    }
}
