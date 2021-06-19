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
import io.trino.connector.CatalogName;
import io.trino.connector.MockConnectorFactory;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.SchemaTableName;
import io.trino.testing.LocalQueryRunner;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.List;

import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.testing.TestingSession.createBogusTestingCatalog;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.testng.Assert.assertEquals;

public class TestShowQueries
{
    private QueryAssertions assertions;

    @BeforeClass
    public void init()
    {
        LocalQueryRunner queryRunner = LocalQueryRunner.create(testSessionBuilder()
                .setCatalog("local")
                .setSchema("default")
                .build());
        queryRunner.createCatalog(
                "mock",
                MockConnectorFactory.builder()
                        .withGetColumns(schemaTableName ->
                                ImmutableList.of(
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
                        .withListTables((session, schemaName) -> ImmutableList.of(new SchemaTableName("mockSchema", "mockTable")))
                        .build(),
                ImmutableMap.of());
        queryRunner.getCatalogManager().registerCatalog(createBogusTestingCatalog("testing_catalog"));
        queryRunner.getMetadata().getSessionPropertyManager().addConnectorSessionProperties(new CatalogName("testing_catalog"), List.of());
        assertions = new QueryAssertions(queryRunner);
    }

    @AfterClass(alwaysRun = true)
    public void teardown()
    {
        assertions.close();
        assertions = null;
    }

    @Test
    public void testShowCatalogsLikeWithEscape()
    {
        assertThatThrownBy(() -> assertions.query("SHOW CATALOGS LIKE 't$_%' ESCAPE ''"))
                .hasMessage("Escape string must be a single character");
        assertThatThrownBy(() -> assertions.query("SHOW CATALOGS LIKE 't$_%' ESCAPE '$$'"))
                .hasMessage("Escape string must be a single character");
        assertThat(assertions.query("SHOW CATALOGS LIKE '%$_%' ESCAPE '$'")).matches("VALUES('testing_catalog')");
        assertThat(assertions.query("SHOW CATALOGS LIKE '$_%' ESCAPE '$'")).matches("SELECT 'testing_catalog' WHERE FALSE");
    }

    @Test
    public void testShowFunctionLike()
    {
        assertThat(assertions.query("SHOW FUNCTIONS LIKE 'split%'"))
                .matches("VALUES " +
                        "(cast('split' AS VARCHAR(30)), cast('array(varchar(x))' AS VARCHAR(28)), cast('varchar(x), varchar(y)' AS VARCHAR(68)), cast('scalar' AS VARCHAR(9)), true, cast('' AS VARCHAR(131)))," +
                        "('split', 'array(varchar(x))', 'varchar(x), varchar(y), bigint', 'scalar', true, '')," +
                        "('split_part', 'varchar(x)', 'varchar(x), varchar(y), bigint', 'scalar', true, 'Splits a string by a delimiter and returns the specified field (counting from one)')," +
                        "('split_to_map', 'map(varchar,varchar)', 'varchar, varchar, varchar', 'scalar', true, 'Creates a map using entryDelimiter and keyValueDelimiter')," +
                        "('split_to_multimap', 'map(varchar,array(varchar))', 'varchar, varchar, varchar', 'scalar', true, 'Creates a multimap by splitting a string into key/value pairs')");
    }

    @Test
    public void testShowFunctionsLikeWithEscape()
    {
        assertThat(assertions.query("SHOW FUNCTIONS LIKE 'split$_to$_%' ESCAPE '$'"))
                .matches("VALUES " +
                        "(cast('split_to_map' AS VARCHAR(30)), cast('map(varchar,varchar)' AS VARCHAR(28)), cast('varchar, varchar, varchar' AS VARCHAR(68)), cast('scalar' AS VARCHAR(9)), true, cast('Creates a map using entryDelimiter and keyValueDelimiter' AS VARCHAR(131)))," +
                        "('split_to_multimap', 'map(varchar,array(varchar))', 'varchar, varchar, varchar', 'scalar', true, 'Creates a multimap by splitting a string into key/value pairs')");
    }

    @Test
    public void testShowSessionLike()
    {
        assertThat(assertions.query(
                "SHOW SESSION LIKE '%page_row_c%'"))
                .matches("VALUES (cast('filter_and_project_min_output_page_row_count' as VARCHAR(53)), cast('256' as VARCHAR(14)), cast('256' as VARCHAR(14)), 'integer', cast('Experimental: Minimum output page row count for filter and project operators' as VARCHAR(103)))");
    }

    @Test
    public void testShowSessionLikeWithEscape()
    {
        assertThatThrownBy(() -> assertions.query("SHOW SESSION LIKE 't$_%' ESCAPE ''"))
                .hasMessage("Escape string must be a single character");
        assertThatThrownBy(() -> assertions.query("SHOW SESSION LIKE 't$_%' ESCAPE '$$'"))
                .hasMessage("Escape string must be a single character");
        assertThat(assertions.query(
                "SHOW SESSION LIKE '%page$_row$_c%' ESCAPE '$'"))
                .matches("VALUES (cast('filter_and_project_min_output_page_row_count' as VARCHAR(53)), cast('256' as VARCHAR(14)), cast('256' as VARCHAR(14)), 'integer', cast('Experimental: Minimum output page row count for filter and project operators' as VARCHAR(103)))");
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
        assertEquals(assertions.execute("SHOW COLUMNS FROM system.runtime.nodes LIKE ''").getRowCount(), 0);
    }

    @Test
    public void testShowColumnsWithLikeWithEscape()
    {
        assertThatThrownBy(() -> assertions.query("SHOW COLUMNS FROM system.runtime.nodes LIKE 't$_%' ESCAPE ''"))
                .hasMessage("Escape string must be a single character");
        assertThatThrownBy(() -> assertions.query("SHOW COLUMNS FROM system.runtime.nodes LIKE 't$_%' ESCAPE '$$'"))
                .hasMessage("Escape string must be a single character");
        assertThat(assertions.query("SHOW COLUMNS FROM mock.mockSchema.mockTable LIKE 'cola$_' ESCAPE '$'"))
                .matches("VALUES (VARCHAR 'cola_', VARCHAR 'bigint' , VARCHAR '', VARCHAR '')");
    }
}
