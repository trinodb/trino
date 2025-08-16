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
import io.trino.connector.TestingTableFunctions.SimpleTableFunction;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ConnectorViewDefinition;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.session.PropertyMetadata;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.Type;
import io.trino.testing.QueryRunner;
import io.trino.testing.StandaloneQueryRunner;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.parallel.Execution;

import java.util.Optional;

import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.CharType.createCharType;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.TimeType.createTimeType;
import static io.trino.spi.type.TimestampType.createTimestampType;
import static io.trino.spi.type.TimestampWithTimeZoneType.createTimestampWithTimeZoneType;
import static io.trino.spi.type.TinyintType.TINYINT;
import static io.trino.spi.type.VarbinaryType.VARBINARY;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.spi.type.VarcharType.createVarcharType;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static org.assertj.core.api.Assertions.assertThat;
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
                .withGetColumns(schemaTableName -> {
                    if (schemaTableName.getTableName().equals("default_column_value")) {
                        return ImmutableList.<ColumnMetadata>builder()
                                .add(columnMetadata("col_null", BOOLEAN, "null"))
                                .add(columnMetadata("col_boolean", BOOLEAN, "true"))
                                .add(columnMetadata("col_tinyint", TINYINT, "1"))
                                .add(columnMetadata("col_smallint", SMALLINT, "10"))
                                .add(columnMetadata("col_int", INTEGER, "100"))
                                .add(columnMetadata("col_bigint", BIGINT, "1000"))
                                .add(columnMetadata("col_real", REAL, "REAL '1.1'"))
                                .add(columnMetadata("col_double", DOUBLE, "DOUBLE '2.2'"))
                                .add(columnMetadata("col_short_decimal", DecimalType.createDecimalType(3, 1), "DECIMAL '32.1'"))
                                .add(columnMetadata("col_long_decimal", DecimalType.createDecimalType(38, 18), "DECIMAL '12345678901234567890.123456789012345678'"))
                                .add(columnMetadata("col_char", createCharType(5), "'char'"))
                                .add(columnMetadata("col_varchar", createVarcharType(10), "'varchar'"))
                                .add(columnMetadata("col_unbounded_varchar", VARCHAR, "'unbounded varchar'"))
                                .add(columnMetadata("col_varbinary", VARBINARY, "X'123456'"))
                                .add(columnMetadata("col_time", createTimeType(0), "TIME '00:00:00'"))
                                .add(columnMetadata("col_long_time", createTimeType(12), "TIME '00:00:00.000000000000'"))
                                .add(columnMetadata("col_date", DATE, "DATE '1970-01-01'"))
                                .add(columnMetadata("col_short_timestamp", createTimestampType(0), "TIMESTAMP '1970-01-01 00:00:00'"))
                                .add(columnMetadata("col_long_timestamp", createTimestampType(12), "TIMESTAMP '1970-01-01 00:00:00.000000999999'"))
                                .add(columnMetadata("col_short_timestamptz", createTimestampWithTimeZoneType(0), "TIMESTAMP '1970-01-01 00:00:00 UTC'"))
                                .add(columnMetadata("col_long_timestamptz", createTimestampWithTimeZoneType(12), "TIMESTAMP '1970-01-01 00:00:00.000000000000 UTC'"))
                                .build();
                    }
                    return ImmutableList.of(
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
                                    .build());
                })
                .withListSchemaNames(session -> ImmutableList.of("mockschema"))
                .withListTables((session, schemaName) -> ImmutableList.of("mockTable"))
                .withTableFunctions(ImmutableList.of(new SimpleTableFunction()))
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
    public void testShowFunctionsWithTableFunction()
    {
        // The table function exists in testing_catalog and mock catalogs
        assertThat(assertions.query("SHOW FUNCTIONS FROM mock.system LIKE 'simple$_table$_function' ESCAPE '$'"))
                .skippingTypesCheck()
                .matches("VALUES " +
                        "('simple_table_function', 'unknown', 'varchar, bigint', 'table', false, '')");

        assertThat(assertions.query("SHOW FUNCTIONS FROM testing_catalog.system LIKE 'simple$_table$_function' ESCAPE '$'"))
                .skippingTypesCheck()
                .matches("VALUES " +
                        "('simple_table_function', 'unknown', 'varchar, bigint', 'table', false, '')");

        assertThat(assertions.query("SHOW FUNCTIONS LIKE 'simple$_table$_function' ESCAPE '$'"))
                .skippingTypesCheck()
                .matches("VALUES " +
                        "('simple_table_function', 'unknown', 'varchar, bigint', 'table', false, '')," +
                        "('simple_table_function', 'unknown', 'varchar, bigint', 'table', false, '')");
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
        assertThat(assertions.execute("SHOW COLUMNS FROM system.runtime.nodes LIKE ''").getRowCount()).isEqualTo(0);
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
    void testShowCreateTableWithDefaultColumnValues()
    {
        assertThat(assertions.getQueryRunner().execute("SHOW CREATE TABLE mock.mockschema.default_column_value").getOnlyValue())
                .isEqualTo(
                        """
                                CREATE TABLE mock.mockschema.default_column_value (
                                   col_null boolean DEFAULT null,
                                   col_boolean boolean DEFAULT true,
                                   col_tinyint tinyint DEFAULT 1,
                                   col_smallint smallint DEFAULT 10,
                                   col_int integer DEFAULT 100,
                                   col_bigint bigint DEFAULT 1000,
                                   col_real real DEFAULT REAL '1.1',
                                   col_double double DEFAULT DOUBLE '2.2',
                                   col_short_decimal decimal(3, 1) DEFAULT DECIMAL '32.1',
                                   col_long_decimal decimal(38, 18) DEFAULT DECIMAL '12345678901234567890.123456789012345678',
                                   col_char char(5) DEFAULT 'char',
                                   col_varchar varchar(10) DEFAULT 'varchar',
                                   col_unbounded_varchar varchar DEFAULT 'unbounded varchar',
                                   col_varbinary varbinary DEFAULT X'123456',
                                   col_time time(0) DEFAULT TIME '00:00:00',
                                   col_long_time time(12) DEFAULT TIME '00:00:00.000000000000',
                                   col_date date DEFAULT DATE '1970-01-01',
                                   col_short_timestamp timestamp(0) DEFAULT TIMESTAMP '1970-01-01 00:00:00',
                                   col_long_timestamp timestamp(12) DEFAULT TIMESTAMP '1970-01-01 00:00:00.000000999999',
                                   col_short_timestamptz timestamp(0) with time zone DEFAULT TIMESTAMP '1970-01-01 00:00:00 UTC',
                                   col_long_timestamptz timestamp(12) with time zone DEFAULT TIMESTAMP '1970-01-01 00:00:00.000000000000 UTC'
                                )""");
    }

    @Test
    public void testShowCreateViewWithProperties()
    {
        assertThat(assertions.getQueryRunner().execute("SHOW CREATE VIEW mock.mockschema.mockview").getOnlyValue())
                .isEqualTo(
                        """
                        CREATE VIEW mock.mockschema.mockview SECURITY INVOKER
                        WITH (
                           boolean_property = true
                        ) AS
                        SELECT cola_ test_column
                        FROM
                          mock_table""");
    }

    private static ColumnMetadata columnMetadata(String name, Type type, String value)
    {
        return ColumnMetadata.builder().setName(name).setType(type).setDefaultValue(Optional.of(value)).build();
    }
}
