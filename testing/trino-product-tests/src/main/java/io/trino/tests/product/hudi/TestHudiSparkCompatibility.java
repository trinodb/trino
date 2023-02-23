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
package io.trino.tests.product.hudi;

import com.google.common.collect.ImmutableList;
import io.trino.jdbc.Row;
import io.trino.jdbc.TrinoArray;
import io.trino.tempto.BeforeTestWithContext;
import io.trino.tempto.ProductTest;
import io.trino.tempto.assertions.QueryAssert;
import io.trino.tempto.query.QueryResult;
import org.assertj.core.api.Assertions;
import org.testng.annotations.Test;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.SQLException;
import java.util.List;

import static io.trino.tempto.assertions.QueryAssert.Row.row;
import static io.trino.tempto.assertions.QueryAssert.assertQueryFailure;
import static io.trino.tempto.assertions.QueryAssert.assertThat;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static io.trino.tests.product.TestGroups.HUDI;
import static io.trino.tests.product.TestGroups.PROFILE_SPECIFIC_TESTS;
import static io.trino.tests.product.utils.QueryExecutors.onHudi;
import static io.trino.tests.product.utils.QueryExecutors.onTrino;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static org.testng.Assert.assertEquals;

public class TestHudiSparkCompatibility
        extends ProductTest
{
    private static final String COW_TABLE_TYPE = "cow";
    private static final String MOR_TABLE_TYPE = "mor";

    private String bucketName;

    @BeforeTestWithContext
    public void setUp()
    {
        bucketName = requireNonNull(System.getenv("S3_BUCKET"), "Environment variable not set: S3_BUCKET");
    }

    @Test(groups = {HUDI, PROFILE_SPECIFIC_TESTS})
    public void testCopyOnWriteTableSchemaEvolution()
    {
        String tableName = "test_hudi_cow_schema_evolution_" + randomNameSuffix();

        createNonPartitionedTable(tableName, COW_TABLE_TYPE);
        try {
            onHudi().executeQuery("SET hoodie.schema.on.read.enable=true");
            onHudi().executeQuery("ALTER TABLE default." + tableName + " ADD COLUMNS(new_col_0 STRING)");
            onHudi().executeQuery("INSERT INTO default." + tableName + " VALUES (3, 'a3', 30, 3000, 'row 3')");

            assertThat(onTrino().executeQuery("SELECT id, name, price, ts, new_col_0 FROM hudi.default." + tableName))
                    .containsOnly(ImmutableList.of(
                            row(1, "a1", 20, 1000, null),
                            row(2, "a2", 40, 2000, null),
                            row(3, "a3", 30, 3000, "row 3")));

            onHudi().executeQuery("ALTER TABLE default." + tableName + " RENAME COLUMN new_col_0 TO col_0");

            assertThat(onTrino().executeQuery("SELECT id, name, price, ts, col_0 FROM hudi.default." + tableName))
                    .containsOnly(ImmutableList.of(
                            row(1, "a1", 20, 1000, null),
                            row(2, "a2", 40, 2000, null),
                            row(3, "a3", 30, 3000, "row 3")));

            assertQueryFailure(() -> onTrino().executeQuery("SELECT id, name, price, ts, new_col_0 FROM hudi.default." + tableName))
                    .hasMessageContaining("Column 'new_col_0' cannot be resolved");

            onHudi().executeQuery("INSERT INTO default." + tableName + " VALUES (4, 'a4', 40, 4000, 'row 4')");
            assertThat(onTrino().executeQuery("SELECT id, name, price, ts, col_0 FROM hudi.default." + tableName))
                    .containsOnly(ImmutableList.of(
                            row(1, "a1", 20, 1000, null),
                            row(2, "a2", 40, 2000, null),
                            row(3, "a3", 30, 3000, "row 3"),
                            row(4, "a4", 40, 4000, "row 4")));

            onHudi().executeQuery("ALTER TABLE default." + tableName + " DROP COLUMN col_0");
            assertQueryFailure(() -> onTrino().executeQuery("SELECT id, name, price, ts, col_0 FROM hudi.default." + tableName))
                    .hasMessageContaining("Column 'col_0' cannot be resolved");

            assertThat(onTrino().executeQuery("SELECT id, name, price, ts FROM hudi.default." + tableName))
                    .containsOnly(ImmutableList.of(
                            row(1, "a1", 20, 1000),
                            row(2, "a2", 40, 2000),
                            row(3, "a3", 30, 3000),
                            row(4, "a4", 40, 4000)));

            onHudi().executeQuery("ALTER TABLE default." + tableName + " ADD COLUMNS(col_0 STRING)");
            assertThat(onTrino().executeQuery("SELECT id, name, price, ts, col_0 FROM hudi.default." + tableName))
                    .containsOnly(ImmutableList.of(
                            row(1, "a1", 20, 1000, null),
                            row(2, "a2", 40, 2000, null),
                            row(3, "a3", 30, 3000, null),
                            row(4, "a4", 40, 4000, null)));
        }
        finally {
            onHudi().executeQuery("DROP TABLE default." + tableName);
        }
    }

    @Test(groups = {HUDI, PROFILE_SPECIFIC_TESTS})
    public void testCopyOnWriteTableSchemaEvolutionAlterIntType()
    {
        String intToLong = "test_hudi_cow_schema_evolution_alter_int_to_long" + randomNameSuffix();
        String intToFloat = "test_hudi_cow_schema_evolution_alter_int_to_float" + randomNameSuffix();
        String intToDouble = "test_hudi_cow_schema_evolution_alter_int_to_double" + randomNameSuffix();
        String intToString = "test_hudi_cow_schema_evolution_alter_int_to_string" + randomNameSuffix();
        String intToDecimal = "test_hudi_cow_schema_evolution_alter_int_to_decimal" + randomNameSuffix();

        createSchemaEvolutionTable(intToLong, COW_TABLE_TYPE);
        createSchemaEvolutionTable(intToFloat, COW_TABLE_TYPE);
        createSchemaEvolutionTable(intToDouble, COW_TABLE_TYPE);
        createSchemaEvolutionTable(intToString, COW_TABLE_TYPE);
        createSchemaEvolutionTable(intToDecimal, COW_TABLE_TYPE);
        try {
            onHudi().executeQuery("SET hoodie.schema.on.read.enable=true");
            onHudi().executeQuery("ALTER TABLE default." + intToLong + " ALTER COLUMN test_int TYPE LONG;");

            assertThat(onTrino().executeQuery("SELECT id, test_int, test_long, test_float, test_double, test_string FROM hudi.default." + intToLong))
                    .containsOnly(ImmutableList.of(
                            row(1, 1L, 1L, 1F, 1D, "2018-12-12"),
                            row(2, 2L, 2L, 2F, 2D, "2018-12-16")));

            onHudi().executeQuery("ALTER TABLE default." + intToFloat + " ALTER COLUMN test_int TYPE FLOAT;");
            assertThat(onTrino().executeQuery("SELECT id, test_int, test_long, test_float, test_double, test_string FROM hudi.default." + intToFloat))
                    .containsOnly(ImmutableList.of(
                            row(1, 1F, 1L, 1F, 1D, "2018-12-12"),
                            row(2, 2F, 2L, 2F, 2D, "2018-12-16")));

            onHudi().executeQuery("ALTER TABLE default." + intToDouble + " ALTER COLUMN test_int TYPE DOUBLE;");
            assertThat(onTrino().executeQuery("SELECT id, test_int, test_long, test_float, test_double, test_string FROM hudi.default." + intToDouble))
                    .containsOnly(ImmutableList.of(
                            row(1, 1D, 1L, 1F, 1D, "2018-12-12"),
                            row(2, 2D, 2L, 2F, 2D, "2018-12-16")));

            onHudi().executeQuery("ALTER TABLE default." + intToString + " ALTER COLUMN test_int TYPE STRING;");
            assertThat(onTrino().executeQuery("SELECT id, test_int, test_long, test_float, test_double, test_string FROM hudi.default." + intToString))
                    .containsOnly(ImmutableList.of(
                            row(1, "1", 1L, 1F, 1D, "2018-12-12"),
                            row(2, "2", 2L, 2F, 2D, "2018-12-16")));

            onHudi().executeQuery("ALTER TABLE default." + intToDecimal + " ALTER COLUMN test_int TYPE DECIMAL(10,4);");
            assertThat(onTrino().executeQuery("SELECT id, test_int, test_long, test_float, test_double, test_string FROM hudi.default." + intToDecimal))
                    .containsOnly(ImmutableList.of(
                            row(1, new BigDecimal(1.0000), 1L, 1F, 1D, "2018-12-12"),
                            row(2, new BigDecimal(2.0000), 2L, 2F, 2D, "2018-12-16")));
        }
        finally {
            onHudi().executeQuery("DROP TABLE default." + intToLong);
            onHudi().executeQuery("DROP TABLE default." + intToFloat);
            onHudi().executeQuery("DROP TABLE default." + intToDouble);
            onHudi().executeQuery("DROP TABLE default." + intToString);
            onHudi().executeQuery("DROP TABLE default." + intToDecimal);
        }
    }

    @Test(groups = {HUDI, PROFILE_SPECIFIC_TESTS})
    public void testCopyOnWriteTableSchemaEvolutionAlterLongType()
    {
        String longToFloat = "test_hudi_cow_schema_evolution_alter_long_to_float" + randomNameSuffix();
        String longToDouble = "test_hudi_cow_schema_evolution_alter_long_to_float" + randomNameSuffix();
        String longToString = "test_hudi_cow_schema_evolution_alter_long_to_float" + randomNameSuffix();
        String longToDecimal = "test_hudi_cow_schema_evolution_alter_long_to_float" + randomNameSuffix();

        createSchemaEvolutionTable(longToFloat, COW_TABLE_TYPE);
        createSchemaEvolutionTable(longToDouble, COW_TABLE_TYPE);
        createSchemaEvolutionTable(longToString, COW_TABLE_TYPE);
        createSchemaEvolutionTable(longToDecimal, COW_TABLE_TYPE);
        try {
            onHudi().executeQuery("SET hoodie.schema.on.read.enable=true");
            onHudi().executeQuery("ALTER TABLE default." + longToFloat + " ALTER COLUMN test_long TYPE FLOAT;");

            assertThat(onTrino().executeQuery("SELECT id, test_int, test_long, test_float, test_double, test_string FROM hudi.default." + longToFloat))
                    .containsOnly(ImmutableList.of(
                            row(1, 1, 1F, 1F, 1D, "2018-12-12"),
                            row(2, 2, 2F, 2F, 2D, "2018-12-16")));

            onHudi().executeQuery("ALTER TABLE default." + longToDouble + " ALTER COLUMN test_long TYPE DOUBLE;");
            assertThat(onTrino().executeQuery("SELECT id, test_int, test_long, test_float, test_double, test_string FROM hudi.default." + longToDouble))
                    .containsOnly(ImmutableList.of(
                            row(1, 1, 1D, 1F, 1D, "2018-12-12"),
                            row(2, 2, 2D, 2F, 2D, "2018-12-16")));

            onHudi().executeQuery("ALTER TABLE default." + longToString + " ALTER COLUMN test_long TYPE STRING;");
            assertThat(onTrino().executeQuery("SELECT id, test_int, test_long, test_float, test_double, test_string FROM hudi.default." + longToString))
                    .containsOnly(ImmutableList.of(
                            row(1, 1, "1", 1F, 1D, "2018-12-12"),
                            row(2, 2, "2", 2F, 2D, "2018-12-16")));

            onHudi().executeQuery("ALTER TABLE default." + longToDecimal + " ALTER COLUMN test_long TYPE DECIMAL(10,4);");
            assertThat(onTrino().executeQuery("SELECT id, test_int, test_long, test_float, test_double, test_string FROM hudi.default." + longToDecimal))
                    .containsOnly(ImmutableList.of(
                            row(1, 1, new BigDecimal(1.0000), 1F, 1D, "2018-12-12"),
                            row(2, 2, new BigDecimal(2.0000), 2F, 2D, "2018-12-16")));
        }
        finally {
            onHudi().executeQuery("DROP TABLE default." + longToFloat);
            onHudi().executeQuery("DROP TABLE default." + longToDouble);
            onHudi().executeQuery("DROP TABLE default." + longToString);
            onHudi().executeQuery("DROP TABLE default." + longToDecimal);
        }
    }

    @Test(groups = {HUDI, PROFILE_SPECIFIC_TESTS})
    public void testCopyOnWriteTableSchemaEvolutionAlterFloatType()
    {
        String floatToDouble = "test_hudi_cow_schema_evolution_alter_float_to_doulbe" + randomNameSuffix();
        String floatToString = "test_hudi_cow_schema_evolution_alter_float_to_string" + randomNameSuffix();
        String floatToDecimal = "test_hudi_cow_schema_evolution_alter_float_to_decimal" + randomNameSuffix();

        createSchemaEvolutionTable(floatToDouble, COW_TABLE_TYPE);
        createSchemaEvolutionTable(floatToString, COW_TABLE_TYPE);
        createSchemaEvolutionTable(floatToDecimal, COW_TABLE_TYPE);
        try {
            onHudi().executeQuery("SET hoodie.schema.on.read.enable=true");
            onHudi().executeQuery("ALTER TABLE default." + floatToDouble + " ALTER COLUMN test_float TYPE DOUBLE;");

            assertThat(onTrino().executeQuery("SELECT id, test_int, test_long, test_float, test_double, test_string FROM hudi.default." + floatToDouble))
                    .containsOnly(ImmutableList.of(
                            row(1, 1, 1L, 1D, 1D, "2018-12-12"),
                            row(2, 2, 2L, 2D, 2D, "2018-12-16")));

            onHudi().executeQuery("ALTER TABLE default." + floatToString + " ALTER COLUMN test_float TYPE STRING;");
            assertThat(onTrino().executeQuery("SELECT id, test_int, test_long, test_float, test_double, test_string FROM hudi.default." + floatToString))
                    .containsOnly(ImmutableList.of(
                            row(1, 1, 1L, "1", 1D, "2018-12-12"),
                            row(2, 2, 2L, "2", 2D, "2018-12-16")));

            onHudi().executeQuery("ALTER TABLE default." + floatToDecimal + " ALTER COLUMN test_float TYPE DECIMAL(10,4);");
            assertThat(onTrino().executeQuery("SELECT id, test_int, test_long, test_float, test_double, test_string FROM hudi.default." + floatToDecimal))
                    .containsOnly(ImmutableList.of(
                            row(1, 1, 1L, new BigDecimal(1.0000), 1D, "2018-12-12"),
                            row(2, 2, 2L, new BigDecimal(2.0000), 2D, "2018-12-16")));
        }
        finally {
            onHudi().executeQuery("DROP TABLE default." + floatToDouble);
            onHudi().executeQuery("DROP TABLE default." + floatToString);
            onHudi().executeQuery("DROP TABLE default." + floatToDecimal);
        }
    }

    @Test(groups = {HUDI, PROFILE_SPECIFIC_TESTS})
    public void testCopyOnWriteTableSchemaEvolutionAlterDoubleType()
    {
        String doubleToString = "test_hudi_cow_schema_evolution_alter_double_to_string" + randomNameSuffix();
        String doubleToDecimal = "test_hudi_cow_schema_evolution_alter_double_to_decimal" + randomNameSuffix();

        createSchemaEvolutionTable(doubleToString, COW_TABLE_TYPE);
        createSchemaEvolutionTable(doubleToDecimal, COW_TABLE_TYPE);
        try {
            onHudi().executeQuery("SET hoodie.schema.on.read.enable=true");
            onHudi().executeQuery("ALTER TABLE default." + doubleToString + " ALTER COLUMN test_double TYPE STRING;");

            assertThat(onTrino().executeQuery("SELECT id, test_int, test_long, test_float, test_double, test_string FROM hudi.default." + doubleToString))
                    .containsOnly(ImmutableList.of(
                            row(1, 1, 1L, 1D, "1", "2018-12-12"),
                            row(2, 2, 2L, 2D, "2", "2018-12-16")));

            onHudi().executeQuery("ALTER TABLE default." + doubleToDecimal + " ALTER COLUMN test_double TYPE DECIMAL(10,4);");
            assertThat(onTrino().executeQuery("SELECT id, test_int, test_long, test_float, test_double, test_string FROM hudi.default." + doubleToDecimal))
                    .containsOnly(ImmutableList.of(
                            row(1, 1, 1L, 1F, new BigDecimal(1.0000), "2018-12-12"),
                            row(2, 2, 2L, 2F, new BigDecimal(2.0000), "2018-12-16")));
        }
        finally {
            onHudi().executeQuery("DROP TABLE default." + doubleToString);
            onHudi().executeQuery("DROP TABLE default." + doubleToDecimal);
        }
    }

    @Test(groups = {HUDI, PROFILE_SPECIFIC_TESTS})
    public void testCopyOnWriteTableSchemaEvolutionAlterArrayNestedType() throws SQLException
    {
        String arrayIntToArrayLong = "test_hudi_cow_schema_evolution_alter_array_int_to_array_long" + randomNameSuffix();
        String arrayIntToArrayFloat = "test_hudi_cow_schema_evolution_alter_array_int_to_array_float" + randomNameSuffix();
        String arrayIntToArrayDouble = "test_hudi_cow_schema_evolution_alter_array_int_to_array_double" + randomNameSuffix();
        String arrayIntToArrayString = "test_hudi_cow_schema_evolution_alter_array_int_to_array_string" + randomNameSuffix();
        String arrayIntToArrayDecimal = "test_hudi_cow_schema_evolution_alter_array_int_to_array_decimal" + randomNameSuffix();
        String arrayStringToArrayDate = "test_hudi_cow_schema_evolution_alter_array_string_to_array_date" + randomNameSuffix();
        String arrayDateToArrayString = "test_hudi_cow_schema_evolution_alter_array_date_to_array_string" + randomNameSuffix();

        createSchemaEvolutionTable(arrayIntToArrayLong, COW_TABLE_TYPE);
        createSchemaEvolutionTable(arrayIntToArrayFloat, COW_TABLE_TYPE);
        createSchemaEvolutionTable(arrayIntToArrayDouble, COW_TABLE_TYPE);
        createSchemaEvolutionTable(arrayIntToArrayString, COW_TABLE_TYPE);
        createSchemaEvolutionTable(arrayIntToArrayDecimal, COW_TABLE_TYPE);
        createSchemaEvolutionTable(arrayStringToArrayDate, COW_TABLE_TYPE);
        createSchemaEvolutionTable(arrayDateToArrayString, COW_TABLE_TYPE);
        try {
            onHudi().executeQuery("SET hoodie.schema.on.read.enable=true");
            onHudi().executeQuery("ALTER TABLE default." + arrayIntToArrayLong + " ALTER COLUMN test_array_struct.element.number TYPE LONG;");

            QueryResult selectAllResult = onTrino().executeQuery("SELECT id, test_array_struct FROM hudi.default." + arrayIntToArrayLong);
            assertEquals(selectAllResult.rows().size(), 2);
            for (List<?> row : selectAllResult.rows()) {
                int id = ((Long) row.get(0)).intValue();
                switch (id) {
                    case 0:
                        Row rowValueFirst = rowBuilder()
                                .addField("name", "2018-12-12")
                                .addField("number", 123L)
                                .addField("start_date", "2018-12-12")
                                .addField("birth", Date.valueOf("2018-12-12")).build();
                        assertStructEquals(((Object[]) (((TrinoArray) row.get(1)).getArray(1, 1)))[0], new Object[] {rowValueFirst});
                        break;
                    case 1:
                        Row rowValueSecond = rowBuilder()
                                .addField("name", "2018-12-16")
                                .addField("number", 123L)
                                .addField("start_date", "2018-12-16")
                                .addField("birth", Date.valueOf("2018-12-16")).build();
                        assertStructEquals(((Object[]) (((TrinoArray) row.get(1)).getArray(1, 1)))[0], new Object[] {rowValueSecond});
                        break;
                }
            }

            onHudi().executeQuery("ALTER TABLE default." + arrayIntToArrayFloat + " ALTER COLUMN test_array_struct.element.number TYPE FLOAT;");
            selectAllResult = onTrino().executeQuery("SELECT id, test_array_struct FROM hudi.default." + arrayIntToArrayFloat);
            assertEquals(selectAllResult.rows().size(), 2);
            for (List<?> row : selectAllResult.rows()) {
                int id = ((Long) row.get(0)).intValue();
                switch (id) {
                    case 0:
                        Row rowValueFirst = rowBuilder()
                                .addField("name", "2018-12-12")
                                .addField("number", 123F)
                                .addField("start_date", "2018-12-12")
                                .addField("birth", Date.valueOf("2018-12-12")).build();
                        assertStructEquals(((Object[]) (((TrinoArray) row.get(1)).getArray(1, 1)))[0], new Object[] {rowValueFirst});
                        break;
                    case 1:
                        Row rowValueSecond = rowBuilder()
                                .addField("name", "2018-12-16")
                                .addField("number", 123F)
                                .addField("start_date", "2018-12-16")
                                .addField("birth", Date.valueOf("2018-12-16")).build();
                        assertStructEquals(((Object[]) (((TrinoArray) row.get(1)).getArray(1, 1)))[0], new Object[] {rowValueSecond});
                        break;
                }
            }

            onHudi().executeQuery("ALTER TABLE default." + arrayIntToArrayDouble + " ALTER COLUMN test_array_struct.element.number TYPE DOUBLE;");
            selectAllResult = onTrino().executeQuery("SELECT id, test_array_struct FROM hudi.default." + arrayIntToArrayDouble);
            assertEquals(selectAllResult.rows().size(), 2);
            for (List<?> row : selectAllResult.rows()) {
                int id = ((Long) row.get(0)).intValue();
                switch (id) {
                    case 0:
                        Row rowValueFirst = rowBuilder()
                                .addField("name", "2018-12-12")
                                .addField("number", 123D)
                                .addField("start_date", "2018-12-12")
                                .addField("birth", Date.valueOf("2018-12-12")).build();
                        assertStructEquals(((Object[]) (((TrinoArray) row.get(1)).getArray(1, 1)))[0], new Object[] {rowValueFirst});
                        break;
                    case 1:
                        Row rowValueSecond = rowBuilder()
                                .addField("name", "2018-12-16")
                                .addField("number", 123D)
                                .addField("start_date", "2018-12-16")
                                .addField("birth", Date.valueOf("2018-12-16")).build();
                        assertStructEquals(((Object[]) (((TrinoArray) row.get(1)).getArray(1, 1)))[0], new Object[] {rowValueSecond});
                        break;
                }
            }

            onHudi().executeQuery("ALTER TABLE default." + arrayIntToArrayString + " ALTER COLUMN test_array_struct.element.number TYPE STRING;");
            selectAllResult = onTrino().executeQuery("SELECT id, test_array_struct FROM hudi.default." + arrayIntToArrayString);
            assertEquals(selectAllResult.rows().size(), 2);
            for (List<?> row : selectAllResult.rows()) {
                int id = ((Long) row.get(0)).intValue();
                switch (id) {
                    case 0:
                        Row rowValueFirst = rowBuilder()
                                .addField("name", "2018-12-12")
                                .addField("number", "123")
                                .addField("start_date", "2018-12-12")
                                .addField("birth", Date.valueOf("2018-12-12")).build();
                        assertStructEquals(((Object[]) (((TrinoArray) row.get(1)).getArray(1, 1)))[0], new Object[] {rowValueFirst});
                        break;
                    case 1:
                        Row rowValueSecond = rowBuilder()
                                .addField("name", "2018-12-16")
                                .addField("number", "123")
                                .addField("start_date", "2018-12-16")
                                .addField("birth", Date.valueOf("2018-12-16")).build();
                        assertStructEquals(((Object[]) (((TrinoArray) row.get(1)).getArray(1, 1)))[0], new Object[] {rowValueSecond});
                        break;
                }
            }

            onHudi().executeQuery("ALTER TABLE default." + arrayIntToArrayDecimal + " ALTER COLUMN test_array_struct.element.number TYPE DECIMAL(10,2);");
            selectAllResult = onTrino().executeQuery("SELECT id, test_array_struct FROM hudi.default." + arrayIntToArrayDecimal);
            assertEquals(selectAllResult.rows().size(), 2);
            for (List<?> row : selectAllResult.rows()) {
                int id = ((Long) row.get(0)).intValue();
                switch (id) {
                    case 0:
                        Row rowValueFirst = rowBuilder()
                                .addField("name", "2018-12-12")
                                .addField("number", new BigDecimal("123.00"))
                                .addField("start_date", "2018-12-12")
                                .addField("birth", Date.valueOf("2018-12-12")).build();
                        assertStructEquals(((Object[]) (((TrinoArray) row.get(1)).getArray(1, 1)))[0], new Object[] {rowValueFirst});
                        break;
                    case 1:
                        Row rowValueSecond = rowBuilder()
                                .addField("name", "2018-12-16")
                                .addField("number", new BigDecimal("123.00"))
                                .addField("start_date", "2018-12-16")
                                .addField("birth", Date.valueOf("2018-12-16")).build();
                        assertStructEquals(((Object[]) (((TrinoArray) row.get(1)).getArray(1, 1)))[0], new Object[] {rowValueSecond});
                        break;
                }
            }

            onHudi().executeQuery("ALTER TABLE default." + arrayStringToArrayDate + " ALTER COLUMN test_array_struct.element.start_date TYPE DATE;");
            selectAllResult = onTrino().executeQuery("SELECT id, test_array_struct FROM hudi.default." + arrayStringToArrayDate);
            assertEquals(selectAllResult.rows().size(), 2);
            for (List<?> row : selectAllResult.rows()) {
                int id = ((Long) row.get(0)).intValue();
                switch (id) {
                    case 0:
                        Row rowValueFirst = rowBuilder()
                                .addField("name", "2018-12-12")
                                .addField("number", new BigDecimal("123.00"))
                                .addField("start_date", Date.valueOf("2018-12-12"))
                                .addField("birth", Date.valueOf("2018-12-12")).build();
                        assertStructEquals(((Object[]) (((TrinoArray) row.get(1)).getArray(1, 1)))[0], new Object[] {rowValueFirst});
                        break;
                    case 1:
                        Row rowValueSecond = rowBuilder()
                                .addField("name", "2018-12-16")
                                .addField("number", new BigDecimal("123.00"))
                                .addField("start_date", Date.valueOf("2018-12-16"))
                                .addField("birth", Date.valueOf("2018-12-16")).build();
                        assertStructEquals(((Object[]) (((TrinoArray) row.get(1)).getArray(1, 1)))[0], new Object[] {rowValueSecond});
                        break;
                }
            }

            onHudi().executeQuery("ALTER TABLE default." + arrayDateToArrayString + " ALTER COLUMN test_array_struct.element.birth TYPE STRING;");
            selectAllResult = onTrino().executeQuery("SELECT id, test_array_struct FROM hudi.default." + arrayDateToArrayString);
            assertEquals(selectAllResult.rows().size(), 2);
            for (List<?> row : selectAllResult.rows()) {
                int id = ((Long) row.get(0)).intValue();
                switch (id) {
                    case 0:
                        Row rowValueFirst = rowBuilder()
                                .addField("name", "2018-12-12")
                                .addField("number", new BigDecimal("123.00"))
                                .addField("start_date", "2018-12-12")
                                .addField("birth", "2018-12-12").build();
                        assertStructEquals(((Object[]) (((TrinoArray) row.get(1)).getArray(1, 1)))[0], new Object[] {rowValueFirst});
                        break;
                    case 1:
                        Row rowValueSecond = rowBuilder()
                                .addField("name", "2018-12-16")
                                .addField("number", new BigDecimal("123.00"))
                                .addField("start_date", "2018-12-16")
                                .addField("birth", "2018-12-16").build();
                        assertStructEquals(((Object[]) (((TrinoArray) row.get(1)).getArray(1, 1)))[0], new Object[] {rowValueSecond});
                        break;
                }
            }
        }
        finally {
            onHudi().executeQuery("DROP TABLE default." + arrayIntToArrayLong);
            onHudi().executeQuery("DROP TABLE default." + arrayIntToArrayFloat);
            onHudi().executeQuery("DROP TABLE default." + arrayIntToArrayDouble);
            onHudi().executeQuery("DROP TABLE default." + arrayIntToArrayString);
            onHudi().executeQuery("DROP TABLE default." + arrayIntToArrayDecimal);
            onHudi().executeQuery("DROP TABLE default." + arrayStringToArrayDate);
            onHudi().executeQuery("DROP TABLE default." + arrayDateToArrayString);
        }
    }

    @Test(groups = {HUDI, PROFILE_SPECIFIC_TESTS})
    public void testCopyOnWriteTableSchemaEvolutionAlterMapNestedType() throws SQLException
    {
        String tableName = "test_hudi_cow_schema_evolution_alter_map_nested_type_" + randomNameSuffix();

        createSchemaEvolutionTable(tableName, COW_TABLE_TYPE);
        try {
            onHudi().executeQuery("SET hoodie.schema.on.read.enable=true");
            onHudi().executeQuery("ALTER TABLE default." + tableName + " ALTER COLUMN test_map_struct.value.number TYPE LONG;");

            QueryResult selectAllResult = onTrino().executeQuery("SELECT id, test_map_struct['row key'] FROM hudi.default." + tableName);
            assertEquals(selectAllResult.rows().size(), 2);
            for (List<?> row : selectAllResult.rows()) {
                int id = ((Long) row.get(0)).intValue();
                switch (id) {
                    case 0:
                        Row rowValueFirst = rowBuilder()
                                .addField("name", "2018-12-12")
                                .addField("number", 123L)
                                .addField("start_date", "2018-12-12")
                                .addField("birth", Date.valueOf("2018-12-12")).build();
                        assertStructEquals(row.get(1), new Object[] {rowValueFirst});
                        break;
                    case 1:
                        rowValueFirst = rowBuilder()
                                .addField("name", "2018-12-16")
                                .addField("number", 123L)
                                .addField("start_date", "2018-12-16")
                                .addField("birth", Date.valueOf("2018-12-16")).build();
                        assertStructEquals(row.get(1), new Object[] {rowValueFirst});
                        break;
                }
            }
        }
        finally {
            onHudi().executeQuery("DROP TABLE default." + tableName);
        }
    }

    @Test(groups = {HUDI, PROFILE_SPECIFIC_TESTS})
    public void testCopyOnWriteTableSchemaEvolutionAlterStructNestedType() throws SQLException
    {
        String tableName = "test_hudi_cow_schema_evolution_alter_struct_nested_type_" + randomNameSuffix();

        createSchemaEvolutionTable(tableName, COW_TABLE_TYPE);
        try {
            onHudi().executeQuery("SET hoodie.schema.on.read.enable=true");
            onHudi().executeQuery("ALTER TABLE default." + tableName + " ALTER COLUMN test_struct_struct.employee.number TYPE LONG;");

            QueryResult selectAllResult = onTrino().executeQuery("SELECT id, test_struct_struct.employee FROM hudi.default." + tableName);
            assertEquals(selectAllResult.rows().size(), 2);
            for (List<?> row : selectAllResult.rows()) {
                int id = ((Long) row.get(0)).intValue();
                switch (id) {
                    case 0:
                        Row rowValueFirst = rowBuilder()
                                .addField("name", "2018-12-12")
                                .addField("number", 123L)
                                .addField("start_date", "2018-12-12")
                                .addField("birth", Date.valueOf("2018-12-12")).build();
                        assertStructEquals(row.get(1), new Object[] {rowValueFirst});
                        break;
                    case 1:
                        Row rowValueSecond = rowBuilder()
                                .addField("name", "2018-12-16")
                                .addField("number", 123L)
                                .addField("start_date", "2018-12-16")
                                .addField("birth", Date.valueOf("2018-12-16")).build();
                        assertStructEquals(row.get(1), new Object[] {rowValueSecond});
                        break;
                }
            }
        }
        finally {
            onHudi().executeQuery("DROP TABLE default." + tableName);
        }
    }

    @Test(groups = {HUDI, PROFILE_SPECIFIC_TESTS})
    public void testCopyOnWriteShowCreateTable()
    {
        String tableName = "test_hudi_cow_show_create_" + randomNameSuffix();

        createNonPartitionedTable(tableName, COW_TABLE_TYPE);

        try {
            Assertions.assertThat((String) onTrino().executeQuery("SHOW CREATE TABLE hudi.default." + tableName).getOnlyValue())
                    .isEqualTo(format(
                            "CREATE TABLE hudi.default.%s (\n" +
                                    "   _hoodie_commit_time varchar,\n" +
                                    "   _hoodie_commit_seqno varchar,\n" +
                                    "   _hoodie_record_key varchar,\n" +
                                    "   _hoodie_partition_path varchar,\n" +
                                    "   _hoodie_file_name varchar,\n" +
                                    "   id bigint,\n" +
                                    "   name varchar,\n" +
                                    "   price integer,\n" +
                                    "   ts bigint\n" +
                                    ")\n" +
                                    "WITH (\n" +
                                    "   location = 's3://%s/%s'\n" +
                                    ")",
                            tableName,
                            bucketName,
                            tableName));
            String lastCommitTimeSync = (String) onHudi().executeQuery("show TBLPROPERTIES " + tableName + " ('last_commit_time_sync')").project(2).getOnlyValue();
            Assertions.assertThat((String) onHudi().executeQuery("SHOW CREATE TABLE default." + tableName).getOnlyValue())
                    .isEqualTo(format("""
                                    CREATE TABLE default.%s (
                                      _hoodie_commit_time STRING,
                                      _hoodie_commit_seqno STRING,
                                      _hoodie_record_key STRING,
                                      _hoodie_partition_path STRING,
                                      _hoodie_file_name STRING,
                                      id BIGINT,
                                      name STRING,
                                      price INT,
                                      ts BIGINT)
                                    USING hudi
                                    LOCATION 's3://%s/%s'
                                    TBLPROPERTIES (
                                      'last_commit_time_sync' = '%s',
                                      'preCombineField' = 'ts',
                                      'primaryKey' = 'id',
                                      'type' = 'cow')
                                    """,
                            tableName,
                            bucketName,
                            tableName,
                            lastCommitTimeSync));
        }
        finally {
            onHudi().executeQuery("DROP TABLE default." + tableName);
        }
    }

    @Test(groups = {HUDI, PROFILE_SPECIFIC_TESTS})
    public void testCopyOnWriteTableSelect()
    {
        String tableName = "test_hudi_cow_select_" + randomNameSuffix();

        createNonPartitionedTable(tableName, COW_TABLE_TYPE);

        List<QueryAssert.Row> expectedRows = ImmutableList.of(
                row(1, "a1"),
                row(2, "a2"));

        try {
            assertThat(onHudi().executeQuery("SELECT id, name FROM default." + tableName))
                    .containsOnly(expectedRows);
            assertThat(onTrino().executeQuery("SELECT id, name FROM hudi.default." + tableName))
                    .containsOnly(expectedRows);
        }
        finally {
            onHudi().executeQuery("DROP TABLE default." + tableName);
        }
    }

    @Test(groups = {HUDI, PROFILE_SPECIFIC_TESTS})
    public void testCopyOnWritePartitionedTableSelect()
    {
        String tableName = "test_hudi_cow_partitioned_select_" + randomNameSuffix();

        createPartitionedTable(tableName, COW_TABLE_TYPE);

        List<QueryAssert.Row> expectedRows = ImmutableList.of(
                row(1, "a1", 1000, "2021-12-09", "10"),
                row(2, "a2", 1000, "2021-12-09", "11"));

        try {
            assertThat(onHudi().executeQuery("SELECT id, name, ts, dt, hh FROM default." + tableName))
                    .containsOnly(expectedRows);
            assertThat(onTrino().executeQuery("SELECT id, name, ts, dt, hh FROM hudi.default." + tableName))
                    .containsOnly(expectedRows);

            expectedRows = ImmutableList.of(row(2, "a2", 1000));
            assertThat(onTrino().executeQuery("SELECT id, name, ts FROM hudi.default." + tableName + " WHERE dt = '2021-12-09' AND hh = '11'"))
                    .containsOnly(expectedRows);
        }
        finally {
            onHudi().executeQuery("DROP TABLE default." + tableName);
        }
    }

    @Test(groups = {HUDI, PROFILE_SPECIFIC_TESTS})
    public void testCopyOnWriteTableSelectAfterUpdate()
    {
        String tableName = "test_hudi_cow_select_after_update" + randomNameSuffix();

        createPartitionedTable(tableName, COW_TABLE_TYPE);

        List<QueryAssert.Row> expectedRows = ImmutableList.of(
                row(1, "a1"),
                row(2, "a2"));

        try {
            assertThat(onHudi().executeQuery("SELECT id, name FROM default." + tableName))
                    .containsOnly(expectedRows);
            assertThat(onTrino().executeQuery("SELECT id, name FROM hudi.default." + tableName))
                    .containsOnly(expectedRows);

            onHudi().executeQuery("UPDATE default." + tableName + " SET name = 'a1_1', ts = 1001 WHERE id = 1");
            expectedRows = ImmutableList.of(
                    row(1, "a1_1", 1001),
                    row(2, "a2", 1000));
            assertThat(onHudi().executeQuery("SELECT id, name, ts FROM default." + tableName))
                    .containsOnly(expectedRows);
            assertThat(onTrino().executeQuery("SELECT id, name, ts FROM hudi.default." + tableName))
                    .containsOnly(expectedRows);
        }
        finally {
            onHudi().executeQuery("DROP TABLE default." + tableName);
        }
    }

    @Test(groups = {HUDI, PROFILE_SPECIFIC_TESTS})
    public void testMergeOnReadTableSelect()
    {
        String tableName = "test_hudi_mor_select_" + randomNameSuffix();

        createNonPartitionedTable(tableName, MOR_TABLE_TYPE);

        List<QueryAssert.Row> expectedRows = ImmutableList.of(
                row(1, "a1", 20, 1000),
                row(2, "a2", 40, 2000));

        try {
            assertThat(onHudi().executeQuery("SELECT id, name, price, ts FROM default." + tableName))
                    .containsOnly(expectedRows);
            assertThat(onTrino().executeQuery("SELECT id, name, price, ts FROM hudi.default." + tableName))
                    .containsOnly(expectedRows);
        }
        finally {
            onHudi().executeQuery("DROP TABLE default." + tableName);
        }
    }

    @Test(groups = {HUDI, PROFILE_SPECIFIC_TESTS})
    public void testMergeOnReadTableSelectAfterUpdate()
    {
        String tableName = "test_hudi_mor_update" + randomNameSuffix();

        createNonPartitionedTable(tableName, MOR_TABLE_TYPE);

        List<QueryAssert.Row> expectedRows = ImmutableList.of(
                row(1, "a1", 20, 1000),
                row(2, "a2", 40, 2000));

        try {
            assertThat(onHudi().executeQuery("SELECT id, name, price, ts FROM default." + tableName))
                    .containsOnly(expectedRows);
            assertThat(onTrino().executeQuery("SELECT id, name, price, ts FROM hudi.default." + tableName))
                    .containsOnly(expectedRows);

            onHudi().executeQuery("UPDATE default." + tableName + " SET ts = 2020 WHERE id = 2");
            List<QueryAssert.Row> expectedRowsAfterUpdate = ImmutableList.of(
                    row(1, "a1", 20, 1000),
                    row(2, "a2", 40, 2020));
            assertThat(onHudi().executeQuery("SELECT id, name, price, ts FROM default." + tableName))
                    .containsOnly(expectedRowsAfterUpdate);
            // NOTE: MOR Snapshot queries are not supported yet.
            // "_ro" suffix to the table indicates read-optimized query.
            assertThat(onTrino().executeQuery("SELECT id, name, price, ts FROM hudi.default." + tableName + "_ro"))
                    .containsOnly(expectedRows);
        }
        finally {
            onHudi().executeQuery("DROP TABLE default." + tableName);
        }
    }

    @Test(groups = {HUDI, PROFILE_SPECIFIC_TESTS})
    public void testMergeOnReadPartitionedTableSelect()
    {
        String tableName = "test_hudi_mor_partitioned_select_" + randomNameSuffix();

        createPartitionedTable(tableName, MOR_TABLE_TYPE);

        List<QueryAssert.Row> expectedRows = ImmutableList.of(
                row(1, "a1", 1000, "2021-12-09", "10"),
                row(2, "a2", 1000, "2021-12-09", "11"));

        try {
            assertThat(onHudi().executeQuery("SELECT id, name, ts, dt, hh FROM default." + tableName))
                    .containsOnly(expectedRows);
            assertThat(onTrino().executeQuery("SELECT id, name, ts, dt, hh FROM hudi.default." + tableName + "_ro"))
                    .containsOnly(expectedRows);

            expectedRows = ImmutableList.of(row(2, "a2", 1000));
            assertThat(onTrino().executeQuery("SELECT id, name, ts FROM hudi.default." + tableName + "_ro WHERE dt = '2021-12-09' AND hh = '11'"))
                    .containsOnly(expectedRows);
        }
        finally {
            onHudi().executeQuery("DROP TABLE default." + tableName);
        }
    }

    @Test(groups = {HUDI, PROFILE_SPECIFIC_TESTS})
    public void testCopyOnWriteTableSelectWithSessionProperties()
    {
        String tableName = "test_hudi_cow_select_session_props" + randomNameSuffix();

        createNonPartitionedTable(tableName, COW_TABLE_TYPE);

        try {
            assertThat(onTrino().executeQuery("SELECT id, name FROM hudi.default." + tableName))
                    .containsOnly(ImmutableList.of(
                            row(1, "a1"),
                            row(2, "a2")));
            onTrino().executeQuery(
                    "SET SESSION hudi.columns_to_hide = ARRAY['_hoodie_commit_time','_hoodie_commit_seqno','_hoodie_record_key','_hoodie_partition_path','_hoodie_file_name']");
            assertThat(onTrino().executeQuery("SELECT * FROM hudi.default." + tableName))
                    .containsOnly(ImmutableList.of(
                            row(1, "a1", 20, 1000),
                            row(2, "a2", 40, 2000)));
        }
        finally {
            onHudi().executeQuery("DROP TABLE default." + tableName);
        }
    }

    @Test(groups = {HUDI, PROFILE_SPECIFIC_TESTS})
    public void testTimelineTable()
    {
        String tableName = "test_hudi_timeline_system_table_" + randomNameSuffix();
        createNonPartitionedTable(tableName, COW_TABLE_TYPE);
        try {
            assertThat(onTrino().executeQuery(format("SELECT action, state FROM hudi.default.\"%s$timeline\"", tableName)))
                    .containsOnly(row("commit", "COMPLETED"));
        }
        finally {
            onHudi().executeQuery("DROP TABLE " + tableName);
        }
    }

    @Test(groups = {HUDI, PROFILE_SPECIFIC_TESTS})
    public void testTimelineTableRedirect()
    {
        String tableName = "test_hudi_timeline_system_table_redirect_" + randomNameSuffix();
        String nonExistingTableName = tableName + "_non_existing";
        createNonPartitionedTable(tableName, COW_TABLE_TYPE);
        try {
            assertThat(onTrino().executeQuery(format("SELECT action, state FROM hive.default.\"%s$timeline\"", tableName)))
                    .containsOnly(row("commit", "COMPLETED"));
            assertQueryFailure(() -> onTrino().executeQuery(format("SELECT * FROM hive.default.\"%s$timeline\"", nonExistingTableName)))
                    .hasMessageMatching(".*Table 'hive.default.test_hudi_timeline_system_table_redirect_.*_non_existing\\$timeline' does not exist");
        }
        finally {
            onHudi().executeQuery("DROP TABLE " + tableName);
        }
    }

    private void createNonPartitionedTable(String tableName, String tableType)
    {
        onHudi().executeQuery(format(
                """
                        CREATE TABLE default.%s (
                          id bigint,
                          name string,
                          price int,
                          ts bigint)
                        USING hudi
                        TBLPROPERTIES (
                          type = '%s',
                          primaryKey = 'id',
                          preCombineField = 'ts')
                        LOCATION 's3://%s/%s'""",
                tableName,
                tableType,
                bucketName,
                tableName));

        onHudi().executeQuery("INSERT INTO default." + tableName + " VALUES (1, 'a1', 20, 1000), (2, 'a2', 40, 2000)");
    }

    private void createSchemaEvolutionTable(String tableName, String tableType)
    {
        onHudi().executeQuery(format(
                """
                        CREATE TABLE default.%s (
                            id bigint,
                            test_int int,
                            test_long long,
                            test_float float,
                            test_double double,
                            test_date date,
                            test_string string,
                            test_decimal decimal(10,2),
                            test_array_struct array<struct<name:string, number:int, start_date:string, birth:date>>,
                            test_map_struct map<string, struct<name:string, number:int, start_date:string, birth:date>>,
                            test_struct_struct struct<employee:struct<name:string, number:int, start_date:string, birth:date>, phone:struct<country_code:string, number:string>>)
                        USING hudi
                        TBLPROPERTIES (
                          type = '%s',
                          primaryKey = 'id')
                        LOCATION 's3://%s/%s'""",
                tableName,
                tableType,
                bucketName,
                tableName));

        onHudi().executeQuery("INSERT INTO default." + tableName +
                " VALUES (1, 1, CAST(1 AS LONG), CAST(1 AS FLOAT), CAST(1 AS DOUBLE), DATE '2018-12-12', '2018-12-12', CAST(1.11 AS DECIMAL(10,2))," +
                "ARRAY(STRUCT('2018-12-12', 123, '2018-12-12', DATE '2018-12-12')), MAP('row key', STRUCT('STRUCT 1', 123, '2018-12-12', DATE '2018-12-12'))," +
                "STRUCT(STRUCT('STRUCT 1', 123, '2018-12-12', DATE '2018-12-12'), STRUCT('STRUCT 1', '2018-12-12'))), " +
                "(2, 2, CAST(2 AS LONG), CAST(2 AS FLOAT), CAST(2 AS DOUBLE), DATE '2018-12-16', '2018-12-16', CAST(2.22 AS DECIMAL(10,2))," +
                "ARRAY(STRUCT('2018-12-16', 123, '2018-12-16', DATE '2018-12-16')), MAP('row key', STRUCT('STRUCT 2', 123, '2018-12-16', DATE '2018-12-16'))," +
                "STRUCT(STRUCT('STRUCT 2', 123, '2018-12-16', DATE '2018-12-16'), STRUCT('STRUCT 2', '2018-12-16')))");
    }

    private void createPartitionedTable(String tableName, String tableType)
    {
        onHudi().executeQuery(format(
                """
                        CREATE TABLE default.%s (
                          id bigint,
                          name string,
                          ts bigint,
                          dt string,
                          hh string)
                        USING hudi
                        TBLPROPERTIES (
                          type = '%s',
                          primaryKey = 'id',
                          preCombineField = 'ts')
                        PARTITIONED BY (dt, hh)
                        LOCATION 's3://%s/%s'""",
                tableName,
                tableType,
                bucketName,
                tableName));

        onHudi().executeQuery("INSERT INTO default." + tableName + " PARTITION (dt, hh) SELECT 1 AS id, 'a1' AS name, 1000 AS ts, '2021-12-09' AS dt, '10' AS hh");
        onHudi().executeQuery("INSERT INTO default." + tableName + " PARTITION (dt = '2021-12-09', hh='11') SELECT 2, 'a2', 1000");
    }

    private static void assertStructEquals(Object actual, Object[] expected)
    {
        Assertions.assertThat(actual).isInstanceOf(Row.class);
        Row actualRow = (Row) actual;
        assertEquals(actualRow.getFields().size(), expected.length);
        for (int i = 0; i < actualRow.getFields().size(); i++) {
            assertEquals(actualRow.getFields().get(i).getValue(), expected[i]);
        }
    }

    private static io.trino.jdbc.Row.Builder rowBuilder()
    {
        return io.trino.jdbc.Row.builder();
    }
}
