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
package io.trino.tests.product.hive;

import io.trino.tempto.Requirement;
import io.trino.tempto.Requirements;
import io.trino.tempto.RequirementsProvider;
import io.trino.tempto.Requires;
import io.trino.tempto.configuration.Configuration;
import io.trino.tempto.fulfillment.table.MutableTableRequirement;
import io.trino.tempto.fulfillment.table.MutableTablesState;
import io.trino.tempto.fulfillment.table.TableDefinition;
import io.trino.tempto.fulfillment.table.TableHandle;
import io.trino.tempto.fulfillment.table.TableInstance;
import io.trino.tempto.query.QueryResult;
import org.testng.annotations.Test;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Timestamp;
import java.time.LocalDateTime;

import static io.trino.tempto.assertions.QueryAssert.Row.row;
import static io.trino.tempto.assertions.QueryAssert.assertThat;
import static io.trino.tempto.context.ThreadLocalTestContextHolder.testContext;
import static io.trino.tempto.fulfillment.table.MutableTableRequirement.State.CREATED;
import static io.trino.tempto.fulfillment.table.TableHandle.tableHandle;
import static io.trino.tempto.fulfillment.table.TableRequirements.immutableTable;
import static io.trino.tests.product.TestGroups.JDBC;
import static io.trino.tests.product.TestGroups.SMOKE;
import static io.trino.tests.product.hive.AllSimpleTypesTableDefinitions.ALL_HIVE_SIMPLE_TYPES_AVRO;
import static io.trino.tests.product.hive.AllSimpleTypesTableDefinitions.ALL_HIVE_SIMPLE_TYPES_ORC;
import static io.trino.tests.product.hive.AllSimpleTypesTableDefinitions.ALL_HIVE_SIMPLE_TYPES_PARQUET;
import static io.trino.tests.product.hive.AllSimpleTypesTableDefinitions.ALL_HIVE_SIMPLE_TYPES_RCFILE;
import static io.trino.tests.product.hive.AllSimpleTypesTableDefinitions.ALL_HIVE_SIMPLE_TYPES_TEXTFILE;
import static io.trino.tests.product.hive.AllSimpleTypesTableDefinitions.populateDataToHiveTable;
import static io.trino.tests.product.utils.QueryExecutors.onHive;
import static io.trino.tests.product.utils.QueryExecutors.onTrino;
import static java.lang.String.format;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.sql.JDBCType.BIGINT;
import static java.sql.JDBCType.BOOLEAN;
import static java.sql.JDBCType.CHAR;
import static java.sql.JDBCType.DATE;
import static java.sql.JDBCType.DECIMAL;
import static java.sql.JDBCType.DOUBLE;
import static java.sql.JDBCType.INTEGER;
import static java.sql.JDBCType.REAL;
import static java.sql.JDBCType.SMALLINT;
import static java.sql.JDBCType.TIMESTAMP;
import static java.sql.JDBCType.TINYINT;
import static java.sql.JDBCType.VARBINARY;
import static java.sql.JDBCType.VARCHAR;

public class TestAllDatatypesFromHiveConnector
        extends HiveProductTest
{
    public static final class TextRequirements
            implements RequirementsProvider
    {
        @Override
        public Requirement getRequirements(Configuration configuration)
        {
            return immutableTable(ALL_HIVE_SIMPLE_TYPES_TEXTFILE);
        }
    }

    public static final class OrcRequirements
            implements RequirementsProvider
    {
        @Override
        public Requirement getRequirements(Configuration configuration)
        {
            return Requirements.compose(
                    MutableTableRequirement.builder(ALL_HIVE_SIMPLE_TYPES_ORC).withState(CREATED).build(),
                    immutableTable(ALL_HIVE_SIMPLE_TYPES_TEXTFILE));
        }
    }

    public static final class RcfileRequirements
            implements RequirementsProvider
    {
        @Override
        public Requirement getRequirements(Configuration configuration)
        {
            return Requirements.compose(
                    MutableTableRequirement.builder(ALL_HIVE_SIMPLE_TYPES_RCFILE).withState(CREATED).build(),
                    immutableTable(ALL_HIVE_SIMPLE_TYPES_TEXTFILE));
        }
    }

    public static final class ParquetRequirements
            implements RequirementsProvider
    {
        @Override
        public Requirement getRequirements(Configuration configuration)
        {
            return MutableTableRequirement.builder(ALL_HIVE_SIMPLE_TYPES_PARQUET).withState(CREATED).build();
        }
    }

    public static final class AvroRequirements
            implements RequirementsProvider
    {
        @Override
        public Requirement getRequirements(Configuration configuration)
        {
            return Requirements.compose(
                    MutableTableRequirement.builder(ALL_HIVE_SIMPLE_TYPES_AVRO).withState(CREATED).build(),
                    immutableTable(ALL_HIVE_SIMPLE_TYPES_TEXTFILE));
        }
    }

    @Requires(TextRequirements.class)
    @Test(groups = SMOKE)
    public void testSelectAllDatatypesTextFile()
    {
        String tableName = ALL_HIVE_SIMPLE_TYPES_TEXTFILE.getName();

        assertProperAllDatatypesSchema(tableName);
        QueryResult queryResult = onTrino().executeQuery(format("SELECT * FROM %s", tableName));

        assertColumnTypes(queryResult);
        assertThat(queryResult).containsOnly(
                row(
                        127,
                        32767,
                        2147483647,
                        9223372036854775807L,
                        123.345f,
                        234.567,
                        new BigDecimal("346"),
                        new BigDecimal("345.67800"),
                        Timestamp.valueOf(LocalDateTime.of(2015, 5, 10, 12, 15, 35, 123_000_000)),
                        Date.valueOf("2015-05-10"),
                        "ala ma kota",
                        "ala ma kot",
                        "ala ma    ",
                        true,
                        "kot binarny".getBytes(UTF_8)));
    }

    @Requires(OrcRequirements.class)
    @Test(groups = JDBC)
    public void testSelectAllDatatypesOrc()
    {
        String tableName = mutableTableInstanceOf(ALL_HIVE_SIMPLE_TYPES_ORC).getNameInDatabase();

        populateDataToHiveTable(tableName);

        assertProperAllDatatypesSchema(tableName);

        QueryResult queryResult = onTrino().executeQuery(format("SELECT * FROM %s", tableName));
        assertColumnTypes(queryResult);
        assertThat(queryResult).containsOnly(
                row(
                        127,
                        32767,
                        2147483647,
                        9223372036854775807L,
                        123.345f,
                        234.567,
                        new BigDecimal("346"),
                        new BigDecimal("345.67800"),
                        Timestamp.valueOf(LocalDateTime.of(2015, 5, 10, 12, 15, 35, 123_000_000)),
                        Date.valueOf("2015-05-10"),
                        "ala ma kota",
                        "ala ma kot",
                        "ala ma    ",
                        true,
                        "kot binarny".getBytes(UTF_8)));
    }

    @Requires(RcfileRequirements.class)
    @Test(groups = JDBC)
    public void testSelectAllDatatypesRcfile()
    {
        String tableName = mutableTableInstanceOf(ALL_HIVE_SIMPLE_TYPES_RCFILE).getNameInDatabase();

        populateDataToHiveTable(tableName);

        assertProperAllDatatypesSchema(tableName);

        QueryResult queryResult = onTrino().executeQuery(format("SELECT * FROM %s", tableName));
        assertColumnTypes(queryResult);
        assertThat(queryResult).containsOnly(
                row(
                        127,
                        32767,
                        2147483647,
                        9223372036854775807L,
                        123.345f,
                        234.567,
                        new BigDecimal("346"),
                        new BigDecimal("345.67800"),
                        Timestamp.valueOf(LocalDateTime.of(2015, 5, 10, 12, 15, 35, 123_000_000)),
                        Date.valueOf("2015-05-10"),
                        "ala ma kota",
                        "ala ma kot",
                        "ala ma    ",
                        true,
                        "kot binarny".getBytes(UTF_8)));
    }

    @Requires(AvroRequirements.class)
    @Test(groups = JDBC)
    public void testSelectAllDatatypesAvro()
    {
        String tableName = mutableTableInstanceOf(ALL_HIVE_SIMPLE_TYPES_AVRO).getNameInDatabase();

        onHive().executeQuery(format("INSERT INTO %s VALUES(" +
                        "2147483647," +
                        "9223372036854775807," +
                        "123.345," +
                        "234.567," +
                        "346," +
                        "345.67800," +
                        "'" + Timestamp.valueOf(LocalDateTime.of(2015, 5, 10, 12, 15, 35, 123_000_000)).toString() + "'," +
                        "'" + Date.valueOf("2015-05-10") + "'," +
                        "'ala ma kota'," +
                        "'ala ma kot'," +
                        "'ala ma    '," +
                        "true," +
                        "'kot binarny'" +
                        ")",
                tableName));

        assertThat(onTrino().executeQuery("SHOW COLUMNS FROM " + tableName).project(1, 2)).containsExactlyInOrder(
                row("c_int", "integer"),
                row("c_bigint", "bigint"),
                row("c_float", "real"),
                row("c_double", "double"),
                row("c_decimal", "decimal(10,0)"),
                row("c_decimal_w_params", "decimal(10,5)"),
                row("c_timestamp", "timestamp(3)"),
                row("c_date", "date"),
                row("c_string", "varchar"),
                row("c_varchar", "varchar(10)"),
                row("c_char", "char(10)"),
                row("c_boolean", "boolean"),
                row("c_binary", "varbinary"));

        QueryResult queryResult = onTrino().executeQuery("SELECT * FROM " + tableName);
        assertThat(queryResult).hasColumns(
                INTEGER,
                BIGINT,
                REAL,
                DOUBLE,
                DECIMAL,
                DECIMAL,
                TIMESTAMP,
                DATE,
                VARCHAR,
                VARCHAR,
                CHAR,
                BOOLEAN,
                VARBINARY);

        assertThat(queryResult).containsOnly(
                row(
                        2147483647,
                        9223372036854775807L,
                        123.345f,
                        234.567,
                        new BigDecimal("346"),
                        new BigDecimal("345.67800"),
                        isHiveWithBrokenAvroTimestamps()
                                // TODO (https://github.com/trinodb/trino/issues/1218) requires https://issues.apache.org/jira/browse/HIVE-21002
                                ? Timestamp.valueOf(LocalDateTime.of(2015, 5, 10, 18, 0, 35, 123_000_000))
                                : Timestamp.valueOf(LocalDateTime.of(2015, 5, 10, 12, 15, 35, 123_000_000)),
                        Date.valueOf("2015-05-10"),
                        "ala ma kota",
                        "ala ma kot",
                        "ala ma    ",
                        true,
                        "kot binarny".getBytes(UTF_8)));
    }

    private void assertProperAllDatatypesSchema(String tableName)
    {
        assertThat(onTrino().executeQuery("SHOW COLUMNS FROM " + tableName).project(1, 2)).containsExactlyInOrder(
                row("c_tinyint", "tinyint"),
                row("c_smallint", "smallint"),
                row("c_int", "integer"),
                row("c_bigint", "bigint"),
                row("c_float", "real"),
                row("c_double", "double"),
                row("c_decimal", "decimal(10,0)"),
                row("c_decimal_w_params", "decimal(10,5)"),
                row("c_timestamp", "timestamp(3)"),
                row("c_date", "date"),
                row("c_string", "varchar"),
                row("c_varchar", "varchar(10)"),
                row("c_char", "char(10)"),
                row("c_boolean", "boolean"),
                row("c_binary", "varbinary"));
    }

    private void assertColumnTypes(QueryResult queryResult)
    {
        assertThat(queryResult).hasColumns(
                TINYINT,
                SMALLINT,
                INTEGER,
                BIGINT,
                REAL,
                DOUBLE,
                DECIMAL,
                DECIMAL,
                TIMESTAMP,
                DATE,
                VARCHAR,
                VARCHAR,
                CHAR,
                BOOLEAN,
                VARBINARY);
    }

    private void assertColumnTypesParquet(QueryResult queryResult)
    {
        assertThat(queryResult).hasColumns(
                TINYINT,
                SMALLINT,
                INTEGER,
                BIGINT,
                REAL,
                DOUBLE,
                DECIMAL,
                DECIMAL,
                TIMESTAMP,
                VARCHAR,
                VARCHAR,
                CHAR,
                BOOLEAN,
                VARBINARY);
    }

    @Requires(ParquetRequirements.class)
    @Test
    public void testSelectAllDatatypesParquetFile()
    {
        String tableName = mutableTableInstanceOf(ALL_HIVE_SIMPLE_TYPES_PARQUET).getNameInDatabase();

        onHive().executeQuery(format("INSERT INTO %s VALUES(" +
                "127," +
                "32767," +
                "2147483647," +
                "9223372036854775807," +
                "123.345," +
                "234.567," +
                "346," +
                "345.67800," +
                "'" + Timestamp.valueOf(LocalDateTime.of(2015, 5, 10, 12, 15, 35, 123_000_000)).toString() + "'," +
                "'ala ma kota'," +
                "'ala ma kot'," +
                "'ala ma    '," +
                "true," +
                "'kot binarny'" +
                ")", tableName));

        assertThat(onTrino().executeQuery(format("SHOW COLUMNS FROM %s", tableName)).project(1, 2)).containsExactlyInOrder(
                row("c_tinyint", "tinyint"),
                row("c_smallint", "smallint"),
                row("c_int", "integer"),
                row("c_bigint", "bigint"),
                row("c_float", "real"),
                row("c_double", "double"),
                row("c_decimal", "decimal(10,0)"),
                row("c_decimal_w_params", "decimal(10,5)"),
                row("c_timestamp", "timestamp(3)"),
                row("c_string", "varchar"),
                row("c_varchar", "varchar(10)"),
                row("c_char", "char(10)"),
                row("c_boolean", "boolean"),
                row("c_binary", "varbinary"));

        QueryResult queryResult = onTrino().executeQuery(format("SELECT * FROM %s", tableName));
        assertColumnTypesParquet(queryResult);
        assertThat(queryResult).containsOnly(
                row(
                        127,
                        32767,
                        2147483647,
                        9223372036854775807L,
                        123.345f,
                        234.567,
                        new BigDecimal("346"),
                        new BigDecimal("345.67800"),
                        Timestamp.valueOf(LocalDateTime.of(2015, 5, 10, 12, 15, 35, 123_000_000)),
                        "ala ma kota",
                        "ala ma kot",
                        "ala ma    ",
                        true,
                        "kot binarny".getBytes(UTF_8)));
    }

    private static TableInstance<?> mutableTableInstanceOf(TableDefinition tableDefinition)
    {
        if (tableDefinition.getDatabase().isPresent()) {
            return mutableTableInstanceOf(tableDefinition, tableDefinition.getDatabase().get());
        }
        else {
            return mutableTableInstanceOf(tableHandleInSchema(tableDefinition));
        }
    }

    private static TableInstance<?> mutableTableInstanceOf(TableDefinition tableDefinition, String database)
    {
        return mutableTableInstanceOf(tableHandleInSchema(tableDefinition).inDatabase(database));
    }

    private static TableInstance<?> mutableTableInstanceOf(TableHandle tableHandle)
    {
        return testContext().getDependency(MutableTablesState.class).get(tableHandle);
    }

    private static TableHandle tableHandleInSchema(TableDefinition tableDefinition)
    {
        TableHandle tableHandle = tableHandle(tableDefinition.getName());
        if (tableDefinition.getSchema().isPresent()) {
            tableHandle = tableHandle.inSchema(tableDefinition.getSchema().get());
        }
        return tableHandle;
    }
}
