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
import io.trino.tempto.RequirementsProvider;
import io.trino.tempto.Requires;
import io.trino.tempto.configuration.Configuration;
import io.trino.tempto.fulfillment.table.MutableTableRequirement;
import io.trino.tempto.fulfillment.table.hive.HiveTableDefinition;
import io.trino.tempto.query.QueryResult;
import org.testng.annotations.Test;

import java.util.Optional;

import static io.trino.tempto.Requirements.compose;
import static io.trino.tempto.assertions.QueryAssert.Row.row;
import static io.trino.tempto.fulfillment.table.MutableTableRequirement.State.CREATED;
import static io.trino.tests.product.TestGroups.HIVE_COERCION;
import static io.trino.tests.product.TestGroups.JDBC;
import static io.trino.tests.product.utils.QueryExecutors.onHive;
import static io.trino.tests.product.utils.QueryExecutors.onTrino;
import static java.lang.String.format;
import static java.sql.JDBCType.BIGINT;
import static java.sql.JDBCType.DOUBLE;
import static java.util.Locale.ENGLISH;
import static org.assertj.core.api.Assertions.assertThat;

public class TestHiveCoercionOnPartitionedTable
        extends BaseTestHiveCoercion
{
    public static final HiveTableDefinition HIVE_COERCION_TEXTFILE = tableDefinitionBuilder("TEXTFILE", Optional.empty(), Optional.of("DELIMITED FIELDS TERMINATED BY '|'"))
            .setNoData()
            .build();

    public static final HiveTableDefinition HIVE_TIMESTAMP_COERCION_TEXTFILE = tableDefinitionForTimestampCoercionBuilder("TEXTFILE", Optional.empty(), Optional.of("DELIMITED FIELDS TERMINATED BY '|'"))
            .setNoData()
            .build();

    public static final HiveTableDefinition HIVE_COERCION_PARQUET = tableDefinitionBuilder("PARQUET", Optional.empty(), Optional.empty())
            .setNoData()
            .build();

    public static final HiveTableDefinition HIVE_TIMESTAMP_COERCION_PARQUET = tableDefinitionForTimestampCoercionBuilder("PARQUET", Optional.empty(), Optional.empty())
            .setNoData()
            .build();

    public static final HiveTableDefinition HIVE_COERCION_AVRO = avroTableDefinitionBuilder()
            .setNoData()
            .build();

    public static final HiveTableDefinition HIVE_COERCION_ORC = tableDefinitionBuilder("ORC", Optional.empty(), Optional.empty())
            .setNoData()
            .build();

    public static final HiveTableDefinition HIVE_TIMESTAMP_COERCION_ORC = tableDefinitionForTimestampCoercionBuilder("ORC", Optional.empty(), Optional.empty())
            .setNoData()
            .build();

    public static final HiveTableDefinition HIVE_COERCION_RCTEXT = tableDefinitionBuilder("RCFILE", Optional.of("RCTEXT"), Optional.of("SERDE 'org.apache.hadoop.hive.serde2.columnar.ColumnarSerDe'"))
            .setNoData()
            .build();

    public static final HiveTableDefinition HIVE_TIMESTAMP_COERCION_RCTEXT = tableDefinitionForTimestampCoercionBuilder("RCFILE", Optional.of("RCTEXT"), Optional.of("SERDE 'org.apache.hadoop.hive.serde2.columnar.ColumnarSerDe'"))
            .setNoData()
            .build();

    public static final HiveTableDefinition HIVE_COERCION_RCBINARY = tableDefinitionBuilder("RCFILE", Optional.of("RCBINARY"), Optional.of("SERDE 'org.apache.hadoop.hive.serde2.columnar.LazyBinaryColumnarSerDe'"))
            .setNoData()
            .build();

    public static final HiveTableDefinition HIVE_TIMESTAMP_COERCION_RCBINARY = tableDefinitionForTimestampCoercionBuilder("RCFILE", Optional.of("RCBINARY"), Optional.of("SERDE 'org.apache.hadoop.hive.serde2.columnar.LazyBinaryColumnarSerDe'"))
            .setNoData()
            .build();

    private static HiveTableDefinition.HiveTableDefinitionBuilder tableDefinitionBuilder(String fileFormat, Optional<String> recommendTableName, Optional<String> rowFormat)
    {
        String tableName = format("%s_hive_coercion", recommendTableName.orElse(fileFormat).toLowerCase(ENGLISH));
        String floatType = fileFormat.toLowerCase(ENGLISH).contains("parquet") ? "DOUBLE" : "FLOAT";
        return HiveTableDefinition.builder(tableName)
                .setCreateTableDDLTemplate("" +
                        "CREATE TABLE %NAME%(" +
                        // all nested primitive coercions and adding/removing trailing nested fields are covered across row_to_row, list_to_list, and map_to_map
                        "    row_to_row                 STRUCT<keep: STRING, ti2si: TINYINT, si2int: SMALLINT, int2bi: INT, bi2vc: BIGINT, lower2uppercase: BIGINT>, " +
                        "    list_to_list               ARRAY<STRUCT<ti2int: TINYINT, si2bi: SMALLINT, bi2vc: BIGINT, remove: STRING>>, " +
                        "    map_to_map                 MAP<TINYINT, STRUCT<ti2bi: TINYINT, int2bi: INT, float2double: " + floatType + ">>, " +
                        "    tinyint_to_smallint        TINYINT," +
                        "    tinyint_to_int             TINYINT," +
                        "    tinyint_to_bigint          TINYINT," +
                        "    smallint_to_int            SMALLINT," +
                        "    smallint_to_bigint         SMALLINT," +
                        "    int_to_bigint              INT," +
                        "    bigint_to_varchar          BIGINT," +
                        "    float_to_double            " + floatType + "," +
                        "    double_to_float            DOUBLE," +
                        "    shortdecimal_to_shortdecimal          DECIMAL(10,2)," +
                        "    shortdecimal_to_longdecimal           DECIMAL(10,2)," +
                        "    longdecimal_to_shortdecimal           DECIMAL(20,12)," +
                        "    longdecimal_to_longdecimal            DECIMAL(20,12)," +
                        "    float_to_decimal           " + floatType + "," +
                        "    double_to_decimal          DOUBLE," +
                        "    decimal_to_float                   DECIMAL(10,5)," +
                        "    decimal_to_double                  DECIMAL(10,5)," +
                        "    short_decimal_to_varchar           DECIMAL(10,5)," +
                        "    long_decimal_to_varchar            DECIMAL(20,12)," +
                        "    short_decimal_to_bounded_varchar   DECIMAL(10,5)," +
                        "    long_decimal_to_bounded_varchar    DECIMAL(20,12)," +
                        "    varchar_to_bigger_varchar          VARCHAR(3)," +
                        "    varchar_to_smaller_varchar         VARCHAR(3)," +
                        "    char_to_bigger_char                CHAR(3)," +
                        "    char_to_smaller_char               CHAR(3)," +
                        "    timestamp_to_string                TIMESTAMP," +
                        "    timestamp_to_bounded_varchar       TIMESTAMP," +
                        "    timestamp_to_smaller_varchar       TIMESTAMP," +
                        "    smaller_varchar_to_timestamp       VARCHAR(4)," +
                        "    varchar_to_timestamp               STRING" +
                        ") " +
                        "PARTITIONED BY (id BIGINT) " +
                        rowFormat.map(s -> format("ROW FORMAT %s ", s)).orElse("") +
                        "STORED AS " + fileFormat);
    }

    private static HiveTableDefinition.HiveTableDefinitionBuilder tableDefinitionForTimestampCoercionBuilder(String fileFormat, Optional<String> tablePrefix, Optional<String> rowFormat)
    {
        String tableName = format("%s_hive_timestamp_coercion", tablePrefix.orElse(fileFormat).toLowerCase(ENGLISH));
        return HiveTableDefinition.builder(tableName)
                .setCreateTableDDLTemplate("" +
                        "CREATE TABLE %NAME%(" +
                        "    timestamp_row_to_row                 STRUCT<keep: TIMESTAMP, si2i: SMALLINT, timestamp2string: TIMESTAMP, string2timestamp: STRING>, " +
                        "    timestamp_list_to_list               ARRAY<STRUCT<keep: TIMESTAMP, si2i: SMALLINT, timestamp2string: TIMESTAMP, string2timestamp: STRING>>, " +
                        "    timestamp_map_to_map                 MAP<SMALLINT, STRUCT<keep: TIMESTAMP, si2i: SMALLINT, timestamp2string: TIMESTAMP, string2timestamp: STRING>>," +
                        "    timestamp_to_string                  TIMESTAMP," +
                        "    string_to_timestamp                  STRING" +
                        ") " +
                        "PARTITIONED BY (id BIGINT) " +
                        rowFormat.map(s -> format("ROW FORMAT %s ", s)).orElse("") +
                        "STORED AS " + fileFormat);
    }

    private static HiveTableDefinition.HiveTableDefinitionBuilder avroTableDefinitionBuilder()
    {
        return HiveTableDefinition.builder("avro_hive_coercion")
                .setCreateTableDDLTemplate("" +
                        "CREATE TABLE %NAME%(" +
                        "    int_to_bigint              INT," +
                        "    float_to_double            DOUBLE" +
                        ") " +
                        "PARTITIONED BY (id BIGINT) " +
                        "STORED AS AVRO");
    }

    public static final class TextRequirements
            implements RequirementsProvider
    {
        @Override
        public Requirement getRequirements(Configuration configuration)
        {
            return compose(
                    MutableTableRequirement.builder(HIVE_COERCION_TEXTFILE).withState(CREATED).build(),
                    MutableTableRequirement.builder(HIVE_TIMESTAMP_COERCION_TEXTFILE).withState(CREATED).build());
        }
    }

    public static final class OrcRequirements
            implements RequirementsProvider
    {
        @Override
        public Requirement getRequirements(Configuration configuration)
        {
            return compose(
                    MutableTableRequirement.builder(HIVE_COERCION_ORC).withState(CREATED).build(),
                    MutableTableRequirement.builder(HIVE_TIMESTAMP_COERCION_ORC).withState(CREATED).build());
        }
    }

    public static final class RcTextRequirements
            implements RequirementsProvider
    {
        @Override
        public Requirement getRequirements(Configuration configuration)
        {
            return compose(
                    MutableTableRequirement.builder(HIVE_COERCION_RCTEXT).withState(CREATED).build(),
                    MutableTableRequirement.builder(HIVE_TIMESTAMP_COERCION_RCTEXT).withState(CREATED).build());
        }
    }

    public static final class RcBinaryRequirements
            implements RequirementsProvider
    {
        @Override
        public Requirement getRequirements(Configuration configuration)
        {
            return compose(
                    MutableTableRequirement.builder(HIVE_COERCION_RCBINARY).withState(CREATED).build(),
                    MutableTableRequirement.builder(HIVE_TIMESTAMP_COERCION_RCBINARY).withState(CREATED).build());
        }
    }

    public static final class ParquetRequirements
            implements RequirementsProvider
    {
        @Override
        public Requirement getRequirements(Configuration configuration)
        {
            return compose(
                    MutableTableRequirement.builder(HIVE_COERCION_PARQUET).withState(CREATED).build(),
                    MutableTableRequirement.builder(HIVE_TIMESTAMP_COERCION_PARQUET).withState(CREATED).build());
        }
    }

    public static final class AvroRequirements
            implements RequirementsProvider
    {
        @Override
        public Requirement getRequirements(Configuration configuration)
        {
            return MutableTableRequirement.builder(HIVE_COERCION_AVRO).withState(CREATED).build();
        }
    }

    @Requires(TextRequirements.class)
    @Test(groups = {HIVE_COERCION, JDBC})
    public void testHiveCoercionTextFile()
    {
        doTestHiveCoercion(HIVE_COERCION_TEXTFILE);
    }

    @Requires(TextRequirements.class)
    @Test(groups = {HIVE_COERCION, JDBC})
    public void testHiveCoercionWithDifferentTimestampPrecisionTextFile()
    {
        doTestHiveCoercionWithDifferentTimestampPrecision(HIVE_TIMESTAMP_COERCION_TEXTFILE);
    }

    @Requires(OrcRequirements.class)
    @Test(groups = {HIVE_COERCION, JDBC})
    public void testHiveCoercionOrc()
    {
        doTestHiveCoercion(HIVE_COERCION_ORC);
    }

    @Requires(OrcRequirements.class)
    @Test(groups = {HIVE_COERCION, JDBC})
    public void testHiveCoercionWithDifferentTimestampPrecisionOrc()
    {
        doTestHiveCoercionWithDifferentTimestampPrecision(HIVE_TIMESTAMP_COERCION_ORC);
    }

    @Requires(RcTextRequirements.class)
    @Test(groups = {HIVE_COERCION, JDBC})
    public void testHiveCoercionRcText()
    {
        doTestHiveCoercion(HIVE_COERCION_RCTEXT);
    }

    @Requires(RcTextRequirements.class)
    @Test(groups = {HIVE_COERCION, JDBC})
    public void testHiveCoercionWithDifferentTimestampPrecisionRcText()
    {
        doTestHiveCoercionWithDifferentTimestampPrecision(HIVE_TIMESTAMP_COERCION_RCTEXT);
    }

    @Requires(RcBinaryRequirements.class)
    @Test(groups = {HIVE_COERCION, JDBC})
    public void testHiveCoercionRcBinary()
    {
        doTestHiveCoercion(HIVE_COERCION_RCBINARY);
    }

    @Requires(RcBinaryRequirements.class)
    @Test(groups = {HIVE_COERCION, JDBC})
    public void testHiveCoercionWithDifferentTimestampPrecisionRcBinary()
    {
        doTestHiveCoercionWithDifferentTimestampPrecision(HIVE_TIMESTAMP_COERCION_RCBINARY);
    }

    @Requires(ParquetRequirements.class)
    @Test(groups = {HIVE_COERCION, JDBC})
    public void testHiveCoercionParquet()
    {
        doTestHiveCoercion(HIVE_COERCION_PARQUET);
    }

    @Requires(ParquetRequirements.class)
    @Test(groups = {HIVE_COERCION, JDBC})
    public void testHiveCoercionWithDifferentTimestampPrecisionParquet()
    {
        doTestHiveCoercionWithDifferentTimestampPrecision(HIVE_TIMESTAMP_COERCION_PARQUET);
    }

    @Requires(AvroRequirements.class)
    @Test(groups = {HIVE_COERCION, JDBC})
    public void testHiveCoercionAvro()
    {
        String tableName = mutableTableInstanceOf(HIVE_COERCION_AVRO).getNameInDatabase();

        onHive().executeQuery(format("INSERT INTO TABLE %s " +
                        "PARTITION (id=1) " +
                        "VALUES" +
                        "(2323, 0.5)," +
                        "(-2323, -1.5)",
                tableName));

        onHive().executeQuery(format("ALTER TABLE %s CHANGE COLUMN int_to_bigint int_to_bigint bigint", tableName));
        onHive().executeQuery(format("ALTER TABLE %s CHANGE COLUMN float_to_double float_to_double double", tableName));

        assertThat(onTrino().executeQuery("SHOW COLUMNS FROM " + tableName).project(1, 2)).containsExactlyInOrder(
                row("int_to_bigint", "bigint"),
                row("float_to_double", "double"),
                row("id", "bigint"));

        QueryResult queryResult = onTrino().executeQuery("SELECT * FROM " + tableName);
        assertThat(queryResult).hasColumns(BIGINT, DOUBLE, BIGINT);

        assertThat(queryResult).containsOnly(
                row(2323L, 0.5, 1),
                row(-2323L, -1.5, 1));
    }
}
