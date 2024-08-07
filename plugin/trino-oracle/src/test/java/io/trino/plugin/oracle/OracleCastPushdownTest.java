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
package io.trino.plugin.oracle;

import com.google.common.collect.ImmutableList;
import io.trino.Session;
import io.trino.plugin.jdbc.BaseCastPushdownTest;
import io.trino.plugin.jdbc.CastDataTypeTest;
import io.trino.sql.planner.plan.ProjectNode;
import io.trino.testing.QueryRunner;
import io.trino.testing.sql.SqlExecutor;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.util.List;
import java.util.Optional;

import static io.trino.plugin.jdbc.CastDataTypeTest.CastTestCase;
import static java.util.Arrays.asList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;

@TestInstance(PER_CLASS)
abstract class OracleCastPushdownTest
        extends BaseCastPushdownTest
{
    public OracleCastPushdownTest(Session session, QueryRunner queryRunner, SqlExecutor sqlExecutor)
    {
        super(session, queryRunner, sqlExecutor);
    }

    @Override
    protected void setupTable()
    {
        table1 = CastDataTypeTest.create()
                .addColumn("id", "number(10)", asList(11, 12, 13))
                .addColumn("c_number_3", "number(3)", asList(1, 2, null)) // tinyint in trino
                .addColumn("c_number_5", "number(5)", asList(1, 2, null)) // smallint in trino
                .addColumn("c_number_10", "number(10)", asList(1, 2, null)) // integer in trino
                .addColumn("c_number_19", "number(19)", asList(1, 2, null)) // bigint in trino
                .addColumn("c_float", "float", asList(1.23, 2.67, null)) // double in trino
                .addColumn("c_float_5", "float(5)", asList(1.23, 2.67, null)) // double in trino
                .addColumn("c_binary_float", "binary_float", asList(1.23, 2.67, null))
                .addColumn("c_binary_double", "binary_double", asList(1.23, 2.67, null))
                .addColumn("c_number_15", "decimal(15)", asList(1, 2, null))
                .addColumn("c_number_10_2", "decimal(10, 2)", asList(1.23, 2.67, null))
                .addColumn("c_number_30_2", "decimal(30, 2)", asList(1.23, 2.67, null))
                .addColumn("c_char_10", "char(10)", asList("'India'", "'Poland'", null))
                .addColumn("c_char_50", "char(50)", asList("'India'", "'Poland'", null))
                .addColumn("c_char_501", "char(501)", asList("'India'", "'Poland'", null)) // greater than ORACLE_CHAR_MAX_CHARS
                .addColumn("c_char_520", "char(520)", asList("'India'", "'Poland'", null)) // greater than ORACLE_CHAR_MAX_CHARS
                .addColumn("c_nchar_10", "nchar(10)", asList("N'India'", "N'Poland'", null))
                .addColumn("c_varchar_10", "varchar2(10)", asList("'India'", "'Poland'", null))
                .addColumn("c_varchar_10_byte", "varchar2(10 byte)", asList("'India'", "'Poland'", null))
                .addColumn("c_varchar_50", "varchar2(50)", asList("'India'", "'Poland'", null))
                .addColumn("c_varchar_1001", "varchar2(1001)", asList("'India'", "'Poland'", null)) // greater than ORACLE_VARCHAR2_MAX_CHARS
                .addColumn("c_varchar_1020", "varchar2(1020)", asList("'India'", "'Poland'", null)) // greater than ORACLE_VARCHAR2_MAX_CHARS
                .addColumn("c_varchar_numeric", "varchar2(50)", asList("'123'", "'456'", null))
                .addColumn("c_varchar_decimal", "varchar2(50)", asList("'1.23'", "'2.67'", null))
                .addColumn("c_varchar_alpha_numeric", "varchar2(50)", asList("'H311o'", "'123Hey'", null))
                .addColumn("c_varchar_date", "varchar2(50)", asList("'2024-09-08'", "'2019-08-15'", null))
                .addColumn("c_varchar_timestamp", "varchar2(50)", asList("'2024-09-08 01:02:03.666'", "'2019-08-15 09:08:07.333'", null))
                .addColumn("c_varchar_timestamptz", "varchar2(50)", asList("'2024-09-08 01:02:03.666 +05:30'", "'2019-08-15 09:08:07.333 +05:30'", null))
                .addColumn("c_nvarchar_100", "nvarchar2(100)", asList("N'India'", "N'Poland'", null)) // varchar(p) in trino
                .addColumn("c_clob", "clob", asList("'India'", "'Poland'", null)) // varchar in trino
                .addColumn("c_nclob", "nclob", asList("N'India'", "N'Poland'", null)) // varchar in trino
                .addColumn("c_blob", "blob", asList("HEXTORAW('496E646961')", "HEXTORAW('506F6C616E64')", null)) // varbinary in trino
                .addColumn("c_raw_200", "raw(200)", asList("HEXTORAW('496E646961')", "HEXTORAW('506F6C616E64')", null)) // varbinary in trino
                .addColumn("c_date", "date", asList("DATE '2024-09-08'", "DATE '2019-08-15'", null))
                .addColumn("c_timestamp", "timestamp", asList("TIMESTAMP '2024-09-08 01:02:03.666'", "TIMESTAMP '2019-08-15 09:08:07.333'", null))
                .addColumn("c_timestamptz", "timestamp with time zone", asList("TIMESTAMP '2024-09-08 01:02:03.666 +05:30'", "TIMESTAMP '2019-08-15 09:08:07.333 +05:30'", null))

                // unsupported in trino
                .addColumn("c_number", "number", asList(1, 2, null))
                .addColumn("c_timestamp_ltz", "timestamp with local time zone", asList("TIMESTAMP '2024-09-08 01:02:03.666'", "TIMESTAMP '2019-08-15 09:08:07.333'", null))
                .addColumn("c_interval_ym", "interval year to month", asList("INTERVAL '1-2' YEAR TO MONTH", "INTERVAL '3-4' YEAR TO MONTH", null))
                .addColumn("c_interval_ds", "interval day to second", asList("INTERVAL '10 11:12:13.456' DAY TO SECOND", "INTERVAL '10 12:13:14.456' DAY TO SECOND", null))
                .addColumn("c_long", "long", asList("'India'", "'Poland'", null))
                .addColumn("c_xmltype", "xmltype", asList("XMLTYPE('<root><element>Sample XML-1</element></root>')", "XMLTYPE('<root><element>Sample XML-2</element></root>')", null))
                .execute(sqlExecutor, "table_1_");

        // 2nd row value is different in table2 than table1
        table2 = CastDataTypeTest.create()
                .addColumn("id", "number(10)", asList(21, 22, 23))
                .addColumn("c_number_3", "number(3)", asList(1, 22, null)) // tinyint in trino
                .addColumn("c_number_5", "number(5)", asList(1, 22, null)) // smallint in trino
                .addColumn("c_number_10", "number(10)", asList(1, 22, null)) // integer in trino
                .addColumn("c_number_19", "number(19)", asList(1, 22, null)) // bigint in trino
                .addColumn("c_float", "float", asList(1.23, 22.67, null)) // double in trino
                .addColumn("c_float_5", "float(5)", asList(1.23, 22.67, null)) // double in trino
                .addColumn("c_binary_float", "binary_float", asList(1.23, 22.67, null))
                .addColumn("c_binary_double", "binary_double", asList(1.23, 22.67, null))
                .addColumn("c_number_15", "decimal(15)", asList(1, 22, null))
                .addColumn("c_number_10_2", "decimal(10, 2)", asList(1.23, 22.67, null))
                .addColumn("c_number_30_2", "decimal(30, 2)", asList(1.23, 22.67, null))
                .addColumn("c_char_10", "char(10)", asList("'India'", "'France'", null))
                .addColumn("c_char_50", "char(50)", asList("'India'", "'France'", null))
                .addColumn("c_char_501", "char(501)", asList("'India'", "'France'", null)) // greater than ORACLE_CHAR_MAX_CHARS
                .addColumn("c_char_520", "char(520)", asList("'India'", "'France'", null)) // greater than ORACLE_CHAR_MAX_CHARS
                .addColumn("c_nchar_10", "nchar(10)", asList("N'India'", "N'France'", null))
                .addColumn("c_varchar_10", "varchar2(10)", asList("'India'", "'France'", null))
                .addColumn("c_varchar_10_byte", "varchar2(10 byte)", asList("'India'", "'France'", null))
                .addColumn("c_varchar_50", "varchar2(50)", asList("'India'", "'France'", null))
                .addColumn("c_varchar_1001", "varchar2(1001)", asList("'India'", "'France'", null)) // greater than ORACLE_VARCHAR2_MAX_CHARS
                .addColumn("c_varchar_1020", "varchar2(1020)", asList("'India'", "'France'", null)) // greater than ORACLE_VARCHAR2_MAX_CHARS
                .addColumn("c_varchar_numeric", "varchar2(50)", asList("'123'", "'234'", null))
                .addColumn("c_varchar_decimal", "varchar2(50)", asList("'1.23'", "'22.67'", null))
                .addColumn("c_varchar_alpha_numeric", "varchar2(50)", asList("'H311o'", "'123Bye'", null))
                .addColumn("c_varchar_date", "varchar2(50)", asList("'2024-09-08'", "'2020-08-15'", null))
                .addColumn("c_varchar_timestamp", "varchar2(50)", asList("'2024-09-08 01:02:03.666'", "'2020-08-15 09:08:07.333'", null))
                .addColumn("c_varchar_timestamptz", "varchar2(50)", asList("'2024-09-08 01:02:03.666 +05:30'", "'2020-08-15 09:08:07.333 +05:30'", null))
                .addColumn("c_nvarchar_100", "nvarchar2(100)", asList("N'India'", "N'France'", null)) // varchar(p) in trino
                .addColumn("c_clob", "clob", asList("'India'", "'France'", null)) // varchar in trino
                .addColumn("c_nclob", "nclob", asList("N'India'", "N'France'", null)) // varchar in trino
                .addColumn("c_blob", "blob", asList("HEXTORAW('496E646961')", "HEXTORAW('4672616E6365')", null)) // varbinary in trino
                .addColumn("c_raw_200", "raw(200)", asList("HEXTORAW('496E646961')", "HEXTORAW('4672616E6365')", null)) // varbinary in trino
                .addColumn("c_date", "date", asList("DATE '2024-09-08'", "DATE '2020-08-15'", null))
                .addColumn("c_timestamp", "timestamp", asList("TIMESTAMP '2024-09-08 01:02:03.666'", "TIMESTAMP '2020-08-15 09:08:07.333'", null))
                .addColumn("c_timestamptz", "timestamp with time zone", asList("TIMESTAMP '2024-09-08 01:02:03.666 +05:30'", "TIMESTAMP '2020-08-15 09:08:07.333 +05:30'", null))

                // unsupported in trino
                .addColumn("c_number", "number", asList(1, 22, null))
                .addColumn("c_timestamp_ltz", "timestamp with local time zone", asList("TIMESTAMP '2024-09-08 01:02:03.666'", "TIMESTAMP '2020-08-15 09:08:07.333'", null))
                .addColumn("c_interval_ym", "interval year to month", asList("INTERVAL '1-2' YEAR TO MONTH", "INTERVAL '4-5' YEAR TO MONTH", null))
                .addColumn("c_interval_ds", "interval day to second", asList("INTERVAL '10 11:12:13.456' DAY TO SECOND", "INTERVAL '11 12:13:14.456' DAY TO SECOND", null))
                .addColumn("c_long", "long", asList("'India'", "'France'", null))
                .addColumn("c_xmltype", "xmltype", asList("XMLTYPE('<root><element>Sample XML-1</element></root>')", "XMLTYPE('<root><element>Sample XML-3</element></root>')", null))
                .execute(sqlExecutor, "table_2_");
    }

    @Test
    public void testCastPushdownSpecialCase()
    {
        for (CastTestCase testCase : specialCaseNClob()) {
            // Projection pushdown is supported, because The clob type internally converted to nclob thus cast does not happen
            assertThat(assertions.query(session, "SELECT CAST(%s AS %s) FROM %s".formatted(testCase.sourceColumn(), testCase.castType(), table1.tableName())))
                    .isFullyPushedDown();
            // join pushdown is not supported, because comparison between nclob is not pushdown
            assertJoinNotFullyPushedDown(session, table1.tableName(), table2.tableName(), "CAST(l.%s AS %s) = r.%s".formatted(testCase.sourceColumn(), testCase.castType(), testCase.targetColumn().orElseThrow()));
        }
    }

    @Test
    public void testJoinPushdownWithNestedCast()
    {
        CastTestCase testCase = new CastTestCase("c_varchar_10", "varchar(100)", Optional.of("c_varchar_50"));
        assertJoinFullyPushedDown(session, table1.tableName(), table2.tableName(), "JOIN", "CAST(CAST(l.%s AS %s) AS VARCHAR(50)) = r.%s".formatted(testCase.sourceColumn(), testCase.castType(), testCase.targetColumn().orElseThrow()));
    }

    @Test
    public void testAllJoinPushdownWithCast()
    {
        // Different joins
        String leftTableName = table1.tableName();
        String rightTableName = table2.tableName();

        CastTestCase testCase = new CastTestCase("c_varchar_10", "varchar(50)", Optional.of("c_varchar_50"));
        assertJoinFullyPushedDown(session, leftTableName, rightTableName, "LEFT JOIN", "CAST(l.%s AS %s) = r.%s".formatted(testCase.sourceColumn(), testCase.castType(), testCase.targetColumn().orElseThrow()));
        assertJoinFullyPushedDown(session, leftTableName, rightTableName, "RIGHT JOIN", "CAST(l.%s AS %s) = r.%s".formatted(testCase.sourceColumn(), testCase.castType(), testCase.targetColumn().orElseThrow()));
        assertJoinFullyPushedDown(session, leftTableName, rightTableName, "INNER JOIN", "CAST(l.%s AS %s) = r.%s".formatted(testCase.sourceColumn(), testCase.castType(), testCase.targetColumn().orElseThrow()));

        testCase = new CastTestCase("c_varchar_10", "varchar(10)", Optional.of("c_varchar_50"));
        assertJoinFullyPushedDown(session, leftTableName, rightTableName, "LEFT JOIN", "l.%s = CAST(r.%s AS %s)".formatted(testCase.sourceColumn(), testCase.targetColumn().orElseThrow(), testCase.castType()));
        assertJoinFullyPushedDown(session, leftTableName, rightTableName, "RIGHT JOIN", "l.%s = CAST(r.%s AS %s)".formatted(testCase.sourceColumn(), testCase.targetColumn().orElseThrow(), testCase.castType()));
        assertJoinFullyPushedDown(session, leftTableName, rightTableName, "INNER JOIN", "l.%s = CAST(r.%s AS %s)".formatted(testCase.sourceColumn(), testCase.targetColumn().orElseThrow(), testCase.castType()));

        testCase = new CastTestCase("c_varchar_10", "varchar(200)", Optional.of("c_varchar_50"));
        assertJoinFullyPushedDown(session, leftTableName, rightTableName, "LEFT JOIN", "CAST(l.%1$s AS %3$s) = CAST(r.%2$s AS %3$s)".formatted(testCase.sourceColumn(), testCase.targetColumn().orElseThrow(), testCase.castType()));
        assertJoinFullyPushedDown(session, leftTableName, rightTableName, "RIGHT JOIN", "CAST(l.%1$s AS %3$s) = CAST(r.%2$s AS %3$s)".formatted(testCase.sourceColumn(), testCase.targetColumn().orElseThrow(), testCase.castType()));
        assertJoinFullyPushedDown(session, leftTableName, rightTableName, "INNER JOIN", "CAST(l.%1$s AS %3$s) = CAST(r.%2$s AS %3$s)".formatted(testCase.sourceColumn(), testCase.targetColumn().orElseThrow(), testCase.castType()));
    }

    @Test
    public void testCastPushdownDisabled()
    {
        Session session = queryRunner.getDefaultSession();
        Session sessionWithoutComplexExpressionPushdown = Session.builder(session)
                .setCatalogSessionProperty(session.getCatalog().orElseThrow(), "complex_expression_pushdown", "false")
                .build();
        assertThat(assertions.query(sessionWithoutComplexExpressionPushdown, "SELECT CAST (c_varchar_10 AS VARCHAR(100)) FROM %s".formatted(table1.tableName())))
                .isNotFullyPushedDown(ProjectNode.class);
    }

    @Override
    protected List<CastTestCase> supportedCastTypePushdown()
    {
        return ImmutableList.of(
                new CastTestCase("c_char_10", "char(50)", Optional.of("c_char_50")),
                new CastTestCase("c_char_50", "char(50)", Optional.of("c_char_50")),
                new CastTestCase("c_char_501", "char(50)", Optional.of("c_char_50")),
                new CastTestCase("c_char_520", "char(50)", Optional.of("c_char_50")),
                new CastTestCase("c_nchar_10", "char(50)", Optional.of("c_char_50")),
                new CastTestCase("c_varchar_10", "char(50)", Optional.of("c_char_50")),
                new CastTestCase("c_varchar_10_byte", "char(50)", Optional.of("c_char_50")),
                new CastTestCase("c_varchar_1001", "char(50)", Optional.of("c_char_50")),
                new CastTestCase("c_varchar_1020", "char(50)", Optional.of("c_char_50")),
                new CastTestCase("c_nvarchar_100", "char(50)", Optional.of("c_char_50")),
                new CastTestCase("c_clob", "char(50)", Optional.of("c_char_50")),
                new CastTestCase("c_nclob", "char(50)", Optional.of("c_char_50")),

                new CastTestCase("c_char_10", "varchar(50)", Optional.of("c_varchar_50")),
                new CastTestCase("c_char_50", "varchar(50)", Optional.of("c_varchar_50")),
                new CastTestCase("c_char_501", "varchar(50)", Optional.of("c_varchar_50")),
                new CastTestCase("c_char_520", "varchar(50)", Optional.of("c_varchar_50")),
                new CastTestCase("c_nchar_10", "varchar(50)", Optional.of("c_varchar_50")),
                new CastTestCase("c_varchar_10", "varchar(50)", Optional.of("c_varchar_50")),
                new CastTestCase("c_varchar_10_byte", "varchar(50)", Optional.of("c_varchar_50")),
                new CastTestCase("c_varchar_1001", "varchar(50)", Optional.of("c_varchar_50")),
                new CastTestCase("c_varchar_1020", "varchar(50)", Optional.of("c_varchar_50")),
                new CastTestCase("c_nvarchar_100", "varchar(50)", Optional.of("c_varchar_50")),
                new CastTestCase("c_clob", "varchar(50)", Optional.of("c_varchar_50")),
                new CastTestCase("c_nclob", "varchar(50)", Optional.of("c_varchar_50")),
                new CastTestCase("c_number_3", "varchar(50)", Optional.of("c_varchar_50")),
                new CastTestCase("c_number_5", "varchar(50)", Optional.of("c_varchar_50")),
                new CastTestCase("c_number_10", "varchar(50)", Optional.of("c_varchar_50")),
                new CastTestCase("c_number_19", "varchar(50)", Optional.of("c_varchar_50")),
                new CastTestCase("c_number_15", "varchar(50)", Optional.of("c_varchar_50")),
                new CastTestCase("c_number_10_2", "varchar(50)", Optional.of("c_varchar_50")),
                new CastTestCase("c_number_30_2", "varchar(50)", Optional.of("c_varchar_50")));
    }

    @Override
    protected List<CastTestCase> unsupportedCastTypePushdown()
    {
        return ImmutableList.of(
                new CastTestCase("c_char_10", "char(501)", Optional.of("c_char_501")),
                new CastTestCase("c_char_501", "char(520)", Optional.of("c_char_520")),
                new CastTestCase("c_varchar_10", "char(501)", Optional.of("c_char_501")),
                new CastTestCase("c_varchar_1001", "char(501)", Optional.of("c_char_501")),
                new CastTestCase("c_clob", "char(501)", Optional.of("c_char_501")),
                new CastTestCase("c_nclob", "char(501)", Optional.of("c_char_501")),

                new CastTestCase("c_char_10", "varchar(1001)", Optional.of("c_varchar_1001")),
                new CastTestCase("c_char_501", "varchar(1001)", Optional.of("c_varchar_1001")),
                new CastTestCase("c_varchar_10", "varchar(1001)", Optional.of("c_varchar_1001")),
                new CastTestCase("c_varchar_1001", "varchar(1020)", Optional.of("c_varchar_1020")),
                new CastTestCase("c_clob", "varchar(1001)", Optional.of("c_varchar_1001")),
                new CastTestCase("c_nclob", "varchar(1001)", Optional.of("c_varchar_1001")),
                new CastTestCase("c_number_3", "varchar(1001)", Optional.of("c_varchar_1001")),
                new CastTestCase("c_number_5", "varchar(1001)", Optional.of("c_varchar_1001")),
                new CastTestCase("c_number_10", "varchar(1001)", Optional.of("c_varchar_1001")),
                new CastTestCase("c_number_19", "varchar(1001)", Optional.of("c_varchar_1001")),
                new CastTestCase("c_float", "varchar(1001)", Optional.of("c_varchar_1001")),
                new CastTestCase("c_float_5", "varchar(1001)", Optional.of("c_varchar_1001")),
                new CastTestCase("c_binary_float", "varchar(1001)", Optional.of("c_varchar_1001")),
                new CastTestCase("c_binary_double", "varchar(1001)", Optional.of("c_varchar_1001")),
                new CastTestCase("c_number_15", "varchar(1001)", Optional.of("c_varchar_1001")),
                new CastTestCase("c_number_10_2", "varchar(1001)", Optional.of("c_varchar_1001")),
                new CastTestCase("c_number_30_2", "varchar(1001)", Optional.of("c_varchar_1001")),

                new CastTestCase("c_char_10", "varchar", Optional.of("c_clob")),
                new CastTestCase("c_char_501", "varchar", Optional.of("c_clob")),
                new CastTestCase("c_varchar_10", "varchar", Optional.of("c_clob")),
                new CastTestCase("c_varchar_1001", "varchar", Optional.of("c_clob")),
                new CastTestCase("c_number_3", "varchar", Optional.of("c_clob")),
                new CastTestCase("c_number_5", "varchar", Optional.of("c_clob")),
                new CastTestCase("c_number_10", "varchar", Optional.of("c_clob")),
                new CastTestCase("c_number_19", "varchar", Optional.of("c_clob")),
                new CastTestCase("c_float", "varchar", Optional.of("c_clob")),
                new CastTestCase("c_float_5", "varchar", Optional.of("c_clob")),
                new CastTestCase("c_binary_float", "varchar", Optional.of("c_clob")),
                new CastTestCase("c_binary_double", "varchar", Optional.of("c_clob")),
                new CastTestCase("c_number_15", "varchar", Optional.of("c_clob")),
                new CastTestCase("c_number_10_2", "varchar", Optional.of("c_clob")),
                new CastTestCase("c_number_30_2", "varchar", Optional.of("c_clob")),

                new CastTestCase("c_char_10", "varchar", Optional.of("c_nclob")),
                new CastTestCase("c_char_501", "varchar", Optional.of("c_nclob")),
                new CastTestCase("c_varchar_10", "varchar", Optional.of("c_nclob")),
                new CastTestCase("c_varchar_1001", "varchar", Optional.of("c_nclob")),
                new CastTestCase("c_number_3", "varchar", Optional.of("c_nclob")),
                new CastTestCase("c_number_5", "varchar", Optional.of("c_nclob")),
                new CastTestCase("c_number_10", "varchar", Optional.of("c_nclob")),
                new CastTestCase("c_number_19", "varchar", Optional.of("c_nclob")),
                new CastTestCase("c_float", "varchar", Optional.of("c_nclob")),
                new CastTestCase("c_float_5", "varchar", Optional.of("c_nclob")),
                new CastTestCase("c_binary_float", "varchar", Optional.of("c_nclob")),
                new CastTestCase("c_binary_double", "varchar", Optional.of("c_nclob")),
                new CastTestCase("c_number_15", "varchar", Optional.of("c_nclob")),
                new CastTestCase("c_number_10_2", "varchar", Optional.of("c_nclob")),
                new CastTestCase("c_number_30_2", "varchar", Optional.of("c_nclob")),

                new CastTestCase("c_number_3", "tinyint", Optional.of("c_number_5")),
                new CastTestCase("c_number_3", "smallint", Optional.of("c_number_10")),
                new CastTestCase("c_number_3", "integer", Optional.of("c_number_19")),
                new CastTestCase("c_number_3", "bigint", Optional.of("c_float")),
                new CastTestCase("c_number_3", "real", Optional.of("c_float_5")),
                new CastTestCase("c_number_3", "double", Optional.of("c_binary_float")),
                new CastTestCase("c_number_3", "double", Optional.of("c_binary_double")),
                new CastTestCase("c_number_3", "decimal(15)", Optional.of("c_number_15")),
                new CastTestCase("c_number_3", "decimal(10, 2)", Optional.of("c_number_10_2")),
                new CastTestCase("c_number_3", "decimal(30, 2)", Optional.of("c_number_30_2")),
                new CastTestCase("c_varchar_10", "varbinary", Optional.of("c_blob")),
                new CastTestCase("c_varchar_10", "varbinary", Optional.of("c_raw_200")),
                new CastTestCase("c_timestamp", "date", Optional.of("c_date")),
                new CastTestCase("c_timestamptz", "timestamp", Optional.of("c_timestamp")),
                new CastTestCase("c_timestamp", "timestamp with time zone", Optional.of("c_timestamptz")),

                // When data inserted from Trino, below cases give mismatched value between pushdown
                // and without pushdown, So not supporting cast pushdown for these cases
                new CastTestCase("c_float", "varchar(50)", Optional.of("c_varchar_50")),
                new CastTestCase("c_float_5", "varchar(50)", Optional.of("c_varchar_50")),
                new CastTestCase("c_binary_float", "varchar(50)", Optional.of("c_varchar_50")),
                new CastTestCase("c_binary_double", "varchar(50)", Optional.of("c_varchar_50")),
                new CastTestCase("c_date", "varchar(50)", Optional.of("c_varchar_50")),
                new CastTestCase("c_timestamp", "varchar(50)", Optional.of("c_varchar_50")),
                new CastTestCase("c_timestamptz", "varchar(50)", Optional.of("c_varchar_50")));
    }

    @Override
    protected List<CastTestCase> failCast()
    {
        return ImmutableList.of(
                new CastTestCase("c_number_3", "char(50)"),
                new CastTestCase("c_number_5", "char(50)"),
                new CastTestCase("c_number_10", "char(50)"),
                new CastTestCase("c_number_19", "char(50)"),
                new CastTestCase("c_float", "char(50)"),
                new CastTestCase("c_float_5", "char(50)"),
                new CastTestCase("c_binary_float", "char(50)"),
                new CastTestCase("c_binary_double", "char(50)"),
                new CastTestCase("c_number_15", "char(50)"),
                new CastTestCase("c_number_10_2", "char(50)"),
                new CastTestCase("c_number_30_2", "char(50)"),
                new CastTestCase("c_date", "char(50)"),
                new CastTestCase("c_timestamp", "char(50)"),
                new CastTestCase("c_timestamptz", "char(50)"),
                new CastTestCase("c_blob", "char(50)"),
                new CastTestCase("c_raw_200", "char(50)"),

                new CastTestCase("c_number_3", "char(501)"),
                new CastTestCase("c_number_5", "char(501)"),
                new CastTestCase("c_number_10", "char(501)"),
                new CastTestCase("c_number_19", "char(501)"),
                new CastTestCase("c_float", "char(501)"),
                new CastTestCase("c_float_5", "char(501)"),
                new CastTestCase("c_binary_float", "char(501)"),
                new CastTestCase("c_binary_double", "char(501)"),
                new CastTestCase("c_number_15", "char(501)"),
                new CastTestCase("c_number_10_2", "char(501)"),
                new CastTestCase("c_number_30_2", "char(501)"),
                new CastTestCase("c_date", "char(501)"),
                new CastTestCase("c_timestamp", "char(501)"),
                new CastTestCase("c_timestamptz", "char(501)"),
                new CastTestCase("c_blob", "char(501)"),
                new CastTestCase("c_raw_200", "char(501)"),

                new CastTestCase("c_blob", "varchar(50)"),
                new CastTestCase("c_raw_200", "varchar(50)"));
    }

    private static List<CastTestCase> specialCaseNClob()
    {
        // The clob type internally converted to nclob
        return ImmutableList.of(
                new CastTestCase("c_clob", "varchar", Optional.of("c_clob")),
                new CastTestCase("c_nclob", "varchar", Optional.of("c_clob")),
                new CastTestCase("c_clob", "varchar", Optional.of("c_nclob")),
                new CastTestCase("c_nclob", "varchar", Optional.of("c_nclob")));
    }
}
