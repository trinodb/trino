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
package io.trino.plugin.mysql;

import com.google.common.collect.ImmutableMap;
import io.trino.testing.sql.TestTable;
import org.testng.SkipException;
import org.testng.annotations.Test;

import java.util.List;
import java.util.function.Function;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.Streams.stream;
import static io.trino.testing.sql.TestTable.fromColumns;
import static java.lang.String.format;
import static java.lang.String.join;

public class TestMySqlTableStatisticsMySql8Histograms
        extends BaseTestMySqlTableStatisticsTest
{
    public TestMySqlTableStatisticsMySql8Histograms()
    {
        super("mysql:8.0.30",
                Function.identity(),
                Function.identity());
    }

    @Test
    @Override
    public void testNumericCornerCases()
    {
        try (TestTable table = fromColumns(
                getQueryRunner()::execute,
                "test_numeric_corner_cases_",
                ImmutableMap.<String, List<String>>builder()
                        // TODO Infinity and NaNs not supported by MySQL
//                        .put("only_negative_infinity double", List.of("-infinity()", "-infinity()", "-infinity()", "-infinity()"))
//                        .put("only_positive_infinity double", List.of("infinity()", "infinity()", "infinity()", "infinity()"))
//                        .put("mixed_infinities double", List.of("-infinity()", "infinity()", "-infinity()", "infinity()"))
//                        .put("mixed_infinities_and_numbers double", List.of("-infinity()", "infinity()", "-5.0", "7.0"))
//                        .put("nans_only double", List.of("nan()", "nan()"))
//                        .put("nans_and_numbers double", List.of("nan()", "nan()", "-5.0", "7.0"))
                        .put("large_doubles double", List.of("CAST(-50371909150609548946090.0 AS DOUBLE)", "CAST(50371909150609548946090.0 AS DOUBLE)")) // 2^77 DIV 3
                        .put("short_decimals_big_fraction decimal(16,15)", List.of("-1.234567890123456", "1.234567890123456"))
                        .put("short_decimals_big_integral decimal(16,1)", List.of("-123456789012345.6", "123456789012345.6"))
                        // DECIMALS up to precision 30 are supported
                        .put("long_decimals_big_fraction decimal(30,29)", List.of("-1.23456789012345678901234567890", "1.23456789012345678901234567890"))
                        .put("long_decimals_middle decimal(30,16)", List.of("-12345678901234.5678901234567890", "12345678901234.5678901234567890"))
                        .put("long_decimals_big_integral decimal(30,1)", List.of("-12345678901234567890123456789.0", "12345678901234567890123456789.0"))
                        .buildOrThrow(),
                "null")) {
            gatherStats(table.getName());
            assertQuery(
                    "SHOW STATS FOR " + table.getName(),
                    "VALUES " +
                            // TODO Infinity and NaNs not supported by MySQL
//                            "('only_negative_infinity', null, 1, 0, null, null, null)," +
//                            "('only_positive_infinity', null, 1, 0, null, null, null)," +
//                            "('mixed_infinities', null, 2, 0, null, null, null)," +
//                            "('mixed_infinities_and_numbers', null, 4.0, 0.0, null, null, null)," +
//                            "('nans_only', null, 1.0, 0.5, null, null, null)," +
//                            "('nans_and_numbers', null, 3.0, 0.0, null, null, null)," +
                            "('large_doubles', null, 2.0, 0.0, null, null, null)," +
                            "('short_decimals_big_fraction', null, 2.0, 0.0, null, null, null)," +
                            "('short_decimals_big_integral', null, 2.0, 0.0, null, null, null)," +
                            "('long_decimals_big_fraction', null, 2.0, 0.0, null, null, null)," +
                            "('long_decimals_middle', null, 2.0, 0.0, null, null, null)," +
                            "('long_decimals_big_integral', null, 2.0, 0.0, null, null, null)," +
                            "(null, null, null, null, 2, null, null)");
        }
    }

    @Override
    public void testNotAnalyzed()
    {
        throw new SkipException("MySql8 automatically calculates stats - https://dev.mysql.com/doc/refman/8.0/en/innodb-parameters.html#sysvar_innodb_stats_auto_recalc");
    }

    @Override
    protected void gatherStats(String tableName)
    {
        List<String> columnNames = stream(computeActual("SHOW COLUMNS FROM " + tableName))
                .map(row -> (String) row.getField(0))
                .collect(toImmutableList());
        executeInMysql(format(
                "ANALYZE TABLE %s UPDATE HISTOGRAM ON %s ",
                tableName,
                join(", ", columnNames)).replace("\"", "`"));
    }
}
