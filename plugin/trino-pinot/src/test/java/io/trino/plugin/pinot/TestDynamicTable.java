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
package io.trino.plugin.pinot;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.slice.Slices;
import io.trino.plugin.pinot.query.DynamicTable;
import io.trino.plugin.pinot.query.OrderByExpression;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.Range;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.predicate.ValueSet;
import org.testng.annotations.Test;

import java.util.List;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.plugin.pinot.query.DynamicTableBuilder.OFFLINE_SUFFIX;
import static io.trino.plugin.pinot.query.DynamicTableBuilder.REALTIME_SUFFIX;
import static io.trino.plugin.pinot.query.DynamicTableBuilder.buildFromPql;
import static io.trino.plugin.pinot.query.DynamicTablePqlExtractor.extractPql;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.lang.String.join;
import static java.util.Locale.ENGLISH;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toList;
import static org.testng.Assert.assertEquals;

public class TestDynamicTable
        extends TestPinotQueryBase
{
    @Test
    public void testSelectNoFilter()
    {
        String tableName = realtimeOnlyTable.getTableName();
        List<String> columnNames = getColumnNames(tableName);
        List<String> orderByColumns = columnNames.subList(0, 5);
        List<OrderByExpression> orderByExpressions = orderByColumns.stream()
                .limit(4)
                .map(columnName -> new OrderByExpression(quoteIdentifier(columnName), true))
                .collect(toList());
        long limit = 230;
        String query = "SELECT %s FROM %s ORDER BY %s DESC LIMIT %s".formatted(
                join(", ", columnNames),
                tableName,
                orderByColumns.stream()
                        .collect(joining(", ")),
                limit);
        DynamicTable dynamicTable = buildFromPql(pinotMetadata, new SchemaTableName("default", query), mockClusterInfoFetcher, TESTING_TYPE_CONVERTER);
        assertEquals(dynamicTable.getProjections().stream()
                .map(PinotColumnHandle::getColumnName)
                .collect(toImmutableList()),
                columnNames);
        orderByExpressions.add(new OrderByExpression(quoteIdentifier(orderByColumns.get(4)), false));
        assertEquals(dynamicTable.getOrderBy(), orderByExpressions);
        assertEquals(dynamicTable.getLimit().getAsLong(), limit);
    }

    @Test
    public void testGroupBy()
    {
        String tableName = realtimeOnlyTable.getTableName();
        long limit = 25;
        String query = "SELECT Origin, AirlineID, max(CarrierDelay), avg(CarrierDelay) FROM %s GROUP BY Origin, AirlineID LIMIT %s".formatted(tableName, limit);
        DynamicTable dynamicTable = buildFromPql(pinotMetadata, new SchemaTableName("default", query), mockClusterInfoFetcher, TESTING_TYPE_CONVERTER);
        assertEquals(dynamicTable.getGroupingColumns().stream()
                        .map(PinotColumnHandle::getColumnName)
                        .collect(toImmutableList()),
                ImmutableList.builder()
                        .add("Origin")
                        .add("AirlineID")
                        .build());
        assertEquals(dynamicTable.getLimit().getAsLong(), limit);
    }

    @Test
    public void testFilter()
    {
        String tableName = realtimeOnlyTable.getTableName();
        String query = """
                SELECT FlightNum, AirlineID
                FROM %s
                WHERE ((CancellationCode IN ('strike', 'weather', 'pilot_bac')) AND (Origin = 'jfk'))
                OR ((OriginCityName != 'catfish paradise') AND (OriginState != 'az') AND (AirTime BETWEEN 1 AND 5))
                AND AirTime NOT IN (7,8,9)
                OR ((DepDelayMinutes < 10) AND (Distance >= 3) AND (ArrDelay > 4) AND (SecurityDelay < 5) AND (LateAircraftDelay <= 7))
                LIMIT 60""".formatted(tableName);

        String expected = """
                SELECT "FlightNum", "AirlineID"\
                 FROM %s\
                 WHERE OR(AND("CancellationCode" IN ('strike', 'weather', 'pilot_bac'), ("Origin") = 'jfk'),\
                 AND(("OriginCityName") != 'catfish paradise', ("OriginState") != 'az', ("AirTime") BETWEEN '1' AND '5', "AirTime" NOT IN ('7', '8', '9')),\
                 AND(("DepDelayMinutes") < '10', ("Distance") >= '3', ("ArrDelay") > '4', ("SecurityDelay") < '5', ("LateAircraftDelay") <= '7'))\
                 LIMIT 60""".formatted(tableName);
        DynamicTable dynamicTable = buildFromPql(pinotMetadata, new SchemaTableName("default", query), mockClusterInfoFetcher, TESTING_TYPE_CONVERTER);
        assertEquals(extractPql(dynamicTable, TupleDomain.all()), expected);
    }

    @Test
    public void testPrimitiveTypes()
    {
        // Note: Pinot treats varbinary literals as strings
        // ci and intellij produce different results when X'abcd' is used:
        // ci will interpret as a string, i.e. "X''ABCD"
        // intellij will expand the X'abcd' to binary 0b10011100001111
        // Pinot will interpret both forms as a string regardless, so we cannot use X'....'
        String query = """
                SELECT string_col, long_col, int_col, bool_col, double_col, float_col, bytes_col
                FROM primitive_types_table
                WHERE string_col = 'string' AND long_col = 12345678901 AND int_col = 123456789
                AND double_col = 3.56 AND float_col = 3.56 AND bytes_col = 'abcd'
                LIMIT 60""";
        String expected = """
                SELECT "string_col", "long_col", "int_col", "bool_col", "double_col", "float_col", "bytes_col"\
                 FROM primitive_types_table\
                 WHERE AND(("string_col") = 'string', ("long_col") = '12345678901',\
                 ("int_col") = '123456789', ("double_col") = '3.56', ("float_col") = '3.56', ("bytes_col") = 'abcd')\
                 LIMIT 60""";
        DynamicTable dynamicTable = buildFromPql(pinotMetadata, new SchemaTableName("default", query), mockClusterInfoFetcher, TESTING_TYPE_CONVERTER);
        assertEquals(extractPql(dynamicTable, TupleDomain.all()), expected);
    }

    @Test
    public void testDoubleWithScientificNotation()
    {
        // Pinot recognizes double literals with scientific notation as of version 0.8.0
        String query = "SELECT string_col FROM primitive_types_table WHERE double_col = 3.5E5";
        String expected = """
                SELECT "string_col" FROM primitive_types_table WHERE ("double_col") = '350000.0' LIMIT 10""";
        DynamicTable dynamicTable = buildFromPql(pinotMetadata, new SchemaTableName("default", query), mockClusterInfoFetcher, TESTING_TYPE_CONVERTER);
        assertEquals(extractPql(dynamicTable, TupleDomain.all()), expected);
    }

    @Test
    public void testFilterWithCast()
    {
        String query = """
                SELECT string_col, long_col
                FROM primitive_types_table
                WHERE string_col = CAST(123 AS STRING) AND long_col = CAST('123' AS LONG)
                LIMIT 60""";
        String expected = """
                SELECT "string_col", "long_col" FROM primitive_types_table\
                 WHERE AND(("string_col") = '123', ("long_col") = '123')\
                 LIMIT 60""";
        DynamicTable dynamicTable = buildFromPql(pinotMetadata, new SchemaTableName("default", query), mockClusterInfoFetcher, TESTING_TYPE_CONVERTER);
        assertEquals(extractPql(dynamicTable, TupleDomain.all()), expected);
    }

    @Test
    public void testFilterWithCaseStatements()
    {
        String tableName = realtimeOnlyTable.getTableName();
        String query = """
                SELECT FlightNum, AirlineID
                FROM %s
                WHERE CASE WHEN cancellationcode = 'strike' THEN 3 ELSE 4 END != 5
                AND CASE origincityname WHEN 'nyc' THEN 'pizza' WHEN 'la' THEN 'burrito' WHEN 'boston' THEN 'clam chowder'
                ELSE 'burger' END != 'salad'""".formatted(tableName);
        String expected = """
                SELECT "FlightNum", "AirlineID"\
                 FROM %s\
                 WHERE AND((CASE WHEN equals("CancellationCode", 'strike')\
                 THEN '3' ELSE '4' END) != '5', (CASE WHEN equals("OriginCityName", 'nyc')\
                 THEN 'pizza' WHEN equals("OriginCityName", 'la') THEN 'burrito' WHEN equals("OriginCityName", 'boston')\
                 THEN 'clam chowder' ELSE 'burger' END) != 'salad') LIMIT 10""".formatted(tableName);
        DynamicTable dynamicTable = buildFromPql(pinotMetadata, new SchemaTableName("default", query), mockClusterInfoFetcher, TESTING_TYPE_CONVERTER);
        assertEquals(extractPql(dynamicTable, TupleDomain.all()), expected);
    }

    @Test
    public void testFilterWithPushdownConstraint()
    {
        String tableName = realtimeOnlyTable.getTableName();
        String query = "SELECT FlightNum FROM %s LIMIT 60".formatted(tableName.toLowerCase(ENGLISH));
        DynamicTable dynamicTable = buildFromPql(pinotMetadata, new SchemaTableName("default", query), mockClusterInfoFetcher, TESTING_TYPE_CONVERTER);
        PinotColumnHandle columnHandle = new PinotColumnHandle("OriginCityName", VARCHAR);
        TupleDomain<ColumnHandle> tupleDomain = TupleDomain.withColumnDomains(ImmutableMap.of(
                columnHandle,
                Domain.create(ValueSet.ofRanges(Range.equal(VARCHAR, Slices.utf8Slice("Catfish Paradise"))), false)));
        String expectedPql = """
                SELECT "FlightNum"\
                 FROM realtimeOnly\
                 WHERE ("OriginCityName" = 'Catfish Paradise')\
                 LIMIT 60""";
        assertEquals(extractPql(dynamicTable, tupleDomain), expectedPql);
    }

    @Test
    public void testFilterWithUdf()
    {
        String tableName = realtimeOnlyTable.getTableName();
        // Note: before Pinot 0.12.1 the below query produced different results due to handling IEEE-754 approximate numerics
        // See https://github.com/apache/pinot/issues/10637
        String query = "SELECT FlightNum FROM %s WHERE DivLongestGTimes = FLOOR(EXP(2 * LN(3)) + 0.1) AND 5 < EXP(CarrierDelay) LIMIT 60".formatted(tableName.toLowerCase(ENGLISH));
        DynamicTable dynamicTable = buildFromPql(pinotMetadata, new SchemaTableName("default", query), mockClusterInfoFetcher, TESTING_TYPE_CONVERTER);
        String expectedPql = """
                SELECT "FlightNum"\
                 FROM realtimeOnly\
                 WHERE AND(("DivLongestGTimes") = '9.0', (exp("CarrierDelay")) > '5')\
                 LIMIT 60""";
        assertEquals(extractPql(dynamicTable, TupleDomain.all()), expectedPql);
    }

    @Test
    public void testSelectStarDynamicTable()
    {
        String tableName = realtimeOnlyTable.getTableName();
        String query = "SELECT * FROM %s LIMIT 70".formatted(tableName.toLowerCase(ENGLISH));
        DynamicTable dynamicTable = buildFromPql(pinotMetadata, new SchemaTableName("default", query), mockClusterInfoFetcher, TESTING_TYPE_CONVERTER);
        String expectedPql = "SELECT %s FROM %s LIMIT 70".formatted(getColumnNames(tableName).stream().map(TestDynamicTable::quoteIdentifier).collect(joining(", ")), tableName);
        assertEquals(extractPql(dynamicTable, TupleDomain.all()), expectedPql);
    }

    @Test
    public void testOfflineDynamicTable()
    {
        String tableName = hybridTable.getTableName();
        String tableNameWithSuffix = tableName + OFFLINE_SUFFIX;
        String query = "SELECT * FROM %s LIMIT 70".formatted(tableNameWithSuffix);
        DynamicTable dynamicTable = buildFromPql(pinotMetadata, new SchemaTableName("default", query), mockClusterInfoFetcher, TESTING_TYPE_CONVERTER);
        String expectedPql = "SELECT %s FROM %s LIMIT 70".formatted(getColumnNames(tableName).stream().map(TestDynamicTable::quoteIdentifier).collect(joining(", ")), tableNameWithSuffix);
        assertEquals(extractPql(dynamicTable, TupleDomain.all()), expectedPql);
        assertEquals(dynamicTable.getTableName(), tableName);
    }

    @Test
    public void testRealtimeOnlyDynamicTable()
    {
        String tableName = hybridTable.getTableName();
        String tableNameWithSuffix = tableName + REALTIME_SUFFIX;
        String query = "SELECT * FROM %s LIMIT 70".formatted(tableNameWithSuffix);
        DynamicTable dynamicTable = buildFromPql(pinotMetadata, new SchemaTableName("default", query), mockClusterInfoFetcher, TESTING_TYPE_CONVERTER);
        String expectedPql = "SELECT %s FROM %s LIMIT 70".formatted(getColumnNames(tableName).stream().map(TestDynamicTable::quoteIdentifier).collect(joining(", ")), tableNameWithSuffix);
        assertEquals(extractPql(dynamicTable, TupleDomain.all()), expectedPql);
        assertEquals(dynamicTable.getTableName(), tableName);
    }

    @Test
    public void testLimitAndOffset()
    {
        String tableName = hybridTable.getTableName();
        String tableNameWithSuffix = tableName + REALTIME_SUFFIX;
        String query = "SELECT * FROM %s LIMIT 70, 40".formatted(tableNameWithSuffix);
        DynamicTable dynamicTable = buildFromPql(pinotMetadata, new SchemaTableName("default", query), mockClusterInfoFetcher, TESTING_TYPE_CONVERTER);
        String expectedPql = "SELECT %s FROM %s LIMIT 70, 40".formatted(getColumnNames(tableName).stream().map(TestDynamicTable::quoteIdentifier).collect(joining(", ")), tableNameWithSuffix);
        assertEquals(extractPql(dynamicTable, TupleDomain.all()), expectedPql);
        assertEquals(dynamicTable.getTableName(), tableName);
    }

    private static String quoteIdentifier(String identifier)
    {
        return "\"" + identifier.replaceAll("\"", "\"\"") + "\"";
    }

    @Test
    public void testRegexpLike()
    {
        String tableName = hybridTable.getTableName();
        String tableNameWithSuffix = tableName + REALTIME_SUFFIX;
        String query = "SELECT origincityname FROM %s WHERE regexp_like(origincityname, '.*york.*') LIMIT 70".formatted(tableNameWithSuffix);
        DynamicTable dynamicTable = buildFromPql(pinotMetadata, new SchemaTableName("default", query), mockClusterInfoFetcher, TESTING_TYPE_CONVERTER);
        String expectedPql = """
                SELECT "OriginCityName" FROM %s WHERE regexp_like("OriginCityName", '.*york.*') LIMIT 70""".formatted(tableNameWithSuffix);
        assertEquals(extractPql(dynamicTable, TupleDomain.all()), expectedPql);
        assertEquals(dynamicTable.getTableName(), tableName);
    }

    @Test
    public void testTextMatch()
    {
        String tableName = hybridTable.getTableName();
        String tableNameWithSuffix = tableName + REALTIME_SUFFIX;
        String query = "SELECT origincityname FROM %s WHERE text_match(origincityname, 'new AND york') LIMIT 70".formatted(tableNameWithSuffix);
        DynamicTable dynamicTable = buildFromPql(pinotMetadata, new SchemaTableName("default", query), mockClusterInfoFetcher, TESTING_TYPE_CONVERTER);
        String expectedPql = """
                SELECT "OriginCityName" FROM %s WHERE text_match("OriginCityName", 'new and york') LIMIT 70""".formatted(tableNameWithSuffix);
        assertEquals(extractPql(dynamicTable, TupleDomain.all()), expectedPql);
        assertEquals(dynamicTable.getTableName(), tableName);
    }

    @Test
    public void testJsonMatch()
    {
        String tableName = hybridTable.getTableName();
        String tableNameWithSuffix = tableName + REALTIME_SUFFIX;
        String query = """
                SELECT origincityname FROM %s WHERE json_match(origincityname, '"$.name"=''new york''') LIMIT 70""".formatted(tableNameWithSuffix);
        DynamicTable dynamicTable = buildFromPql(pinotMetadata, new SchemaTableName("default", query), mockClusterInfoFetcher, TESTING_TYPE_CONVERTER);
        String expectedPql = """
                SELECT "OriginCityName" FROM %s WHERE json_match("OriginCityName", '"$.name"=''new york''') LIMIT 70""".formatted(tableNameWithSuffix);
        assertEquals(extractPql(dynamicTable, TupleDomain.all()), expectedPql);
        assertEquals(dynamicTable.getTableName(), tableName);
    }

    @Test
    public void testSelectExpressionsWithAliases()
    {
        String tableName = hybridTable.getTableName();
        String tableNameWithSuffix = tableName + REALTIME_SUFFIX;
        String query = """
                SELECT datetimeconvert(dayssinceEpoch, '1:seconds:epoch', '1:milliseconds:epoch', '15:minutes'),
                CASE origincityname WHEN 'nyc' THEN 'pizza' WHEN 'la' THEN 'burrito' WHEN 'boston' THEN 'clam chowder'
                ELSE 'burger' END != 'salad',
                timeconvert(dayssinceEpoch, 'seconds', 'minutes') AS foo
                FROM %s
                LIMIT 70""".formatted(tableNameWithSuffix);

        DynamicTable dynamicTable = buildFromPql(pinotMetadata, new SchemaTableName("default", query), mockClusterInfoFetcher, TESTING_TYPE_CONVERTER);
        String expectedPql = """
                SELECT datetimeconvert("DaysSinceEpoch", '1:SECONDS:EPOCH', '1:MILLISECONDS:EPOCH', '15:MINUTES'),\
                 not_equals(CASE WHEN equals("OriginCityName", 'nyc') THEN 'pizza' WHEN equals("OriginCityName", 'la') THEN 'burrito' WHEN equals(\"OriginCityName\", 'boston') THEN 'clam chowder' ELSE 'burger' END, 'salad'),\
                 timeconvert("DaysSinceEpoch", 'SECONDS', 'MINUTES') AS "foo"\
                 FROM %s\
                 LIMIT 70""".formatted(tableNameWithSuffix);
        assertEquals(extractPql(dynamicTable, TupleDomain.all()), expectedPql);
        assertEquals(dynamicTable.getTableName(), tableName);
    }

    @Test
    public void testAggregateExpressionsWithAliases()
    {
        String tableName = hybridTable.getTableName();
        String tableNameWithSuffix = tableName + REALTIME_SUFFIX;
        String query = """
                SELECT datetimeconvert(dayssinceEpoch, '1:seconds:epoch', '1:milliseconds:epoch', '15:minutes'),
                count(*) AS bar,
                CASE origincityname WHEN 'nyc' then 'pizza' WHEN 'la' THEN 'burrito' WHEN 'boston' THEN 'clam chowder'
                ELSE 'burger' END != 'salad',
                timeconvert(dayssinceEpoch, 'seconds', 'minutes') AS foo,
                max(airtime) as baz
                FROM %s
                GROUP BY 1, 3, 4
                LIMIT 70""".formatted(tableNameWithSuffix);

        DynamicTable dynamicTable = buildFromPql(pinotMetadata, new SchemaTableName("default", query), mockClusterInfoFetcher, TESTING_TYPE_CONVERTER);
        String expectedPql = """
                SELECT datetimeconvert("DaysSinceEpoch", '1:SECONDS:EPOCH',\
                 '1:MILLISECONDS:EPOCH', '15:MINUTES'), count(*) AS "bar",\
                 not_equals(CASE WHEN equals("OriginCityName", 'nyc') THEN 'pizza' WHEN equals("OriginCityName", 'la') THEN 'burrito'\
                 WHEN equals("OriginCityName", 'boston') THEN 'clam chowder' ELSE 'burger' END, 'salad'),\
                 timeconvert("DaysSinceEpoch", 'SECONDS', 'MINUTES') AS "foo",\
                 max("AirTime") AS "baz"\
                 FROM %s\
                 GROUP BY datetimeconvert("DaysSinceEpoch", '1:SECONDS:EPOCH', '1:MILLISECONDS:EPOCH', '15:MINUTES'),\
                 not_equals(CASE WHEN equals("OriginCityName", 'nyc') THEN 'pizza' WHEN equals("OriginCityName", 'la') THEN 'burrito' WHEN equals("OriginCityName", 'boston') THEN 'clam chowder' ELSE 'burger' END, 'salad'),\
                 timeconvert("DaysSinceEpoch", 'SECONDS', 'MINUTES')\
                 LIMIT 70""".formatted(tableNameWithSuffix);
        assertEquals(extractPql(dynamicTable, TupleDomain.all()), expectedPql);
        assertEquals(dynamicTable.getTableName(), tableName);
    }

    @Test
    public void testOrderBy()
    {
        String tableName = hybridTable.getTableName();
        String tableNameWithSuffix = tableName + REALTIME_SUFFIX;
        String query = "SELECT ArrDelay + 34 - DaysSinceEpoch, FlightNum FROM %s ORDER BY ArrDelay ASC, DaysSinceEpoch DESC".formatted(tableNameWithSuffix);
        DynamicTable dynamicTable = buildFromPql(pinotMetadata, new SchemaTableName("default", query), mockClusterInfoFetcher, TESTING_TYPE_CONVERTER);
        String expectedPql = """
                SELECT plus("ArrDelay", '34') - "DaysSinceEpoch", "FlightNum"\
                 FROM %s\
                 ORDER BY "ArrDelay", "DaysSinceEpoch" DESC\
                 LIMIT 10""".formatted(tableNameWithSuffix);
        assertEquals(extractPql(dynamicTable, TupleDomain.all()), expectedPql);
        assertEquals(dynamicTable.getTableName(), tableName);
    }

    @Test
    public void testOrderByCountStar()
    {
        String tableName = hybridTable.getTableName();
        String tableNameWithSuffix = tableName + REALTIME_SUFFIX;
        String query = "SELECT count(*) FROM %s ORDER BY count(*)".formatted(tableNameWithSuffix);
        DynamicTable dynamicTable = buildFromPql(pinotMetadata, new SchemaTableName("default", query), mockClusterInfoFetcher, TESTING_TYPE_CONVERTER);
        String expectedPql = """
                SELECT count(*)\
                 FROM %s\
                 ORDER BY count(*)\
                 LIMIT 10""".formatted(tableNameWithSuffix);
        assertEquals(extractPql(dynamicTable, TupleDomain.all()), expectedPql);
        assertEquals(dynamicTable.getTableName(), tableName);
    }

    @Test
    public void testOrderByExpression()
    {
        String tableName = hybridTable.getTableName();
        String tableNameWithSuffix = tableName + REALTIME_SUFFIX;
        String query = "SELECT ArrDelay + 34 - DaysSinceEpoch, FlightNum FROM %s ORDER BY ArrDelay + 34 - DaysSinceEpoch desc".formatted(tableNameWithSuffix);
        DynamicTable dynamicTable = buildFromPql(pinotMetadata, new SchemaTableName("default", query), mockClusterInfoFetcher, TESTING_TYPE_CONVERTER);
        String expectedPql = """
                SELECT plus("ArrDelay", '34') - "DaysSinceEpoch", "FlightNum"\
                 FROM %s\
                 ORDER BY plus("ArrDelay", '34') - "DaysSinceEpoch" DESC\
                 LIMIT 10""".formatted(tableNameWithSuffix);
        assertEquals(extractPql(dynamicTable, TupleDomain.all()), expectedPql);
        assertEquals(dynamicTable.getTableName(), tableName);
    }

    @Test
    public void testQuotesInAlias()
    {
        String tableName = "quotes_in_column_names";
        String tableNameWithSuffix = tableName + REALTIME_SUFFIX;
        String query = """
                SELECT non_quoted AS "non""quoted"
                FROM %s
                LIMIT 50""".formatted(tableNameWithSuffix);
        DynamicTable dynamicTable = buildFromPql(pinotMetadata, new SchemaTableName("default", query), mockClusterInfoFetcher, TESTING_TYPE_CONVERTER);
        String expectedPql = """
                SELECT "non_quoted" AS "non""quoted"\
                 FROM %s\
                 LIMIT 50""".formatted(tableNameWithSuffix);
        assertEquals(extractPql(dynamicTable, TupleDomain.all()), expectedPql);
        assertEquals(dynamicTable.getTableName(), tableName);
    }

    @Test
    public void testQuotesInColumnName()
    {
        String tableName = "quotes_in_column_names";
        String tableNameWithSuffix = tableName + REALTIME_SUFFIX;
        String query = """
                SELECT "qu""ot""ed"
                FROM %s
                LIMIT 50""".formatted(tableNameWithSuffix);
        DynamicTable dynamicTable = buildFromPql(pinotMetadata, new SchemaTableName("default", query), mockClusterInfoFetcher, TESTING_TYPE_CONVERTER);
        String expectedPql = """
                SELECT "qu""ot""ed"\
                 FROM %s\
                 LIMIT 50""".formatted(tableNameWithSuffix);
        assertEquals(extractPql(dynamicTable, TupleDomain.all()), expectedPql);
        assertEquals(dynamicTable.getTableName(), tableName);
    }
}
