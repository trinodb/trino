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
import static java.lang.String.format;
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
        String query = format("select %s from %s order by %s limit %s",
                columnNames.stream()
                        .collect(joining(", ")),
                tableName,
                orderByColumns.stream()
                        .collect(joining(", ")) + " desc",
                limit);
        DynamicTable dynamicTable = buildFromPql(pinotMetadata, new SchemaTableName("default", query), mockClusterInfoFetcher);
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
        String query = format("SELECT Origin, AirlineID, max(CarrierDelay), avg(CarrierDelay) FROM %s GROUP BY Origin, AirlineID LIMIT %s", tableName, limit);
        DynamicTable dynamicTable = buildFromPql(pinotMetadata, new SchemaTableName("default", query), mockClusterInfoFetcher);
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
        String query = format("select FlightNum, AirlineID from %s where ((CancellationCode IN ('strike', 'weather', 'pilot_bac')) AND (Origin = 'jfk')) " +
                        "OR ((OriginCityName != 'catfish paradise') AND (OriginState != 'az') AND (AirTime between 1 and 5)) " +
                        "AND AirTime NOT IN (7,8,9) " +
                        "OR ((DepDelayMinutes < 10) AND (Distance >= 3) AND (ArrDelay > 4) AND (SecurityDelay < 5) AND (LateAircraftDelay <= 7)) limit 60",
                tableName.toLowerCase(ENGLISH));

        String expected = format("select \"FlightNum\", \"AirlineID\" from %s where OR(AND(\"CancellationCode\" IN ('strike', 'weather', 'pilot_bac'), (\"Origin\") = 'jfk'), " +
                        "AND((\"OriginCityName\") != 'catfish paradise', (\"OriginState\") != 'az', (\"AirTime\") BETWEEN '1' AND '5', \"AirTime\" NOT IN ('7', '8', '9')), " +
                        "AND((\"DepDelayMinutes\") < '10', (\"Distance\") >= '3', (\"ArrDelay\") > '4', (\"SecurityDelay\") < '5', (\"LateAircraftDelay\") <= '7')) limit 60",
                tableName.toLowerCase(ENGLISH));
        DynamicTable dynamicTable = buildFromPql(pinotMetadata, new SchemaTableName("default", query), mockClusterInfoFetcher);
        assertEquals(extractPql(dynamicTable, TupleDomain.all(), ImmutableList.of()), expected);
    }

    @Test
    public void testPrimitiveTypes()
    {
        // Note: Pinot treats varbinary literals as strings
        // ci and intellij produce different results when X'abcd' is used:
        // ci will interpret as a string, i.e. "X''ABCD"
        // intellij will expand the X'abcd' to binary 0b10011100001111
        // Pinot will interpret both forms as a string regardless, so we cannot use X'....'
        String tableName = "primitive_types_table";
        String query = "SELECT string_col, long_col, int_col, bool_col, double_col, float_col, bytes_col" +
                "  FROM " + tableName + " WHERE string_col = 'string' AND long_col = 12345678901 AND int_col = 123456789" +
                "  AND double_col = 3.56 AND float_col = 3.56 AND bytes_col = 'abcd' LIMIT 60";
        String expected = "select \"string_col\", \"long_col\", \"int_col\", \"bool_col\", \"double_col\", \"float_col\", \"bytes_col\"" +
                " from primitive_types_table where AND((\"string_col\") = 'string', (\"long_col\") = '12345678901'," +
                " (\"int_col\") = '123456789', (\"double_col\") = '3.56', (\"float_col\") = '3.56', (\"bytes_col\") = 'abcd') limit 60";
        DynamicTable dynamicTable = buildFromPql(pinotMetadata, new SchemaTableName("default", query), mockClusterInfoFetcher);
        assertEquals(extractPql(dynamicTable, TupleDomain.all(), ImmutableList.of()), expected);
    }

    @Test
    public void testDoubleWithScientificNotation()
    {
        // Pinot recognizes double literals with scientific notation as of version 0.8.0
        String tableName = "primitive_types_table";
        String query = "SELECT string_col FROM " + tableName + " WHERE double_col = 3.5E5";
        String expected = "select \"string_col\" from primitive_types_table where (\"double_col\") = '350000.0' limit 10";
        DynamicTable dynamicTable = buildFromPql(pinotMetadata, new SchemaTableName("default", query), mockClusterInfoFetcher);
        assertEquals(extractPql(dynamicTable, TupleDomain.all(), ImmutableList.of()), expected);
    }

    @Test
    public void testFilterWithCast()
    {
        String tableName = "primitive_types_table";
        String query = "SELECT string_col, long_col" +
                " FROM " + tableName + " WHERE string_col = CAST(123 AS STRING) AND long_col = CAST('123' AS LONG) LIMIT 60";
        String expected = "select \"string_col\", \"long_col\" from primitive_types_table " +
                "where AND((\"string_col\") = (CAST('123' AS string)), (\"long_col\") = (CAST('123' AS long))) limit 60";
        DynamicTable dynamicTable = buildFromPql(pinotMetadata, new SchemaTableName("default", query), mockClusterInfoFetcher);
        assertEquals(extractPql(dynamicTable, TupleDomain.all(), ImmutableList.of()), expected);
    }

    @Test
    public void testFilterWithCaseStatements()
    {
        String tableName = realtimeOnlyTable.getTableName();
        String query = format("select FlightNum, AirlineID from %s " +
                "where case when cancellationcode = 'strike' then 3 else 4 end != 5 " +
                "AND case origincityname when 'nyc' then 'pizza' when 'la' then 'burrito' when 'boston' then 'clam chowder' " +
                "else 'burger' end != 'salad'", tableName.toLowerCase(ENGLISH));
        String expected = format("select \"FlightNum\", \"AirlineID\" from %s where AND((CASE WHEN equals(\"CancellationCode\", 'strike') " +
                        "THEN '3' ELSE '4' END) != '5', (CASE WHEN equals(\"OriginCityName\", 'nyc') " +
                        "THEN 'pizza' WHEN equals(\"OriginCityName\", 'la') THEN 'burrito' WHEN equals(\"OriginCityName\", 'boston') " +
                        "THEN 'clam chowder' ELSE 'burger' END) != 'salad') limit 10",
                tableName.toLowerCase(ENGLISH));
        DynamicTable dynamicTable = buildFromPql(pinotMetadata, new SchemaTableName("default", query), mockClusterInfoFetcher);
        assertEquals(extractPql(dynamicTable, TupleDomain.all(), ImmutableList.of()), expected);
    }

    @Test
    public void testFilterWithPushdownConstraint()
    {
        String tableName = realtimeOnlyTable.getTableName();
        String query = format("select FlightNum from %s limit 60", tableName.toLowerCase(ENGLISH));
        DynamicTable dynamicTable = buildFromPql(pinotMetadata, new SchemaTableName("default", query), mockClusterInfoFetcher);
        PinotColumnHandle columnHandle = new PinotColumnHandle("OriginCityName", VARCHAR);
        TupleDomain<ColumnHandle> tupleDomain = TupleDomain.withColumnDomains(ImmutableMap.<ColumnHandle, Domain>builder()
                .put(columnHandle,
                        Domain.create(ValueSet.ofRanges(Range.equal(VARCHAR, Slices.utf8Slice("Catfish Paradise"))), false))
                .buildOrThrow());
        String expectedPql = "select \"FlightNum\" from realtimeonly where (\"OriginCityName\" = 'Catfish Paradise') limit 60";
        assertEquals(extractPql(dynamicTable, tupleDomain, ImmutableList.<PinotColumnHandle>builder()
                .add(columnHandle)
                .build()), expectedPql);
    }

    @Test
    public void testFilterWithUdf()
    {
        String tableName = realtimeOnlyTable.getTableName();
        String query = format("select FlightNum from %s where DivLongestGTimes = FLOOR(EXP(2 * LN(3))) AND 5 < EXP(CarrierDelay) limit 60", tableName.toLowerCase(ENGLISH));
        DynamicTable dynamicTable = buildFromPql(pinotMetadata, new SchemaTableName("default", query), mockClusterInfoFetcher);
        String expectedPql = "select \"FlightNum\" from realtimeonly where AND((\"DivLongestGTimes\") = '9.0', (exp(\"CarrierDelay\")) > '5') limit 60";
        assertEquals(extractPql(dynamicTable, TupleDomain.all(), ImmutableList.of()), expectedPql);
    }

    @Test
    public void testSelectStarDynamicTable()
    {
        String tableName = realtimeOnlyTable.getTableName();
        String query = format("select * from %s limit 70", tableName.toLowerCase(ENGLISH));
        DynamicTable dynamicTable = buildFromPql(pinotMetadata, new SchemaTableName("default", query), mockClusterInfoFetcher);
        String expectedPql = format("select %s from %s limit 70", getColumnNames(tableName).stream().map(TestDynamicTable::quoteIdentifier).collect(joining(", ")), tableName.toLowerCase(ENGLISH));
        assertEquals(extractPql(dynamicTable, TupleDomain.all(), ImmutableList.of()), expectedPql);
    }

    @Test
    public void testOfflineDynamicTable()
    {
        String tableName = hybridTable.getTableName();
        String tableNameWithSuffix = tableName + OFFLINE_SUFFIX;
        String query = format("select * from %s limit 70", tableNameWithSuffix);
        DynamicTable dynamicTable = buildFromPql(pinotMetadata, new SchemaTableName("default", query), mockClusterInfoFetcher);
        String expectedPql = format("select %s from %s limit 70", getColumnNames(tableName).stream().map(TestDynamicTable::quoteIdentifier).collect(joining(", ")), tableNameWithSuffix);
        assertEquals(extractPql(dynamicTable, TupleDomain.all(), ImmutableList.of()), expectedPql);
        assertEquals(dynamicTable.getTableName(), tableName);
    }

    @Test
    public void testRealtimeOnlyDynamicTable()
    {
        String tableName = hybridTable.getTableName();
        String tableNameWithSuffix = tableName + REALTIME_SUFFIX;
        String query = format("select * from %s limit 70", tableNameWithSuffix);
        DynamicTable dynamicTable = buildFromPql(pinotMetadata, new SchemaTableName("default", query), mockClusterInfoFetcher);
        String expectedPql = format("select %s from %s limit 70", getColumnNames(tableName).stream().map(TestDynamicTable::quoteIdentifier).collect(joining(", ")), tableNameWithSuffix);
        assertEquals(extractPql(dynamicTable, TupleDomain.all(), ImmutableList.of()), expectedPql);
        assertEquals(dynamicTable.getTableName(), tableName);
    }

    @Test
    public void testLimitAndOffset()
    {
        String tableName = hybridTable.getTableName();
        String tableNameWithSuffix = tableName + REALTIME_SUFFIX;
        String query = format("select * from %s limit 70, 40", tableNameWithSuffix);
        DynamicTable dynamicTable = buildFromPql(pinotMetadata, new SchemaTableName("default", query), mockClusterInfoFetcher);
        String expectedPql = format("select %s from %s limit 70, 40", getColumnNames(tableName).stream().map(TestDynamicTable::quoteIdentifier).collect(joining(", ")), tableNameWithSuffix);
        assertEquals(extractPql(dynamicTable, TupleDomain.all(), ImmutableList.of()), expectedPql);
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
        String query = format("select origincityname from %s where regexp_like(origincityname, '.*york.*') limit 70", tableNameWithSuffix);
        DynamicTable dynamicTable = buildFromPql(pinotMetadata, new SchemaTableName("default", query), mockClusterInfoFetcher);
        String expectedPql = format("select \"OriginCityName\" from %s where regexp_like(\"OriginCityName\", '.*york.*') limit 70", tableNameWithSuffix);
        assertEquals(extractPql(dynamicTable, TupleDomain.all(), ImmutableList.of()), expectedPql);
        assertEquals(dynamicTable.getTableName(), tableName);
    }

    @Test
    public void testTextMatch()
    {
        String tableName = hybridTable.getTableName();
        String tableNameWithSuffix = tableName + REALTIME_SUFFIX;
        String query = format("select origincityname from %s where text_match(origincityname, 'new AND york') limit 70", tableNameWithSuffix);
        DynamicTable dynamicTable = buildFromPql(pinotMetadata, new SchemaTableName("default", query), mockClusterInfoFetcher);
        String expectedPql = format("select \"OriginCityName\" from %s where text_match(\"OriginCityName\", 'new and york') limit 70", tableNameWithSuffix);
        assertEquals(extractPql(dynamicTable, TupleDomain.all(), ImmutableList.of()), expectedPql);
        assertEquals(dynamicTable.getTableName(), tableName);
    }

    @Test
    public void testJsonMatch()
    {
        String tableName = hybridTable.getTableName();
        String tableNameWithSuffix = tableName + REALTIME_SUFFIX;
        String query = format("select origincityname from %s where json_match(origincityname, '\"$.name\"=''new york''') limit 70", tableNameWithSuffix);
        DynamicTable dynamicTable = buildFromPql(pinotMetadata, new SchemaTableName("default", query), mockClusterInfoFetcher);
        String expectedPql = format("select \"OriginCityName\" from %s where json_match(\"OriginCityName\", '\"$.name\"=''new york''') limit 70", tableNameWithSuffix);
        assertEquals(extractPql(dynamicTable, TupleDomain.all(), ImmutableList.of()), expectedPql);
        assertEquals(dynamicTable.getTableName(), tableName);
    }

    @Test
    public void testSelectExpressionsWithAliases()
    {
        String tableName = hybridTable.getTableName();
        String tableNameWithSuffix = tableName + REALTIME_SUFFIX;
        String query = format("select datetimeconvert(dayssinceEpoch, '1:seconds:epoch', '1:milliseconds:epoch', '15:minutes'), " +
                "case origincityname when 'nyc' then 'pizza' when 'la' then 'burrito' when 'boston' then 'clam chowder'" +
                " else 'burger' end != 'salad'," +
                " timeconvert(dayssinceEpoch, 'seconds', 'minutes') as foo" +
                " from %s  limit 70", tableNameWithSuffix);

        DynamicTable dynamicTable = buildFromPql(pinotMetadata, new SchemaTableName("default", query), mockClusterInfoFetcher);
        String expectedPql = format("select datetimeconvert(\"DaysSinceEpoch\", '1:SECONDS:EPOCH', '1:MILLISECONDS:EPOCH', '15:MINUTES')," +
                " not_equals(CASE WHEN equals(\"OriginCityName\", 'nyc') THEN 'pizza' WHEN equals(\"OriginCityName\", 'la') THEN 'burrito' WHEN equals(\"OriginCityName\", 'boston') THEN 'clam chowder' ELSE 'burger' END, 'salad')," +
                " timeconvert(\"DaysSinceEpoch\", 'SECONDS', 'MINUTES') AS \"foo\" from %s limit 70", tableNameWithSuffix);
        assertEquals(extractPql(dynamicTable, TupleDomain.all(), ImmutableList.of()), expectedPql);
        assertEquals(dynamicTable.getTableName(), tableName);
    }

    @Test
    public void testAggregateExpressionsWithAliases()
    {
        String tableName = hybridTable.getTableName();
        String tableNameWithSuffix = tableName + REALTIME_SUFFIX;
        String query = format("select datetimeconvert(dayssinceEpoch, '1:seconds:epoch', '1:milliseconds:epoch', '15:minutes'), " +
                " count(*) as bar," +
                " case origincityname when 'nyc' then 'pizza' when 'la' then 'burrito' when 'boston' then 'clam chowder'" +
                " else 'burger' end != 'salad'," +
                " timeconvert(dayssinceEpoch, 'seconds', 'minutes') as foo," +
                " max(airtime) as baz" +
                " from %s  group by 1, 3, 4 limit 70", tableNameWithSuffix);

        DynamicTable dynamicTable = buildFromPql(pinotMetadata, new SchemaTableName("default", query), mockClusterInfoFetcher);
        String expectedPql = format("select datetimeconvert(\"DaysSinceEpoch\", '1:SECONDS:EPOCH'," +
                " '1:MILLISECONDS:EPOCH', '15:MINUTES'), count(*) AS \"bar\"," +
                " not_equals(CASE WHEN equals(\"OriginCityName\", 'nyc') THEN 'pizza' WHEN equals(\"OriginCityName\", 'la') THEN 'burrito'" +
                " WHEN equals(\"OriginCityName\", 'boston') THEN 'clam chowder' ELSE 'burger' END, 'salad')," +
                " timeconvert(\"DaysSinceEpoch\", 'SECONDS', 'MINUTES') AS \"foo\"," +
                " max(\"AirTime\") AS \"baz\"" +
                " from %s" +
                " group by datetimeconvert(\"DaysSinceEpoch\", '1:SECONDS:EPOCH', '1:MILLISECONDS:EPOCH', '15:MINUTES')," +
                " not_equals(CASE WHEN equals(\"OriginCityName\", 'nyc') THEN 'pizza' WHEN equals(\"OriginCityName\", 'la') THEN 'burrito' WHEN equals(\"OriginCityName\", 'boston') THEN 'clam chowder' ELSE 'burger' END, 'salad')," +
                " timeconvert(\"DaysSinceEpoch\", 'SECONDS', 'MINUTES')" +
                " limit 70", tableNameWithSuffix);
        assertEquals(extractPql(dynamicTable, TupleDomain.all(), ImmutableList.of()), expectedPql);
        assertEquals(dynamicTable.getTableName(), tableName);
    }

    @Test
    public void testOrderBy()
    {
        String tableName = hybridTable.getTableName();
        String tableNameWithSuffix = tableName + REALTIME_SUFFIX;
        String query = format("select ArrDelay + 34 - DaysSinceEpoch, FlightNum from %s order by ArrDelay asc, DaysSinceEpoch desc", tableNameWithSuffix);
        DynamicTable dynamicTable = buildFromPql(pinotMetadata, new SchemaTableName("default", query), mockClusterInfoFetcher);
        String expectedPql = format("select plus(\"ArrDelay\", '34') - \"DaysSinceEpoch\", \"FlightNum\" from %s order by \"ArrDelay\", \"DaysSinceEpoch\" desc limit 10", tableNameWithSuffix);
        assertEquals(extractPql(dynamicTable, TupleDomain.all(), ImmutableList.of()), expectedPql);
        assertEquals(dynamicTable.getTableName(), tableName);
    }

    @Test
    public void testOrderByCountStar()
    {
        String tableName = hybridTable.getTableName();
        String tableNameWithSuffix = tableName + REALTIME_SUFFIX;
        String query = format("select count(*) from %s order by count(*)", tableNameWithSuffix);
        DynamicTable dynamicTable = buildFromPql(pinotMetadata, new SchemaTableName("default", query), mockClusterInfoFetcher);
        String expectedPql = format("select count(*) from %s order by count(*) limit 10", tableNameWithSuffix);
        assertEquals(extractPql(dynamicTable, TupleDomain.all(), ImmutableList.of()), expectedPql);
        assertEquals(dynamicTable.getTableName(), tableName);
    }

    @Test
    public void testOrderByExpression()
    {
        String tableName = hybridTable.getTableName();
        String tableNameWithSuffix = tableName + REALTIME_SUFFIX;
        String query = format("select ArrDelay + 34 - DaysSinceEpoch, FlightNum from %s order by ArrDelay + 34 - DaysSinceEpoch desc", tableNameWithSuffix);
        DynamicTable dynamicTable = buildFromPql(pinotMetadata, new SchemaTableName("default", query), mockClusterInfoFetcher);
        String expectedPql = format("select plus(\"ArrDelay\", '34') - \"DaysSinceEpoch\", \"FlightNum\" from %s order by plus(\"ArrDelay\", '34') - \"DaysSinceEpoch\" desc limit 10", tableNameWithSuffix);
        assertEquals(extractPql(dynamicTable, TupleDomain.all(), ImmutableList.of()), expectedPql);
        assertEquals(dynamicTable.getTableName(), tableName);
    }

    @Test
    public void testQuotesInAlias()
    {
        String tableName = "quotes_in_column_names";
        String tableNameWithSuffix = tableName + REALTIME_SUFFIX;
        String query = format("select non_quoted AS \"non\"\"quoted\" from %s limit 50", tableNameWithSuffix);
        DynamicTable dynamicTable = buildFromPql(pinotMetadata, new SchemaTableName("default", query), mockClusterInfoFetcher);
        String expectedPql = format("select \"non_quoted\" AS \"non\"\"quoted\" from %s limit 50", tableNameWithSuffix);
        assertEquals(extractPql(dynamicTable, TupleDomain.all(), ImmutableList.of()), expectedPql);
        assertEquals(dynamicTable.getTableName(), tableName);
    }

    @Test
    public void testQuotesInColumnName()
    {
        String tableName = "quotes_in_column_names";
        String tableNameWithSuffix = tableName + REALTIME_SUFFIX;
        String query = format("select \"qu\"\"ot\"\"ed\" from %s limit 50", tableNameWithSuffix);
        DynamicTable dynamicTable = buildFromPql(pinotMetadata, new SchemaTableName("default", query), mockClusterInfoFetcher);
        String expectedPql = format("select \"qu\"\"ot\"\"ed\" from %s limit 50", tableNameWithSuffix);
        assertEquals(extractPql(dynamicTable, TupleDomain.all(), ImmutableList.of()), expectedPql);
        assertEquals(dynamicTable.getTableName(), tableName);
    }
}
