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
package io.prestosql.pinot.query;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.slice.Slices;
import io.prestosql.pinot.PinotColumnHandle;
import io.prestosql.pinot.TestPinotQueryBase;
import io.prestosql.spi.connector.ColumnHandle;
import io.prestosql.spi.connector.SchemaTableName;
import io.prestosql.spi.predicate.Domain;
import io.prestosql.spi.predicate.Range;
import io.prestosql.spi.predicate.TupleDomain;
import io.prestosql.spi.predicate.ValueSet;
import org.testng.annotations.Test;

import java.util.List;

import static io.prestosql.pinot.query.DynamicTableBuilder.OFFLINE_SUFFIX;
import static io.prestosql.pinot.query.DynamicTableBuilder.REALTIME_SUFFIX;
import static io.prestosql.pinot.query.DynamicTableBuilder.buildFromPql;
import static io.prestosql.pinot.query.DynamicTablePqlExtractor.extractPql;
import static io.prestosql.spi.type.VarcharType.VARCHAR;
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
                .map(columnName -> new OrderByExpression(columnName, true))
                .collect(toList());
        long limit = 230;
        String query = format("select %s from %s order by %s limit %s",
                columnNames.stream()
                        .collect(joining(", ")),
                tableName,
                orderByColumns.stream()
                        .collect(joining(", ")) + " desc",
                limit);
        DynamicTable dynamicTable = buildFromPql(pinotMetadata, new SchemaTableName("default", query));
        assertEquals(dynamicTable.getSelections(), columnNames);
        orderByExpressions.add(new OrderByExpression(orderByColumns.get(4), false));
        assertEquals(dynamicTable.getOrderBy(), orderByExpressions);
        assertEquals(dynamicTable.getLimit().getAsLong(), limit);
    }

    @Test
    public void testGroupBy()
    {
        String tableName = realtimeOnlyTable.getTableName();
        long limit = 25;
        String query = format("SELECT Origin, AirlineID, max(CarrierDelay), avg(CarrierDelay) FROM %s GROUP BY Origin, AirlineID LIMIT %s", tableName, limit);
        DynamicTable dynamicTable = buildFromPql(pinotMetadata, new SchemaTableName("default", query));
        assertEquals(dynamicTable.getGroupingColumns(), ImmutableList.builder()
                .add("Origin")
                .add("AirlineID")
                .build());
        assertEquals(dynamicTable.getAggregateColumns(), ImmutableList.builder()
                .add(new AggregationExpression("max(carrierdelay)", "CarrierDelay", "MAX"))
                .add(new AggregationExpression("avg(carrierdelay)", "CarrierDelay", "AVG"))
                .build());
        assertEquals(dynamicTable.getLimit().getAsLong(), limit);
    }

    @Test
    public void testFilter()
    {
        String tableName = realtimeOnlyTable.getTableName();
        String query = format("select FlightNum, AirlineID from realtimeonly where (((CancellationCode IN ('strike', 'weather', 'pilot_bac')) AND (Origin = 'jfk')) " +
                        "OR (((OriginCityName != 'catfish paradise') AND (OriginState != 'az')) AND (AirTime between 1 and 5))) " +
                        "OR (((((DepDelayMinutes < 10) AND (Distance >= 3)) AND (ArrDelay > 4)) AND (SecurityDelay < 5)) AND (LateAircraftDelay <= 7)) limit 60",
                tableName.toLowerCase(ENGLISH));
        DynamicTable dynamicTable = buildFromPql(pinotMetadata, new SchemaTableName("default", query));
        assertEquals(extractPql(dynamicTable, TupleDomain.all(), ImmutableList.of()), query);
    }

    @Test
    public void testFilterWithPushdownConstraint()
    {
        String tableName = realtimeOnlyTable.getTableName();
        String query = format("select FlightNum from %s limit 60", tableName.toLowerCase(ENGLISH));
        DynamicTable dynamicTable = buildFromPql(pinotMetadata, new SchemaTableName("default", query));
        PinotColumnHandle columnHandle = new PinotColumnHandle("OriginCityName", VARCHAR);
        TupleDomain<ColumnHandle> tupleDomain = TupleDomain.withColumnDomains(ImmutableMap.<ColumnHandle, Domain>builder()
                .put(columnHandle,
                        Domain.create(ValueSet.ofRanges(Range.equal(VARCHAR, Slices.utf8Slice("Catfish Paradise"))), false))
                .build());
        String expectedPql = "select FlightNum from realtimeonly where (OriginCityName = 'Catfish Paradise') limit 60";
        assertEquals(extractPql(dynamicTable, tupleDomain, ImmutableList.<PinotColumnHandle>builder()
                .add(columnHandle)
                .build()), expectedPql);
    }

    @Test
    public void testFilterWithUdf()
    {
        String tableName = realtimeOnlyTable.getTableName();
        String query = format("select FlightNum from %s where DivLongestGTimes = POW(3, 2) limit 60", tableName.toLowerCase(ENGLISH));
        DynamicTable dynamicTable = buildFromPql(pinotMetadata, new SchemaTableName("default", query));
        String expectedPql = "select FlightNum from realtimeonly where minus(divlongestgtimes,pow('3','2')) = '0' limit 60";
        assertEquals(extractPql(dynamicTable, TupleDomain.all(), ImmutableList.of()), expectedPql);
    }

    @Test
    public void testSelectStarDynamicTable()
    {
        String tableName = realtimeOnlyTable.getTableName();
        String query = format("select * from %s limit 70", tableName.toLowerCase(ENGLISH));
        DynamicTable dynamicTable = buildFromPql(pinotMetadata, new SchemaTableName("default", query));
        String expectedPql = format("select %s from %s limit 70", getColumnNames(tableName).stream().collect(joining(", ")), tableName.toLowerCase(ENGLISH));
        assertEquals(extractPql(dynamicTable, TupleDomain.all(), ImmutableList.of()), expectedPql);
    }

    @Test
    public void testOfflineDynamicTable()
    {
        String tableName = hybridTable.getTableName();
        String tableNameWithSuffix = tableName + OFFLINE_SUFFIX;
        String query = format("select * from %s limit 70", tableNameWithSuffix);
        DynamicTable dynamicTable = buildFromPql(pinotMetadata, new SchemaTableName("default", query));
        String expectedPql = format("select %s from %s limit 70", getColumnNames(tableName).stream().collect(joining(", ")), tableNameWithSuffix);
        assertEquals(extractPql(dynamicTable, TupleDomain.all(), ImmutableList.of()), expectedPql);
        assertEquals(dynamicTable.getTableName(), tableName);
    }

    @Test
    public void testRealtimeOnlyDynamicTable()
    {
        String tableName = hybridTable.getTableName();
        String tableNameWithSuffix = tableName + REALTIME_SUFFIX;
        String query = format("select * from %s limit 70", tableNameWithSuffix);
        DynamicTable dynamicTable = buildFromPql(pinotMetadata, new SchemaTableName("default", query));
        String expectedPql = format("select %s from %s limit 70", getColumnNames(tableName).stream().collect(joining(", ")), tableNameWithSuffix);
        assertEquals(extractPql(dynamicTable, TupleDomain.all(), ImmutableList.of()), expectedPql);
        assertEquals(dynamicTable.getTableName(), tableName);
    }
}
