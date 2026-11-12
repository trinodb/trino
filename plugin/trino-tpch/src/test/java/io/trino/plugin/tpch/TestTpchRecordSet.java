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
package io.trino.plugin.tpch;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.RecordCursor;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.Range;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.predicate.ValueSet;
import io.trino.spi.type.Type;
import io.trino.tpch.OrderColumn;
import io.trino.tpch.PartColumn;
import io.trino.tpch.TpchColumn;
import io.trino.tpch.TpchEntity;
import io.trino.tpch.TpchTable;
import org.junit.jupiter.api.Test;

import java.time.LocalDate;
import java.util.List;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.VarcharType.createVarcharType;
import static org.assertj.core.api.Assertions.assertThat;

public class TestTpchRecordSet
{
    private static final double SCALE_FACTOR = 0.01;

    private static final Type ORDER_STATUS_TYPE = createVarcharType(1);
    private static final Type PART_TYPE_TYPE = createVarcharType(25);
    private static final Type PART_CONTAINER_TYPE = createVarcharType(10);

    private static final TpchColumnHandle ORDER_STATUS_HANDLE = new TpchColumnHandle("orderstatus", ORDER_STATUS_TYPE);
    private static final TpchColumnHandle PART_TYPE_HANDLE = new TpchColumnHandle("type", PART_TYPE_TYPE);
    private static final TpchColumnHandle PART_CONTAINER_HANDLE = new TpchColumnHandle("container", PART_CONTAINER_TYPE);

    @Test
    public void testUnrestrictedPredicate()
    {
        List<String> allStatuses = readColumn(TpchTable.ORDERS, OrderColumn.ORDER_STATUS, TupleDomain.all());
        assertThat(allStatuses).hasSize(15_000);
        assertThat(allStatuses).containsOnly("F", "O", "P");
    }

    @Test
    public void testNonePredicate()
    {
        assertThat(readColumn(TpchTable.ORDERS, OrderColumn.ORDER_STATUS, TupleDomain.none())).isEmpty();
    }

    @Test
    public void testOrderStatusPredicate()
    {
        List<String> allStatuses = readColumn(TpchTable.ORDERS, OrderColumn.ORDER_STATUS, TupleDomain.all());

        assertThat(readColumn(TpchTable.ORDERS, OrderColumn.ORDER_STATUS, domain(ORDER_STATUS_HANDLE, "F")))
                .isNotEmpty()
                .containsOnly("F")
                .hasSize((int) allStatuses.stream().filter("F"::equals).count());

        assertThat(readColumn(TpchTable.ORDERS, OrderColumn.ORDER_STATUS, domain(ORDER_STATUS_HANDLE, "F", "P")))
                .isEqualTo(allStatuses.stream()
                        .filter(status -> status.equals("F") || status.equals("P"))
                        .collect(toImmutableList()));

        assertThat(readColumn(TpchTable.ORDERS, OrderColumn.ORDER_STATUS, domain(ORDER_STATUS_HANDLE, "NO SUCH STATUS"))).isEmpty();
        assertThat(readColumn(TpchTable.ORDERS, OrderColumn.ORDER_STATUS, domain(ORDER_STATUS_HANDLE, "F", "O", "P"))).isEqualTo(allStatuses);
    }

    @Test
    public void testPartPredicate()
    {
        List<String> allTypes = readColumn(TpchTable.PART, PartColumn.TYPE, TupleDomain.all());
        List<String> allContainers = readColumn(TpchTable.PART, PartColumn.CONTAINER, TupleDomain.all());
        assertThat(allTypes).hasSameSizeAs(allContainers);

        String type = allTypes.getFirst();
        String container = allContainers.getFirst();

        // single column
        assertThat(readColumn(TpchTable.PART, PartColumn.TYPE, domain(PART_TYPE_HANDLE, type)))
                .isNotEmpty()
                .containsOnly(type)
                .hasSize((int) allTypes.stream().filter(type::equals).count());

        // all columns have to match at the same time
        TupleDomain<ColumnHandle> both = TupleDomain.withColumnDomains(ImmutableMap.of(
                PART_TYPE_HANDLE, domain(PART_TYPE_TYPE, type),
                PART_CONTAINER_HANDLE, domain(PART_CONTAINER_TYPE, container)));
        int expectedRows = 0;
        for (int i = 0; i < allTypes.size(); i++) {
            if (allTypes.get(i).equals(type) && allContainers.get(i).equals(container)) {
                expectedRows++;
            }
        }
        assertThat(readRows(TpchTable.PART, ImmutableList.of(PartColumn.TYPE, PartColumn.CONTAINER), both))
                .isNotEmpty()
                .containsOnly(ImmutableList.of(type, container))
                .hasSize(expectedRows);
    }

    @Test
    public void testNonDiscretePredicate()
    {
        Domain prefix = Domain.create(
                ValueSet.ofRanges(Range.range(PART_TYPE_TYPE, utf8Slice("LARGE PLATED "), true, utf8Slice("LARGE PLATED!"), false)),
                false);
        List<String> allTypes = readColumn(TpchTable.PART, PartColumn.TYPE, TupleDomain.all());

        assertThat(readColumn(TpchTable.PART, PartColumn.TYPE, TupleDomain.withColumnDomains(ImmutableMap.of(PART_TYPE_HANDLE, prefix))))
                .isNotEmpty()
                .isEqualTo(allTypes.stream()
                        .filter(type -> type.startsWith("LARGE PLATED "))
                        .collect(toImmutableList()));
    }

    @Test
    public void testLongRangePredicate()
    {
        assertLongRange(TpchTable.ORDERS, OrderColumn.ORDER_DATE, DATE, LocalDate.of(1995, 1, 1).toEpochDay(), LocalDate.of(1995, 12, 31).toEpochDay());
        assertLongRange(TpchTable.PART, PartColumn.SIZE, INTEGER, 10, 20);
        assertLongRange(TpchTable.ORDERS, OrderColumn.ORDER_KEY, BIGINT, 1_000, 2_000);
    }

    private static <E extends TpchEntity> void assertLongRange(TpchTable<E> table, TpchColumn<E> column, Type type, long low, long high)
    {
        List<Long> allValues = readLongColumn(table, column, TupleDomain.all());
        List<Long> expected = allValues.stream()
                .filter(value -> value >= low && value <= high)
                .collect(toImmutableList());
        assertThat(expected)
                .isNotEmpty()
                .hasSizeLessThan(allValues.size());

        TpchColumnHandle handle = new TpchColumnHandle(column.getColumnName(), type);
        Domain range = Domain.create(ValueSet.ofRanges(Range.range(type, low, true, high, true)), false);
        assertThat(readLongColumn(table, column, TupleDomain.withColumnDomains(ImmutableMap.of(handle, range))))
                .isEqualTo(expected);
    }

    private static TupleDomain<ColumnHandle> domain(TpchColumnHandle column, String... values)
    {
        return TupleDomain.withColumnDomains(ImmutableMap.of(column, domain(column.type(), values)));
    }

    private static Domain domain(Type type, String... values)
    {
        ImmutableList.Builder<Object> slices = ImmutableList.builder();
        for (String value : values) {
            slices.add(utf8Slice(value));
        }
        return Domain.create(ValueSet.copyOf(type, slices.build()), false);
    }

    private static <E extends TpchEntity> List<String> readColumn(TpchTable<E> table, TpchColumn<E> column, TupleDomain<ColumnHandle> predicate)
    {
        return readRows(table, ImmutableList.of(column), predicate).stream()
                .map(List::getFirst)
                .collect(toImmutableList());
    }

    private static <E extends TpchEntity> List<Long> readLongColumn(TpchTable<E> table, TpchColumn<E> column, TupleDomain<ColumnHandle> predicate)
    {
        RecordCursor cursor = TpchRecordSet.createTpchRecordSet(table, ImmutableList.of(column), DecimalTypeMapping.DOUBLE, SCALE_FACTOR, 1, 1, predicate).cursor();
        ImmutableList.Builder<Long> values = ImmutableList.builder();
        while (cursor.advanceNextPosition()) {
            values.add(cursor.getLong(0));
        }
        return values.build();
    }

    private static <E extends TpchEntity> List<List<String>> readRows(TpchTable<E> table, List<TpchColumn<E>> columns, TupleDomain<ColumnHandle> predicate)
    {
        RecordCursor cursor = TpchRecordSet.createTpchRecordSet(table, columns, DecimalTypeMapping.DOUBLE, SCALE_FACTOR, 1, 1, predicate).cursor();
        ImmutableList.Builder<List<String>> rows = ImmutableList.builder();
        while (cursor.advanceNextPosition()) {
            ImmutableList.Builder<String> row = ImmutableList.builder();
            for (int field = 0; field < columns.size(); field++) {
                row.add(cursor.getSlice(field).toStringUtf8());
            }
            rows.add(row.build());
        }
        return rows.build();
    }
}
