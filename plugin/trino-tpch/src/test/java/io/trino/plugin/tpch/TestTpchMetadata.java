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
import io.trino.plugin.tpch.util.PredicateUtils;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.Constraint;
import io.trino.spi.connector.ConstraintApplicationResult;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.predicate.NullableValue;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.statistics.ColumnStatistics;
import io.trino.spi.statistics.DoubleRange;
import io.trino.spi.statistics.Estimate;
import io.trino.spi.statistics.TableStatistics;
import io.trino.tpch.PartColumn;
import io.trino.tpch.TpchColumn;
import io.trino.tpch.TpchTable;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Stream;

import static com.google.common.base.Preconditions.checkArgument;
import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.plugin.tpch.TpchMetadata.getTrinoType;
import static io.trino.plugin.tpch.util.PredicateUtils.filterOutColumnFromPredicate;
import static io.trino.spi.connector.Constraint.alwaysFalse;
import static io.trino.spi.connector.Constraint.alwaysTrue;
import static io.trino.tpch.CustomerColumn.MARKET_SEGMENT;
import static io.trino.tpch.CustomerColumn.NAME;
import static io.trino.tpch.LineItemColumn.LINE_NUMBER;
import static io.trino.tpch.NationColumn.NATION_KEY;
import static io.trino.tpch.OrderColumn.CLERK;
import static io.trino.tpch.OrderColumn.ORDER_DATE;
import static io.trino.tpch.OrderColumn.ORDER_KEY;
import static io.trino.tpch.OrderColumn.ORDER_STATUS;
import static io.trino.tpch.PartColumn.PART_KEY;
import static io.trino.tpch.PartColumn.RETAIL_PRICE;
import static io.trino.tpch.TpchTable.CUSTOMER;
import static io.trino.tpch.TpchTable.LINE_ITEM;
import static io.trino.tpch.TpchTable.NATION;
import static io.trino.tpch.TpchTable.ORDERS;
import static io.trino.tpch.TpchTable.PART;
import static io.trino.tpch.TpchTable.PART_SUPPLIER;
import static io.trino.tpch.TpchTable.REGION;
import static io.trino.tpch.TpchTable.SUPPLIER;
import static java.lang.String.format;
import static java.util.Arrays.stream;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

public class TestTpchMetadata
{
    private static final double TOLERANCE = 0.01;

    private static final List<String> SUPPORTED_SCHEMAS = ImmutableList.of("tiny", "sf1");

    private final TpchMetadata tpchMetadata = new TpchMetadata();
    private final ConnectorSession session = null;

    @Test
    public void testTableStats()
    {
        SUPPORTED_SCHEMAS.forEach(schema -> {
            double scaleFactor = TpchMetadata.schemaNameToScaleFactor(schema);
            testTableStats(schema, REGION, 5);
            testTableStats(schema, NATION, 25);
            testTableStats(schema, SUPPLIER, 10_000 * scaleFactor);
            testTableStats(schema, CUSTOMER, 150_000 * scaleFactor);
            testTableStats(schema, PART, 200_000 * scaleFactor);
            testTableStats(schema, PART_SUPPLIER, 800_000 * scaleFactor);
            testTableStats(schema, ORDERS, 1_500_000 * scaleFactor);
            testTableStats(schema, LINE_ITEM, 6_000_000 * scaleFactor);
        });
    }

    @Test
    public void testNoTableStats()
    {
        Stream.of("sf10", "sf100").forEach(schema -> {
            testNoTableStats(schema, REGION);
            testNoTableStats(schema, NATION);
            testNoTableStats(schema, SUPPLIER);
            testNoTableStats(schema, CUSTOMER);
            testNoTableStats(schema, PART);
            testNoTableStats(schema, PART_SUPPLIER);
            testNoTableStats(schema, ORDERS);
            testNoTableStats(schema, LINE_ITEM);
        });
    }

    @Test
    public void testTableStatsWithConstraints()
    {
        SUPPORTED_SCHEMAS.forEach(schema -> {
            double scaleFactor = TpchMetadata.schemaNameToScaleFactor(schema);

            testTableStats(schema, ORDERS, alwaysFalse(), 0);
            testTableStats(schema, ORDERS, constraint(ORDER_STATUS, "NO SUCH STATUS"), 0);
            testTableStats(schema, ORDERS, constraint(ORDER_STATUS, "F"), 730_400 * scaleFactor);
            testTableStats(schema, ORDERS, constraint(ORDER_STATUS, "O"), 733_300 * scaleFactor);
            testTableStats(schema, ORDERS, constraint(ORDER_STATUS, "F", "NO SUCH STATUS"), 730_400 * scaleFactor);
            testTableStats(schema, ORDERS, constraint(ORDER_STATUS, "F", "O", "P"), 1_500_000 * scaleFactor);
        });
    }

    @Test
    public void testGetTableMetadata()
    {
        Stream.of("sf0.01", "tiny", "sf1.0", "sf1.000", "sf2.01", "sf3.1", "sf10.0", "sf100.0", "sf30000.0", "sf30000.2").forEach(
                schemaName -> {
                    testGetTableMetadata(schemaName, REGION);
                    testGetTableMetadata(schemaName, NATION);
                    testGetTableMetadata(schemaName, SUPPLIER);
                    testGetTableMetadata(schemaName, CUSTOMER);
                    testGetTableMetadata(schemaName, PART);
                    testGetTableMetadata(schemaName, PART_SUPPLIER);
                    testGetTableMetadata(schemaName, ORDERS);
                    testGetTableMetadata(schemaName, LINE_ITEM);
                });
    }

    private void testGetTableMetadata(String schema, TpchTable<?> table)
    {
        TpchTableHandle tableHandle = tpchMetadata.getTableHandle(session, new SchemaTableName(schema, table.getTableName()));
        ConnectorTableMetadata tableMetadata = tpchMetadata.getTableMetadata(session, tableHandle);
        assertEquals(tableMetadata.getTableSchema().getTable().getTableName(), table.getTableName());
        assertEquals(tableMetadata.getTableSchema().getTable().getSchemaName(), schema);
    }

    @Test
    public void testHiddenSchemas()
    {
        assertTrue(tpchMetadata.schemaExists(session, "sf1"));
        assertTrue(tpchMetadata.schemaExists(session, "sf3000.0"));
        assertFalse(tpchMetadata.schemaExists(session, "sf0"));
        assertFalse(tpchMetadata.schemaExists(session, "hf1"));
        assertFalse(tpchMetadata.schemaExists(session, "sf"));
        assertFalse(tpchMetadata.schemaExists(session, "sfabc"));
    }

    private void testTableStats(String schema, TpchTable<?> table, double expectedRowCount)
    {
        testTableStats(schema, table, alwaysTrue(), expectedRowCount);
    }

    private void testTableStats(String schema, TpchTable<?> table, Constraint constraint, double expectedRowCount)
    {
        TpchTableHandle tableHandle = tpchMetadata.getTableHandle(session, new SchemaTableName(schema, table.getTableName()));
        Optional<ConstraintApplicationResult<ConnectorTableHandle>> result = tpchMetadata.applyFilter(session, tableHandle, constraint);
        if (result.isPresent()) {
            tableHandle = (TpchTableHandle) result.get().getHandle();
        }
        TableStatistics tableStatistics = tpchMetadata.getTableStatistics(session, tableHandle);

        double actualRowCountValue = tableStatistics.getRowCount().getValue();
        assertEquals(tableStatistics.getRowCount(), Estimate.of(actualRowCountValue));
        assertEquals(actualRowCountValue, expectedRowCount, expectedRowCount * TOLERANCE);
    }

    private void testNoTableStats(String schema, TpchTable<?> table)
    {
        TpchTableHandle tableHandle = tpchMetadata.getTableHandle(session, new SchemaTableName(schema, table.getTableName()));
        TableStatistics tableStatistics = tpchMetadata.getTableStatistics(session, tableHandle);
        assertTrue(tableStatistics.getRowCount().isUnknown());
    }

    @Test
    public void testColumnStats()
    {
        Stream.of("tiny", "sf1").forEach(schema -> {
            double scaleFactor = TpchMetadata.schemaNameToScaleFactor(schema);

            //id column
            testColumnStats(schema, NATION, NATION_KEY, columnStatistics(25, 0, 24));

            //foreign key to dictionary identifier columns
            testColumnStats(schema, SUPPLIER, NATION_KEY, columnStatistics(25, 0, 24));

            //foreign key to scalable identifier column
            testColumnStats(schema, PART_SUPPLIER, PART_KEY, columnStatistics(200_000 * scaleFactor, 1, 200_000 * scaleFactor));

            //dictionary
            testColumnStats(schema, CUSTOMER, MARKET_SEGMENT, columnStatistics(5, 45));

            //low-valued numeric column
            testColumnStats(schema, LINE_ITEM, LINE_NUMBER, columnStatistics(7, 1, 7));

            //date
            testColumnStats(schema, ORDERS, ORDER_DATE, columnStatistics(2_400, 8_035, 10_440));

            //varchar and double columns
            if (schema.equals("tiny")) {
                testColumnStats(schema, CUSTOMER, NAME, columnStatistics(150_000 * scaleFactor, 27000));
                testColumnStats(schema, PART, RETAIL_PRICE, columnStatistics(1_099, 901, 1900.99));
            }
            else if (schema.equals("sf1")) {
                testColumnStats(schema, CUSTOMER, NAME, columnStatistics(150_000 * scaleFactor, 2700000));
                testColumnStats(schema, PART, RETAIL_PRICE, columnStatistics(20899, 901, 2089.99));
            }
        });
    }

    @Test
    public void testColumnStatsWithConstraints()
    {
        SUPPORTED_SCHEMAS.forEach(schema -> {
            double scaleFactor = TpchMetadata.schemaNameToScaleFactor(schema);

            //value count, min and max are supported for the constrained column
            testColumnStats(schema, ORDERS, ORDER_STATUS, constraint(ORDER_STATUS, "F"), columnStatistics(1, 1));
            testColumnStats(schema, ORDERS, ORDER_STATUS, constraint(ORDER_STATUS, "O"), columnStatistics(1, 1));
            testColumnStats(schema, ORDERS, ORDER_STATUS, constraint(ORDER_STATUS, "P"), columnStatistics(1, 1));

            //only min and max values for non-scaling columns can be estimated for non-constrained columns
            testColumnStats(schema, ORDERS, ORDER_KEY, constraint(ORDER_STATUS, "F"), rangeStatistics(3, 6_000_000 * scaleFactor));
            testColumnStats(schema, ORDERS, ORDER_KEY, constraint(ORDER_STATUS, "O"), rangeStatistics(1, 6_000_000 * scaleFactor));
            testColumnStats(schema, ORDERS, ORDER_KEY, constraint(ORDER_STATUS, "P"), rangeStatistics(65, 6_000_000 * scaleFactor));
            testColumnStats(schema, ORDERS, CLERK, constraint(ORDER_STATUS, "O"), createColumnStatistics(Optional.empty(), Optional.empty(), Optional.of(15000.0)));

            //nothing can be said for always false constraints
            testColumnStats(schema, ORDERS, ORDER_STATUS, alwaysFalse(), noColumnStatistics());
            testColumnStats(schema, ORDERS, ORDER_KEY, alwaysFalse(), noColumnStatistics());
            testColumnStats(schema, ORDERS, ORDER_STATUS, constraint(ORDER_STATUS, "NO SUCH STATUS"), noColumnStatistics());
            testColumnStats(schema, ORDERS, ORDER_KEY, constraint(ORDER_STATUS, "NO SUCH STATUS"), noColumnStatistics());

            //unmodified stats are returned for the always true constraint
            testColumnStats(schema, ORDERS, ORDER_STATUS, alwaysTrue(), columnStatistics(3, 3));
            testColumnStats(schema, ORDERS, ORDER_KEY, alwaysTrue(), columnStatistics(1_500_000 * scaleFactor, 1, 6_000_000 * scaleFactor));

            //constraints on columns other than ORDER_STATUS are not supported and are ignored
            testColumnStats(schema, ORDERS, ORDER_STATUS, constraint(CLERK, "NO SUCH CLERK"), columnStatistics(3, 3));
            testColumnStats(schema, ORDERS, ORDER_KEY, constraint(CLERK, "Clerk#000000001"), columnStatistics(1_500_000 * scaleFactor, 1, 6_000_000 * scaleFactor));

            //compound constraints are supported
            testColumnStats(schema, ORDERS, ORDER_STATUS, constraint(ORDER_STATUS, "F", "NO SUCH STATUS"), columnStatistics(1, 1));
            testColumnStats(schema, ORDERS, ORDER_KEY, constraint(ORDER_STATUS, "F", "NO SUCH STATUS"), rangeStatistics(3, 6_000_000 * scaleFactor));

            testColumnStats(schema, ORDERS, ORDER_STATUS, constraint(ORDER_STATUS, "F", "O"), columnStatistics(2, 2));
            testColumnStats(schema, ORDERS, ORDER_KEY, constraint(ORDER_STATUS, "F", "O"), rangeStatistics(1, 6_000_000 * scaleFactor));

            testColumnStats(schema, ORDERS, ORDER_STATUS, constraint(ORDER_STATUS, "F", "O", "P"), columnStatistics(3, 3));
            testColumnStats(schema, ORDERS, ORDER_KEY, constraint(ORDER_STATUS, "F", "O", "P"), columnStatistics(1_500_000 * scaleFactor, 1, 6_000_000 * scaleFactor));
        });
    }

    private void testColumnStats(String schema, TpchTable<?> table, TpchColumn<?> column, ColumnStatistics expectedStatistics)
    {
        testColumnStats(schema, table, column, alwaysTrue(), expectedStatistics);
    }

    private void testColumnStats(String schema, TpchTable<?> table, TpchColumn<?> column, Constraint constraint, ColumnStatistics expected)
    {
        TpchTableHandle tableHandle = tpchMetadata.getTableHandle(session, new SchemaTableName(schema, table.getTableName()));
        Optional<ConstraintApplicationResult<ConnectorTableHandle>> result = tpchMetadata.applyFilter(session, tableHandle, constraint);
        if (result.isPresent()) {
            tableHandle = (TpchTableHandle) result.get().getHandle();
        }
        TableStatistics tableStatistics = tpchMetadata.getTableStatistics(session, tableHandle);
        ColumnHandle columnHandle = tpchMetadata.getColumnHandles(session, tableHandle).get(column.getSimplifiedColumnName());

        ColumnStatistics actual = tableStatistics.getColumnStatistics().get(columnHandle);

        EstimateAssertion estimateAssertion = new EstimateAssertion(TOLERANCE);

        estimateAssertion.assertClose(actual.getDistinctValuesCount(), expected.getDistinctValuesCount(), "distinctValuesCount");
        estimateAssertion.assertClose(actual.getDataSize(), expected.getDataSize(), "dataSize");
        estimateAssertion.assertClose(actual.getNullsFraction(), expected.getNullsFraction(), "nullsFraction");
        estimateAssertion.assertClose(actual.getRange(), expected.getRange(), "range");
    }

    @Test
    public void testOrdersOrderStatusPredicatePushdown()
    {
        TpchTableHandle tableHandle = tpchMetadata.getTableHandle(session, new SchemaTableName("sf1", ORDERS.getTableName()));

        TupleDomain<ColumnHandle> domain;
        ConstraintApplicationResult<ConnectorTableHandle> result;

        domain = fixedValueTupleDomain(tpchMetadata, ORDER_STATUS, utf8Slice("P"));
        result = tpchMetadata.applyFilter(session, tableHandle, new Constraint(domain, convertToPredicate(domain, ORDER_STATUS), Set.of(tpchMetadata.toColumnHandle(ORDER_STATUS)))).get();
        assertTupleDomainEquals(result.getRemainingFilter(), TupleDomain.all(), session);
        assertTupleDomainEquals(((TpchTableHandle) result.getHandle()).getConstraint(), domain, session);

        domain = fixedValueTupleDomain(tpchMetadata, ORDER_KEY, 42L);
        result = tpchMetadata.applyFilter(session, tableHandle, new Constraint(domain, convertToPredicate(domain, ORDER_STATUS), Set.of(tpchMetadata.toColumnHandle(ORDER_STATUS)))).get();
        assertTupleDomainEquals(result.getRemainingFilter(), domain, session);
        assertTupleDomainEquals(
                ((TpchTableHandle) result.getHandle()).getConstraint(),
                // The most important thing about the expected value that it is NOT TupleDomain.none() (or equivalent).
                // Using concrete expected value instead of checking TupleDomain::isNone to make sure the test doesn't pass on some other wrong value.
                TupleDomain.columnWiseUnion(
                        fixedValueTupleDomain(tpchMetadata, ORDER_STATUS, utf8Slice("F")),
                        fixedValueTupleDomain(tpchMetadata, ORDER_STATUS, utf8Slice("O")),
                        fixedValueTupleDomain(tpchMetadata, ORDER_STATUS, utf8Slice("P"))),
                session);
    }

    @Test
    public void testPartTypeAndPartContainerPredicatePushdown()
    {
        TpchTableHandle tableHandle = tpchMetadata.getTableHandle(session, new SchemaTableName("sf1", PART.getTableName()));

        TupleDomain<ColumnHandle> domain;
        ConstraintApplicationResult<ConnectorTableHandle> result;

        domain = fixedValueTupleDomain(tpchMetadata, PartColumn.TYPE, utf8Slice("SMALL BRUSHED COPPER"));
        result = tpchMetadata.applyFilter(session, tableHandle, new Constraint(
                domain,
                convertToPredicate(domain, PartColumn.TYPE),
                Set.of(tpchMetadata.toColumnHandle(PartColumn.TYPE)))).get();
        assertTupleDomainEquals(result.getRemainingFilter(), TupleDomain.all(), session);
        assertTupleDomainEquals(
                filterOutColumnFromPredicate(((TpchTableHandle) result.getHandle()).getConstraint(), tpchMetadata.toColumnHandle(PartColumn.CONTAINER)),
                domain,
                session);

        domain = fixedValueTupleDomain(tpchMetadata, PartColumn.TYPE, utf8Slice("UNKNOWN"));
        result = tpchMetadata.applyFilter(session, tableHandle, new Constraint(
                domain,
                convertToPredicate(domain, PartColumn.TYPE),
                Set.of(tpchMetadata.toColumnHandle(PartColumn.TYPE)))).get();
        assertTupleDomainEquals(result.getRemainingFilter(), TupleDomain.all(), session);
        assertTupleDomainEquals(((TpchTableHandle) result.getHandle()).getConstraint(), TupleDomain.none(), session);

        domain = fixedValueTupleDomain(tpchMetadata, PartColumn.CONTAINER, utf8Slice("SM BAG"));
        result = tpchMetadata.applyFilter(session, tableHandle, new Constraint(
                domain,
                convertToPredicate(domain, PartColumn.CONTAINER),
                Set.of(tpchMetadata.toColumnHandle(PartColumn.CONTAINER)))).get();
        assertTupleDomainEquals(result.getRemainingFilter(), TupleDomain.all(), session);
        assertTupleDomainEquals(
                filterOutColumnFromPredicate(((TpchTableHandle) result.getHandle()).getConstraint(), tpchMetadata.toColumnHandle(PartColumn.TYPE)),
                domain,
                session);

        domain = fixedValueTupleDomain(tpchMetadata, PartColumn.CONTAINER, utf8Slice("UNKNOWN"));
        result = tpchMetadata.applyFilter(session, tableHandle, new Constraint(
                domain,
                convertToPredicate(domain, PartColumn.CONTAINER),
                Set.of(tpchMetadata.toColumnHandle(PartColumn.CONTAINER)))).get();
        assertTupleDomainEquals(result.getRemainingFilter(), TupleDomain.all(), session);
        assertTupleDomainEquals(((TpchTableHandle) result.getHandle()).getConstraint(), TupleDomain.none(), session);

        domain = fixedValueTupleDomain(tpchMetadata, PartColumn.TYPE, utf8Slice("SMALL BRUSHED COPPER"), PartColumn.CONTAINER, utf8Slice("SM BAG"));
        result = tpchMetadata.applyFilter(session, tableHandle, new Constraint(
                domain,
                convertToPredicate(domain, PartColumn.CONTAINER),
                Set.of(tpchMetadata.toColumnHandle(PartColumn.CONTAINER)))).get();
        assertTupleDomainEquals(result.getRemainingFilter(), TupleDomain.all(), session);
        assertTupleDomainEquals(((TpchTableHandle) result.getHandle()).getConstraint(), domain, session);

        domain = fixedValueTupleDomain(tpchMetadata, PartColumn.TYPE, utf8Slice("UNKNOWN"), PartColumn.CONTAINER, utf8Slice("UNKNOWN"));
        result = tpchMetadata.applyFilter(session, tableHandle, new Constraint(
                domain,
                convertToPredicate(domain, PartColumn.TYPE, PartColumn.CONTAINER),
                Set.of(tpchMetadata.toColumnHandle(PartColumn.TYPE), tpchMetadata.toColumnHandle(PartColumn.CONTAINER)))).get();
        assertTupleDomainEquals(result.getRemainingFilter(), TupleDomain.all(), session);
        assertTupleDomainEquals(((TpchTableHandle) result.getHandle()).getConstraint(), TupleDomain.none(), session);
    }

    private Predicate<Map<ColumnHandle, NullableValue>> convertToPredicate(TupleDomain<ColumnHandle> domain, TpchColumn<?>... columns)
    {
        checkArgument(columns.length > 0, "No columns given");
        return bindings -> {
            for (TpchColumn<?> column : columns) {
                ColumnHandle columnHandle = tpchMetadata.toColumnHandle(column);
                if (bindings.containsKey(columnHandle)) {
                    NullableValue nullableValue = requireNonNull(bindings.get(columnHandle), "binding is null");
                    if (!PredicateUtils.convertToPredicate(domain, tpchMetadata.toColumnHandle(column)).test(nullableValue)) {
                        return false;
                    }
                }
            }
            return true;
        };
    }

    private void assertTupleDomainEquals(TupleDomain<?> actual, TupleDomain<?> expected, ConnectorSession session)
    {
        if (!Objects.equals(actual, expected)) {
            fail(format("expected [%s] but found [%s]", expected.toString(session), actual.toString(session)));
        }
    }

    private Constraint constraint(TpchColumn<?> column, String... values)
    {
        List<TupleDomain<ColumnHandle>> valueDomains = stream(values)
                .map(value -> fixedValueTupleDomain(tpchMetadata, column, utf8Slice(value)))
                .collect(toList());
        TupleDomain<ColumnHandle> domain = TupleDomain.columnWiseUnion(valueDomains);
        return new Constraint(domain, convertToPredicate(domain, column), Set.of(tpchMetadata.toColumnHandle(column)));
    }

    private static TupleDomain<ColumnHandle> fixedValueTupleDomain(TpchMetadata tpchMetadata, TpchColumn<?> column, Object value)
    {
        requireNonNull(column, "column is null");
        requireNonNull(value, "value is null");
        return TupleDomain.fromFixedValues(
                ImmutableMap.of(tpchMetadata.toColumnHandle(column), new NullableValue(getTrinoType(column, DecimalTypeMapping.DOUBLE), value)));
    }

    private static TupleDomain<ColumnHandle> fixedValueTupleDomain(TpchMetadata tpchMetadata, TpchColumn<?> column1, Object value1, TpchColumn<?> column2, Object value2)
    {
        return TupleDomain.fromFixedValues(
                ImmutableMap.of(
                        tpchMetadata.toColumnHandle(column1), new NullableValue(getTrinoType(column1, DecimalTypeMapping.DOUBLE), value1),
                        tpchMetadata.toColumnHandle(column2), new NullableValue(getTrinoType(column2, DecimalTypeMapping.DOUBLE), value2)));
    }

    private ColumnStatistics noColumnStatistics()
    {
        return createColumnStatistics(Optional.of(0.0), Optional.empty(), Optional.of(0.0));
    }

    private ColumnStatistics columnStatistics(double distinctValuesCount, double dataSize)
    {
        return createColumnStatistics(Optional.of(distinctValuesCount), Optional.empty(), Optional.of(dataSize));
    }

    private ColumnStatistics columnStatistics(double distinctValuesCount, double min, double max)
    {
        return createColumnStatistics(Optional.of(distinctValuesCount), Optional.of(new DoubleRange(min, max)), Optional.empty());
    }

    private ColumnStatistics rangeStatistics(double min, double max)
    {
        return createColumnStatistics(Optional.empty(), Optional.of(new DoubleRange(min, max)), Optional.empty());
    }

    private static ColumnStatistics createColumnStatistics(Optional<Double> distinctValuesCount, Optional<DoubleRange> range, Optional<Double> dataSize)
    {
        return ColumnStatistics.builder()
                .setNullsFraction(Estimate.zero())
                .setDistinctValuesCount(toEstimate(distinctValuesCount))
                .setRange(range)
                .setDataSize(toEstimate(dataSize))
                .build();
    }

    private static Estimate toEstimate(Optional<Double> value)
    {
        return value
                .map(Estimate::of)
                .orElse(Estimate.unknown());
    }
}
