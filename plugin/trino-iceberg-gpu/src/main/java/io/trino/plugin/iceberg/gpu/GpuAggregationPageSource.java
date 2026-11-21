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
package io.trino.plugin.iceberg.gpu;

import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slices;
import io.trino.plugin.iceberg.IcebergColumnHandle;
import io.trino.plugin.iceberg.gpu.engine.DeviceTable;
import io.trino.plugin.iceberg.gpu.engine.GpuEngine;
import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.SourcePage;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarcharType;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static java.util.Objects.requireNonNull;

/**
 * A pushed-down aggregation, answered by the device: one page with a row per group.
 */
final class GpuAggregationPageSource
        implements ConnectorPageSource
{
    private static final int CONSTANT_ONE = -1;
    private static final int NO_FACTOR = -2;

    private final Page page;
    private final long readTimeNanos;
    private boolean finished;

    GpuAggregationPageSource(GpuTableHandle table, GpuEngine.Table files, List<ColumnHandle> requested)
    {
        requireNonNull(table, "table is null");
        GpuAggregation aggregation = table.aggregation().orElseThrow();
        long start = System.nanoTime();

        // The table columns the device reads, in first-use order.
        Map<String, Integer> index = new LinkedHashMap<>();
        List<DeviceTable.Column> columns = new ArrayList<>();
        ColumnIndex use = name -> index.computeIfAbsent(name, _ -> {
            GpuColumnHandle column = aggregation.columns().get(name);
            columns.add(new DeviceTable.Column(column.fileIndex(), column.kind().orElseThrow()));
            return columns.size() - 1;
        });

        int[] predicates = new int[2 * table.predicates().size()];
        double[] constants = new double[table.predicates().size()];
        for (int p = 0; p < constants.length; p++) {
            GpuPredicate predicate = table.predicates().get(p);
            predicates[2 * p] = use.of(predicate.column());
            predicates[2 * p + 1] = predicate.operator();
            constants[p] = predicate.constant();
        }
        int[] keys = new int[Math.max(1, aggregation.groupBy().size())];
        keys[0] = CONSTANT_ONE;
        for (int k = 0; k < aggregation.groupBy().size(); k++) {
            keys[k] = use.of(aggregation.groupBy().get(k).name());
        }

        // One value expression per distinct argument, and one for the row count.
        List<Integer> factorColumns = new ArrayList<>();
        List<Double> a = new ArrayList<>();
        List<Double> b = new ArrayList<>();
        Map<String, Integer> valueIndex = new LinkedHashMap<>();
        ValueIndex value = argument -> valueIndex.computeIfAbsent(argument == null ? "*" : argument.name(), _ -> {
            int factors = 0;
            if (argument == null) {
                factorColumns.add(CONSTANT_ONE);
                a.add(0.0);
                b.add(1.0);
                factors = 1;
            }
            else if (argument.isTableColumn()) {
                factorColumns.add(use.of(argument.name()));
                a.add(1.0);
                b.add(0.0);
                factors = 1;
            }
            else {
                GpuExpression expression = argument.expression().orElseThrow();
                for (int f = 0; f < expression.columns().size(); f++) {
                    String column = expression.columns().get(f);
                    factorColumns.add(column.isEmpty() ? CONSTANT_ONE : use.of(column));
                    a.add(expression.a().get(f));
                    b.add(expression.b().get(f));
                    factors++;
                }
            }
            for (; factors < IcebergGpuMetadata.FACTORS; factors++) {
                factorColumns.add(NO_FACTOR);
                a.add(0.0);
                b.add(0.0);
            }
            return valueIndex.size();
        });
        int count = value.of(null);
        int[] sumOf = new int[aggregation.aggregates().size()];
        for (int i = 0; i < sumOf.length; i++) {
            GpuAggregate aggregate = aggregation.aggregates().get(i).aggregate().orElseThrow();
            if (aggregate.function().equals("count")) {
                sumOf[i] = count;
            }
            else {
                sumOf[i] = value.of(aggregate.argument().orElseThrow());
            }
        }

        GpuEngine.Query query = new GpuEngine.Query(
                predicates,
                constants,
                keys,
                factorColumns.stream().mapToInt(Integer::intValue).toArray(),
                a.stream().mapToDouble(Double::doubleValue).toArray(),
                b.stream().mapToDouble(Double::doubleValue).toArray());
        DeviceTable.Result result = GpuEngine.instance().aggregate(files, ImmutableList.copyOf(columns), query);

        boolean global = aggregation.groupBy().isEmpty();
        int groups = result.groups();
        int positions = global ? 1 : groups;
        List<String> groupNames = aggregation.groupBy().stream().map(GpuColumnHandle::name).toList();
        Block[] blocks = new Block[requested.size()];
        for (int c = 0; c < blocks.length; c++) {
            ColumnHandle handle = requested.get(c);
            // A group key is the scan's Iceberg column; an aggregate is this connector's column.
            Type type;
            int key;
            int aggregateIndex = -1;
            String function = "";
            if (handle instanceof IcebergColumnHandle iceberg) {
                type = iceberg.getType();
                key = groupNames.indexOf(iceberg.getName());
            }
            else {
                GpuColumnHandle column = (GpuColumnHandle) handle;
                type = column.type();
                key = aggregation.groupBy().indexOf(column);
                aggregateIndex = aggregation.aggregates().indexOf(column);
                if (column.aggregate().isPresent()) {
                    function = column.aggregate().get().function();
                }
            }
            BlockBuilder builder = type.createBlockBuilder(null, positions);
            for (int g = 0; g < positions; g++) {
                if (key >= 0) {
                    long packed = result.keys()[key][g];
                    if (type instanceof VarcharType) {
                        type.writeSlice(builder, Slices.utf8Slice(unpack(packed)));
                    }
                    else {
                        type.writeLong(builder, packed);
                    }
                    continue;
                }
                double rows = 0;
                if (groups > 0) {
                    rows = result.sums()[count][g];
                }
                switch (function) {
                    case "count" -> type.writeLong(builder, (long) rows);
                    case "sum" -> {
                        if (rows == 0) {
                            builder.appendNull();
                        }
                        else {
                            type.writeDouble(builder, result.sums()[sumOf[aggregateIndex]][g]);
                        }
                    }
                    case "avg" -> {
                        if (rows == 0) {
                            builder.appendNull();
                        }
                        else {
                            type.writeDouble(builder, result.sums()[sumOf[aggregateIndex]][g] / rows);
                        }
                    }
                    default -> throw new IllegalStateException("Unexpected aggregate " + function);
                }
            }
            blocks[c] = builder.build();
        }
        this.page = new Page(positions, blocks);
        this.readTimeNanos = System.nanoTime() - start;
    }

    /**
     * A string packed big-endian into a long, zero-padded, back to the string.
     */
    static String unpack(long packed)
    {
        byte[] bytes = new byte[8];
        int length = 0;
        for (int i = 7; i >= 0; i--) {
            byte b = (byte) (packed >>> (8 * i));
            if (b == 0) {
                break;
            }
            bytes[length++] = b;
        }
        return new String(bytes, 0, length, StandardCharsets.UTF_8);
    }

    @Override
    public SourcePage getNextSourcePage()
    {
        if (finished) {
            return null;
        }
        finished = true;
        return SourcePage.create(page);
    }

    @Override
    public long getCompletedBytes()
    {
        return 0;
    }

    @Override
    public long getReadTimeNanos()
    {
        return readTimeNanos;
    }

    @Override
    public boolean isFinished()
    {
        return finished;
    }

    @Override
    public void close() {}

    private interface ColumnIndex
    {
        int of(String name);
    }

    private interface ValueIndex
    {
        int of(GpuColumnHandle argument);
    }
}
