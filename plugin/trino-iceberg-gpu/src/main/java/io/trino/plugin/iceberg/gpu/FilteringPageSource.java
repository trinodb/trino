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

import io.trino.plugin.iceberg.IcebergColumnHandle;
import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.SourcePage;
import io.trino.spi.metrics.Metrics;
import io.trino.spi.type.DoubleType;
import io.trino.spi.type.Type;

import java.io.IOException;
import java.util.List;
import java.util.OptionalLong;
import java.util.concurrent.CompletableFuture;

import static io.trino.plugin.iceberg.gpu.engine.DeviceTable.EQ;
import static io.trino.plugin.iceberg.gpu.engine.DeviceTable.GE;
import static io.trino.plugin.iceberg.gpu.engine.DeviceTable.GT;
import static io.trino.plugin.iceberg.gpu.engine.DeviceTable.LE;
import static io.trino.plugin.iceberg.gpu.engine.DeviceTable.LT;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static java.util.Objects.requireNonNull;

/**
 * The Iceberg connector's scan, with the predicates this connector took over enforced on its pages
 * and pushed-down expression columns computed: what the plan would have done had this connector not
 * taken them, for queries whose aggregation did not go to the device.
 *
 * <p>Only the predicate columns are read to select rows; the other columns stay lazy until the page
 * is consumed.
 */
final class FilteringPageSource
        implements ConnectorPageSource
{
    private final ConnectorPageSource delegate;
    // per predicate: channel in the scan, operator, constant, column type
    private final int[] predicateChannel;
    private final int[] predicateOp;
    private final double[] predicateConstant;
    private final Type[] predicateType;
    // per output column: channel in the scan, or -1 for an expression
    private final int[] outputChannel;
    // per expression output: per factor, channel (-1 for the constant 1)
    private final int[][] factorChannel;
    private final double[][] factorA;
    private final double[][] factorB;
    private int[] positions = new int[0];

    FilteringPageSource(ConnectorPageSource delegate, GpuTableHandle table, List<ColumnHandle> read, List<ColumnHandle> outputs)
    {
        this.delegate = requireNonNull(delegate, "delegate is null");
        int n = table.predicates().size();
        predicateChannel = new int[n];
        predicateOp = new int[n];
        predicateConstant = new double[n];
        predicateType = new Type[n];
        for (int p = 0; p < n; p++) {
            GpuPredicate predicate = table.predicates().get(p);
            ColumnHandle column = table.sources().get(predicate.column());
            predicateChannel[p] = read.indexOf(column);
            predicateOp[p] = predicate.operator();
            predicateConstant[p] = predicate.constant();
            predicateType[p] = ((IcebergColumnHandle) column).getType();
        }
        outputChannel = new int[outputs.size()];
        factorChannel = new int[outputs.size()][];
        factorA = new double[outputs.size()][];
        factorB = new double[outputs.size()][];
        for (int o = 0; o < outputs.size(); o++) {
            ColumnHandle column = outputs.get(o);
            if (column instanceof GpuColumnHandle own) {
                outputChannel[o] = -1;
                GpuExpression expression = own.expression().orElseThrow();
                List<String> columns = expression.columns();
                factorChannel[o] = new int[columns.size()];
                factorA[o] = new double[columns.size()];
                factorB[o] = new double[columns.size()];
                for (int f = 0; f < columns.size(); f++) {
                    factorChannel[o][f] = -1;
                    if (!columns.get(f).isEmpty()) {
                        factorChannel[o][f] = read.indexOf(table.sources().get(columns.get(f)));
                    }
                    factorA[o][f] = expression.a().get(f);
                    factorB[o][f] = expression.b().get(f);
                }
            }
            else {
                outputChannel[o] = read.indexOf(column);
            }
        }
    }

    @Override
    public SourcePage getNextSourcePage()
    {
        SourcePage page = delegate.getNextSourcePage();
        if (page == null) {
            return null;
        }
        int rows = page.getPositionCount();
        if (positions.length < rows) {
            positions = new int[rows];
        }
        int selected = 0;
        if (predicateChannel.length == 0) {
            for (int i = 0; i < rows; i++) {
                positions[i] = i;
            }
            selected = rows;
        }
        else {
            Block[] blocks = new Block[predicateChannel.length];
            for (int p = 0; p < blocks.length; p++) {
                blocks[p] = page.getBlock(predicateChannel[p]);
            }
            for (int i = 0; i < rows; i++) {
                if (passes(blocks, i)) {
                    positions[selected++] = i;
                }
            }
            if (selected < rows) {
                page.selectPositions(positions, 0, selected);
            }
        }
        Block[] out = new Block[outputChannel.length];
        for (int o = 0; o < out.length; o++) {
            out[o] = outputChannel[o] >= 0 ? page.getBlock(outputChannel[o]) : expression(page, o, selected);
        }
        return SourcePage.create(new Page(selected, out));
    }

    private boolean passes(Block[] blocks, int position)
    {
        for (int p = 0; p < blocks.length; p++) {
            Block block = blocks[p];
            if (block.isNull(position)) {
                return false;
            }
            Type type = predicateType[p];
            double x = type instanceof DoubleType ? DOUBLE.getDouble(block, position) : type.getLong(block, position);
            double k = predicateConstant[p];
            boolean ok = switch (predicateOp[p]) {
                case EQ -> x == k;
                case LT -> x < k;
                case LE -> x <= k;
                case GT -> x > k;
                case GE -> x >= k;
                default -> false;
            };
            if (!ok) {
                return false;
            }
        }
        return true;
    }

    private Block expression(SourcePage page, int output, int rows)
    {
        int[] channels = factorChannel[output];
        Block[] blocks = new Block[channels.length];
        for (int f = 0; f < channels.length; f++) {
            blocks[f] = channels[f] < 0 ? null : page.getBlock(channels[f]);
        }
        BlockBuilder builder = DOUBLE.createFixedSizeBlockBuilder(rows);
        for (int i = 0; i < rows; i++) {
            double product = 1.0;
            for (int f = 0; f < channels.length; f++) {
                double x = blocks[f] == null ? 1.0 : DOUBLE.getDouble(blocks[f], i);
                product *= factorA[output][f] * x + factorB[output][f];
            }
            DOUBLE.writeDouble(builder, product);
        }
        return builder.build();
    }

    @Override
    public long getCompletedBytes()
    {
        return delegate.getCompletedBytes();
    }

    @Override
    public OptionalLong getCompletedPositions()
    {
        return delegate.getCompletedPositions();
    }

    @Override
    public long getReadTimeNanos()
    {
        return delegate.getReadTimeNanos();
    }

    @Override
    public boolean isFinished()
    {
        return delegate.isFinished();
    }

    @Override
    public CompletableFuture<?> isBlocked()
    {
        return delegate.isBlocked();
    }

    @Override
    public Metrics getMetrics()
    {
        return delegate.getMetrics();
    }

    @Override
    public void close()
            throws IOException
    {
        delegate.close();
    }
}
