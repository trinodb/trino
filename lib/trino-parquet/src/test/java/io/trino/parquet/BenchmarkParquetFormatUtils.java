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
package io.trino.parquet;

import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slices;
import io.airlift.units.DataSize;
import io.trino.spi.Page;
import io.trino.spi.PageBuilder;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.type.Type;
import io.trino.tpch.TpchColumn;
import io.trino.tpch.TpchEntity;
import io.trino.tpch.TpchTable;
import org.openjdk.jmh.results.RunResult;
import org.openjdk.jmh.util.Statistics;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Collection;
import java.util.List;
import java.util.Random;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.VarcharType.createUnboundedVarcharType;
import static java.lang.String.format;
import static java.nio.file.Files.createTempDirectory;

public final class BenchmarkParquetFormatUtils
{
    public static final long MIN_DATA_SIZE = DataSize.of(50, MEGABYTE).toBytes();

    private BenchmarkParquetFormatUtils() {}

    @SafeVarargs
    public static <E extends TpchEntity> TestData createTpchDataSet(TpchTable<E> tpchTable, TpchColumn<E>... columns)
    {
        return createTpchDataSet(tpchTable, ImmutableList.copyOf(columns));
    }

    public static <E extends TpchEntity> TestData createTpchDataSet(TpchTable<E> tpchTable, List<TpchColumn<E>> columns)
    {
        List<String> columnNames = columns.stream().map(TpchColumn::getColumnName).collect(toImmutableList());
        List<Type> columnTypes = columns.stream().map(BenchmarkParquetFormatUtils::getColumnType)
                .collect(toImmutableList());

        PageBuilder pageBuilder = new PageBuilder(columnTypes);
        ImmutableList.Builder<Page> pages = ImmutableList.builder();
        long dataSize = 0;
        for (E row : tpchTable.createGenerator(10, 1, 1)) {
            pageBuilder.declarePosition();
            for (int i = 0; i < columns.size(); i++) {
                TpchColumn<E> column = columns.get(i);
                BlockBuilder blockBuilder = pageBuilder.getBlockBuilder(i);
                switch (column.getType().getBase()) {
                    case IDENTIFIER:
                        BIGINT.writeLong(blockBuilder, column.getIdentifier(row));
                        break;
                    case INTEGER:
                        INTEGER.writeLong(blockBuilder, column.getInteger(row));
                        break;
                    case DATE:
                        DATE.writeLong(blockBuilder, column.getDate(row));
                        break;
                    case DOUBLE:
                        DOUBLE.writeDouble(blockBuilder, column.getDouble(row));
                        break;
                    case VARCHAR:
                        createUnboundedVarcharType().writeSlice(blockBuilder, Slices.utf8Slice(column.getString(row)));
                        break;
                    default:
                        throw new IllegalArgumentException("Unsupported type " + column.getType());
                }
            }
            if (pageBuilder.isFull()) {
                Page page = pageBuilder.build();
                pages.add(page);
                pageBuilder.reset();
                dataSize += page.getSizeInBytes();

                if (dataSize >= MIN_DATA_SIZE) {
                    break;
                }
            }
        }
        if (!pageBuilder.isEmpty()) {
            pages.add(pageBuilder.build());
        }
        return new TestData(columnNames, columnTypes, pages.build());
    }

    public static Type getColumnType(TpchColumn<?> input)
    {
        return switch (input.getType().getBase()) {
            case IDENTIFIER -> BIGINT;
            case INTEGER -> INTEGER;
            case DATE -> DATE;
            case DOUBLE -> DOUBLE;
            case VARCHAR -> createUnboundedVarcharType();
        };
    }

    public static void printResults(Collection<RunResult> results)
    {
        for (RunResult result : results) {
            Statistics inputSizeStats = result.getSecondaryResults().get("inputSize").getStatistics();
            Statistics outputSizeStats = result.getSecondaryResults().get("outputSize").getStatistics();
            double compressionRatio = inputSizeStats.getSum() / outputSizeStats.getSum();
            String compression = result.getParams().getParam("compression");
            String dataSet = result.getParams().getParam("dataSet");
            System.out.printf("  %-10s  %-30s  %-10s  %2.2f  %10s ± %11s (%5.2f%%) (N = %d, α = 99.9%%)\n",
                    result.getPrimaryResult().getLabel(),
                    dataSet,
                    compression,
                    compressionRatio,
                    toHumanReadableSpeed((long) inputSizeStats.getMean()),
                    toHumanReadableSpeed((long) inputSizeStats.getMeanErrorAt(0.999)),
                    inputSizeStats.getMeanErrorAt(0.999) * 100 / inputSizeStats.getMean(),
                    inputSizeStats.getN());
        }
        System.out.println();
    }

    public static String toHumanReadableSpeed(long bytesPerSecond)
    {
        String humanReadableSpeed;
        if (bytesPerSecond < 1024 * 10L) {
            humanReadableSpeed = format("%dB/s", bytesPerSecond);
        }
        else if (bytesPerSecond < 1024 * 1024 * 10L) {
            humanReadableSpeed = format("%.1fkB/s", bytesPerSecond / 1024.0f);
        }
        else if (bytesPerSecond < 1024 * 1024 * 1024 * 10L) {
            humanReadableSpeed = format("%.1fMB/s", bytesPerSecond / (1024.0f * 1024.0f));
        }
        else {
            humanReadableSpeed = format("%.1fGB/s", bytesPerSecond / (1024.0f * 1024.0f * 1024.0f));
        }
        return humanReadableSpeed;
    }

    public static int nextRandomBetween(Random random, int min, int max)
    {
        return min + random.nextInt(max - min);
    }

    public static File createTempDir(String prefix)
    {
        try {
            return createTempDirectory(prefix).toFile();
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public static class TestData
    {
        private final List<String> columnNames;
        private final List<Type> columnTypes;
        private final List<Page> pages;
        private final long inputSize;

        public TestData(List<String> columnNames, List<Type> columnTypes, List<Page> pages)
        {
            this.columnNames = ImmutableList.copyOf(columnNames);
            this.columnTypes = ImmutableList.copyOf(columnTypes);
            this.pages = ImmutableList.copyOf(pages);
            this.inputSize = pages.stream().mapToLong(Page::getSizeInBytes).sum();
        }

        public List<String> getColumnNames()
        {
            return columnNames;
        }

        public List<Type> getColumnTypes()
        {
            return columnTypes;
        }

        public List<Page> getPages()
        {
            return pages;
        }

        public long getInputSize()
        {
            return inputSize;
        }
    }
}
