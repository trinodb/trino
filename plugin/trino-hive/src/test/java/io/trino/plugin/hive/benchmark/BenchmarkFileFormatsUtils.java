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
package io.trino.plugin.hive.benchmark;

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

import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.VarcharType.createUnboundedVarcharType;
import static java.lang.String.format;
import static java.nio.file.Files.createTempDirectory;
import static java.util.stream.Collectors.toList;

public final class BenchmarkFileFormatsUtils
{
    public static final long MIN_DATA_SIZE = DataSize.of(50, MEGABYTE).toBytes();

    private BenchmarkFileFormatsUtils()
    {
    }

    @SafeVarargs
    public static <E extends TpchEntity> TestData createTpchDataSet(FileFormat format, TpchTable<E> tpchTable, TpchColumn<E>... columns)
    {
        return createTpchDataSet(format, tpchTable, ImmutableList.copyOf(columns));
    }

    public static <E extends TpchEntity> TestData createTpchDataSet(FileFormat format, TpchTable<E> tpchTable, List<TpchColumn<E>> columns)
    {
        List<String> columnNames = columns.stream().map(TpchColumn::getColumnName).collect(toList());
        List<Type> columnTypes = columns.stream().map(BenchmarkFileFormatsUtils::getColumnType)
                .map(type -> format.supportsDate() || !DATE.equals(type) ? type : createUnboundedVarcharType())
                .collect(toList());

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
                        if (format.supportsDate()) {
                            DATE.writeLong(blockBuilder, column.getDate(row));
                        }
                        else {
                            createUnboundedVarcharType().writeString(blockBuilder, column.getString(row));
                        }
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
        switch (input.getType().getBase()) {
            case IDENTIFIER:
                return BIGINT;
            case INTEGER:
                return INTEGER;
            case DATE:
                return DATE;
            case DOUBLE:
                return DOUBLE;
            case VARCHAR:
                return createUnboundedVarcharType();
        }
        throw new IllegalArgumentException("Unsupported type " + input.getType());
    }

    public static void printResults(Collection<RunResult> results)
    {
        for (RunResult result : results) {
            Statistics inputSizeStats = result.getSecondaryResults().get("inputSize").getStatistics();
            Statistics outputSizeStats = result.getSecondaryResults().get("outputSize").getStatistics();
            double compressionRatio = inputSizeStats.getSum() / outputSizeStats.getSum();
            String compression = result.getParams().getParam("compression");
            String fileFormat = result.getParams().getParam("benchmarkFileFormat");
            String dataSet = result.getParams().getParam("dataSet");
            System.out.printf("  %-10s  %-30s  %-10s  %-25s  %2.2f  %10s ± %11s (%5.2f%%) (N = %d, \u03B1 = 99.9%%)\n",
                    result.getPrimaryResult().getLabel(),
                    dataSet,
                    compression,
                    fileFormat,
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

    @SuppressWarnings("SameParameterValue")
    public static File createTempDir(String prefix)
    {
        try {
            return createTempDirectory(prefix).toFile();
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
