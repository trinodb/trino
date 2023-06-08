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

import java.util.List;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.VarcharType.createUnboundedVarcharType;
import static java.util.stream.Collectors.toList;

public final class BenchmarkFileFormatsUtils
{
    private static final long MIN_DATA_SIZE = DataSize.of(50, MEGABYTE).toBytes();

    private BenchmarkFileFormatsUtils()
    {
    }

    @SafeVarargs
    public static <E extends TpchEntity> TestData createTpchDataSet(TpchTable<E> tpchTable, TpchColumn<E>... columns)
    {
        return createTpchDataSet(tpchTable, ImmutableList.copyOf(columns));
    }

    public static <E extends TpchEntity> TestData createTpchDataSet(TpchTable<E> tpchTable, List<TpchColumn<E>> columns)
    {
        List<String> columnNames = columns.stream().map(TpchColumn::getColumnName).collect(toList());
        List<Type> columnTypes = columns.stream().map(BenchmarkFileFormatsUtils::getColumnType)
                .map(type -> !DATE.equals(type) ? type : createUnboundedVarcharType())
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
                    case IDENTIFIER -> BIGINT.writeLong(blockBuilder, column.getIdentifier(row));
                    case INTEGER -> INTEGER.writeLong(blockBuilder, column.getInteger(row));
                    case DATE, VARCHAR -> createUnboundedVarcharType().writeSlice(blockBuilder, Slices.utf8Slice(column.getString(row)));
                    case DOUBLE -> DOUBLE.writeDouble(blockBuilder, column.getDouble(row));
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

    public record TestData(List<String> columnNames, List<Type> columnTypes, List<Page> pages)
    {
        public TestData(List<String> columnNames, List<Type> columnTypes, List<Page> pages)
        {
            this.columnNames = ImmutableList.copyOf(columnNames);
            this.columnTypes = ImmutableList.copyOf(columnTypes);
            this.pages = ImmutableList.copyOf(pages);
        }
    }
}
