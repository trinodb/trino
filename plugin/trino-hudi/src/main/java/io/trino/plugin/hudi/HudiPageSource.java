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
package io.trino.plugin.hudi;

import com.google.common.collect.ImmutableList;
import io.trino.plugin.hive.HiveColumnHandle;
import io.trino.plugin.hive.HivePageSource;
import io.trino.plugin.hive.HiveType;
import io.trino.plugin.hive.coercions.FloatToDoubleCoercer;
import io.trino.plugin.hive.coercions.IntegerNumberToVarcharCoercer;
import io.trino.plugin.hive.coercions.IntegerNumberUpscaleCoercer;
import io.trino.plugin.hive.type.Category;
import io.trino.plugin.hudi.coercer.DateToVarcharCoercer;
import io.trino.spi.Page;
import io.trino.spi.TrinoException;
import io.trino.spi.block.Block;
import io.trino.spi.block.LazyBlock;
import io.trino.spi.block.RunLengthEncodedBlock;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.BigintType;
import io.trino.spi.type.DateType;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.DoubleType;
import io.trino.spi.type.IntegerType;
import io.trino.spi.type.MapType;
import io.trino.spi.type.RealType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeManager;
import io.trino.spi.type.VarcharType;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;

import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.plugin.base.util.Closables.closeAllSuppress;
import static io.trino.plugin.hive.HiveColumnHandle.FILE_MODIFIED_TIME_COLUMN_NAME;
import static io.trino.plugin.hive.HiveColumnHandle.FILE_MODIFIED_TIME_TYPE_SIGNATURE;
import static io.trino.plugin.hive.HiveColumnHandle.FILE_SIZE_COLUMN_NAME;
import static io.trino.plugin.hive.HiveColumnHandle.FILE_SIZE_TYPE_SIGNATURE;
import static io.trino.plugin.hive.HiveColumnHandle.PARTITION_COLUMN_NAME;
import static io.trino.plugin.hive.HiveColumnHandle.PARTITION_TYPE_SIGNATURE;
import static io.trino.plugin.hive.HiveColumnHandle.PATH_COLUMN_NAME;
import static io.trino.plugin.hive.HiveColumnHandle.PATH_TYPE;
import static io.trino.plugin.hive.HiveType.HIVE_BYTE;
import static io.trino.plugin.hive.HiveType.HIVE_FLOAT;
import static io.trino.plugin.hive.HiveType.HIVE_INT;
import static io.trino.plugin.hive.HiveType.HIVE_LONG;
import static io.trino.plugin.hive.HiveType.HIVE_SHORT;
import static io.trino.plugin.hive.coercions.DecimalCoercers.createDecimalToVarcharCoercer;
import static io.trino.plugin.hive.coercions.DecimalCoercers.createDoubleToDecimalCoercer;
import static io.trino.plugin.hive.coercions.DecimalCoercers.createRealToDecimalCoercer;
import static io.trino.plugin.hudi.coercer.IntegerCoercers.createIntegerToDecimalCoercer;
import static io.trino.plugin.hudi.coercer.IntegerCoercers.createIntegerToDoubleCoercer;
import static io.trino.plugin.hudi.coercer.IntegerCoercers.createIntegerToRealCoercer;
import static io.trino.plugin.hudi.coercer.NestedTypeCoercers.createListCoercer;
import static io.trino.plugin.hudi.coercer.NestedTypeCoercers.createMapCoercer;
import static io.trino.plugin.hudi.coercer.NestedTypeCoercers.createStructCoercer;
import static io.trino.plugin.hudi.coercer.VarcharCoercers.createDoubleToVarcharCoercer;
import static io.trino.plugin.hudi.coercer.VarcharCoercers.createVarcharToDateCoercer;
import static io.trino.plugin.hudi.coercer.VarcharCoercers.createVarcharToDecimalCoercer;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.predicate.Utils.nativeValueToBlock;
import static io.trino.spi.type.DateTimeEncoding.packDateTimeWithZone;
import static io.trino.spi.type.TimeZoneKey.UTC_KEY;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class HudiPageSource
        implements ConnectorPageSource
{
    private final Block[] prefilledBlocks;
    private final int[] delegateIndexes;
    private final ConnectorPageSource dataPageSource;
    private final List<Optional<Function<Block, Block>>> coercers;

    public HudiPageSource(
            TypeManager typeManager,
            String partitionName,
            List<HiveColumnHandle> columnHandles,
            Map<String, Block> partitionBlocks,
            ConnectorPageSource dataPageSource,
            Path path,
            long fileSize,
            long fileModifiedTime,
            Optional<Map<Integer, HiveColumnHandle>> schemaEvolutionColumns)
    {
        requireNonNull(columnHandles, "columnHandles is null");
        this.dataPageSource = requireNonNull(dataPageSource, "dataPageSource is null");

        int size = columnHandles.size();
        this.prefilledBlocks = new Block[size];
        this.delegateIndexes = new int[size];

        int outputIndex = 0;
        int delegateIndex = 0;
        ImmutableList.Builder<Optional<Function<Block, Block>>> coercers = ImmutableList.builder();

        for (HiveColumnHandle column : columnHandles) {
            if (partitionBlocks.containsKey(column.getName())) {
                Block partitionValue = partitionBlocks.get(column.getName());
                prefilledBlocks[outputIndex] = partitionValue;
                delegateIndexes[outputIndex] = -1;
            }
            else if (column.getName().equals(PARTITION_COLUMN_NAME)) {
                prefilledBlocks[outputIndex] = nativeValueToBlock(PARTITION_TYPE_SIGNATURE, utf8Slice(partitionName));
                delegateIndexes[outputIndex] = -1;
            }
            else if (column.getName().equals(PATH_COLUMN_NAME)) {
                prefilledBlocks[outputIndex] = nativeValueToBlock(PATH_TYPE, utf8Slice(path.toString()));
                delegateIndexes[outputIndex] = -1;
            }
            else if (column.getName().equals(FILE_SIZE_COLUMN_NAME)) {
                prefilledBlocks[outputIndex] = nativeValueToBlock(FILE_SIZE_TYPE_SIGNATURE, fileSize);
                delegateIndexes[outputIndex] = -1;
            }
            else if (column.getName().equals(FILE_MODIFIED_TIME_COLUMN_NAME)) {
                long packedTimestamp = packDateTimeWithZone(fileModifiedTime, UTC_KEY);
                prefilledBlocks[outputIndex] = nativeValueToBlock(FILE_MODIFIED_TIME_TYPE_SIGNATURE, packedTimestamp);
                delegateIndexes[outputIndex] = -1;
            }
            else {
                delegateIndexes[outputIndex] = delegateIndex;
                delegateIndex++;
                if (schemaEvolutionColumns.isPresent()
                        && !schemaEvolutionColumns.get().isEmpty()
                        && schemaEvolutionColumns.get().containsKey(column.getBaseHiveColumnIndex())) {
                    coercers.add(createCoercer(typeManager, schemaEvolutionColumns.get().get(column.getBaseHiveColumnIndex()).getHiveType(), column.getHiveType()));
                }
                else {
                    coercers.add(Optional.empty());
                }
            }
            outputIndex++;
        }
        this.coercers = coercers.build();
    }

    @Override
    public long getCompletedBytes()
    {
        return dataPageSource.getCompletedBytes();
    }

    @Override
    public long getReadTimeNanos()
    {
        return dataPageSource.getReadTimeNanos();
    }

    @Override
    public boolean isFinished()
    {
        return dataPageSource.isFinished();
    }

    @Override
    public Page getNextPage()
    {
        try {
            Page page = dataPageSource.getNextPage();
            if (page == null) {
                return null;
            }
            int positionCount = page.getPositionCount();
            Block[] blocks = new Block[prefilledBlocks.length];
            for (int i = 0; i < prefilledBlocks.length; i++) {
                if (prefilledBlocks[i] != null) {
                    blocks[i] = RunLengthEncodedBlock.create(prefilledBlocks[i], positionCount);
                }
                else {
                    blocks[i] = page.getBlock(delegateIndexes[i]);
                    Optional<Function<Block, Block>> coercer = coercers.get(delegateIndexes[i]);
                    if (coercer.isPresent()) {
                        blocks[i] = new LazyBlock(positionCount, new HivePageSource.CoercionLazyBlockLoader(blocks[i], coercer.get()));
                    }
                }
            }
            return new Page(positionCount, blocks);
        }
        catch (RuntimeException e) {
            closeAllSuppress(e, this);
            throw e;
        }
    }

    @Override
    public long getMemoryUsage()
    {
        return dataPageSource.getMemoryUsage();
    }

    @Override
    public void close()
            throws IOException
    {
        dataPageSource.close();
    }

    public static Optional<Function<Block, Block>> createCoercer(TypeManager typeManager, HiveType fromHiveType, HiveType toHiveType)
    {
        if (fromHiveType.equals(toHiveType)) {
            return Optional.empty();
        }

        Type fromType = fromHiveType.getType(typeManager);
        Type toType = toHiveType.getType(typeManager);

        if (toType instanceof VarcharType toVarcharType) {
            if (fromHiveType.equals(HIVE_BYTE)
                    || fromHiveType.equals(HIVE_SHORT)
                    || fromHiveType.equals(HIVE_INT)
                    || fromHiveType.equals(HIVE_LONG)
                    || fromHiveType.equals(HIVE_FLOAT)) {
                return Optional.of(new IntegerNumberToVarcharCoercer<>(fromType, toVarcharType));
            }
            else if (fromType instanceof DecimalType fromDecimalType) {
                return Optional.of(createDecimalToVarcharCoercer(fromDecimalType, toVarcharType));
            }
            else if (fromType instanceof DateType fromDateType) {
                return Optional.of(new DateToVarcharCoercer(fromDateType, toVarcharType));
            }
            else if (fromType instanceof DoubleType fromDoubleType) {
                return Optional.of(createDoubleToVarcharCoercer(fromDoubleType, toVarcharType));
            }
            throw new TrinoException(NOT_SUPPORTED, format("Unsupported coercion from %s to %s", fromHiveType, toHiveType));
        }

        if (fromType instanceof VarcharType fromVarcharType) {
            if (toType instanceof DecimalType toDecimalType) {
                return Optional.of(createVarcharToDecimalCoercer(fromVarcharType, toDecimalType));
            }
            else if (toType instanceof DateType toDateType) {
                return Optional.of(createVarcharToDateCoercer(fromVarcharType, toDateType));
            }
            throw new TrinoException(NOT_SUPPORTED, format("Unsupported coercion from %s to %s", fromHiveType, toHiveType));
        }

        if (fromType instanceof IntegerType
                || fromType instanceof BigintType) {
            if (toHiveType.equals(HIVE_LONG)) {
                return Optional.of(new IntegerNumberUpscaleCoercer<>(fromType, toType));
            }
            else if (toType instanceof RealType toRealType) {
                return Optional.of(createIntegerToRealCoercer(fromType, toRealType));
            }
            else if (toType instanceof DoubleType toDoubleType) {
                return Optional.of(createIntegerToDoubleCoercer(fromType, toDoubleType));
            }
            else if (toType instanceof DecimalType toDecimalType) {
                return Optional.of(createIntegerToDecimalCoercer(fromType, toDecimalType));
            }
            throw new TrinoException(NOT_SUPPORTED, format("Unsupported coercion from %s to %s", fromHiveType, toHiveType));
        }

        if (fromType instanceof RealType) {
            if (toType instanceof DoubleType) {
                return Optional.of(new FloatToDoubleCoercer());
            }
            else if (toType instanceof DecimalType toDecimalType) {
                return Optional.of(createRealToDecimalCoercer(toDecimalType));
            }
            throw new TrinoException(NOT_SUPPORTED, format("Unsupported coercion from %s to %s", fromHiveType, toHiveType));
        }

        if (fromType instanceof DoubleType) {
            if (toType instanceof DecimalType toDecimalType) {
                return Optional.of(createDoubleToDecimalCoercer(toDecimalType));
            }
            throw new TrinoException(NOT_SUPPORTED, format("Unsupported coercion from %s to %s", fromHiveType, toHiveType));
        }

        if ((fromType instanceof ArrayType) && (toType instanceof ArrayType)) {
            return Optional.of(createListCoercer(typeManager, fromHiveType, toHiveType));
        }
        if ((fromType instanceof MapType) && (toType instanceof MapType)) {
            return Optional.of(createMapCoercer(typeManager, fromHiveType, toHiveType));
        }
        if ((fromType instanceof RowType) && (toType instanceof RowType)) {
            HiveType fromHiveTypeStruct = (fromHiveType.getCategory() == Category.UNION) ? HiveType.toHiveType(fromType) : fromHiveType;
            HiveType toHiveTypeStruct = (toHiveType.getCategory() == Category.UNION) ? HiveType.toHiveType(toType) : toHiveType;

            return Optional.of(createStructCoercer(typeManager, fromHiveTypeStruct, toHiveTypeStruct));
        }

        throw new TrinoException(NOT_SUPPORTED, format("Unsupported coercion from %s to %s", fromHiveType, toHiveType));
    }
}
