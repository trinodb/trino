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
package io.trino.plugin.hive;

import com.google.common.collect.ImmutableList;
import io.trino.plugin.hive.HivePageSourceProvider.BucketAdaptation;
import io.trino.plugin.hive.HivePageSourceProvider.ColumnMapping;
import io.trino.plugin.hive.coercions.DoubleToFloatCoercer;
import io.trino.plugin.hive.coercions.FloatToDoubleCoercer;
import io.trino.plugin.hive.coercions.IntegerNumberToVarcharCoercer;
import io.trino.plugin.hive.coercions.IntegerNumberUpscaleCoercer;
import io.trino.plugin.hive.coercions.VarcharCoercer;
import io.trino.plugin.hive.coercions.VarcharToIntegerNumberCoercer;
import io.trino.plugin.hive.util.HiveBucketing.BucketingVersion;
import io.trino.spi.Page;
import io.trino.spi.TrinoException;
import io.trino.spi.block.ArrayBlock;
import io.trino.spi.block.Block;
import io.trino.spi.block.ColumnarArray;
import io.trino.spi.block.ColumnarMap;
import io.trino.spi.block.ColumnarRow;
import io.trino.spi.block.DictionaryBlock;
import io.trino.spi.block.LazyBlock;
import io.trino.spi.block.LazyBlockLoader;
import io.trino.spi.block.RowBlock;
import io.trino.spi.block.RunLengthEncodedBlock;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.RecordCursor;
import io.trino.spi.metrics.Metrics;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.MapType;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeManager;
import io.trino.spi.type.VarcharType;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.serde2.typeinfo.ListTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.MapTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;

import javax.annotation.Nullable;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.function.Function;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.plugin.base.util.Closables.closeAllSuppress;
import static io.trino.plugin.hive.HiveColumnHandle.isRowIdColumnHandle;
import static io.trino.plugin.hive.HiveErrorCode.HIVE_CURSOR_ERROR;
import static io.trino.plugin.hive.HiveErrorCode.HIVE_INVALID_BUCKET_FILES;
import static io.trino.plugin.hive.HivePageSourceProvider.ColumnMappingKind.EMPTY;
import static io.trino.plugin.hive.HivePageSourceProvider.ColumnMappingKind.PREFILLED;
import static io.trino.plugin.hive.HiveType.HIVE_BYTE;
import static io.trino.plugin.hive.HiveType.HIVE_DOUBLE;
import static io.trino.plugin.hive.HiveType.HIVE_FLOAT;
import static io.trino.plugin.hive.HiveType.HIVE_INT;
import static io.trino.plugin.hive.HiveType.HIVE_LONG;
import static io.trino.plugin.hive.HiveType.HIVE_SHORT;
import static io.trino.plugin.hive.coercions.DecimalCoercers.createDecimalToDecimalCoercer;
import static io.trino.plugin.hive.coercions.DecimalCoercers.createDecimalToDoubleCoercer;
import static io.trino.plugin.hive.coercions.DecimalCoercers.createDecimalToRealCoercer;
import static io.trino.plugin.hive.coercions.DecimalCoercers.createDoubleToDecimalCoercer;
import static io.trino.plugin.hive.coercions.DecimalCoercers.createRealToDecimalCoercer;
import static io.trino.plugin.hive.util.HiveBucketing.getHiveBucket;
import static io.trino.plugin.hive.util.HiveUtil.extractStructFieldTypes;
import static io.trino.plugin.hive.util.HiveUtil.isArrayType;
import static io.trino.plugin.hive.util.HiveUtil.isMapType;
import static io.trino.plugin.hive.util.HiveUtil.isRowType;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.block.ColumnarArray.toColumnarArray;
import static io.trino.spi.block.ColumnarMap.toColumnarMap;
import static io.trino.spi.block.ColumnarRow.toColumnarRow;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.RealType.REAL;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class HivePageSource
        implements ConnectorPageSource
{
    private final List<ColumnMapping> columnMappings;
    private final Optional<BucketAdapter> bucketAdapter;
    private final Optional<BucketValidator> bucketValidator;
    private final Object[] prefilledValues;
    private final Type[] types;
    private final List<Optional<Function<Block, Block>>> coercers;
    private final Optional<ReaderProjectionsAdapter> projectionsAdapter;

    private final ConnectorPageSource delegate;

    public HivePageSource(
            List<ColumnMapping> columnMappings,
            Optional<BucketAdaptation> bucketAdaptation,
            Optional<BucketValidator> bucketValidator,
            Optional<ReaderProjectionsAdapter> projectionsAdapter,
            TypeManager typeManager,
            ConnectorPageSource delegate)
    {
        requireNonNull(columnMappings, "columnMappings is null");
        requireNonNull(typeManager, "typeManager is null");

        this.delegate = requireNonNull(delegate, "delegate is null");
        this.columnMappings = columnMappings;
        this.bucketAdapter = bucketAdaptation.map(BucketAdapter::new);
        this.bucketValidator = requireNonNull(bucketValidator, "bucketValidator is null");

        this.projectionsAdapter = requireNonNull(projectionsAdapter, "projectionsAdapter is null");

        int size = columnMappings.size();

        prefilledValues = new Object[size];
        types = new Type[size];
        ImmutableList.Builder<Optional<Function<Block, Block>>> coercers = ImmutableList.builder();

        for (int columnIndex = 0; columnIndex < size; columnIndex++) {
            ColumnMapping columnMapping = columnMappings.get(columnIndex);
            HiveColumnHandle column = columnMapping.getHiveColumnHandle();

            Type type = column.getType();
            types[columnIndex] = type;

            if (columnMapping.getKind() != EMPTY && columnMapping.getBaseTypeCoercionFrom().isPresent()) {
                List<Integer> dereferenceIndices = column.getHiveColumnProjectionInfo()
                        .map(HiveColumnProjectionInfo::getDereferenceIndices)
                        .orElse(ImmutableList.of());
                HiveType fromType = columnMapping.getBaseTypeCoercionFrom().get().getHiveTypeForDereferences(dereferenceIndices).get();
                HiveType toType = columnMapping.getHiveColumnHandle().getHiveType();
                coercers.add(createCoercer(typeManager, fromType, toType));
            }
            else {
                coercers.add(Optional.empty());
            }

            if (columnMapping.getKind() == EMPTY || isRowIdColumnHandle(column)) {
                prefilledValues[columnIndex] = null;
            }
            else if (columnMapping.getKind() == PREFILLED) {
                prefilledValues[columnIndex] = columnMapping.getPrefilledValue().getValue();
            }
        }
        this.coercers = coercers.build();
    }

    public ConnectorPageSource getDelegate()
    {
        return delegate;
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
    public Page getNextPage()
    {
        try {
            Page dataPage = delegate.getNextPage();
            if (dataPage == null) {
                return null;
            }

            if (projectionsAdapter.isPresent()) {
                dataPage = projectionsAdapter.get().adaptPage(dataPage);
            }

            if (bucketAdapter.isPresent()) {
                dataPage = bucketAdapter.get().filterPageToEligibleRowsOrDiscard(dataPage);
                if (dataPage == null) {
                    return null;
                }
            }

            int batchSize = dataPage.getPositionCount();
            List<Block> blocks = new ArrayList<>();
            for (int fieldId = 0; fieldId < columnMappings.size(); fieldId++) {
                ColumnMapping columnMapping = columnMappings.get(fieldId);
                switch (columnMapping.getKind()) {
                    case PREFILLED:
                    case EMPTY:
                        blocks.add(RunLengthEncodedBlock.create(types[fieldId], prefilledValues[fieldId], batchSize));
                        break;
                    case REGULAR:
                    case SYNTHESIZED:
                        Block block = dataPage.getBlock(columnMapping.getIndex());
                        Optional<Function<Block, Block>> coercer = coercers.get(fieldId);
                        if (coercer.isPresent()) {
                            block = new LazyBlock(batchSize, new CoercionLazyBlockLoader(block, coercer.get()));
                        }
                        blocks.add(block);
                        break;
                    case INTERIM:
                        // interim columns don't show up in output
                        break;
                    default:
                        throw new UnsupportedOperationException();
                }
            }

            Page page = new Page(batchSize, blocks.toArray(new Block[0]));

            // bucket adaptation already validates that data is in the right bucket
            if (bucketAdapter.isEmpty()) {
                bucketValidator.ifPresent(validator -> validator.validate(page));
            }

            return page;
        }
        catch (TrinoException e) {
            closeAllSuppress(e, this);
            throw e;
        }
        catch (RuntimeException e) {
            closeAllSuppress(e, this);
            throw new TrinoException(HIVE_CURSOR_ERROR, e);
        }
    }

    @Override
    public void close()
    {
        try {
            delegate.close();
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public String toString()
    {
        return delegate.toString();
    }

    @Override
    public long getMemoryUsage()
    {
        return delegate.getMemoryUsage();
    }

    @Override
    public Metrics getMetrics()
    {
        return delegate.getMetrics();
    }

    public ConnectorPageSource getPageSource()
    {
        return delegate;
    }

    private static Optional<Function<Block, Block>> createCoercer(TypeManager typeManager, HiveType fromHiveType, HiveType toHiveType)
    {
        if (fromHiveType.equals(toHiveType)) {
            return Optional.empty();
        }

        Type fromType = fromHiveType.getType(typeManager);
        Type toType = toHiveType.getType(typeManager);

        if (toType instanceof VarcharType && (fromHiveType.equals(HIVE_BYTE) || fromHiveType.equals(HIVE_SHORT) || fromHiveType.equals(HIVE_INT) || fromHiveType.equals(HIVE_LONG))) {
            return Optional.of(new IntegerNumberToVarcharCoercer<>(fromType, (VarcharType) toType));
        }
        if (fromType instanceof VarcharType && (toHiveType.equals(HIVE_BYTE) || toHiveType.equals(HIVE_SHORT) || toHiveType.equals(HIVE_INT) || toHiveType.equals(HIVE_LONG))) {
            return Optional.of(new VarcharToIntegerNumberCoercer<>((VarcharType) fromType, toType));
        }
        if (fromType instanceof VarcharType && toType instanceof VarcharType) {
            VarcharType toVarcharType = (VarcharType) toType;
            VarcharType fromVarcharType = (VarcharType) fromType;

            if (narrowerThan(toVarcharType, fromVarcharType)) {
                return Optional.of(new VarcharCoercer(fromVarcharType, toVarcharType));
            }

            return Optional.empty();
        }
        if (fromHiveType.equals(HIVE_BYTE) && (toHiveType.equals(HIVE_SHORT) || toHiveType.equals(HIVE_INT) || toHiveType.equals(HIVE_LONG))) {
            return Optional.of(new IntegerNumberUpscaleCoercer<>(fromType, toType));
        }
        if (fromHiveType.equals(HIVE_SHORT) && (toHiveType.equals(HIVE_INT) || toHiveType.equals(HIVE_LONG))) {
            return Optional.of(new IntegerNumberUpscaleCoercer<>(fromType, toType));
        }
        if (fromHiveType.equals(HIVE_INT) && toHiveType.equals(HIVE_LONG)) {
            return Optional.of(new IntegerNumberUpscaleCoercer<>(fromType, toType));
        }
        if (fromHiveType.equals(HIVE_FLOAT) && toHiveType.equals(HIVE_DOUBLE)) {
            return Optional.of(new FloatToDoubleCoercer());
        }
        if (fromHiveType.equals(HIVE_DOUBLE) && toHiveType.equals(HIVE_FLOAT)) {
            return Optional.of(new DoubleToFloatCoercer());
        }
        if (fromType instanceof DecimalType && toType instanceof DecimalType) {
            return Optional.of(createDecimalToDecimalCoercer((DecimalType) fromType, (DecimalType) toType));
        }
        if (fromType instanceof DecimalType && toType == DOUBLE) {
            return Optional.of(createDecimalToDoubleCoercer((DecimalType) fromType));
        }
        if (fromType instanceof DecimalType && toType == REAL) {
            return Optional.of(createDecimalToRealCoercer((DecimalType) fromType));
        }
        if (fromType == DOUBLE && toType instanceof DecimalType) {
            return Optional.of(createDoubleToDecimalCoercer((DecimalType) toType));
        }
        if (fromType == REAL && toType instanceof DecimalType) {
            return Optional.of(createRealToDecimalCoercer((DecimalType) toType));
        }
        if (isArrayType(fromType) && isArrayType(toType)) {
            return Optional.of(new ListCoercer(typeManager, fromHiveType, toHiveType));
        }
        if (isMapType(fromType) && isMapType(toType)) {
            return Optional.of(new MapCoercer(typeManager, fromHiveType, toHiveType));
        }
        if (isRowType(fromType) && isRowType(toType)) {
            return Optional.of(new StructCoercer(typeManager, fromHiveType, toHiveType));
        }

        throw new TrinoException(NOT_SUPPORTED, format("Unsupported coercion from %s to %s", fromHiveType, toHiveType));
    }

    public static boolean narrowerThan(VarcharType first, VarcharType second)
    {
        requireNonNull(first, "first is null");
        requireNonNull(second, "second is null");
        if (first.isUnbounded() || second.isUnbounded()) {
            return !first.isUnbounded();
        }
        return first.getBoundedLength() < second.getBoundedLength();
    }

    private static class ListCoercer
            implements Function<Block, Block>
    {
        private final Optional<Function<Block, Block>> elementCoercer;

        public ListCoercer(TypeManager typeManager, HiveType fromHiveType, HiveType toHiveType)
        {
            requireNonNull(typeManager, "typeManager is null");
            requireNonNull(fromHiveType, "fromHiveType is null");
            requireNonNull(toHiveType, "toHiveType is null");
            HiveType fromElementHiveType = HiveType.valueOf(((ListTypeInfo) fromHiveType.getTypeInfo()).getListElementTypeInfo().getTypeName());
            HiveType toElementHiveType = HiveType.valueOf(((ListTypeInfo) toHiveType.getTypeInfo()).getListElementTypeInfo().getTypeName());
            this.elementCoercer = createCoercer(typeManager, fromElementHiveType, toElementHiveType);
        }

        @Override
        public Block apply(Block block)
        {
            if (elementCoercer.isEmpty()) {
                return block;
            }
            ColumnarArray arrayBlock = toColumnarArray(block);
            Block elementsBlock = elementCoercer.get().apply(arrayBlock.getElementsBlock());
            boolean[] valueIsNull = new boolean[arrayBlock.getPositionCount()];
            int[] offsets = new int[arrayBlock.getPositionCount() + 1];
            for (int i = 0; i < arrayBlock.getPositionCount(); i++) {
                valueIsNull[i] = arrayBlock.isNull(i);
                offsets[i + 1] = offsets[i] + arrayBlock.getLength(i);
            }
            return ArrayBlock.fromElementBlock(arrayBlock.getPositionCount(), Optional.of(valueIsNull), offsets, elementsBlock);
        }
    }

    private static class MapCoercer
            implements Function<Block, Block>
    {
        private final Type toType;
        private final Optional<Function<Block, Block>> keyCoercer;
        private final Optional<Function<Block, Block>> valueCoercer;

        public MapCoercer(TypeManager typeManager, HiveType fromHiveType, HiveType toHiveType)
        {
            requireNonNull(typeManager, "typeManager is null");
            requireNonNull(fromHiveType, "fromHiveType is null");
            this.toType = requireNonNull(toHiveType, "toHiveType is null").getType(typeManager);
            HiveType fromKeyHiveType = HiveType.valueOf(((MapTypeInfo) fromHiveType.getTypeInfo()).getMapKeyTypeInfo().getTypeName());
            HiveType fromValueHiveType = HiveType.valueOf(((MapTypeInfo) fromHiveType.getTypeInfo()).getMapValueTypeInfo().getTypeName());
            HiveType toKeyHiveType = HiveType.valueOf(((MapTypeInfo) toHiveType.getTypeInfo()).getMapKeyTypeInfo().getTypeName());
            HiveType toValueHiveType = HiveType.valueOf(((MapTypeInfo) toHiveType.getTypeInfo()).getMapValueTypeInfo().getTypeName());
            this.keyCoercer = createCoercer(typeManager, fromKeyHiveType, toKeyHiveType);
            this.valueCoercer = createCoercer(typeManager, fromValueHiveType, toValueHiveType);
        }

        @Override
        public Block apply(Block block)
        {
            ColumnarMap mapBlock = toColumnarMap(block);
            Block keysBlock = keyCoercer.isEmpty() ? mapBlock.getKeysBlock() : keyCoercer.get().apply(mapBlock.getKeysBlock());
            Block valuesBlock = valueCoercer.isEmpty() ? mapBlock.getValuesBlock() : valueCoercer.get().apply(mapBlock.getValuesBlock());
            boolean[] valueIsNull = new boolean[mapBlock.getPositionCount()];
            int[] offsets = new int[mapBlock.getPositionCount() + 1];
            for (int i = 0; i < mapBlock.getPositionCount(); i++) {
                valueIsNull[i] = mapBlock.isNull(i);
                offsets[i + 1] = offsets[i] + mapBlock.getEntryCount(i);
            }
            return ((MapType) toType).createBlockFromKeyValue(Optional.of(valueIsNull), offsets, keysBlock, valuesBlock);
        }
    }

    private static class StructCoercer
            implements Function<Block, Block>
    {
        private final List<Optional<Function<Block, Block>>> coercers;
        private final Block[] nullBlocks;

        public StructCoercer(TypeManager typeManager, HiveType fromHiveType, HiveType toHiveType)
        {
            requireNonNull(typeManager, "typeManager is null");
            requireNonNull(fromHiveType, "fromHiveType is null");
            requireNonNull(toHiveType, "toHiveType is null");
            List<HiveType> fromFieldTypes = extractStructFieldTypes(fromHiveType);
            List<HiveType> toFieldTypes = extractStructFieldTypes(toHiveType);
            ImmutableList.Builder<Optional<Function<Block, Block>>> coercers = ImmutableList.builder();
            this.nullBlocks = new Block[toFieldTypes.size()];
            for (int i = 0; i < toFieldTypes.size(); i++) {
                if (i >= fromFieldTypes.size()) {
                    nullBlocks[i] = toFieldTypes.get(i).getType(typeManager).createBlockBuilder(null, 1).appendNull().build();
                    coercers.add(Optional.empty());
                }
                else {
                    coercers.add(createCoercer(typeManager, fromFieldTypes.get(i), toFieldTypes.get(i)));
                }
            }
            this.coercers = coercers.build();
        }

        @Override
        public Block apply(Block block)
        {
            ColumnarRow rowBlock = toColumnarRow(block);
            Block[] fields = new Block[coercers.size()];
            int[] ids = new int[rowBlock.getField(0).getPositionCount()];
            for (int i = 0; i < coercers.size(); i++) {
                Optional<Function<Block, Block>> coercer = coercers.get(i);
                if (coercer.isPresent()) {
                    fields[i] = coercer.get().apply(rowBlock.getField(i));
                }
                else if (i < rowBlock.getFieldCount()) {
                    fields[i] = rowBlock.getField(i);
                }
                else {
                    fields[i] = new DictionaryBlock(nullBlocks[i], ids);
                }
            }
            boolean[] valueIsNull = null;
            if (rowBlock.mayHaveNull()) {
                valueIsNull = new boolean[rowBlock.getPositionCount()];
                for (int i = 0; i < rowBlock.getPositionCount(); i++) {
                    valueIsNull[i] = rowBlock.isNull(i);
                }
            }
            return RowBlock.fromFieldBlocks(rowBlock.getPositionCount(), Optional.ofNullable(valueIsNull), fields);
        }
    }

    private static final class CoercionLazyBlockLoader
            implements LazyBlockLoader
    {
        private final Function<Block, Block> coercer;
        private Block block;

        public CoercionLazyBlockLoader(Block block, Function<Block, Block> coercer)
        {
            this.block = requireNonNull(block, "block is null");
            this.coercer = requireNonNull(coercer, "coercer is null");
        }

        @Override
        public Block load()
        {
            checkState(block != null, "Already loaded");

            Block loaded = coercer.apply(block.getLoadedBlock());
            // clear reference to loader to free resources, since load was successful
            block = null;

            return loaded;
        }
    }

    public static class BucketAdapter
    {
        private final int[] bucketColumns;
        private final BucketingVersion bucketingVersion;
        private final int bucketToKeep;
        private final int tableBucketCount;
        private final int partitionBucketCount; // for sanity check only
        private final List<TypeInfo> typeInfoList;

        public BucketAdapter(BucketAdaptation bucketAdaptation)
        {
            this.bucketColumns = bucketAdaptation.getBucketColumnIndices();
            this.bucketingVersion = bucketAdaptation.getBucketingVersion();
            this.bucketToKeep = bucketAdaptation.getBucketToKeep();
            this.typeInfoList = bucketAdaptation.getBucketColumnHiveTypes().stream()
                    .map(HiveType::getTypeInfo)
                    .collect(toImmutableList());
            this.tableBucketCount = bucketAdaptation.getTableBucketCount();
            this.partitionBucketCount = bucketAdaptation.getPartitionBucketCount();
        }

        @Nullable
        public Page filterPageToEligibleRowsOrDiscard(Page page)
        {
            IntArrayList ids = new IntArrayList(page.getPositionCount());
            Page bucketColumnsPage = page.getColumns(bucketColumns);
            for (int position = 0; position < page.getPositionCount(); position++) {
                int bucket = getHiveBucket(bucketingVersion, tableBucketCount, typeInfoList, bucketColumnsPage, position);
                if ((bucket - bucketToKeep) % partitionBucketCount != 0) {
                    throw new TrinoException(HIVE_INVALID_BUCKET_FILES, format(
                            "A row that is supposed to be in bucket %s is encountered. Only rows in bucket %s (modulo %s) are expected",
                            bucket, bucketToKeep % partitionBucketCount, partitionBucketCount));
                }
                if (bucket == bucketToKeep) {
                    ids.add(position);
                }
            }
            int retainedRowCount = ids.size();
            if (retainedRowCount == 0) {
                return null;
            }
            if (retainedRowCount == page.getPositionCount()) {
                return page;
            }
            return page.getPositions(ids.elements(), 0, retainedRowCount);
        }
    }

    public static class BucketValidator
    {
        // validate every ~100 rows but using a prime number
        public static final int VALIDATION_STRIDE = 97;

        private final Path path;
        private final int[] bucketColumnIndices;
        private final List<TypeInfo> bucketColumnTypes;
        private final BucketingVersion bucketingVersion;
        private final int bucketCount;
        private final int expectedBucket;

        public BucketValidator(
                Path path,
                int[] bucketColumnIndices,
                List<TypeInfo> bucketColumnTypes,
                BucketingVersion bucketingVersion,
                int bucketCount,
                int expectedBucket)
        {
            this.path = requireNonNull(path, "path is null");
            this.bucketColumnIndices = requireNonNull(bucketColumnIndices, "bucketColumnIndices is null");
            this.bucketColumnTypes = requireNonNull(bucketColumnTypes, "bucketColumnTypes is null");
            this.bucketingVersion = requireNonNull(bucketingVersion, "bucketingVersion is null");
            this.bucketCount = bucketCount;
            this.expectedBucket = expectedBucket;
            checkArgument(bucketColumnIndices.length == bucketColumnTypes.size(), "indices and types counts mismatch");
        }

        public void validate(Page page)
        {
            Page bucketColumnsPage = page.getColumns(bucketColumnIndices);
            for (int position = 0; position < page.getPositionCount(); position += VALIDATION_STRIDE) {
                int bucket = getHiveBucket(bucketingVersion, bucketCount, bucketColumnTypes, bucketColumnsPage, position);
                if (bucket != expectedBucket) {
                    throw new TrinoException(HIVE_INVALID_BUCKET_FILES,
                            format("Hive table is corrupt. File '%s' is for bucket %s, but contains a row for bucket %s.", path, expectedBucket, bucket));
                }
            }
        }

        public RecordCursor wrapRecordCursor(RecordCursor delegate, TypeManager typeManager)
        {
            return new HiveBucketValidationRecordCursor(
                    path,
                    bucketColumnIndices,
                    bucketColumnTypes.stream()
                            .map(HiveType::toHiveType)
                            .collect(toImmutableList()),
                    bucketingVersion,
                    bucketCount,
                    expectedBucket,
                    typeManager,
                    delegate);
        }
    }
}
