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
package io.prestosql.parquet.reader;

import io.airlift.slice.Slice;
import io.prestosql.parquet.DataPage;
import io.prestosql.parquet.DataPageV1;
import io.prestosql.parquet.DataPageV2;
import io.prestosql.parquet.DictionaryPage;
import io.prestosql.parquet.ParquetEncoding;
import io.prestosql.parquet.ParquetTypeUtils;
import io.prestosql.parquet.RichColumnDescriptor;
import io.prestosql.parquet.dictionary.Dictionary;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.block.BlockBuilder;
import io.prestosql.spi.type.BigintType;
import io.prestosql.spi.type.BooleanType;
import io.prestosql.spi.type.DateType;
import io.prestosql.spi.type.DecimalType;
import io.prestosql.spi.type.DoubleType;
import io.prestosql.spi.type.IntegerType;
import io.prestosql.spi.type.RealType;
import io.prestosql.spi.type.TimeType;
import io.prestosql.spi.type.TimestampType;
import io.prestosql.spi.type.Type;
import io.prestosql.spi.type.VarbinaryType;
import io.prestosql.type.coercions.TypeCoercer;
import io.prestosql.type.coercions.TypeCoercers;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntList;
import org.apache.parquet.bytes.ByteBufferInputStream;
import org.apache.parquet.bytes.BytesUtils;
import org.apache.parquet.column.values.ValuesReader;
import org.apache.parquet.column.values.rle.RunLengthBitPackingHybridDecoder;
import org.apache.parquet.io.ParquetDecodingException;
import org.apache.parquet.schema.OriginalType;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Verify.verify;
import static io.prestosql.parquet.ParquetReaderUtils.toInputStream;
import static io.prestosql.parquet.ValuesType.DEFINITION_LEVEL;
import static io.prestosql.parquet.ValuesType.REPETITION_LEVEL;
import static io.prestosql.parquet.ValuesType.VALUES;
import static io.prestosql.spi.StandardErrorCode.NOT_SUPPORTED;
import static java.util.Objects.requireNonNull;

public abstract class PrimitiveColumnReader
{
    private static final int EMPTY_LEVEL_VALUE = -1;
    protected final RichColumnDescriptor columnDescriptor;
    protected final Type sourceType;
    protected final Type targetType;
    protected final TypeCoercer coercer;
    protected int definitionLevel = EMPTY_LEVEL_VALUE;
    protected int repetitionLevel = EMPTY_LEVEL_VALUE;
    protected ValuesReader valuesReader;

    private int nextBatchSize;
    private LevelReader repetitionReader;
    private LevelReader definitionReader;
    private long totalValueCount;
    private PageReader pageReader;
    private Dictionary dictionary;
    private int currentValueCount;
    private DataPage page;
    private int remainingValueCountInPage;
    private int readOffset;

    protected abstract void readValue(BlockBuilder blockBuilder);

    protected abstract void skipValue();

    protected boolean isValueNull()
    {
        return ParquetTypeUtils.isValueNull(columnDescriptor.isRequired(), definitionLevel, columnDescriptor.getMaxDefinitionLevel());
    }

    public static PrimitiveColumnReader createReader(RichColumnDescriptor descriptor, Type targetType)
    {
        Type sourceType = getReadType(descriptor);

        if (sourceType instanceof DecimalType) {
            return DecimalColumnReaderFactory.createReader(descriptor, (DecimalType) sourceType, targetType);
        }

        if (sourceType instanceof TimestampType) {
            return new TimestampColumnReader(descriptor, sourceType, targetType);
        }

        if (sourceType instanceof DateType) {
            return new DateColumnReader(descriptor, sourceType, targetType);
        }

        switch (descriptor.getPrimitiveType().getPrimitiveTypeName()) {
            case BOOLEAN:
                return new BooleanColumnReader(descriptor, sourceType, targetType);
            case INT32:
                return new IntColumnReader(descriptor, sourceType, targetType);
            case INT64:
                return new LongColumnReader(descriptor, sourceType, targetType);
            case FLOAT:
                return new FloatColumnReader(descriptor, sourceType, targetType);
            case DOUBLE:
                return new DoubleColumnReader(descriptor, sourceType, targetType);
            case BINARY:
            case FIXED_LEN_BYTE_ARRAY:
                return new BinaryColumnReader(descriptor, sourceType, targetType);
            default:
                throw new PrestoException(NOT_SUPPORTED, "Unsupported parquet type: " + descriptor.getPrimitiveType().getPrimitiveTypeName());
        }
    }

    private static Type getReadType(RichColumnDescriptor descriptor)
    {
        Optional<DecimalType> decimalType = ParquetTypeUtils.createDecimalType(descriptor);

        if (decimalType.isPresent()) {
            return decimalType.get();
        }

        if (descriptor.getPrimitiveType().getOriginalType() == OriginalType.TIMESTAMP_MICROS) {
            return TimestampType.TIMESTAMP;
        }

        if (descriptor.getPrimitiveType().getOriginalType() == OriginalType.TIMESTAMP_MILLIS) {
            return TimestampType.TIMESTAMP;
        }

        if (descriptor.getPrimitiveType().getOriginalType() == OriginalType.DATE) {
            return DateType.DATE;
        }

        if (descriptor.getPrimitiveType().getOriginalType() == OriginalType.TIME_MILLIS) {
            return TimeType.TIME;
        }

        if (descriptor.getPrimitiveType().getOriginalType() == OriginalType.TIME_MICROS) {
            return TimeType.TIME;
        }

        switch (descriptor.getPrimitiveType().getPrimitiveTypeName()) {
            case BOOLEAN:
                return BooleanType.BOOLEAN;
            case INT32:
                return IntegerType.INTEGER;
            case INT64:
                return BigintType.BIGINT;
            case INT96:
                return TimestampType.TIMESTAMP;
            case FLOAT:
                return RealType.REAL;
            case DOUBLE:
                return DoubleType.DOUBLE;
            case BINARY:
            case FIXED_LEN_BYTE_ARRAY:
                return VarbinaryType.VARBINARY;
            default:
                throw new PrestoException(NOT_SUPPORTED, "Unsupported parquet type: " + descriptor.getPrimitiveType().getPrimitiveTypeName());
        }
    }

    public PrimitiveColumnReader(RichColumnDescriptor columnDescriptor, Type sourceType, Type targetType)
    {
        this.columnDescriptor = requireNonNull(columnDescriptor, "columnDescriptor");
        this.sourceType = requireNonNull(sourceType, "prestoType is null");
        this.targetType = requireNonNull(targetType, "targetType is null");
        this.coercer = TypeCoercers.get(sourceType, targetType);

        pageReader = null;
    }

    public Type getSourceType()
    {
        return sourceType;
    }

    public Type getTargetType()
    {
        return targetType;
    }

    public PageReader getPageReader()
    {
        return pageReader;
    }

    public void setPageReader(PageReader pageReader)
    {
        this.pageReader = requireNonNull(pageReader, "pageReader");
        DictionaryPage dictionaryPage = pageReader.readDictionaryPage();

        if (dictionaryPage != null) {
            try {
                dictionary = dictionaryPage.getEncoding().initDictionary(columnDescriptor, dictionaryPage);
            }
            catch (IOException e) {
                throw new ParquetDecodingException("could not decode the dictionary for " + columnDescriptor, e);
            }
        }
        else {
            dictionary = null;
        }
        checkArgument(pageReader.getTotalValueCount() > 0, "page is empty");
        totalValueCount = pageReader.getTotalValueCount();
    }

    public void prepareNextRead(int batchSize)
    {
        readOffset = readOffset + nextBatchSize;
        nextBatchSize = batchSize;
    }

    public ColumnChunk readPrimitive()
    {
        IntList definitionLevels = new IntArrayList();
        IntList repetitionLevels = new IntArrayList();
        seek();

        BlockBuilder blockBuilder = sourceType.createBlockBuilder(null, nextBatchSize);
        int valueCount = 0;
        while (valueCount < nextBatchSize) {
            if (page == null) {
                readNextPage();
            }
            int valuesToRead = Math.min(remainingValueCountInPage, nextBatchSize - valueCount);
            readValues(blockBuilder, valuesToRead, definitionLevels, repetitionLevels);
            valueCount += valuesToRead;
        }
        checkArgument(valueCount == nextBatchSize, "valueCount %s not equals to batchSize %s", valueCount, nextBatchSize);

        readOffset = 0;
        nextBatchSize = 0;

        return new ColumnChunk(coercer.apply(blockBuilder.build()), definitionLevels.toIntArray(), repetitionLevels.toIntArray());
    }

    protected void readValues(BlockBuilder blockBuilder, int valuesToRead, IntList definitionLevels, IntList repetitionLevels)
    {
        processValues(valuesToRead, () -> {
            readValue(blockBuilder);
            definitionLevels.add(definitionLevel);
            repetitionLevels.add(repetitionLevel);
        });
    }

    private void skipValues(int valuesToRead)
    {
        processValues(valuesToRead, this::skipValue);
    }

    protected void processValues(int valuesToRead, Runnable valueReader)
    {
        if (definitionLevel == EMPTY_LEVEL_VALUE && repetitionLevel == EMPTY_LEVEL_VALUE) {
            definitionLevel = definitionReader.readLevel();
            repetitionLevel = repetitionReader.readLevel();
        }
        int valueCount = 0;
        for (int i = 0; i < valuesToRead; i++) {
            do {
                valueReader.run();
                valueCount++;
                if (valueCount == remainingValueCountInPage) {
                    updateValueCounts(valueCount);
                    if (!readNextPage()) {
                        return;
                    }
                    valueCount = 0;
                }
                repetitionLevel = repetitionReader.readLevel();
                definitionLevel = definitionReader.readLevel();
            }
            while (repetitionLevel != 0);
        }
        updateValueCounts(valueCount);
    }

    private void seek()
    {
        checkArgument(currentValueCount <= totalValueCount, "Already read all values in column chunk");
        if (readOffset == 0) {
            return;
        }
        int valuePosition = 0;
        while (valuePosition < readOffset) {
            if (page == null) {
                readNextPage();
            }
            int offset = Math.min(remainingValueCountInPage, readOffset - valuePosition);
            skipValues(offset);
            valuePosition = valuePosition + offset;
        }
        checkArgument(valuePosition == readOffset, "valuePosition %s must be equal to readOffset %s", valuePosition, readOffset);
    }

    private boolean readNextPage()
    {
        verify(page == null, "readNextPage has to be called when page is null");
        page = pageReader.readPage();
        if (page == null) {
            // we have read all pages
            return false;
        }
        remainingValueCountInPage = page.getValueCount();
        if (page instanceof DataPageV1) {
            valuesReader = readPageV1((DataPageV1) page);
        }
        else {
            valuesReader = readPageV2((DataPageV2) page);
        }
        return true;
    }

    private void updateValueCounts(int valuesRead)
    {
        if (valuesRead == remainingValueCountInPage) {
            page = null;
            valuesReader = null;
        }
        remainingValueCountInPage -= valuesRead;
        currentValueCount += valuesRead;
    }

    private ValuesReader readPageV1(DataPageV1 page)
    {
        ValuesReader rlReader = page.getRepetitionLevelEncoding().getValuesReader(columnDescriptor, REPETITION_LEVEL);
        ValuesReader dlReader = page.getDefinitionLevelEncoding().getValuesReader(columnDescriptor, DEFINITION_LEVEL);
        repetitionReader = new LevelValuesReader(rlReader);
        definitionReader = new LevelValuesReader(dlReader);
        try {
            ByteBufferInputStream in = toInputStream(page.getSlice());
            rlReader.initFromPage(page.getValueCount(), in);
            dlReader.initFromPage(page.getValueCount(), in);
            return initDataReader(page.getValueEncoding(), page.getValueCount(), in);
        }
        catch (IOException e) {
            throw new ParquetDecodingException("Error reading parquet page " + page + " in column " + columnDescriptor, e);
        }
    }

    private ValuesReader readPageV2(DataPageV2 page)
    {
        repetitionReader = buildLevelRLEReader(columnDescriptor.getMaxRepetitionLevel(), page.getRepetitionLevels());
        definitionReader = buildLevelRLEReader(columnDescriptor.getMaxDefinitionLevel(), page.getDefinitionLevels());
        return initDataReader(page.getDataEncoding(), page.getValueCount(), toInputStream(page.getSlice()));
    }

    private LevelReader buildLevelRLEReader(int maxLevel, Slice slice)
    {
        if (maxLevel == 0) {
            return new LevelNullReader();
        }
        return new LevelRLEReader(new RunLengthBitPackingHybridDecoder(BytesUtils.getWidthFromMaxInt(maxLevel), new ByteArrayInputStream(slice.getBytes())));
    }

    private ValuesReader initDataReader(ParquetEncoding dataEncoding, int valueCount, ByteBufferInputStream in)
    {
        ValuesReader valuesReader;
        if (dataEncoding.usesDictionary()) {
            if (dictionary == null) {
                throw new ParquetDecodingException("Dictionary is missing for Page");
            }
            valuesReader = dataEncoding.getDictionaryBasedValuesReader(columnDescriptor, VALUES, dictionary);
        }
        else {
            valuesReader = dataEncoding.getValuesReader(columnDescriptor, VALUES);
        }

        try {
            valuesReader.initFromPage(valueCount, in);
            return valuesReader;
        }
        catch (IOException e) {
            throw new ParquetDecodingException("Error reading parquet page in column " + columnDescriptor, e);
        }
    }
}
