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

import com.google.common.base.VerifyException;
import io.airlift.slice.Slice;
import io.prestosql.parquet.DataPage;
import io.prestosql.parquet.DataPageV1;
import io.prestosql.parquet.DataPageV2;
import io.prestosql.parquet.DictionaryPage;
import io.prestosql.parquet.Field;
import io.prestosql.parquet.ParquetEncoding;
import io.prestosql.parquet.ParquetTypeUtils;
import io.prestosql.parquet.RichColumnDescriptor;
import io.prestosql.parquet.dictionary.Dictionary;
import io.prestosql.spi.block.BlockBuilder;
import io.prestosql.spi.type.BigintType;
import io.prestosql.spi.type.BooleanType;
import io.prestosql.spi.type.CharType;
import io.prestosql.spi.type.DateType;
import io.prestosql.spi.type.DecimalType;
import io.prestosql.spi.type.DoubleType;
import io.prestosql.spi.type.IntegerType;
import io.prestosql.spi.type.RealType;
import io.prestosql.spi.type.SmallintType;
import io.prestosql.spi.type.TimestampType;
import io.prestosql.spi.type.TinyintType;
import io.prestosql.spi.type.Type;
import io.prestosql.spi.type.VarbinaryType;
import io.prestosql.spi.type.VarcharType;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntList;
import org.apache.parquet.bytes.ByteBufferInputStream;
import org.apache.parquet.bytes.BytesUtils;
import org.apache.parquet.column.values.ValuesReader;
import org.apache.parquet.column.values.rle.RunLengthBitPackingHybridDecoder;
import org.apache.parquet.io.ParquetDecodingException;
import org.apache.parquet.schema.OriginalType;
import org.joda.time.DateTimeZone;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;

import java.io.IOException;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Verify.verify;
import static io.prestosql.parquet.ParquetReaderUtils.toInputStream;
import static io.prestosql.parquet.ParquetTypeUtils.createDecimalType;
import static io.prestosql.parquet.ValuesType.DEFINITION_LEVEL;
import static io.prestosql.parquet.ValuesType.REPETITION_LEVEL;
import static io.prestosql.parquet.ValuesType.VALUES;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public abstract class PrimitiveColumnReader
{
    private static final int EMPTY_LEVEL_VALUE = -1;
    protected final RichColumnDescriptor columnDescriptor;
    protected final Type prestoType;

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

    public static PrimitiveColumnReader createReader(RichColumnDescriptor descriptor, Type prestoType, DateTimeZone timeZone)
    {
        requireNonNull(descriptor, "descriptor is null");
        requireNonNull(prestoType, "prestoType is null");

        Optional<DecimalType> parquetDecimalType = createDecimalType(descriptor);
        Optional<PrimitiveColumnReader> decimalColumnReader = createDecimalColumnReader(descriptor, prestoType);
        verify(parquetDecimalType.isPresent() == decimalColumnReader.isPresent(), "parquetDecimalType and decimalColumnReader should be present at the same time");

        if (prestoType == BooleanType.BOOLEAN && descriptor.getPrimitiveType().getPrimitiveTypeName() == PrimitiveTypeName.BOOLEAN) {
            return new BooleanColumnReader(descriptor, prestoType);
        }

        if (prestoType == TinyintType.TINYINT
                || prestoType == SmallintType.SMALLINT
                || prestoType == IntegerType.INTEGER
                || prestoType == BigintType.BIGINT) {
            // TODO verify no truncation occurs
            if (parquetDecimalType.isPresent() && parquetDecimalType.get().getScale() == 0) {
                // TODO use IntColumnReader or LongColumnReader when applicable
                return decimalColumnReader.orElseThrow(() -> new VerifyException("decimalColumnReader not present when parquetDecimalType is present"));
            }

            if (!parquetDecimalType.isPresent()) {
                if (descriptor.getPrimitiveType().getPrimitiveTypeName() == PrimitiveTypeName.INT32) {
                    return new IntColumnReader(descriptor, prestoType);
                }
                if (descriptor.getPrimitiveType().getPrimitiveTypeName() == PrimitiveTypeName.INT64) {
                    return new LongColumnReader(descriptor, prestoType);
                }
            }
        }

        if (prestoType instanceof DecimalType) {
            if (parquetDecimalType.isPresent() && ((DecimalType) prestoType).getScale() == parquetDecimalType.get().getScale()) {
                // TODO allow rescaling
                return decimalColumnReader.orElseThrow(() -> new VerifyException("decimalColumnReader not present when parquetDecimalType is present"));
            }
        }

        if (prestoType == RealType.REAL && descriptor.getPrimitiveType().getPrimitiveTypeName() == PrimitiveTypeName.FLOAT) {
            return new FloatColumnReader(descriptor, prestoType);
        }

        if (prestoType == DoubleType.DOUBLE && descriptor.getPrimitiveType().getPrimitiveTypeName() == PrimitiveTypeName.DOUBLE) {
            return new DoubleColumnReader(descriptor, prestoType);
        }

        if (prestoType == DateType.DATE && descriptor.getPrimitiveType().getPrimitiveTypeName() == PrimitiveTypeName.INT32) {
            // TODO verify no truncation occurs
            return new IntColumnReader(descriptor, prestoType);
        }

        if (prestoType == TimestampType.TIMESTAMP && descriptor.getPrimitiveType().getPrimitiveTypeName() == PrimitiveTypeName.INT96) {
            return new TimestampColumnReader(descriptor, prestoType, timeZone);
        }

        if (prestoType == TimestampType.TIMESTAMP && descriptor.getPrimitiveType().getPrimitiveTypeName() == PrimitiveTypeName.INT64 && descriptor.getPrimitiveType().getOriginalType() == OriginalType.TIMESTAMP_MICROS) {
            return new TimestampMicrosColumnReader(descriptor, prestoType);
        }

        if (prestoType instanceof VarcharType && descriptor.getPrimitiveType().getPrimitiveTypeName() == PrimitiveTypeName.BINARY) {
            return new BinaryColumnReader(descriptor, prestoType);
        }

        if (prestoType instanceof CharType && descriptor.getPrimitiveType().getPrimitiveTypeName() == PrimitiveTypeName.BINARY) {
            return new BinaryColumnReader(descriptor, prestoType);
        }

        if (prestoType instanceof VarbinaryType && descriptor.getPrimitiveType().getPrimitiveTypeName() == PrimitiveTypeName.BINARY) {
            return new BinaryColumnReader(descriptor, prestoType);
        }

        throw new ParquetDecodingException(format("Cannot read Presto %s from \"%s\" column", prestoType, descriptor));
    }

    private static Optional<PrimitiveColumnReader> createDecimalColumnReader(RichColumnDescriptor descriptor, Type prestoType)
    {
        return createDecimalType(descriptor)
                .map(decimalType -> DecimalColumnReaderFactory.createReader(descriptor, decimalType, prestoType));
    }

    public PrimitiveColumnReader(RichColumnDescriptor columnDescriptor, Type prestoType)
    {
        this.columnDescriptor = requireNonNull(columnDescriptor, "columnDescriptor");
        this.prestoType = requireNonNull(prestoType, "prestoType is null");
        pageReader = null;
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

    public ColumnChunk readPrimitive(Field field)
    {
        IntList definitionLevels = new IntArrayList();
        IntList repetitionLevels = new IntArrayList();
        seek();
        BlockBuilder blockBuilder = field.getType().createBlockBuilder(null, nextBatchSize);
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
        return new ColumnChunk(blockBuilder.build(), definitionLevels.toIntArray(), repetitionLevels.toIntArray());
    }

    private void readValues(BlockBuilder blockBuilder, int valuesToRead, IntList definitionLevels, IntList repetitionLevels)
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

    private void processValues(int valuesToRead, Runnable valueReader)
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
        return new LevelRLEReader(new RunLengthBitPackingHybridDecoder(BytesUtils.getWidthFromMaxInt(maxLevel), slice.getInput()));
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
