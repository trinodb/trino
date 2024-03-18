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
package io.trino.plugin.varada.dictionary;

import io.airlift.log.Logger;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.trino.plugin.varada.TestingTxService;
import io.trino.plugin.varada.configuration.DictionaryConfiguration;
import io.trino.plugin.varada.dispatcher.model.DictionaryKey;
import io.trino.plugin.varada.dispatcher.model.DictionaryState;
import io.trino.plugin.varada.dispatcher.model.RegularColumn;
import io.trino.plugin.varada.dispatcher.model.SchemaTableColumn;
import io.trino.plugin.varada.dispatcher.model.WarmUpElement;
import io.trino.plugin.varada.dispatcher.model.WarmUpElementState;
import io.trino.plugin.varada.metrics.MetricsManager;
import io.trino.plugin.varada.storage.write.WarmupElementStats;
import io.trino.plugin.varada.type.TypeUtils;
import io.trino.plugin.warp.gen.constants.RecTypeCode;
import io.trino.plugin.warp.gen.constants.WarmUpType;
import io.trino.spi.NodeManager;
import io.trino.spi.block.Block;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.type.BigintType;
import io.trino.spi.type.IntegerType;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarcharType;
import org.apache.commons.math3.random.RandomDataGenerator;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static io.trino.plugin.varada.util.NodeUtils.mockNodeManager;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.params.provider.Arguments.arguments;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class DictionaryCacheServiceTest
{
    private static final Logger logger = Logger.get(DictionaryCacheServiceTest.class);

    private DictionaryConfiguration dictionaryConfiguration;
    private MetricsManager metricsManager;
    private DictionaryCacheService dictionaryCacheService;
    private NodeManager nodeManager;
    private AttachDictionaryService attachDictionaryService;

    static Stream<Arguments> testParams()
    {
        int dictionarySize = 100;
        return Stream.of(
                arguments(RecTypeCode.REC_TYPE_BIGINT, BigintType.BIGINT, dictionarySize),
                arguments(RecTypeCode.REC_TYPE_BIGINT, BigintType.BIGINT, dictionarySize + 1),
                arguments(RecTypeCode.REC_TYPE_BIGINT, BigintType.BIGINT, dictionarySize - 1),
                arguments(RecTypeCode.REC_TYPE_INTEGER, IntegerType.INTEGER, dictionarySize),
                arguments(RecTypeCode.REC_TYPE_INTEGER, IntegerType.INTEGER, dictionarySize + 1),
                arguments(RecTypeCode.REC_TYPE_INTEGER, IntegerType.INTEGER, dictionarySize - 1),
                arguments(RecTypeCode.REC_TYPE_VARCHAR, VarcharType.VARCHAR, dictionarySize),
                arguments(RecTypeCode.REC_TYPE_VARCHAR, VarcharType.VARCHAR, dictionarySize + 1),
                arguments(RecTypeCode.REC_TYPE_VARCHAR, VarcharType.VARCHAR, dictionarySize - 1),
                arguments(RecTypeCode.REC_TYPE_DECIMAL_SHORT, BigintType.BIGINT, dictionarySize),
                arguments(RecTypeCode.REC_TYPE_DECIMAL_SHORT, BigintType.BIGINT, dictionarySize + 1),
                arguments(RecTypeCode.REC_TYPE_DECIMAL_SHORT, BigintType.BIGINT, dictionarySize - 1),
                arguments(RecTypeCode.REC_TYPE_VARCHAR, VarcharType.createVarcharType(20), dictionarySize),
                arguments(RecTypeCode.REC_TYPE_VARCHAR, VarcharType.createVarcharType(20), dictionarySize + 1),
                arguments(RecTypeCode.REC_TYPE_VARCHAR, VarcharType.createVarcharType(20), dictionarySize - 1));
    }

    @BeforeEach
    public void before()
    {
        nodeManager = mockNodeManager();
        dictionaryConfiguration = new DictionaryConfiguration();
        dictionaryConfiguration.setEnableDictionary(true);
        metricsManager = TestingTxService.createMetricsManager();
        attachDictionaryService = mock(AttachDictionaryService.class);
        this.dictionaryCacheService = new DictionaryCacheService(dictionaryConfiguration,
                metricsManager,
                attachDictionaryService);
    }

    @Test
    public void testIsDictionaryValidForColumnValidColumns()
    {
        List<WarmUpElement> warmupElements = getWarmupElements(RecTypeCode.REC_TYPE_BIGINT,
                RecTypeCode.REC_TYPE_INTEGER,
                RecTypeCode.REC_TYPE_DECIMAL_SHORT,
                RecTypeCode.REC_TYPE_VARCHAR);

        for (WarmUpElement warmUpElement : warmupElements) {
            DictionaryState res = dictionaryCacheService.calculateDictionaryStateForWrite(warmUpElement, null);
            assertThat(res).isEqualTo(DictionaryState.DICTIONARY_VALID);
        }
    }

    @Test
    public void testIsDictionaryValidForColumn_testWarmUpType()
    {
        List<WarmUpElement> warmupElements = getWarmupElements(WarmUpType.WARM_UP_TYPE_BASIC,
                WarmUpType.WARM_UP_TYPE_LUCENE,
                WarmUpType.WARM_UP_TYPE_BLOOM_HIGH,
                WarmUpType.WARM_UP_TYPE_BLOOM_LOW,
                WarmUpType.WARM_UP_TYPE_BLOOM_MEDIUM);
        DictionaryCacheService dictionaryCacheService = new DictionaryCacheService(dictionaryConfiguration,
                metricsManager,
                attachDictionaryService);
        for (WarmUpElement warmUpElement : warmupElements) {
            DictionaryState actual = dictionaryCacheService.calculateDictionaryStateForWrite(warmUpElement, null);
            assertThat(actual).isEqualTo(DictionaryState.DICTIONARY_NOT_EXIST);
        }
    }

    @Test
    public void testIsDictionaryValidForColumnNotSuitableColumn()
    {
        List<WarmUpElement> warmupElements = getWarmupElements(RecTypeCode.REC_TYPE_BOOLEAN,
                RecTypeCode.REC_TYPE_TIME,
                RecTypeCode.REC_TYPE_TIMESTAMP_WITH_TZ,
                RecTypeCode.REC_TYPE_TIMESTAMP,
                RecTypeCode.REC_TYPE_TINYINT,
                RecTypeCode.REC_TYPE_DATE,
                RecTypeCode.REC_TYPE_REAL,
                RecTypeCode.REC_TYPE_DOUBLE,
                RecTypeCode.REC_TYPE_ARRAY_BIGINT,
                RecTypeCode.REC_TYPE_ARRAY_INT,
                RecTypeCode.REC_TYPE_ARRAY_VARCHAR,
                RecTypeCode.REC_TYPE_DECIMAL_LONG);

        for (WarmUpElement warmUpElement : warmupElements) {
            DictionaryState actual = dictionaryCacheService.calculateDictionaryStateForWrite(warmUpElement, null);
            assertThat(actual).isEqualTo(DictionaryState.DICTIONARY_NOT_EXIST);
        }
    }

    @Test
    public void testOverTheLimitOfElementsPerDictionary()
    {
        String nodeIdentifier = nodeManager.getCurrentNode().getNodeIdentifier();
        assertThatThrownBy(() -> {
            int totalValuesWithOverflow = Short.MAX_VALUE * 2 + 2;
            String columnName = "column1";
            SchemaTableColumn schemaTableColumn = new SchemaTableColumn(new SchemaTableName("schema", "table"), columnName);

            WarmUpElement warmUpElement = mock(WarmUpElement.class);
            RecTypeCode recTypeCode = RecTypeCode.REC_TYPE_INTEGER;
            when(warmUpElement.getWarmUpType()).thenReturn(WarmUpType.WARM_UP_TYPE_DATA);
            when(warmUpElement.getRecTypeCode()).thenReturn(recTypeCode);
            when(warmUpElement.getRecTypeLength()).thenReturn(4);

            DictionaryKey dictionaryKey = new DictionaryKey(schemaTableColumn, nodeIdentifier, DictionaryKey.CREATED_TIMESTAMP_UNKNOWN);
            DictionaryState actual = dictionaryCacheService.calculateDictionaryStateForWrite(warmUpElement, null);

            assertThat(actual).isEqualTo(DictionaryState.DICTIONARY_VALID);

            WriteDictionary writeDictionary = dictionaryCacheService.computeWriteIfAbsent(dictionaryKey, recTypeCode);
            for (int i = 0; i <= totalValuesWithOverflow; i++) {
                writeDictionary.get("number=" + i);
            }
        }).isInstanceOf(DictionaryMaxException.class);
    }

    private WriteDictionary createWriteDictionary(String columnName, RecTypeCode recTypeCode)
    {
        SchemaTableColumn schemaTableColumn = new SchemaTableColumn(new SchemaTableName("schema", "table"), columnName);
        DictionaryKey dictionaryKey = new DictionaryKey(schemaTableColumn, nodeManager.getCurrentNode().getNodeIdentifier(), DictionaryKey.CREATED_TIMESTAMP_UNKNOWN);
        return dictionaryCacheService.computeWriteIfAbsent(dictionaryKey, recTypeCode);
    }

    @Test
    public void testWriteDictionaryRecTypeLengthIsBiggerThanWarmupElementRecType()
    {
        String columnName = "column1";
        RecTypeCode recTypeCode = RecTypeCode.REC_TYPE_VARCHAR;
        WriteDictionary writeDictionary = createWriteDictionary(columnName, recTypeCode);
        DictionaryKey dictionaryKey = writeDictionary.getDictionaryKey();
        Slice sliceKey = Slices.wrappedBuffer("some longer key".getBytes(Charset.defaultCharset()));
        writeDictionary.get(sliceKey);
        dictionaryCacheService.writeDictionary(
                dictionaryKey,
                recTypeCode,
                0,
                "rowGroupFilePath");
    }

    @Test
    public void testWriteDictionaryRecTypeLengthDictionaryNotAttachedInDb()
    {
        String columnName = "column1";
        RecTypeCode recTypeCode = RecTypeCode.REC_TYPE_VARCHAR;
        WriteDictionary writeDictionary = createWriteDictionary(columnName, recTypeCode);
        DictionaryKey dictionaryKey = writeDictionary.getDictionaryKey();
        Slice sliceKey = Slices.wrappedBuffer("some key".getBytes(Charset.defaultCharset()));
        writeDictionary.get(sliceKey);
        dictionaryCacheService.writeDictionary(
                dictionaryKey,
                recTypeCode,
                0,
                "rowGroupFilePath");
    }

    @MethodSource("testParams")
    public void testReadAndWrite(RecTypeCode recTypeCode, Type type, int dictionarySize)
    {
        String nodeIdentifier = nodeManager.getCurrentNode().getNodeIdentifier();
        String columnName = "column1";
        SchemaTableColumn schemaTableColumn = new SchemaTableColumn(new SchemaTableName("schema", "table"), columnName);
        DictionaryKey dictionaryKey = new DictionaryKey(schemaTableColumn, nodeIdentifier, DictionaryKey.CREATED_TIMESTAMP_UNKNOWN);

        WarmUpElement warmUpElement = mock(WarmUpElement.class);
        when(warmUpElement.getWarmUpType()).thenReturn(WarmUpType.WARM_UP_TYPE_DATA);
        when(warmUpElement.getRecTypeCode()).thenReturn(recTypeCode);
        when(warmUpElement.getRecTypeLength()).thenReturn(4);
        when(warmUpElement.getVaradaColumn()).thenReturn(new RegularColumn(columnName));

//        when(attachDictionaryService.attachDictionary(any(), any(), any(), any())).thenReturn(new DictionaryAttachResult(new byte[1], 5));
        DictionaryState dictionaryState = dictionaryCacheService.calculateDictionaryStateForWrite(warmUpElement, null);
        assertThat(dictionaryState).isEqualTo(DictionaryState.DICTIONARY_VALID);

        WriteDictionary writeDictionary = dictionaryCacheService.computeWriteIfAbsent(dictionaryKey, recTypeCode);
        writeDictionary = dictionaryCacheService.computeWriteIfAbsent(writeDictionary.getDictionaryKey(), recTypeCode);

        Map<Short, Object> expectedValues = new HashMap<>();
        int recTypeLength = 0;
        RandomDataGenerator randomDataGenerator = new RandomDataGenerator();
        switch (type.getBaseName()) {
            case "bigint" -> {
                recTypeLength = TypeUtils.getTypeLength(type, 2000);
                when(warmUpElement.getRecTypeLength()).thenReturn(recTypeLength);
                for (int i = 0; i < dictionarySize; i++) {
                    long generatedLong = randomDataGenerator.nextLong(Long.MIN_VALUE, Long.MAX_VALUE);
                    Short index = writeDictionary.get(generatedLong);
                    expectedValues.put(index, generatedLong);
                }
            }
            case "integer" -> {
                assertThat(recTypeCode).isEqualTo(RecTypeCode.REC_TYPE_INTEGER);
                for (int i = 0; i < dictionarySize; i++) {
                    int generatedInt = randomDataGenerator.nextInt(Integer.MIN_VALUE, Integer.MAX_VALUE);
                    Short index = writeDictionary.get(generatedInt);
                    expectedValues.put(index, generatedInt);
                }
            }
            case "varchar" -> {
                assertThat(recTypeCode).isEqualTo(RecTypeCode.REC_TYPE_VARCHAR);
                String prefixOfSlice = "number=";
                recTypeLength = prefixOfSlice.length() + dictionarySize;
                for (int i = 0; i < dictionarySize; i++) {
                    Slice dictionaryObject = Slices.wrappedBuffer((prefixOfSlice + i).getBytes(Charset.defaultCharset()));
                    Short index = writeDictionary.get(dictionaryObject);
                    expectedValues.put(index, dictionaryObject);
                }
            }
        }
        when(warmUpElement.getRecTypeLength()).thenReturn(recTypeLength);
//        AllocationParams dummyAllocationParams = mock(AllocationParams.class);
//        BlockAppender dummyBlockAppender = mock(BlockAppender.class);
//        WriteJuffersManager dummyJufferManager = mock(WriteJuffersManager.class);
//        int dummyColIx = 0;
//        int dummyTxId = 0;
//        ColumnAllocationParams columnAllocationParams = mock(ColumnAllocationParams.class);
//        when(columnAllocationParams.getRecTypeCode()).thenReturn(recTypeCode);
//        when(dummyAllocationParams.getColumnAllocationParam(eq(dummyColIx))).thenReturn(columnAllocationParams);
        dictionaryCacheService.writeDictionary(
                writeDictionary.getDictionaryKey(),
                recTypeCode,
                0,
                "rowGroupFilePath");
//        dictionaryCacheService.attachDictionaryIfNeeded(dictionaryKey, recTypeCode);

        ReadDictionary readDictionary = dictionaryCacheService.computeReadIfAbsent(writeDictionary.getDictionaryKey(), dictionarySize, recTypeCode, recTypeLength, 0, "rowGroupFilePath");
        assertThat(readDictionary.getReadSize()).isEqualTo(dictionarySize);
        expectedValues.forEach((key, value) -> assertThat(readDictionary.get(Short.toUnsignedInt(key))).isEqualTo(value));

        Block preBlock = readDictionary.getPreBlockDictionaryIfExists(expectedValues.size());
        for (short position = 0; position < preBlock.getPositionCount() - 1; position++) {
            logger.debug("verify values of pre block, null value at the end of the block");
            Object expected = expectedValues.get(position);
            switch (type.getBaseName()) {
                case "bigint" -> assertThat(BigintType.BIGINT.getLong(preBlock, position)).isEqualTo((long) expected);
                case "varchar" -> {
                    String actual = BigintType.BIGINT.getSlice(preBlock, position).toStringUtf8();
                    assertThat(actual).isEqualTo(((Slice) expected).toStringUtf8());
                }
                default -> logger.debug("got base name %s", type.getBaseName());
            }
            assertThat(preBlock.getPositionCount()).isEqualTo(expectedValues.size() + 1); // including null
            assertThat(preBlock.isNull(preBlock.getPositionCount() - 1)).isTrue();
        }
    }

    private List<WarmUpElement> getWarmupElements(RecTypeCode... recTypeCodes)
    {
        List<WarmUpElement> warmUpElements = new ArrayList<>();
        for (RecTypeCode recTypeCode : recTypeCodes) {
            warmUpElements.add(WarmUpElement.builder()
                    .warmUpType(WarmUpType.WARM_UP_TYPE_DATA)
                    .recTypeCode(recTypeCode)
                    .recTypeLength(4)
                    .colName("columnName1")
                    .state(WarmUpElementState.VALID)
                    .warmupElementStats(new WarmupElementStats(0, Long.MIN_VALUE, Long.MAX_VALUE))
                    .build());
        }
        return warmUpElements;
    }

    private List<WarmUpElement> getWarmupElements(WarmUpType... warmUpTypes)
    {
        List<WarmUpElement> warmUpElements = new ArrayList<>();
        for (WarmUpType warmUpType : warmUpTypes) {
            warmUpElements.add(WarmUpElement.builder()
                    .warmUpType(warmUpType)
                    .recTypeCode(RecTypeCode.REC_TYPE_INTEGER)
                    .recTypeLength(4)
                    .colName("columnName1")
                    .state(WarmUpElementState.VALID)
                    .warmupElementStats(new WarmupElementStats(0, Long.MIN_VALUE, Long.MAX_VALUE))
                    .build());
        }
        return warmUpElements;
    }
}
