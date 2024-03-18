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
package io.trino.plugin.varada.storage.read.fill;

import io.airlift.log.Logger;
import io.airlift.slice.Slice;
import io.trino.plugin.varada.TestingTxService;
import io.trino.plugin.varada.configuration.DictionaryConfiguration;
import io.trino.plugin.varada.configuration.NativeConfiguration;
import io.trino.plugin.varada.dictionary.AttachDictionaryService;
import io.trino.plugin.varada.dictionary.DataValueDictionary;
import io.trino.plugin.varada.dictionary.DictionaryCacheService;
import io.trino.plugin.varada.dictionary.ReadDictionary;
import io.trino.plugin.varada.dictionary.WriteDictionary;
import io.trino.plugin.varada.dispatcher.model.DictionaryKey;
import io.trino.plugin.varada.dispatcher.model.DictionaryState;
import io.trino.plugin.varada.dispatcher.model.RegularColumn;
import io.trino.plugin.varada.dispatcher.model.SchemaTableColumn;
import io.trino.plugin.varada.dispatcher.model.WarmUpElement;
import io.trino.plugin.varada.metrics.MetricsManager;
import io.trino.plugin.varada.storage.engine.StubsStorageEngineConstants;
import io.trino.plugin.varada.storage.juffers.BaseJuffer;
import io.trino.plugin.varada.storage.juffers.JuffersWarmUpElementBase;
import io.trino.plugin.varada.storage.juffers.ReadJuffersWarmUpElement;
import io.trino.plugin.warp.gen.constants.QueryResultType;
import io.trino.plugin.warp.gen.constants.RecTypeCode;
import io.trino.plugin.warp.gen.constants.WarmUpType;
import io.trino.spi.NodeManager;
import io.trino.spi.block.Block;
import io.trino.spi.block.DictionaryBlock;
import io.trino.spi.block.RunLengthEncodedBlock;
import io.trino.spi.connector.SchemaTableName;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.params.provider.Arguments;

import java.nio.ByteBuffer;
import java.nio.ShortBuffer;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.stream.Stream;

import static io.trino.plugin.varada.util.NodeUtils.mockNodeManager;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.params.provider.Arguments.arguments;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public abstract class BaseDictionaryBlockTest
{
    private static final Logger logger = Logger.get(BaseDictionaryBlockTest.class);
    protected DictionaryConfiguration dictionaryConfiguration;
    protected NativeConfiguration nativeConfiguration;
    protected StubsStorageEngineConstants storageEngineConstants;
    private DictionaryKey dictionaryKey;
    private DictionaryCacheService dictionaryCacheService;
    private BlockFiller blockFiller;

    /**
     * position[0] - int dictionarySize: dictionary size
     * position[1] - boolean collectNulls: simulate Block with nulls
     * position[2] - int rowsToFill: required number of rows to fill in Block
     */
    static Stream<Arguments> testParams()
    {
        int dictionarySize = 12;
        int doubleDictionarySize = 12 * 2;
        return Stream.of(
                arguments(dictionarySize, false, dictionarySize * dictionarySize, QueryResultType.QUERY_RESULT_TYPE_SINGLE_NO_NULL),
                arguments(dictionarySize, true, dictionarySize * dictionarySize, QueryResultType.QUERY_RESULT_TYPE_ALL_NULL),
                arguments(dictionarySize, true, dictionarySize * dictionarySize, QueryResultType.QUERY_RESULT_TYPE_SINGLE),
                arguments(dictionarySize, false, dictionarySize * dictionarySize, QueryResultType.QUERY_RESULT_TYPE_SINGLE),
                arguments(dictionarySize, false, dictionarySize * dictionarySize, QueryResultType.QUERY_RESULT_TYPE_RAW),
                arguments(dictionarySize, false, dictionarySize * dictionarySize, QueryResultType.QUERY_RESULT_TYPE_RAW_NO_NULL),
                arguments(dictionarySize, false, dictionarySize / 2 - 1, QueryResultType.QUERY_RESULT_TYPE_RAW), //without dictionaryBlock
                arguments(dictionarySize, true, dictionarySize * dictionarySize, QueryResultType.QUERY_RESULT_TYPE_RAW),
                arguments(dictionarySize, true, dictionarySize / 2 - 1, QueryResultType.QUERY_RESULT_TYPE_RAW), //without dictionaryBlock
                arguments(doubleDictionarySize, false, doubleDictionarySize * doubleDictionarySize, QueryResultType.QUERY_RESULT_TYPE_RAW),
                arguments(doubleDictionarySize, false, doubleDictionarySize / 2 - 1, QueryResultType.QUERY_RESULT_TYPE_RAW), //without dictionaryBlock
                arguments(doubleDictionarySize, true, doubleDictionarySize * doubleDictionarySize, QueryResultType.QUERY_RESULT_TYPE_RAW),
                arguments(doubleDictionarySize, true, doubleDictionarySize / 2 - 1, QueryResultType.QUERY_RESULT_TYPE_RAW)); //without dictionaryBlock
    }

    @BeforeAll
    public void beforeAll()
    {
        dictionaryConfiguration = new DictionaryConfiguration();
        dictionaryConfiguration.setEnableDictionary(true);
        nativeConfiguration = new NativeConfiguration();
        storageEngineConstants = new StubsStorageEngineConstants();
        createConversionFunction();
        createValueFunction();
        NodeManager nodeManager = mockNodeManager();
        dictionaryKey = new DictionaryKey(new SchemaTableColumn(new SchemaTableName("schema", "table"), new RegularColumn("c1")),
                nodeManager.getCurrentNode().getNodeIdentifier(), DictionaryKey.CREATED_TIMESTAMP_UNKNOWN);
    }

    @BeforeEach
    public void before()
    {
        MetricsManager metricsManager = TestingTxService.createMetricsManager();
        dictionaryCacheService = new DictionaryCacheService(dictionaryConfiguration,
                metricsManager,
                mock(AttachDictionaryService.class));
        blockFiller = createBlockFiller();
    }

    abstract BlockFiller createBlockFiller();

    abstract void createConversionFunction();

    abstract void createValueFunction();

    void createDictionary()
    {
        WarmUpElement warmUpElement = mock(WarmUpElement.class);
        RecTypeCode recTypeCode = RecTypeCode.REC_TYPE_INTEGER;
        when(warmUpElement.getWarmUpType()).thenReturn(WarmUpType.WARM_UP_TYPE_DATA);
        when(warmUpElement.getRecTypeCode()).thenReturn(recTypeCode);
        when(warmUpElement.getRecTypeLength()).thenReturn(4);

        DictionaryState dictionaryState = dictionaryCacheService.calculateDictionaryStateForWrite(warmUpElement, null);
        assertThat(dictionaryState).isEqualTo(DictionaryState.DICTIONARY_VALID);

        WriteDictionary writeDictionary = getWriteDictionary(recTypeCode);
        dictionaryKey = writeDictionary.getDictionaryKey();
    }

    <T> void act(int rowsToFill,
            boolean collectNulls,
            int dictionarySize,
            BiFunction<ReadDictionary, Integer, T> getValueFunction,
            BiFunction<Block, Integer, T> conversionFunction,
            RecTypeCode recTypeCode,
            QueryResultType queryResultTypeRaw)
    {
        createDictionary();
        WriteDictionary writeDictionary = getWriteDictionary(recTypeCode);

        int maxIndexValue = 0;
        if (queryResultTypeRaw == QueryResultType.QUERY_RESULT_TYPE_RAW ||
                queryResultTypeRaw == QueryResultType.QUERY_RESULT_TYPE_SINGLE ||
                queryResultTypeRaw == QueryResultType.QUERY_RESULT_TYPE_SINGLE_NO_NULL ||
                queryResultTypeRaw == QueryResultType.QUERY_RESULT_TYPE_RAW_NO_NULL) {
            maxIndexValue = buildReadWriteDictionary(dictionarySize, writeDictionary);
        }

        ReadJuffersWarmUpElement juffersWE = mock(ReadJuffersWarmUpElement.class);
        ReadDictionary readDictionary = getReadDictionary(recTypeCode);

        List<T> expectedOutput = createBuffersToReadFrom(readDictionary, rowsToFill, maxIndexValue, juffersWE, collectNulls, getValueFunction, queryResultTypeRaw);
        int recTypeLength = -1;
        logger.info("going to fill block using %s", blockFiller);
        if (queryResultTypeRaw != QueryResultType.QUERY_RESULT_TYPE_ALL_NULL) {
            recTypeLength = switch (recTypeCode) {
                case REC_TYPE_BIGINT, REC_TYPE_DOUBLE -> Long.BYTES;
                case REC_TYPE_VARCHAR ->
                        ((Slice) readDictionary.get(maxIndexValue)).toStringUtf8().length(); // max length value will be at  the end of the dictionary
                default -> {
                    Assertions.fail("unknown recTypeCode " + recTypeCode);
                    throw new UnsupportedOperationException();
                }
            };
        }

        Block block = blockFiller.fillBlockWithDictionary(juffersWE,
                queryResultTypeRaw,
                rowsToFill,
                recTypeCode,
                recTypeLength,
                collectNulls,
                readDictionary);
        if (queryResultTypeRaw == QueryResultType.QUERY_RESULT_TYPE_ALL_NULL ||
                queryResultTypeRaw == QueryResultType.QUERY_RESULT_TYPE_SINGLE_NO_NULL) {
            assertThat(block).isInstanceOf(RunLengthEncodedBlock.class);
        }
        else if (queryResultTypeRaw == QueryResultType.QUERY_RESULT_TYPE_SINGLE) {
            assertThat(block).isInstanceOf(DictionaryBlock.class);
        }
        else if (readDictionary.getPreBlockDictionaryIfExists(rowsToFill) != null) {
            assertThat(block).isInstanceOf(DictionaryBlock.class);
        }
        else {
            assertThat(block).isNotInstanceOf(DictionaryBlock.class);
        }

        assertThat(block.getPositionCount()).isEqualTo(rowsToFill);
        logger.info("returned block type %s, going to assert and verify each row", block);
        for (int rowNumber = 0; rowNumber < block.getPositionCount(); rowNumber++) {
            if (expectedOutput.get(rowNumber) == null) {
                Assertions.assertTrue(block.isNull(rowNumber));
            }
            else {
                T expected = expectedOutput.get(rowNumber);
                T actual = conversionFunction.apply(block, rowNumber);
                assertThat(actual).isEqualTo(expected);
            }
        }
        if (collectNulls && queryResultTypeRaw != QueryResultType.QUERY_RESULT_TYPE_ALL_NULL) {
            verify(juffersWE, times(1)).getNullBuffer();
        }
        if (readDictionary.getReadSize() > 0) {
            assertThat(readDictionary.getPreBlockDictionaryIfExists(rowsToFill)).isNotNull();
        }
    }

    abstract <T> List<T> generateDictionaryValues(int dictionarySize);

    abstract Block generateDictionaryBlock(List<?> dictionaryValues);

    /**
     * fill buffer with values from readDictionary, in order to 'mock' original fill flow.
     *
     * @param <T> - type of Filler
     * @param readDictionary - original dictionary created at warm
     * @param rowsToFill - rowsToFillIn
     * @param maxIndexValue - max index value in dictionary, will generate random value between 0 to @maxIndexValue
     * @param juffersWE - juffersWE
     * @param collectNulls - if need to add null valued
     * @param getValueFunction - read value from dictionary function
     * @return - expectedValues list - will compare this with actual result
     */
    private <T> List<T> createBuffersToReadFrom(ReadDictionary readDictionary,
            int rowsToFill,
            int maxIndexValue,
            JuffersWarmUpElementBase juffersWE,
            boolean collectNulls,
            BiFunction<ReadDictionary, Integer, T> getValueFunction,
            QueryResultType queryResultTypeRaw)
    {
        ByteBuffer nullBuffer = null;
        if (collectNulls || queryResultTypeRaw == QueryResultType.QUERY_RESULT_TYPE_ALL_NULL || queryResultTypeRaw == QueryResultType.QUERY_RESULT_TYPE_SINGLE) {
            nullBuffer = ByteBuffer.allocate(rowsToFill);
        }

        List<T> expectedOutput = new ArrayList<>();
        ShortBuffer shortBuffer;
        if (queryResultTypeRaw == QueryResultType.QUERY_RESULT_TYPE_SINGLE || queryResultTypeRaw == QueryResultType.QUERY_RESULT_TYPE_SINGLE_NO_NULL) {
            shortBuffer = ShortBuffer.allocate(1);
            Random rnd = new Random(maxIndexValue);
            short value = maxIndexValue == 0 ? 0 : (short) rnd.nextInt(maxIndexValue);
            int indexValueAsInt = Short.toUnsignedInt(value);
            shortBuffer.put(value);
            for (int i = 0; i < rowsToFill; i++) {
                if (collectNulls && i % 7 == 0) {
                    nullBuffer.put((byte) -1);
                    expectedOutput.add(null);
                }
                else {
                    if (queryResultTypeRaw == QueryResultType.QUERY_RESULT_TYPE_SINGLE) {
                        nullBuffer.put((byte) 0);
                    }
                    expectedOutput.add(getValueFunction.apply(readDictionary, indexValueAsInt));
                }
            }
        }
        else {
            shortBuffer = ShortBuffer.allocate(rowsToFill);
            Random rnd = new Random(maxIndexValue);
            for (int rowNumber = 0; rowNumber < rowsToFill; rowNumber++) {
                if ((collectNulls && rowNumber % 7 == 0) || (queryResultTypeRaw == QueryResultType.QUERY_RESULT_TYPE_ALL_NULL)) { // mock nulls values
                    nullBuffer.put((byte) -1);
                    shortBuffer.put((byte) 0);
                    expectedOutput.add(null);
                }
                else {
                    short value = maxIndexValue == 0 ? 0 : (short) rnd.nextInt(maxIndexValue);
                    int indexValueAsInt = Short.toUnsignedInt(value);
                    expectedOutput.add(getValueFunction.apply(readDictionary, indexValueAsInt));
                    shortBuffer.put(value);
                    if (collectNulls) {
                        nullBuffer.put((byte) 0);
                    }
                }
            }
        }
        logger.info("fill buffer with %s expected filled/read rows", shortBuffer.position());
        shortBuffer.position(0);
        when(juffersWE.getRecordBuffer()).thenReturn(shortBuffer);
        @SuppressWarnings("unused")
        BaseJuffer juffer = mock(BaseJuffer.class);
        if (collectNulls || queryResultTypeRaw == QueryResultType.QUERY_RESULT_TYPE_ALL_NULL || queryResultTypeRaw == QueryResultType.QUERY_RESULT_TYPE_SINGLE) {
            when(juffersWE.getNullBuffer()).thenReturn(nullBuffer);
            nullBuffer.position(0);
        }
        return expectedOutput;
    }

    private <T> int buildReadWriteDictionary(int dictionarySize,
            WriteDictionary writeDictionary)
    {
        List<T> dictionaryAsList = generateDictionaryValues(dictionarySize);
        Set<Integer> indexes = new HashSet<>();
        int maxIndexValue = 0;
        for (T value : dictionaryAsList) {
            int indexValueAsInt = Short.toUnsignedInt(writeDictionary.get(value));
            if (indexes.add(indexValueAsInt)) {
                maxIndexValue = Math.max(maxIndexValue, indexValueAsInt);
            }
        }
        DataValueDictionary dictionary = (DataValueDictionary) writeDictionary;
        assertThat(dictionary.getWriteSize()).isEqualTo(dictionaryAsList.size());

        //need to mock dictionaryBlock
        Block prePrepareDictionaryBlock = generateDictionaryBlock(dictionaryAsList);
        dictionary.setDictionaryPreBlock(prePrepareDictionaryBlock);

        logger.info("created dictionary of size=%s, prePrepareDictionaryBlock=%s ,maxIndexValue=%s",
                dictionary.getReadSize(),
                dictionary.getPreBlockDictionaryIfExists(prePrepareDictionaryBlock.getPositionCount()) == null ? null : dictionary.getPreBlockDictionaryIfExists(prePrepareDictionaryBlock.getPositionCount()).getPositionCount(),
                maxIndexValue);
        return maxIndexValue;
    }

    WriteDictionary getWriteDictionary(RecTypeCode recTypeCode)
    {
        return dictionaryCacheService.computeWriteIfAbsent(dictionaryKey, recTypeCode);
    }

    ReadDictionary getReadDictionary(RecTypeCode recTypeCode)
    {
        return dictionaryCacheService.computeReadIfAbsent(dictionaryKey, 0, recTypeCode, 0, 0, "rowGroupFilePath");
    }
}
