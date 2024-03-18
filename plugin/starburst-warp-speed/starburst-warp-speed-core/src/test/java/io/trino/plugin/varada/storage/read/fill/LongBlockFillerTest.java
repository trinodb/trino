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

import io.trino.plugin.varada.dictionary.ReadDictionary;
import io.trino.plugin.warp.gen.constants.QueryResultType;
import io.trino.plugin.warp.gen.constants.RecTypeCode;
import io.trino.spi.block.Block;
import io.trino.spi.block.LongArrayBlock;
import io.trino.spi.type.BigintType;
import org.apache.commons.math3.random.RandomDataGenerator;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.function.BiFunction;

public class LongBlockFillerTest
        extends BaseDictionaryBlockTest
{
    private BiFunction<Block, Integer, Long> conversionFunction;
    private BiFunction<ReadDictionary, Integer, Long> getValueFunction;

    @Override
    BlockFiller<Long> createBlockFiller()
    {
        return new LongBlockFiller();
    }

    @Override
    void createConversionFunction()
    {
        conversionFunction = BigintType.BIGINT::getLong;
    }

    @Override
    void createValueFunction()
    {
        getValueFunction = (readDictionary, indexValueAsInt) -> (Long) readDictionary.get(indexValueAsInt);
    }

    @ParameterizedTest
    @MethodSource("testParams")
    public void testFillLongBlockWithDictionary(int dictionarySize, boolean collectNulls, int rowsToFill, QueryResultType queryResultType)
    {
        createDictionary();

        final RecTypeCode recTypeCode = RecTypeCode.REC_TYPE_BIGINT;
        act(rowsToFill, collectNulls, dictionarySize, getValueFunction, conversionFunction, recTypeCode, queryResultType);
    }

    @Override
    List<Long> generateDictionaryValues(int dictionarySize)
    {
        List<Long> dictionaryValues = new ArrayList<>();
        for (int i = 0; i < dictionarySize; i++) {
            long generatedLong = new RandomDataGenerator().nextLong(Long.MIN_VALUE, Long.MAX_VALUE);
            dictionaryValues.add(generatedLong);
        }
        return dictionaryValues;
    }

    @Override
    Block generateDictionaryBlock(List<?> dictionaryValues)
    {
        int positionCount = dictionaryValues.size() + 1;
        boolean[] valueIsNull = new boolean[positionCount];
        long[] values = new long[positionCount];

        for (int i = 0; i < dictionaryValues.size(); i++) {
            values[i] = (long) dictionaryValues.get(i);
        }
        valueIsNull[positionCount - 1] = true;
        return new LongArrayBlock(positionCount, Optional.of(valueIsNull), values);
    }
}
