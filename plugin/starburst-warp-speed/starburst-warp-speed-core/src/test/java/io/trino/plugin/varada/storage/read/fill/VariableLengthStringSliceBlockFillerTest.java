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

import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.trino.plugin.varada.dictionary.ReadDictionary;
import io.trino.plugin.warp.gen.constants.QueryResultType;
import io.trino.plugin.warp.gen.constants.RecTypeCode;
import io.trino.spi.block.Block;
import io.trino.spi.block.VariableWidthBlock;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.function.BiFunction;

import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.lang.String.format;

public class VariableLengthStringSliceBlockFillerTest
        extends BaseDictionaryBlockTest
{
    private BiFunction<Block, Integer, Slice> conversionFunction;
    private BiFunction<ReadDictionary, Integer, Slice> getValueFunction;

    @Override
    BlockFiller<Slice> createBlockFiller()
    {
        return new VariableLengthStringSliceBlockFiller(storageEngineConstants, nativeConfiguration);
    }

    @Override
    void createConversionFunction()
    {
        conversionFunction = VARCHAR::getSlice;
    }

    @Override
    void createValueFunction()
    {
        getValueFunction = (readDictionary, indexValueAsInt) -> (Slice) readDictionary.get(indexValueAsInt);
    }

    @ParameterizedTest
    @MethodSource("testParams")
    public void testFillVarcharBlockWithDictionary(int dictionarySize, boolean collectNulls, int rowsToFill, QueryResultType queryResultType)
    {
        createDictionary();

        final RecTypeCode recTypeCode = RecTypeCode.REC_TYPE_VARCHAR;
        act(rowsToFill, collectNulls, dictionarySize, getValueFunction, conversionFunction, recTypeCode, queryResultType);
    }

    @Override
    List<Slice> generateDictionaryValues(int dictionarySize)
    {
        List<Slice> dictionaryValues = new ArrayList<>();
        for (int i = 0; i < dictionarySize; i++) {
            String generatedString = "number%d";
            Slice slice = Slices.utf8Slice(format(generatedString, i));
            dictionaryValues.add(slice);
        }
        return dictionaryValues;
    }

    @Override
    Block generateDictionaryBlock(List<?> dictionaryValues)
    {
        int positionCount = dictionaryValues.size() + 1;
        boolean[] valueIsNull = new boolean[positionCount];
        int[] outputOffsets = new int[positionCount + 1];
        Slice[] slices = new Slice[positionCount];
        int totalLength = 0;

        for (int i = 0; i < dictionaryValues.size(); i++) {
            slices[i] = (Slice) dictionaryValues.get(i);
            totalLength += slices[i].length();
            outputOffsets[i + 1] = totalLength;
        }
        outputOffsets[positionCount] = totalLength;
        byte[] values = new byte[totalLength];
        Slice outputSlice = Slices.wrappedBuffer(values);
        for (int i = 0; i < dictionaryValues.size(); i++) {
            outputSlice.setBytes(outputOffsets[i], slices[i]);
        }
        valueIsNull[positionCount - 1] = true;
        return new VariableWidthBlock(positionCount, outputSlice, outputOffsets, Optional.of(valueIsNull));
    }
}
