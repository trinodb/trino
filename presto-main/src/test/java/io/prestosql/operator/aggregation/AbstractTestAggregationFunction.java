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
package io.prestosql.operator.aggregation;

import com.google.common.collect.Lists;
import io.prestosql.metadata.MetadataManager;
import io.prestosql.metadata.Signature;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.block.BlockBuilder;
import io.prestosql.spi.block.RunLengthEncodedBlock;
import io.prestosql.spi.type.Type;
import io.prestosql.spi.type.TypeSignature;
import io.prestosql.sql.analyzer.TypeSignatureProvider;
import io.prestosql.sql.tree.QualifiedName;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.List;

import static io.prestosql.metadata.MetadataManager.createTestMetadataManager;
import static io.prestosql.operator.aggregation.AggregationTestUtils.assertAggregation;
import static io.prestosql.sql.analyzer.TypeSignatureProvider.fromTypeSignatures;

public abstract class AbstractTestAggregationFunction
{
    protected MetadataManager metadata;

    @BeforeClass
    public final void initTestAggregationFunction()
    {
        metadata = createTestMetadataManager();
    }

    protected abstract Block[] getSequenceBlocks(int start, int length);

    protected final InternalAggregationFunction getFunction()
    {
        List<TypeSignatureProvider> parameterTypes = fromTypeSignatures(Lists.transform(getFunctionParameterTypes(), TypeSignature::parseTypeSignature));
        Signature signature = metadata.getFunctionRegistry().resolveFunction(QualifiedName.of(getFunctionName()), parameterTypes);
        return metadata.getFunctionRegistry().getAggregateFunctionImplementation(signature);
    }

    protected abstract String getFunctionName();

    protected abstract List<String> getFunctionParameterTypes();

    protected abstract Object getExpectedValue(int start, int length);

    protected Object getExpectedValueIncludingNulls(int start, int length, int lengthIncludingNulls)
    {
        return getExpectedValue(start, length);
    }

    @Test
    public void testNoPositions()
    {
        testAggregation(getExpectedValue(0, 0), getSequenceBlocks(0, 0));
    }

    @Test
    public void testSinglePosition()
    {
        testAggregation(getExpectedValue(0, 1), getSequenceBlocks(0, 1));
    }

    @Test
    public void testMultiplePositions()
    {
        testAggregation(getExpectedValue(0, 5), getSequenceBlocks(0, 5));
    }

    @Test
    public void testAllPositionsNull()
    {
        // if there are no parameters skip this test
        List<Type> parameterTypes = getFunction().getParameterTypes();
        if (parameterTypes.isEmpty()) {
            return;
        }
        Block[] blocks = new Block[parameterTypes.size()];
        for (int i = 0; i < parameterTypes.size(); i++) {
            blocks[i] = RunLengthEncodedBlock.create(parameterTypes.get(0), null, 10);
        }

        testAggregation(getExpectedValueIncludingNulls(0, 0, 10), blocks);
    }

    @Test
    public void testMixedNullAndNonNullPositions()
    {
        // if there are no parameters skip this test
        List<Type> parameterTypes = getFunction().getParameterTypes();
        if (parameterTypes.isEmpty()) {
            return;
        }

        Block[] alternatingNullsBlocks = createAlternatingNullsBlock(parameterTypes, getSequenceBlocks(0, 10));
        testAggregation(getExpectedValueIncludingNulls(0, 10, 20), alternatingNullsBlocks);
    }

    @Test
    public void testNegativeOnlyValues()
    {
        testAggregation(getExpectedValue(-10, 5), getSequenceBlocks(-10, 5));
    }

    @Test
    public void testPositiveOnlyValues()
    {
        testAggregation(getExpectedValue(2, 4), getSequenceBlocks(2, 4));
    }

    protected Block[] createAlternatingNullsBlock(List<Type> types, Block... sequenceBlocks)
    {
        Block[] alternatingNullsBlocks = new Block[sequenceBlocks.length];
        for (int i = 0; i < sequenceBlocks.length; i++) {
            int positionCount = sequenceBlocks[i].getPositionCount();
            Type type = types.get(i);
            BlockBuilder blockBuilder = type.createBlockBuilder(null, positionCount);
            for (int position = 0; position < positionCount; position++) {
                // append null
                blockBuilder.appendNull();
                // append value
                type.appendTo(sequenceBlocks[i], position, blockBuilder);
            }
            alternatingNullsBlocks[i] = blockBuilder.build();
        }
        return alternatingNullsBlocks;
    }

    protected void testAggregation(Object expectedValue, Block... blocks)
    {
        assertAggregation(getFunction(), expectedValue, blocks);
    }
}
