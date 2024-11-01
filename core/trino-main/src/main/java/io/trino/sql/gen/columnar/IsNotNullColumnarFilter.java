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
package io.trino.sql.gen.columnar;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.trino.operator.project.InputChannels;
import io.trino.spi.block.ByteArrayBlock;
import io.trino.spi.block.ValueBlock;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.SourcePage;
import io.trino.sql.relational.InputReferenceExpression;
import io.trino.sql.relational.SpecialForm;

import java.util.Optional;
import java.util.function.Supplier;

import static com.google.common.base.Preconditions.checkArgument;
import static io.trino.sql.relational.SpecialForm.Form.IS_NULL;
import static java.lang.System.arraycopy;

public final class IsNotNullColumnarFilter
        implements ColumnarFilter
{
    private final InputChannels inputChannels;

    public static Supplier<ColumnarFilter> createIsNotNullColumnarFilter(SpecialForm specialForm)
    {
        checkArgument(specialForm.form() == IS_NULL, "specialForm %s should be IS_NULL", specialForm);
        checkArgument(specialForm.arguments().size() == 1, "specialForm %s should have single argument", specialForm);
        if (!(specialForm.arguments().getFirst() instanceof InputReferenceExpression inputReference)) {
            throw new UnsupportedOperationException("NOT(IS_NULL) columnar evaluation is supported only for InputReferenceExpression");
        }
        return () -> new IsNotNullColumnarFilter(inputReference);
    }

    private IsNotNullColumnarFilter(InputReferenceExpression inputReference)
    {
        this.inputChannels = new InputChannels(ImmutableList.of(inputReference.field()), ImmutableSet.of(inputReference.field()));
    }

    @Override
    public InputChannels getInputChannels()
    {
        return inputChannels;
    }

    @Override
    public int filterPositionsRange(ConnectorSession session, int[] outputPositions, int offset, int size, SourcePage page)
    {
        ValueBlock block = (ValueBlock) page.getBlock(0);
        int nonNullPositionsCount = 0;
        int position;
        if (block.mayHaveNull()) {
            Optional<ByteArrayBlock> isNullsBlock = block.getNulls();
            if (isNullsBlock.isPresent()) {
                byte[] isNull = isNullsBlock.get().getRawValues();
                int isNullOffset = isNullsBlock.get().getRawValuesOffset();
                for (position = offset; position < offset + size; position++) {
                    outputPositions[nonNullPositionsCount] = position;
                    nonNullPositionsCount += isNull[isNullOffset + position] == 0 ? 1 : 0;
                }
                return nonNullPositionsCount;
            }
        }

        for (position = offset; position < offset + size; position++) {
            outputPositions[nonNullPositionsCount++] = position;
        }
        return nonNullPositionsCount;
    }

    @Override
    public int filterPositionsList(ConnectorSession session, int[] outputPositions, int[] activePositions, int offset, int size, SourcePage page)
    {
        ValueBlock block = (ValueBlock) page.getBlock(0);
        if (block.mayHaveNull()) {
            Optional<ByteArrayBlock> isNullsBlock = block.getNulls();
            if (isNullsBlock.isPresent()) {
                int nonNullPositionsCount = 0;
                byte[] isNull = isNullsBlock.get().getRawValues();
                int isNullOffset = isNullsBlock.get().getRawValuesOffset();
                for (int index = offset; index < offset + size; index++) {
                    int position = activePositions[index];
                    outputPositions[nonNullPositionsCount] = position;
                    nonNullPositionsCount += isNull[isNullOffset + position] == 0 ? 1 : 0;
                }
                return nonNullPositionsCount;
            }
        }

        arraycopy(activePositions, offset, outputPositions, 0, size);
        return size;
    }
}
