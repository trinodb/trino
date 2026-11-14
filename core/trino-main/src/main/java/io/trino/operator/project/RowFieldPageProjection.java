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
package io.trino.operator.project;

import io.trino.spi.block.Block;
import io.trino.spi.block.DictionaryBlock;
import io.trino.spi.block.RowBlock;
import io.trino.spi.block.RunLengthEncodedBlock;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.SourcePage;
import io.trino.sql.ir.Expression;

import java.util.List;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public final class RowFieldPageProjection
        implements PageProjection
{
    private final Expression expression;
    private final InputChannels inputChannels;
    private final List<Integer> fields;

    public RowFieldPageProjection(Expression expression, int inputChannel, List<Integer> fields)
    {
        this.expression = requireNonNull(expression, "expression is null");
        this.inputChannels = new InputChannels(inputChannel);
        this.fields = List.copyOf(fields);
    }

    @Override
    public boolean isDeterministic()
    {
        return true;
    }

    @Override
    public InputChannels getInputChannels()
    {
        return inputChannels;
    }

    @Override
    public Block project(ConnectorSession session, SourcePage page, SelectedPositions selectedPositions)
    {
        Block block = page.getBlock(0);
        for (int field : fields) {
            block = getRowField(block, field);
        }
        return selectedPositions.select(block);
    }

    private static Block getRowField(Block block, int field)
    {
        return switch (block) {
            case RowBlock rowBlock -> rowBlock.getFieldBlock(field);
            case DictionaryBlock dictionaryBlock -> dictionaryBlock.createProjection(((RowBlock) dictionaryBlock.getDictionary()).getFieldBlock(field));
            case RunLengthEncodedBlock rleBlock -> RunLengthEncodedBlock.create(((RowBlock) rleBlock.getValue()).getFieldBlock(field), rleBlock.getPositionCount());
            default -> throw new IllegalArgumentException("Unexpected block type: " + block.getClass().getSimpleName());
        };
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("expression", expression)
                .toString();
    }
}
