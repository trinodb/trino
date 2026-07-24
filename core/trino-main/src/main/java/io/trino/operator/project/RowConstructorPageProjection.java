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

import io.trino.operator.project.ColumnarScalarFunctionPageProjection.Argument;
import io.trino.operator.project.ColumnarScalarFunctionPageProjection.ArgumentsSourcePage;
import io.trino.spi.block.Block;
import io.trino.spi.block.RowBlock;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.SourcePage;
import io.trino.sql.ir.Expression;

import java.util.List;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public final class RowConstructorPageProjection
        implements PageProjection
{
    private final Expression expression;
    private final boolean deterministic;
    private final InputChannels inputChannels;
    private final List<Argument> arguments;

    public RowConstructorPageProjection(Expression expression, boolean deterministic, InputChannels inputChannels, List<Argument> arguments)
    {
        this.expression = requireNonNull(expression, "expression is null");
        this.deterministic = deterministic;
        this.inputChannels = requireNonNull(inputChannels, "inputChannels is null");
        this.arguments = List.copyOf(arguments);
    }

    @Override
    public boolean isDeterministic()
    {
        return deterministic;
    }

    @Override
    public InputChannels getInputChannels()
    {
        return inputChannels;
    }

    @Override
    public Block project(ConnectorSession session, SourcePage page, SelectedPositions selectedPositions)
    {
        SourcePage argumentsPage = new ArgumentsSourcePage(page, selectedPositions, arguments);
        Block[] fields = new Block[arguments.size()];
        for (int field = 0; field < fields.length; field++) {
            fields[field] = argumentsPage.getBlock(field);
        }
        return RowBlock.fromFieldBlocks(selectedPositions.size(), fields);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("expression", expression)
                .toString();
    }
}
