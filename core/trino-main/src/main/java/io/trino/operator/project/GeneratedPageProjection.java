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

import io.trino.operator.DriverYieldSignal;
import io.trino.operator.Work;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.SourcePage;
import io.trino.spi.type.Type;
import io.trino.sql.relational.RowExpression;

import java.lang.invoke.MethodHandle;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public class GeneratedPageProjection
        implements PageProjection
{
    private final RowExpression projection;
    private final boolean isDeterministic;
    private final InputChannels inputChannels;
    private final MethodHandle pageProjectionWorkFactory;

    private BlockBuilder blockBuilder;

    public GeneratedPageProjection(RowExpression projection, boolean isDeterministic, InputChannels inputChannels, MethodHandle pageProjectionWorkFactory)
    {
        this.projection = requireNonNull(projection, "projection is null");
        this.isDeterministic = isDeterministic;
        this.inputChannels = requireNonNull(inputChannels, "inputChannels is null");
        this.pageProjectionWorkFactory = requireNonNull(pageProjectionWorkFactory, "pageProjectionWorkFactory is null");
        this.blockBuilder = projection.type().createBlockBuilder(null, 1);
    }

    @Override
    public Type getType()
    {
        return projection.type();
    }

    @Override
    public boolean isDeterministic()
    {
        return isDeterministic;
    }

    @Override
    public InputChannels getInputChannels()
    {
        return inputChannels;
    }

    @Override
    public Work<Block> project(ConnectorSession session, DriverYieldSignal yieldSignal, SourcePage page, SelectedPositions selectedPositions)
    {
        blockBuilder = blockBuilder.newBlockBuilderLike(selectedPositions.size(), null);
        try {
            return (Work<Block>) pageProjectionWorkFactory.invoke(blockBuilder, session, page, selectedPositions);
        }
        catch (Throwable e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("projection", projection)
                .toString();
    }
}
