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
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.SourcePage;
import io.trino.sql.gen.PageProjectionWork;
import io.trino.sql.relational.RowExpression;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public class GeneratedPageProjection
        implements PageProjection
{
    private final RowExpression projection;
    private final boolean isDeterministic;
    private final InputChannels inputChannels;
    private final PageProjectionWork pageProjectionWork;

    private BlockBuilder blockBuilder;

    public GeneratedPageProjection(RowExpression projection, boolean isDeterministic, InputChannels inputChannels, PageProjectionWork pageProjectionWork)
    {
        this.projection = requireNonNull(projection, "projection is null");
        this.isDeterministic = isDeterministic;
        this.inputChannels = requireNonNull(inputChannels, "inputChannels is null");
        this.pageProjectionWork = requireNonNull(pageProjectionWork, "pageProjectionWork is null");
        this.blockBuilder = projection.type().createBlockBuilder(null, 1);
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
    public Block project(ConnectorSession session, SourcePage page, SelectedPositions selectedPositions)
    {
        blockBuilder = blockBuilder.newBlockBuilderLike(selectedPositions.size(), null);
        return pageProjectionWork.process(session, page, selectedPositions, blockBuilder);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("projection", projection)
                .toString();
    }
}
