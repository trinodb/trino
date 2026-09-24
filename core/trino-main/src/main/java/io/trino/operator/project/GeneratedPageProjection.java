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
import io.trino.sql.ir.Expression;
import io.trino.sql.ir.SecureExpressions;

import java.lang.invoke.MethodHandle;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Throwables.throwIfUnchecked;
import static java.util.Objects.requireNonNull;

public class GeneratedPageProjection
        implements PageProjection
{
    private final Expression projection;
    private final boolean isDeterministic;
    private final boolean secure;
    private final InputChannels inputChannels;
    private final MethodHandle pageProjectionWorkFactory;

    private BlockBuilder blockBuilder;

    public GeneratedPageProjection(Expression projection, boolean isDeterministic, boolean secure, InputChannels inputChannels, MethodHandle pageProjectionWorkFactory)
    {
        this.projection = requireNonNull(projection, "projection is null");
        this.isDeterministic = isDeterministic;
        this.secure = secure;
        this.inputChannels = requireNonNull(inputChannels, "inputChannels is null");
        this.pageProjectionWorkFactory = requireNonNull(pageProjectionWorkFactory, "pageProjectionWorkFactory is null");
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
        PageProjectionWork work;
        try {
            work = (PageProjectionWork) pageProjectionWorkFactory.invoke(blockBuilder, session, page, selectedPositions);
        }
        catch (Throwable throwable) {
            if (secure && throwable instanceof Exception e) {
                // The constructor creates the function instances of the secure expression
                throw SecureExpressions.redactFailure(e);
            }
            throw propagate(throwable);
        }
        try {
            return work.process();
        }
        catch (Throwable throwable) {
            throw propagate(throwable);
        }
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("projection", projection)
                .toString();
    }

    private static RuntimeException propagate(Throwable throwable)
    {
        if (throwable instanceof InterruptedException) {
            Thread.currentThread().interrupt();
        }
        throwIfUnchecked(throwable);
        throw new RuntimeException(throwable);
    }
}
