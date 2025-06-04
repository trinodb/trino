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
package io.trino.operator.window.pattern;

import io.trino.operator.DriverYieldSignal;
import io.trino.operator.Work;
import io.trino.operator.project.PageProjection;
import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.SourcePage;
import io.trino.spi.type.Type;

import java.util.List;
import java.util.function.Supplier;

import static io.trino.operator.project.SelectedPositions.positionsRange;
import static java.util.Objects.requireNonNull;

public class ArgumentComputation
{
    private final PageProjection projection;
    private final Type outputType;
    private final List<Integer> inputChannels;
    private final ConnectorSession session;

    private ArgumentComputation(PageProjection projection, Type outputType, List<Integer> inputChannels, ConnectorSession session)
    {
        this.projection = requireNonNull(projection, "projection is null");
        this.outputType = requireNonNull(outputType, "outputType is null");
        this.inputChannels = requireNonNull(inputChannels, "inputChannels is null");
        this.session = requireNonNull(session, "session is null");
    }

    public Type getOutputType()
    {
        return outputType;
    }

    public List<Integer> getInputChannels()
    {
        return inputChannels;
    }

    public Block compute(Block[] blocks)
    {
        // wrap block array into a single-row page
        SourcePage page = SourcePage.create(new Page(1, blocks));

        // evaluate expression
        Work<Block> work = projection.project(session, new DriverYieldSignal(), projection.getInputChannels().getInputChannels(page), positionsRange(0, 1));
        boolean done = false;
        while (!done) {
            done = work.process();
        }
        return work.getResult();
    }

    public static class ArgumentComputationSupplier
    {
        private final Supplier<PageProjection> projection;
        private final Type outputType;
        private final List<Integer> inputChannels;
        private final ConnectorSession session;

        public ArgumentComputationSupplier(Supplier<PageProjection> projection, Type outputType, List<Integer> inputChannels, ConnectorSession session)
        {
            this.projection = requireNonNull(projection, "projection is null");
            this.outputType = requireNonNull(outputType, "outputType is null");
            this.inputChannels = requireNonNull(inputChannels, "inputChannels is null");
            this.session = requireNonNull(session, "session is null");
        }

        public ArgumentComputation get()
        {
            return new ArgumentComputation(projection.get(), outputType, inputChannels, session);
        }
    }
}
