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

import io.trino.operator.project.SelectedPositions;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.SourcePage;

import static io.trino.operator.project.SelectedPositions.positionsList;
import static io.trino.operator.project.SelectedPositions.positionsRange;
import static java.util.Objects.requireNonNull;

public final class ColumnarFilterEvaluator
        implements FilterEvaluator
{
    private final ColumnarFilter filter;
    private int[] outputPositions = new int[0];

    public ColumnarFilterEvaluator(ColumnarFilter filter)
    {
        this.filter = requireNonNull(filter, "filter is null");
    }

    @Override
    public SelectionResult evaluate(ConnectorSession session, SelectedPositions activePositions, SourcePage page)
    {
        if (activePositions.isEmpty()) {
            return new SelectionResult(activePositions, 0);
        }
        // Should load only the blocks necessary for evaluating the kernel and unwrap lazy blocks
        SourcePage loadedPage = filter.getInputChannels().getInputChannels(page);
        if (outputPositions.length < activePositions.size()) {
            outputPositions = new int[activePositions.size()];
        }
        int outputPositionsCount;
        long start = System.nanoTime();
        if (activePositions.isList()) {
            outputPositionsCount = filter.filterPositionsList(session, outputPositions, activePositions.getPositions(), activePositions.getOffset(), activePositions.size(), loadedPage);
        }
        else {
            outputPositionsCount = filter.filterPositionsRange(session, outputPositions, activePositions.getOffset(), activePositions.size(), loadedPage);
            // full range was selected
            if (outputPositionsCount == activePositions.size()) {
                return new SelectionResult(positionsRange(activePositions.getOffset(), outputPositionsCount), System.nanoTime() - start);
            }
        }
        return new SelectionResult(positionsList(outputPositions, 0, outputPositionsCount), System.nanoTime() - start);
    }
}
