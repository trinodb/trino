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
import io.trino.spi.Page;

import static io.trino.operator.project.SelectedPositions.positionsList;
import static io.trino.operator.project.SelectedPositions.positionsRange;

public class ColumnFilterProcessor
{
    private final ColumnarFilter filter;
    private int[] outputPositions = new int[0];

    public ColumnFilterProcessor(ColumnarFilter filter)
    {
        this.filter = filter;
    }

    public SelectedPositions processFilter(SelectedPositions activePositions, Page page)
    {
        if (activePositions.isEmpty()) {
            return activePositions;
        }
        // Should load only the blocks necessary for evaluating the kernel and unwrap lazy blocks
        Page loadedPage = filter.getInputChannels().getInputChannels(page);
        if (outputPositions.length < activePositions.size()) {
            outputPositions = new int[activePositions.size()];
        }
        int outputPositionsCount;
        if (activePositions.isList()) {
            outputPositionsCount = filter.filterPositionsList(outputPositions, activePositions.getPositions(), activePositions.getOffset(), activePositions.size(), loadedPage);
        }
        else {
            outputPositionsCount = filter.filterPositionsRange(outputPositions, activePositions.getOffset(), activePositions.size(), loadedPage);
            // full range was selected
            if (outputPositionsCount == activePositions.size()) {
                return positionsRange(activePositions.getOffset(), outputPositionsCount);
            }
        }
        return positionsList(outputPositions, 0, outputPositionsCount);
    }
}
