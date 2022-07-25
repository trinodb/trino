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

import io.trino.spi.Page;
import io.trino.spi.connector.ConnectorSession;

public interface PageFilter
{
    boolean isDeterministic();

    InputChannels getInputChannels();

    SelectedPositions filter(ConnectorSession session, Page page);

    static SelectedPositions positionsArrayToSelectedPositions(boolean[] selectedPositions, int size)
    {
        int selectedCount = 0;
        for (int i = 0; i < size; i++) {
            boolean selectedPosition = selectedPositions[i];
            // Avoid branching by casting boolean to integer.
            // This improves CPU utilization by avoiding branch mispredictions.
            selectedCount += selectedPosition ? 1 : 0;
        }

        if (selectedCount == 0 || selectedCount == size) {
            return SelectedPositions.positionsRange(0, selectedCount);
        }

        int[] positions = new int[selectedCount];
        int index = 0;
        for (int position = 0; index < selectedCount; position++) {
            // Improve CPU utilization by avoiding setting of position conditionally.
            // This improves CPU utilization by avoiding branch mispredictions.
            positions[index] = position;
            index += selectedPositions[position] ? 1 : 0;
        }
        return SelectedPositions.positionsList(positions, 0, selectedCount);
    }
}
