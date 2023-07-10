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
package io.trino.split;

import io.trino.execution.StageId;
import io.trino.execution.TaskId;
import io.trino.spi.QueryId;
import org.testng.annotations.Test;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.testng.Assert.assertEquals;

public class TestPageSinkId
{
    private PageSinkId fromTaskId(int stageId, int partitionId, int attemptId)
    {
        return PageSinkId.fromTaskId(new TaskId(new StageId(new QueryId("query"), stageId), partitionId, attemptId));
    }

    @Test
    public void testFromTaskId()
    {
        PageSinkId pageSinkId = fromTaskId(1, 2, 3);
        long expected = (1L << 32) + (2L << 8) + 3L;
        assertEquals(pageSinkId.getId(), expected);
    }

    @Test
    public void testFromTaskIdChecks()
    {
        assertThatThrownBy(() -> {
            fromTaskId(1, 1 << 24, 3);
        }).hasMessageContaining("partitionId is out of allowable range");

        assertThatThrownBy(() -> {
            fromTaskId(1, -1, 3);
        }).hasMessageContaining("partitionId is negative");

        assertThatThrownBy(() -> {
            fromTaskId(1, 2, 256);
        }).hasMessageContaining("attemptId is out of allowable range");

        assertThatThrownBy(() -> {
            fromTaskId(1, 2, -1);
        }).hasMessageContaining("attemptId is negative");
    }
}
