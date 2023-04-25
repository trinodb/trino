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
package io.trino.operator.join.smj;

import java.util.ArrayList;
import java.util.List;

public class SortMergeJoinBridge
{
    private final String taskId;

    private List<PageIterator> buildPages;

    public SortMergeJoinBridge(String taskId, Integer driverCount, Integer maxBufferPageCount)
    {
        this.taskId = taskId;
        this.buildPages = new ArrayList<>(driverCount);
        for (int i = 0; i < driverCount; i++) {
            buildPages.add(new PageIterator(maxBufferPageCount));
        }
    }

    public PageIterator getBuildPages(Integer index)
    {
        return buildPages.get(index);
    }

    public String getTaskId()
    {
        return taskId;
    }
}
