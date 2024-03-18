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
package io.trino.plugin.warp.extension.execution.debugtools;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.trino.plugin.varada.execution.TaskData;

public class ResetStatsData
        extends TaskData
{
    private final String statsKey;

    @JsonCreator
    public ResetStatsData(@JsonProperty("statsKey") String statsKey)
    {
        this.statsKey = statsKey;
    }

    public String getStatsKey()
    {
        return statsKey;
    }

    @Override
    protected String getTaskName()
    {
        return ResetStatsTask.RESET_STAT;
    }
}
