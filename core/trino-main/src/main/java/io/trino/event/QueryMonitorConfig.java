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
package io.trino.event;

import io.airlift.configuration.Config;
import io.airlift.units.DataSize;
import io.airlift.units.DataSize.Unit;
import io.airlift.units.MaxDataSize;
import io.airlift.units.MinDataSize;
import jakarta.validation.constraints.NotNull;

public class QueryMonitorConfig
{
    private DataSize maxOutputStageJsonSize = DataSize.of(16, Unit.MEGABYTE);

    @MinDataSize("1kB")
    @MaxDataSize("1GB")
    @NotNull
    public DataSize getMaxOutputStageJsonSize()
    {
        return maxOutputStageJsonSize;
    }

    @Config("event.max-output-stage-size")
    public QueryMonitorConfig setMaxOutputStageJsonSize(DataSize maxOutputStageJsonSize)
    {
        this.maxOutputStageJsonSize = maxOutputStageJsonSize;
        return this;
    }
}
