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
package io.trino.plugin.bigquery;

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import io.airlift.units.DataSize;

import static io.airlift.units.DataSize.Unit.MEGABYTE;

public class BigQueryArrowConfig
{
    private DataSize maxAllocation = DataSize.of(100, MEGABYTE);

    public DataSize getMaxAllocation()
    {
        return maxAllocation;
    }

    @ConfigDescription("Maximum memory allocation allowed for Arrow library")
    @Config("bigquery.arrow-serialization.max-allocation")
    public BigQueryArrowConfig setMaxAllocation(DataSize maxAllocation)
    {
        this.maxAllocation = maxAllocation;
        return this;
    }
}
