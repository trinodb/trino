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
package io.trino.spooling.filesystem;

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;

public class PartitionedLayoutConfig
{
    private int partitions = 32;

    @Min(2)
    @Max(1024)
    public int getPartitions()
    {
        return partitions;
    }

    @ConfigDescription("Number of file system partitions to use")
    @Config("fs.layout.partitions")
    public PartitionedLayoutConfig setPartitions(int partitions)
    {
        this.partitions = partitions;
        return this;
    }
}
