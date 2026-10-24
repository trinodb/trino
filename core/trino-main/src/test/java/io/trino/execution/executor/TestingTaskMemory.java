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
package io.trino.execution.executor;

import io.airlift.units.DataSize;

import static io.airlift.units.DataSize.Unit.GIGABYTE;

public record TestingTaskMemory(long leafPipelineBytes, long leafDriverMaxBytes, long queryFreeBytes, long queryMaxBytes, long poolFreeBytes)
        implements TaskMemory
{
    // One leaf driver holds all leaf memory and the pool has 1 GB free.
    public TestingTaskMemory(DataSize leafBytes, DataSize queryFreeBytes, DataSize queryMaxBytes)
    {
        this(leafBytes.toBytes(), leafBytes.toBytes(), queryFreeBytes.toBytes(), queryMaxBytes.toBytes(), DataSize.of(1, GIGABYTE).toBytes());
    }

    @Override
    public long getLeafPipelineBytes()
    {
        return leafPipelineBytes;
    }

    @Override
    public long getLeafDriverMaxBytes()
    {
        return leafDriverMaxBytes;
    }

    @Override
    public long getQueryFreeBytes()
    {
        return queryFreeBytes;
    }

    @Override
    public long getQueryMaxBytes()
    {
        return queryMaxBytes;
    }

    @Override
    public long getPoolFreeBytes()
    {
        return poolFreeBytes;
    }
}
