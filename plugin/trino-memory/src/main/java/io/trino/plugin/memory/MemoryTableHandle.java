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
package io.trino.plugin.memory;

import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.SchemaTableName;

import java.util.OptionalDouble;
import java.util.OptionalLong;

import static java.util.Objects.requireNonNull;

public record MemoryTableHandle(
        long id,
        SchemaTableName name,
        OptionalLong limit,
        OptionalDouble sampleRatio)
        implements ConnectorTableHandle
{
    public MemoryTableHandle
    {
        requireNonNull(name, "name is null");
        requireNonNull(limit, "limit is null");
        requireNonNull(sampleRatio, "sampleRatio is null");
    }

    @Override
    public String toString()
    {
        StringBuilder builder = new StringBuilder();
        builder.append(name);
        limit.ifPresent(value -> builder.append(" limit=").append(value));
        sampleRatio.ifPresent(value -> builder.append(" sampleRatio=").append(value));
        return builder.toString();
    }

    public MemoryTableHandle withLimit(long limit)
    {
        return new MemoryTableHandle(id, name, OptionalLong.of(limit), sampleRatio);
    }

    public MemoryTableHandle withSampleRatio(double sampleRatio)
    {
        return new MemoryTableHandle(id, name, limit, OptionalDouble.of(sampleRatio));
    }
}
