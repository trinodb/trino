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
package io.trino.plugin.iceberg.gpu;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorTableHandle;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

/**
 * The Iceberg connector's table handle ({@code base}) with what this connector took over from the
 * plan: predicates it enforces, the Iceberg columns those predicates and pushed-down expressions
 * read ({@code sources}, by name), and an aggregation the device answers.
 *
 * <p>Without an aggregation the scan stays Iceberg's, and this connector filters its pages.
 */
public record GpuTableHandle(
        ConnectorTableHandle base,
        List<GpuPredicate> predicates,
        Map<String, ColumnHandle> sources,
        Optional<GpuAggregation> aggregation)
        implements ConnectorTableHandle
{
    public GpuTableHandle
    {
        requireNonNull(base, "base is null");
        predicates = ImmutableList.copyOf(predicates);
        sources = ImmutableMap.copyOf(sources);
        requireNonNull(aggregation, "aggregation is null");
    }

    public static ConnectorTableHandle unwrap(ConnectorTableHandle handle)
    {
        if (handle instanceof GpuTableHandle gpu) {
            return gpu.base();
        }
        return handle;
    }
}
