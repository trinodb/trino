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

import io.trino.plugin.iceberg.gpu.engine.DeviceTable;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.type.Type;

import java.util.Optional;

import static io.trino.spi.type.DoubleType.DOUBLE;
import static java.util.Objects.requireNonNull;

/**
 * A column this connector provides: a table column as the device holds it ({@code kind}, at
 * {@code fileIndex} in the data files), a value expression pushed down by projection, or an
 * aggregate pushed down by aggregation.
 */
public record GpuColumnHandle(
        String name,
        Type type,
        int fileIndex,
        Optional<DeviceTable.Kind> kind,
        Optional<GpuExpression> expression,
        Optional<GpuAggregate> aggregate)
        implements ColumnHandle
{
    public GpuColumnHandle
    {
        requireNonNull(name, "name is null");
        requireNonNull(type, "type is null");
        requireNonNull(kind, "kind is null");
        requireNonNull(expression, "expression is null");
        requireNonNull(aggregate, "aggregate is null");
    }

    public static GpuColumnHandle tableColumn(String name, Type type, int fileIndex, DeviceTable.Kind kind)
    {
        return new GpuColumnHandle(name, type, fileIndex, Optional.of(kind), Optional.empty(), Optional.empty());
    }

    public static GpuColumnHandle expressionColumn(String name, GpuExpression expression)
    {
        return new GpuColumnHandle(name, DOUBLE, -1, Optional.empty(), Optional.of(expression), Optional.empty());
    }

    public static GpuColumnHandle aggregateColumn(String name, Type type, GpuAggregate aggregate)
    {
        return new GpuColumnHandle(name, type, -1, Optional.empty(), Optional.empty(), Optional.of(aggregate));
    }

    public boolean isTableColumn()
    {
        return kind.isPresent();
    }
}
