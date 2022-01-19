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
package io.trino.spi.connector;

import java.util.Optional;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toSet;

@Deprecated
public interface ConnectorHandleResolver
{
    default Class<? extends ConnectorTableHandle> getTableHandleClass()
    {
        throw new UnsupportedOperationException();
    }

    default Class<? extends ColumnHandle> getColumnHandleClass()
    {
        throw new UnsupportedOperationException();
    }

    default Class<? extends ConnectorSplit> getSplitClass()
    {
        throw new UnsupportedOperationException();
    }

    default Class<? extends ConnectorIndexHandle> getIndexHandleClass()
    {
        throw new UnsupportedOperationException();
    }

    default Class<? extends ConnectorOutputTableHandle> getOutputTableHandleClass()
    {
        throw new UnsupportedOperationException();
    }

    default Class<? extends ConnectorInsertTableHandle> getInsertTableHandleClass()
    {
        throw new UnsupportedOperationException();
    }

    default Class<? extends ConnectorTableExecuteHandle> getTableExecuteHandleClass()
    {
        throw new UnsupportedOperationException();
    }

    default Class<? extends ConnectorPartitioningHandle> getPartitioningHandleClass()
    {
        throw new UnsupportedOperationException();
    }

    default Class<? extends ConnectorTransactionHandle> getTransactionHandleClass()
    {
        throw new UnsupportedOperationException();
    }

    default Set<Class<?>> getHandleClasses()
    {
        return Stream.of(
                value(this::getTableHandleClass),
                value(this::getColumnHandleClass),
                value(this::getSplitClass),
                value(this::getIndexHandleClass),
                value(this::getOutputTableHandleClass),
                value(this::getInsertTableHandleClass),
                value(this::getTableExecuteHandleClass),
                value(this::getPartitioningHandleClass),
                value(this::getTransactionHandleClass))
                .flatMap(Optional::stream)
                .collect(toSet());
    }

    private static Optional<Class<?>> value(Supplier<Class<?>> supplier)
    {
        try {
            return Optional.of(supplier.get());
        }
        catch (UnsupportedOperationException e) {
            return Optional.empty();
        }
    }
}
