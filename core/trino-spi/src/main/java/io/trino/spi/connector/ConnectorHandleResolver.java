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
}
