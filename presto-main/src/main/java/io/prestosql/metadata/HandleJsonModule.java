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
package io.prestosql.metadata;

import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Scopes;
import com.google.inject.multibindings.ProvidesIntoSet;
import io.prestosql.spi.connector.ColumnHandle;
import io.prestosql.spi.connector.ConnectorIndexHandle;
import io.prestosql.spi.connector.ConnectorInsertTableHandle;
import io.prestosql.spi.connector.ConnectorOutputTableHandle;
import io.prestosql.spi.connector.ConnectorPartitioningHandle;
import io.prestosql.spi.connector.ConnectorSplit;
import io.prestosql.spi.connector.ConnectorTableHandle;
import io.prestosql.spi.connector.ConnectorTableLayoutHandle;
import io.prestosql.spi.connector.ConnectorTransactionHandle;

public class HandleJsonModule
        implements Module
{
    @Override
    public void configure(Binder binder)
    {
        binder.bind(HandleResolver.class).in(Scopes.SINGLETON);
    }

    @ProvidesIntoSet
    public static com.fasterxml.jackson.databind.Module tableHandleModule(HandleResolver resolver)
    {
        return new AbstractTypedJacksonModule<ConnectorTableHandle>(ConnectorTableHandle.class, resolver::getId, resolver::getTableHandleClass) {};
    }

    @ProvidesIntoSet
    public static com.fasterxml.jackson.databind.Module tableLayoutHandleModule(HandleResolver resolver)
    {
        return new AbstractTypedJacksonModule<ConnectorTableLayoutHandle>(ConnectorTableLayoutHandle.class, resolver::getId, resolver::getTableLayoutHandleClass) {};
    }

    @ProvidesIntoSet
    public static com.fasterxml.jackson.databind.Module columnHandleModule(HandleResolver resolver)
    {
        return new AbstractTypedJacksonModule<ColumnHandle>(ColumnHandle.class, resolver::getId, resolver::getColumnHandleClass) {};
    }

    @ProvidesIntoSet
    public static com.fasterxml.jackson.databind.Module splitModule(HandleResolver resolver)
    {
        return new AbstractTypedJacksonModule<ConnectorSplit>(ConnectorSplit.class, resolver::getId, resolver::getSplitClass) {};
    }

    @ProvidesIntoSet
    public static com.fasterxml.jackson.databind.Module outputTableHandleModule(HandleResolver resolver)
    {
        return new AbstractTypedJacksonModule<ConnectorOutputTableHandle>(ConnectorOutputTableHandle.class, resolver::getId, resolver::getOutputTableHandleClass) {};
    }

    @ProvidesIntoSet
    public static com.fasterxml.jackson.databind.Module insertTableHandleModule(HandleResolver resolver)
    {
        return new AbstractTypedJacksonModule<ConnectorInsertTableHandle>(ConnectorInsertTableHandle.class, resolver::getId, resolver::getInsertTableHandleClass) {};
    }

    @ProvidesIntoSet
    public static com.fasterxml.jackson.databind.Module indexHandleModule(HandleResolver resolver)
    {
        return new AbstractTypedJacksonModule<ConnectorIndexHandle>(ConnectorIndexHandle.class, resolver::getId, resolver::getIndexHandleClass) {};
    }

    @ProvidesIntoSet
    public static com.fasterxml.jackson.databind.Module transactionHandleModule(HandleResolver resolver)
    {
        return new AbstractTypedJacksonModule<ConnectorTransactionHandle>(ConnectorTransactionHandle.class, resolver::getId, resolver::getTransactionHandleClass) {};
    }

    @ProvidesIntoSet
    public static com.fasterxml.jackson.databind.Module partitioningHandleModule(HandleResolver resolver)
    {
        return new AbstractTypedJacksonModule<ConnectorPartitioningHandle>(ConnectorPartitioningHandle.class, resolver::getId, resolver::getPartitioningHandleClass) {};
    }
}
