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
package io.trino.plugin.kinesis;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.inject.Inject;
import io.trino.spi.connector.Connector;
import io.trino.spi.connector.ConnectorMetadata;
import io.trino.spi.connector.ConnectorRecordSetProvider;
import io.trino.spi.connector.ConnectorSplitManager;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.session.PropertyMetadata;
import io.trino.spi.transaction.IsolationLevel;

import java.util.List;
import java.util.Set;

import static io.trino.spi.transaction.IsolationLevel.READ_COMMITTED;
import static io.trino.spi.transaction.IsolationLevel.checkConnectorSupports;
import static java.util.Objects.requireNonNull;

public class KinesisConnector
        implements Connector
{
    private final KinesisMetadata metadata;
    private final KinesisSplitManager splitManager;
    private final KinesisRecordSetProvider recordSetProvider;

    private final List<PropertyMetadata<?>> propertyList;

    @Inject
    public KinesisConnector(
            KinesisMetadata metadata,
            KinesisSplitManager splitManager,
            KinesisRecordSetProvider recordSetProvider,
            KinesisSessionProperties properties)
    {
        this.metadata = requireNonNull(metadata, "metadata is null");
        this.splitManager = requireNonNull(splitManager, "splitManager is null");
        this.recordSetProvider = requireNonNull(recordSetProvider, "recordSetProvider is null");
        this.propertyList = ImmutableList.copyOf(properties.getSessionProperties());
    }

    @Override
    public Set<Class<?>> getHandleClasses()
    {
        return ImmutableSet.<Class<?>>builder()
                .add(KinesisTableHandle.class)
                .add(KinesisColumnHandle.class)
                .add(KinesisSplit.class)
                .add(KinesisTransactionHandle.class)
                .build();
    }

    @Override
    public ConnectorMetadata getMetadata(ConnectorTransactionHandle transactionHandle)
    {
        return metadata;
    }

    @Override
    public ConnectorTransactionHandle beginTransaction(IsolationLevel isolationLevel, boolean readOnly, boolean autoCommit)
    {
        checkConnectorSupports(READ_COMMITTED, isolationLevel);
        return KinesisTransactionHandle.INSTANCE;
    }

    @Override
    public ConnectorSplitManager getSplitManager()
    {
        return splitManager;
    }

    @Override
    public ConnectorRecordSetProvider getRecordSetProvider()
    {
        return recordSetProvider;
    }

    @Override
    public List<PropertyMetadata<?>> getSessionProperties()
    {
        return propertyList;
    }
}
