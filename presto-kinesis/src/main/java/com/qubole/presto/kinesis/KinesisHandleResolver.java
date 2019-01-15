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
package com.qubole.presto.kinesis;

import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorHandleResolver;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.ConnectorTableHandle;
import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.google.inject.name.Named;

import javax.inject.Inject;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

// TODO note: made constructor public for testing purposes
public class KinesisHandleResolver
        implements ConnectorHandleResolver
{
    private final String connectorId;

    @Inject
    public KinesisHandleResolver(@Named("connectorId") String connectorId)
    {
        this.connectorId = requireNonNull(connectorId, "connectorId is null");
    }

    @Override
    public Class<? extends ConnectorTableHandle> getTableHandleClass()
    {
        return KinesisTableHandle.class;
    }

    @Override
    public Class<? extends ConnectorTableLayoutHandle> getTableLayoutHandleClass()
    {
        return KinesisTableLayoutHandle.class;
    }

    @Override
    public Class<? extends ConnectorTransactionHandle> getTransactionHandleClass()
    {
        return KinesisTransactionHandle.class;
    }

    @Override
    public Class<? extends ColumnHandle> getColumnHandleClass()
    {
        return KinesisColumnHandle.class;
    }

    @Override
    public Class<? extends ConnectorSplit> getSplitClass()
    {
        return KinesisSplit.class;
    }

    KinesisTableHandle convertTableHandle(ConnectorTableHandle tableHandle)
    {
        requireNonNull(tableHandle, "tableHandle is null");
        checkArgument(tableHandle instanceof KinesisTableHandle, "tableHandle is not an instance of KinesisTableHandle");
        KinesisTableHandle kinesisTableHandle = (KinesisTableHandle) tableHandle;
        checkArgument(kinesisTableHandle.getConnectorId().equals(connectorId), "tableHandle is not for this connector");

        return kinesisTableHandle;
    }

    static KinesisColumnHandle convertColumnHandle(ColumnHandle columnHandle)
    {
        requireNonNull(columnHandle, "columnHandle is null");
        checkArgument(columnHandle instanceof KinesisColumnHandle, "columnHandle is not an instance of KinesisColumnHandle");
        KinesisColumnHandle kinesisColumnHandle = (KinesisColumnHandle) columnHandle;
        return kinesisColumnHandle;
    }

    KinesisSplit convertSplit(ConnectorSplit split)
    {
        requireNonNull(split, "split is null");
        checkArgument(split instanceof KinesisSplit, "split is not an instance of KinesisSplit");
        KinesisSplit kinesisSplit = (KinesisSplit) split;
        checkArgument(kinesisSplit.getConnectorId().equals(connectorId), "split is not for this connector");
        return kinesisSplit;
    }

    KinesisTableLayoutHandle convertLayout(ConnectorTableLayoutHandle layout)
    {
        requireNonNull(layout, "layout is null");
        checkArgument(layout instanceof KinesisTableLayoutHandle, "layout is not an instance of KinesisTableLayoutHandle");
        KinesisTableLayoutHandle kinesisLayout = (KinesisTableLayoutHandle) layout;
        checkArgument(kinesisLayout.getConnectorId().equals(connectorId), "split is not for this connector");
        return kinesisLayout;
    }
}
