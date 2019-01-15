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

import com.facebook.presto.spi.connector.Connector;
import com.facebook.presto.spi.connector.ConnectorMetadata;
import com.facebook.presto.spi.connector.ConnectorRecordSetProvider;
import com.facebook.presto.spi.connector.ConnectorSplitManager;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.facebook.presto.spi.session.PropertyMetadata;
import com.facebook.presto.spi.transaction.IsolationLevel;
import com.google.inject.Inject;
import io.airlift.log.Logger;

import java.util.ArrayList;
import java.util.List;

import static com.facebook.presto.spi.transaction.IsolationLevel.READ_COMMITTED;
import static com.facebook.presto.spi.transaction.IsolationLevel.checkConnectorSupports;
import static java.util.Objects.requireNonNull;

/**
 * Kinesis connector implementation that includes a record set provider.
 * <p>
 * The first 3 methods are mandatory, the remaining Connector methods have defaults.
 * Here a ConnectorRecordSetProvider is applicable.
 */
public class KinesisConnector
        implements Connector
{
    private static final Logger log = Logger.get(KinesisConnector.class);

    private final KinesisMetadata metadata;
    private final KinesisSplitManager splitManager;
    private final KinesisRecordSetProvider recordSetProvider;

    private final ArrayList<PropertyMetadata<?>> propertyList;

    private ArrayList<ConnectorShutdown> shutdownObjects;

    @Inject
    public KinesisConnector(
            KinesisMetadata metadata,
            KinesisSplitManager splitManager,
            KinesisRecordSetProvider recordSetProvider)
    {
        this.metadata = requireNonNull(metadata, "metadata is null");
        this.splitManager = requireNonNull(splitManager, "splitManager is null");
        this.recordSetProvider = requireNonNull(recordSetProvider, "recordSetProvider is null");

        this.propertyList = new ArrayList<PropertyMetadata<?>>();
        buildPropertyList();

        this.shutdownObjects = new ArrayList<ConnectorShutdown>();
    }

    @Override
    public ConnectorMetadata getMetadata(ConnectorTransactionHandle transactionHandle)
    {
        return metadata;
    }

    @Override
    public ConnectorTransactionHandle beginTransaction(IsolationLevel isolationLevel, boolean b)
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

    /**
     * Return the session properties.
     *
     * @return the system properties for this connector
     */
    @Override
    public List<PropertyMetadata<?>> getSessionProperties()
    {
        return this.propertyList;
    }

    /**
     * Build the list of session properties we support to supply them to Presto.
     */
    protected void buildPropertyList()
    {
        KinesisConnectorConfig cfg = this.metadata.getConnectorConfig();

        this.propertyList.add(PropertyMetadata.booleanProperty(
                SessionVariables.CHECKPOINT_ENABLED, "Are checkpoints used in this session?", cfg.isCheckpointEnabled(), false));
        this.propertyList.add(PropertyMetadata.integerProperty(
                SessionVariables.ITERATION_NUMBER, "checkpoint iteration number", cfg.getIterationNumber(), false));
        this.propertyList.add(PropertyMetadata.stringProperty(
                SessionVariables.CHECKPOINT_LOGICAL_NAME, "checkpoint logical name", cfg.getLogicalProcessName(), false));

        this.propertyList.add(PropertyMetadata.integerProperty(
                SessionVariables.MAX_BATCHES, "max number of calls to Kinesis per query", cfg.getMaxBatches(), false));
        this.propertyList.add(PropertyMetadata.integerProperty(
                SessionVariables.BATCH_SIZE, "Record limit in calls to Kinesis", cfg.getBatchSize(), false));
        this.propertyList.add(PropertyMetadata.booleanProperty(
                SessionVariables.ITER_FROM_TIMESTAMP, "Start from timestamp not trim horizon", cfg.isIterFromTimestamp(), false));
        this.propertyList.add(PropertyMetadata.longProperty(
                SessionVariables.ITER_OFFSET_SECONDS, "Seconds before current time to start iterator", cfg.getIterOffsetSeconds(), false));
        // This does not have a corresponding configuration setting, since when not set we can use ITER_OFFSET_SECONDS
        this.propertyList.add(PropertyMetadata.stringProperty(
                SessionVariables.ITER_START_TIMESTAMP, "Timestamp in Presto format to start iterator", SessionVariables.UNSET_TIMESTAMP, false));
    }

    public void registerShutdownObject(ConnectorShutdown obj)
    {
        this.shutdownObjects.add(obj);
    }

    @Override
    public final void shutdown()
    {
        for (ConnectorShutdown obj : this.shutdownObjects) {
            try {
                obj.shutdown();
            }
            catch (Exception ex) {
                log.error("Error when shutting down class in Kinesis connector.", ex);
            }
        }

        return;
    }
}
