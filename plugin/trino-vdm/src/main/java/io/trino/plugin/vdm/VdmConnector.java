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
package io.trino.plugin.vdm;

import com.google.inject.Inject;
import io.airlift.bootstrap.LifeCycleManager;
import io.airlift.log.Logger;
import io.trino.spi.connector.CachedConnectorMetadata;
import io.trino.spi.connector.Connector;
import io.trino.spi.connector.ConnectorMetadata;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.metastore.HetuMetastore;
import io.trino.spi.transaction.IsolationLevel;

import static java.util.Objects.requireNonNull;

/**
 * vdm connector
 *
 * @since 2023-04-06
 */
public class VdmConnector
        implements Connector
{
    private static final Logger LOGGER = Logger.get(VdmConnectorFactory.class);
    private final LifeCycleManager lifeCycleManager;
    private final HetuMetastore metastore;
    private final ConnectorMetadata metadata;

    /**
     * vdm connector
     *
     * @param vdmName vdm name
     * @param lifeCycleManager life cycle manager
     * @param metastore vdm metastore
     * @param version hetu version
     * @param config config of vdm
     */
    @Inject
    public VdmConnector(VdmName vdmName, LifeCycleManager lifeCycleManager, HetuMetastore metastore,
            NodeVersion version, VdmConfig config)
    {
        requireNonNull(vdmName, "vdmName is null");
        requireNonNull(version, "version is null");
        requireNonNull(config, "config null");
        this.lifeCycleManager = requireNonNull(lifeCycleManager, "lifeCycleManager is null");
        this.metastore = requireNonNull(metastore, "metastore is null");

        if (config.isMetadataCacheEnabled()) {
            this.metadata = new CachedConnectorMetadata(new VdmMetadata(vdmName, this.metastore, version.toString()),
                    config.getMetadataCacheTtl(), config.getMetadataCacheMaximumSize());
        }
        else {
            this.metadata = new VdmMetadata(vdmName, this.metastore, version.toString());
        }
    }

    @Override
    public ConnectorTransactionHandle beginTransaction(IsolationLevel isolationLevel, boolean isReadOnly, boolean autoCommit)
    {
        return new DefaultVdmTransactionHandle();
    }

    @Override
    public ConnectorMetadata getMetadata(ConnectorSession session, ConnectorTransactionHandle transaction)
    {
        return metadata;
    }

    @Override
    public final void shutdown()
    {
        try {
            lifeCycleManager.stop();
        }
        catch (Exception e) {
            LOGGER.error(e, "Error shutting down vdm connector");
        }
    }
}
