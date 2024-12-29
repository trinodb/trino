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
package io.trino.plugin.lakehouse;

import com.google.inject.Inject;
import io.trino.plugin.deltalake.DeltaLakeTransactionManager;
import io.trino.plugin.hive.HiveTransactionHandle;
import io.trino.plugin.hive.HiveTransactionManager;
import io.trino.plugin.hudi.HudiTransactionManager;
import io.trino.plugin.iceberg.IcebergTransactionManager;
import io.trino.spi.connector.ConnectorMetadata;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.security.ConnectorIdentity;

import static java.util.Objects.requireNonNull;

public final class LakehouseTransactionManager
{
    private final LakehouseTableProperties lakehouseTableProperties;
    private final HiveTransactionManager hiveTransactionManager;
    private final IcebergTransactionManager icebergTransactionManager;
    private final DeltaLakeTransactionManager deltaTransactionManager;
    private final HudiTransactionManager hudiTransactionManager;

    @Inject
    public LakehouseTransactionManager(
            LakehouseTableProperties lakehouseTableProperties,
            HiveTransactionManager hiveTransactionManager,
            IcebergTransactionManager icebergTransactionManager,
            DeltaLakeTransactionManager deltaTransactionManager,
            HudiTransactionManager hudiTransactionManager)
    {
        this.lakehouseTableProperties = requireNonNull(lakehouseTableProperties, "lakehouseTableProperties is null");
        this.hiveTransactionManager = requireNonNull(hiveTransactionManager, "hiveTransactionManager is null");
        this.icebergTransactionManager = requireNonNull(icebergTransactionManager, "icebergTransactionManager is null");
        this.deltaTransactionManager = requireNonNull(deltaTransactionManager, "deltaTransactionManager is null");
        this.hudiTransactionManager = requireNonNull(hudiTransactionManager, "hudiTransactionManager is null");
    }

    public ConnectorTransactionHandle begin()
    {
        ConnectorTransactionHandle handle = new HiveTransactionHandle(true);
        hiveTransactionManager.begin(handle);
        icebergTransactionManager.begin(handle);
        deltaTransactionManager.begin(handle);
        hudiTransactionManager.put(handle);
        return handle;
    }

    public ConnectorMetadata get(ConnectorTransactionHandle transaction, ConnectorIdentity identity)
    {
        return new LakehouseMetadata(
                lakehouseTableProperties,
                hiveTransactionManager.get(transaction, identity),
                icebergTransactionManager.get(transaction, identity),
                deltaTransactionManager.get(transaction, identity),
                hudiTransactionManager.get(transaction, identity));
    }

    public void commit(ConnectorTransactionHandle transaction)
    {
        hiveTransactionManager.commit(transaction);
        icebergTransactionManager.commit(transaction);
        deltaTransactionManager.commit(transaction);
        hudiTransactionManager.commit(transaction);
    }

    public void rollback(ConnectorTransactionHandle transaction)
    {
        hiveTransactionManager.rollback(transaction);
        icebergTransactionManager.rollback(transaction);
        deltaTransactionManager.rollback(transaction);
        hudiTransactionManager.rollback(transaction);
    }
}
