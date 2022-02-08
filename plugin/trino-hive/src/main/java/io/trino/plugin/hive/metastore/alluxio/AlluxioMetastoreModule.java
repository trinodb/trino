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
package io.trino.plugin.hive.metastore.alluxio;

import alluxio.ClientContext;
import alluxio.client.table.RetryHandlingTableMasterClient;
import alluxio.client.table.TableMasterClient;
import alluxio.conf.InstancedConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.exception.status.AlluxioStatusException;
import alluxio.master.MasterClientContext;
import alluxio.util.ConfigurationUtils;
import com.google.inject.Binder;
import com.google.inject.Provides;
import com.google.inject.Scopes;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.airlift.log.Logger;
import io.trino.plugin.hive.metastore.HiveMetastore;
import io.trino.plugin.hive.metastore.HiveMetastoreFactory;
import io.trino.plugin.hive.metastore.RawHiveMetastoreFactory;

import java.net.InetSocketAddress;

import static io.airlift.configuration.ConfigBinder.configBinder;

/**
 * Module for an Alluxio metastore implementation of the {@link HiveMetastore} interface.
 */
public class AlluxioMetastoreModule
        extends AbstractConfigurationAwareModule
{
    private static final Logger log = Logger.get(AlluxioMetastoreModule.class);

    @Override
    protected void setup(Binder binder)
    {
        configBinder(binder).bindConfig(AlluxioHiveMetastoreConfig.class);

        binder.bind(HiveMetastoreFactory.class).annotatedWith(RawHiveMetastoreFactory.class).to(AlluxioHiveMetastoreFactory.class).in(Scopes.SINGLETON);
    }

    @Provides
    public TableMasterClient provideCatalogMasterClient(AlluxioHiveMetastoreConfig config)
    {
        return createCatalogMasterClient(config);
    }

    public static TableMasterClient createCatalogMasterClient(AlluxioHiveMetastoreConfig config)
    {
        InstancedConfiguration conf = new InstancedConfiguration(ConfigurationUtils.defaults());
        String addr = config.getMasterAddress();
        String[] parts = addr.split(":", 2);
        conf.set(PropertyKey.MASTER_HOSTNAME, parts[0]);
        if (parts.length > 1) {
            conf.set(PropertyKey.MASTER_RPC_PORT, parts[1]);
        }

        // Uses default port from alluxio properties, when not specified in catalog config, to ensure port is not null.
        InetSocketAddress masterAddr = InetSocketAddress.createUnresolved(parts[0], Integer.parseInt(conf.get(PropertyKey.MASTER_RPC_PORT)));
        ClientContext context = ClientContext.create(conf);

        try {
            context.loadConf(masterAddr, true, false);
            log.info("Successfully loaded config from Alluxio cluster");
        }
        catch (AlluxioStatusException e) {
            String errorMessage = "Error while loading cluster config from Alluxio master:" + addr;
            log.error(e, "%s", errorMessage);
        }

        MasterClientContext mContext = MasterClientContext
                .newBuilder(context).build();
        return new RetryHandlingTableMasterClient(mContext);
    }
}
