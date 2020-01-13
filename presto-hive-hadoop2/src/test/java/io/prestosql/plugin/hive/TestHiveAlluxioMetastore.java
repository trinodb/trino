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
package io.prestosql.plugin.hive;

import alluxio.client.table.TableMasterClient;
import io.prestosql.plugin.hive.metastore.HiveMetastore;
import io.prestosql.plugin.hive.metastore.alluxio.AlluxioHiveMetastore;
import io.prestosql.plugin.hive.metastore.alluxio.AlluxioHiveMetastoreConfig;
import io.prestosql.plugin.hive.metastore.alluxio.AlluxioMetastoreModule;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Parameters;

public class TestHiveAlluxioMetastore
        extends AbstractTestHive
{
    @Parameters({
            "hive.hadoop2.alluxio.host",
            "hive.hadoop2.alluxio.port"
    })
    @BeforeClass
    public void setup(String host, String port)
    {
        // this.alluxioAddress = host + ":" + port;
        HiveConfig hiveConfig = new HiveConfig()
                .setTimeZone("America/Los_Angeles");
        System.out.println(host);
        System.out.println(port);
        setup("test", hiveConfig, createMetastore());
    }

    protected HiveMetastore createMetastore()
    {
        AlluxioHiveMetastoreConfig alluxioConfig = new AlluxioHiveMetastoreConfig();
        alluxioConfig.setMasterAddress("localhost:19999");
        TableMasterClient client = AlluxioMetastoreModule.createCatalogMasterClient(alluxioConfig);
        return new AlluxioHiveMetastore(client);
    }
}
