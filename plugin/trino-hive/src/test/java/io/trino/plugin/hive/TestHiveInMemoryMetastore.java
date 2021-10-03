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
package io.trino.plugin.hive;

import io.trino.plugin.hive.authentication.HiveIdentity;
import io.trino.plugin.hive.metastore.HiveMetastore;
import io.trino.plugin.hive.metastore.thrift.BridgingHiveMetastore;
import io.trino.plugin.hive.metastore.thrift.InMemoryThriftMetastore;
import io.trino.plugin.hive.metastore.thrift.ThriftMetastoreConfig;
import org.testng.SkipException;
import org.testng.annotations.Test;

import java.io.File;

// staging directory is shared mutable state
@Test(singleThreaded = true)
public class TestHiveInMemoryMetastore
        extends AbstractTestHiveLocal
{
    @Override
    protected HiveMetastore createMetastore(File tempDir, HiveIdentity identity)
    {
        File baseDir = new File(tempDir, "metastore");
        ThriftMetastoreConfig metastoreConfig = new ThriftMetastoreConfig();
        InMemoryThriftMetastore hiveMetastore = new InMemoryThriftMetastore(baseDir, metastoreConfig);
        return new BridgingHiveMetastore(hiveMetastore, identity);
    }

    @Test
    public void forceTestNgToRespectSingleThreaded()
    {
        // TODO: Remove after updating TestNG to 7.4.0+ (https://github.com/trinodb/trino/issues/8571)
        // TestNG doesn't enforce @Test(singleThreaded = true) when tests are defined in base class. According to
        // https://github.com/cbeust/testng/issues/2361#issuecomment-688393166 a workaround it to add a dummy test to the leaf test class.
    }

    @Override
    public void testMetadataDelete()
    {
        // InMemoryHiveMetastore ignores "removeData" flag in dropPartition
    }

    @Override
    public void testTransactionDeleteInsert()
    {
        // InMemoryHiveMetastore does not check whether partition exist in createPartition and dropPartition
    }

    @Override
    public void testHideDeltaLakeTables()
    {
        throw new SkipException("not supported");
    }

    @Override
    public void testDisallowQueryingOfIcebergTables()
    {
        throw new SkipException("not supported");
    }
}
