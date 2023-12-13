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

import io.trino.plugin.hive.metastore.HiveMetastore;
import io.trino.plugin.hive.metastore.Table;
import io.trino.plugin.hive.metastore.thrift.BridgingHiveMetastore;
import io.trino.plugin.hive.metastore.thrift.InMemoryThriftMetastore;
import io.trino.plugin.hive.metastore.thrift.ThriftMetastoreConfig;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.net.URI;

import static java.nio.file.Files.createDirectories;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assumptions.abort;

public class TestHiveInMemoryMetastore
        extends AbstractTestHiveLocal
{
    @Override
    protected HiveMetastore createMetastore(File tempDir)
    {
        File baseDir = new File(tempDir, "metastore");
        ThriftMetastoreConfig metastoreConfig = new ThriftMetastoreConfig();
        InMemoryThriftMetastore hiveMetastore = new InMemoryThriftMetastore(baseDir, metastoreConfig);
        return new BridgingHiveMetastore(hiveMetastore);
    }

    @Override
    protected void createTestTable(Table table)
            throws Exception
    {
        createDirectories(new File(URI.create(table.getStorage().getLocation())).toPath());
        super.createTestTable(table);
    }

    @Test
    @Override
    public void testMetadataDelete()
    {
        // InMemoryHiveMetastore ignores "removeData" flag in dropPartition
    }

    @Test
    @Override
    public void testTransactionDeleteInsert()
    {
        // InMemoryHiveMetastore does not check whether partition exist in createPartition and dropPartition
    }

    @Test
    @Override
    public void testHideDeltaLakeTables()
    {
        abort("not supported");
    }

    @Test
    @Override
    public void testDisallowQueryingOfIcebergTables()
    {
        abort("not supported");
    }

    @Test
    @Override
    public void testDataColumnProperties()
    {
        // Column properties are currently not supported in ThriftHiveMetastore
        assertThatThrownBy(super::testDataColumnProperties)
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Persisting column properties is not supported: Column{name=id, type=bigint}");
    }

    @Test
    @Override
    public void testPartitionColumnProperties()
    {
        // Column properties are currently not supported in ThriftHiveMetastore
        assertThatThrownBy(super::testPartitionColumnProperties)
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Persisting column properties is not supported: Column{name=part_key, type=varchar(256)}");
    }

    @Test
    @Override
    public void testPartitionSchemaMismatch()
    {
        abort("not supported");
    }
}
