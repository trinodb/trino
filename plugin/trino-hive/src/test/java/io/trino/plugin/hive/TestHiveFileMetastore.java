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
import io.trino.plugin.hive.metastore.file.FileHiveMetastore;
import io.trino.plugin.hive.metastore.file.FileHiveMetastoreConfig;
import org.junit.jupiter.api.Test;

import java.io.File;

import static io.trino.plugin.hive.HiveTestUtils.HDFS_FILE_SYSTEM_FACTORY;
import static org.junit.jupiter.api.Assumptions.abort;

public class TestHiveFileMetastore
        extends AbstractTestHiveLocal
{
    @Override
    protected HiveMetastore createMetastore(File tempDir)
    {
        File baseDir = new File(tempDir, "metastore");
        return new FileHiveMetastore(
                new NodeVersion("test_version"),
                HDFS_FILE_SYSTEM_FACTORY,
                true,
                new FileHiveMetastoreConfig()
                        .setCatalogDirectory(baseDir.toURI().toString())
                        .setMetastoreUser("test"));
    }

    @Test
    @Override
    public void testMismatchSchemaTable()
    {
        // FileHiveMetastore only supports replaceTable() for views
    }

    @Test
    @Override
    public void testPartitionSchemaMismatch()
    {
        // test expects an exception to be thrown
        abort("FileHiveMetastore only supports replaceTable() for views");
    }

    @Test
    @Override
    public void testBucketedTableEvolution()
    {
        // FileHiveMetastore only supports replaceTable() for views
    }

    @Test
    @Override
    public void testBucketedTableEvolutionWithDifferentReadBucketCount()
    {
        // FileHiveMetastore has various incompatibilities
    }

    @Test
    @Override
    public void testTransactionDeleteInsert()
    {
        // FileHiveMetastore has various incompatibilities
    }

    @Test
    @Override
    public void testInsertOverwriteUnpartitioned()
    {
        // FileHiveMetastore has various incompatibilities
    }
}
