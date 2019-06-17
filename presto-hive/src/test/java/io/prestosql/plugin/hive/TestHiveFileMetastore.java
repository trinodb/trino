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

import com.google.common.collect.ImmutableSet;
import io.prestosql.plugin.hive.authentication.NoHdfsAuthentication;
import io.prestosql.plugin.hive.metastore.HiveMetastore;
import io.prestosql.plugin.hive.metastore.file.FileHiveMetastore;
import org.testng.SkipException;

import java.io.File;

public class TestHiveFileMetastore
        extends AbstractTestHiveLocal
{
    @Override
    protected HiveMetastore createMetastore(File tempDir)
    {
        File baseDir = new File(tempDir, "metastore");
        HiveConfig hiveConfig = new HiveConfig();
        HdfsConfigurationInitializer updator = new HdfsConfigurationInitializer(hiveConfig);
        HdfsConfiguration hdfsConfiguration = new HiveHdfsConfiguration(updator, ImmutableSet.of());
        HdfsEnvironment hdfsEnvironment = new HdfsEnvironment(hdfsConfiguration, hiveConfig, new NoHdfsAuthentication());
        return new FileHiveMetastore(hdfsEnvironment, baseDir.toURI().toString(), "test");
    }

    @Override
    public void testMismatchSchemaTable()
    {
        // FileHiveMetastore only supports replaceTable() for views
    }

    @Override
    public void testPartitionSchemaMismatch()
    {
        // test expects an exception to be thrown
        throw new SkipException("FileHiveMetastore only supports replaceTable() for views");
    }

    @Override
    public void testBucketedTableEvolution()
    {
        // FileHiveMetastore only supports replaceTable() for views
    }

    @Override
    public void testTransactionDeleteInsert()
    {
        // FileHiveMetastore has various incompatibilities
    }

    @Override
    public void testInsertOverwriteUnpartitioned()
    {
        // FileHiveMetastore has various incompatibilities
    }
}
