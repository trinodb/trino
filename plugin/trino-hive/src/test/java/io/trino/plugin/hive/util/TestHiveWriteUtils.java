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
package io.trino.plugin.hive.util;

import io.trino.hdfs.HdfsContext;
import org.apache.hadoop.fs.Path;
import org.testng.annotations.Test;

import static io.trino.plugin.hive.HiveTestUtils.HDFS_ENVIRONMENT;
import static io.trino.plugin.hive.util.HiveWriteUtils.isS3FileSystem;
import static io.trino.plugin.hive.util.HiveWriteUtils.isViewFileSystem;
import static io.trino.testing.TestingConnectorSession.SESSION;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class TestHiveWriteUtils
{
    private static final HdfsContext CONTEXT = new HdfsContext(SESSION);
    private static final String RANDOM_SUFFIX = randomNameSuffix();

    @Test
    public void testIsS3FileSystem()
    {
        assertTrue(isS3FileSystem(CONTEXT, HDFS_ENVIRONMENT, new Path("s3://test-bucket-%s/test-folder".formatted(RANDOM_SUFFIX))));
        assertFalse(isS3FileSystem(CONTEXT, HDFS_ENVIRONMENT, new Path("/test-dir-%s/test-folder".formatted(RANDOM_SUFFIX))));
    }

    @Test
    public void testIsViewFileSystem()
    {
        Path viewfsPath = new Path("viewfs://ns-default-%s/test-folder".formatted(RANDOM_SUFFIX));
        Path nonViewfsPath = new Path("hdfs://localhost/test-dir/test-folder");

        // ViewFS check requires the mount point config
        HDFS_ENVIRONMENT.getConfiguration(CONTEXT, viewfsPath).set("fs.viewfs.mounttable.ns-default-%s.link./test-folder".formatted(RANDOM_SUFFIX), "hdfs://localhost/app");

        assertTrue(isViewFileSystem(CONTEXT, HDFS_ENVIRONMENT, viewfsPath));
        assertFalse(isViewFileSystem(CONTEXT, HDFS_ENVIRONMENT, nonViewfsPath));
    }
}
