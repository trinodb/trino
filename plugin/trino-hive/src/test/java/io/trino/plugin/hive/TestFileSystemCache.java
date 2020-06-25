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

import com.google.common.collect.ImmutableSet;
import io.trino.plugin.hive.authentication.ImpersonatingHdfsAuthentication;
import io.trino.plugin.hive.authentication.SimpleHadoopAuthentication;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.testng.annotations.Test;

import java.io.IOException;

import static org.testng.Assert.assertNotSame;
import static org.testng.Assert.assertSame;

public class TestFileSystemCache
{
    @Test
    public void testFileSystemCache()
            throws IOException
    {
        HdfsEnvironment environment = new HdfsEnvironment(
                new HiveHdfsConfiguration(new HdfsConfigurationInitializer(new HdfsConfig()), ImmutableSet.of()),
                new HdfsConfig(),
                new ImpersonatingHdfsAuthentication(new SimpleHadoopAuthentication()));
        FileSystem fs1 = getFileSystem(environment, "user");
        FileSystem fs2 = getFileSystem(environment, "user");
        assertSame(fs1, fs2);

        FileSystem fs3 = getFileSystem(environment, "other_user");
        assertNotSame(fs1, fs3);

        FileSystem fs4 = getFileSystem(environment, "other_user");
        assertSame(fs3, fs4);

        FileSystem.closeAll();

        FileSystem fs5 = getFileSystem(environment, "user");
        assertNotSame(fs5, fs1);
    }

    private FileSystem getFileSystem(HdfsEnvironment environment, String user)
            throws IOException
    {
        return environment.getFileSystem(user, new Path("/"), new Configuration(false));
    }
}
