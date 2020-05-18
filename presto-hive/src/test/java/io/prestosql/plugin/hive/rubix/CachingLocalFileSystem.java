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
package io.prestosql.plugin.hive.rubix;

import com.qubole.rubix.core.CachingFileSystem;
import com.qubole.rubix.core.ClusterManagerInitilizationException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.LocalFileSystem;

import java.io.IOException;
import java.net.URI;

import static com.qubole.rubix.spi.ClusterType.PRESTOSQL_CLUSTER_MANAGER;

public class CachingLocalFileSystem
        extends CachingFileSystem<LocalFileSystem>
{
    private static final String SCHEME = "file";

    @Override
    public void initialize(URI uri, Configuration conf)
            throws IOException
    {
        try {
            initializeClusterManager(conf, PRESTOSQL_CLUSTER_MANAGER);
            super.initialize(uri, conf);
        }
        catch (ClusterManagerInitilizationException exception) {
            throw new IOException(exception);
        }
    }

    @Override
    public String getScheme()
    {
        return SCHEME;
    }
}
