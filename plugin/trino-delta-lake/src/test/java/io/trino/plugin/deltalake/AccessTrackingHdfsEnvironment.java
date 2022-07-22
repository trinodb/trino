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
package io.trino.plugin.deltalake;

import com.google.common.collect.ImmutableMap;
import io.trino.plugin.hive.HdfsConfig;
import io.trino.plugin.hive.HdfsConfiguration;
import io.trino.plugin.hive.HdfsEnvironment;
import io.trino.plugin.hive.authentication.HdfsAuthentication;
import io.trino.spi.security.ConnectorIdentity;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class AccessTrackingHdfsEnvironment
        extends HdfsEnvironment
{
    private Map<String, Integer> fileSystemPathNames = new HashMap<>();

    public AccessTrackingHdfsEnvironment(HdfsConfiguration hdfsConfiguration, HdfsConfig config, HdfsAuthentication hdfsAuthentication)
    {
        super(hdfsConfiguration, config, hdfsAuthentication);
    }

    @Override
    public FileSystem getFileSystem(ConnectorIdentity identity, Path path, Configuration configuration)
            throws IOException
    {
        incrementAccessedPathNamesCount(path.getName());
        return super.getFileSystem(identity, path, configuration);
    }

    public Map<String, Integer> getAccessedPathNames()
    {
        return ImmutableMap.copyOf(fileSystemPathNames);
    }

    private void incrementAccessedPathNamesCount(String fileName)
    {
        fileSystemPathNames.put(fileName, fileSystemPathNames.getOrDefault(fileName, 0) + 1);
    }
}
