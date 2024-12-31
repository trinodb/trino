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
package io.trino.plugin.paimon.catalog;

import com.google.inject.Inject;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.hdfs.ConfigurationUtils;
import io.trino.hdfs.HdfsConfig;
import io.trino.hdfs.HdfsConfigurationInitializer;
import io.trino.spi.security.ConnectorIdentity;
import org.apache.hadoop.conf.Configuration;
import org.apache.paimon.options.Options;

public class PaimonTrinoCatalogFactory
{
    private final Options options;

    private final Configuration configuration;

    private final TrinoFileSystemFactory trinoFileSystemFactory;

    @Inject
    public PaimonTrinoCatalogFactory(
            Options options,
            HdfsConfigurationInitializer hdfsConfigurationInitializer,
            HdfsConfig hdfsConfig,
            TrinoFileSystemFactory trinoFileSystemFactory)
    {
        Configuration configuration = null;
        if (!hdfsConfig.getResourceConfigFiles().isEmpty()) {
            configuration = ConfigurationUtils.getInitialConfiguration();
            hdfsConfigurationInitializer.initializeConfiguration(configuration);
        }

        this.options = options;
        this.configuration = configuration;
        this.trinoFileSystemFactory = trinoFileSystemFactory;
    }

    public PaimonTrinoCatalog create(ConnectorIdentity identity)
    {
        return new PaimonTrinoCatalog(options, configuration, trinoFileSystemFactory, identity);
    }
}
