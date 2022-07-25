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
package io.trino.plugin.hive.orc;

import io.trino.orc.OrcReaderOptions;
import io.trino.plugin.hive.FileFormatDataSourceStats;
import io.trino.plugin.hive.HdfsEnvironment;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.security.ConnectorIdentity;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import java.util.Optional;

import static io.trino.plugin.hive.orc.OrcDeleteDeltaPageSource.createOrcDeleteDeltaPageSource;
import static java.util.Objects.requireNonNull;

public class OrcDeleteDeltaPageSourceFactory
{
    private final OrcReaderOptions options;
    private final ConnectorIdentity identity;
    private final Configuration configuration;
    private final HdfsEnvironment hdfsEnvironment;
    private final FileFormatDataSourceStats stats;

    public OrcDeleteDeltaPageSourceFactory(
            OrcReaderOptions options,
            ConnectorIdentity identity,
            Configuration configuration,
            HdfsEnvironment hdfsEnvironment,
            FileFormatDataSourceStats stats)
    {
        this.options = requireNonNull(options, "options is null");
        this.identity = requireNonNull(identity, "identity is null");
        this.configuration = requireNonNull(configuration, "configuration is null");
        this.hdfsEnvironment = requireNonNull(hdfsEnvironment, "hdfsEnvironment is null");
        this.stats = requireNonNull(stats, "stats is null");
    }

    public Optional<ConnectorPageSource> createPageSource(Path path, long fileSize)
    {
        return createOrcDeleteDeltaPageSource(
                path,
                fileSize,
                options,
                identity,
                configuration,
                hdfsEnvironment,
                stats);
    }
}
