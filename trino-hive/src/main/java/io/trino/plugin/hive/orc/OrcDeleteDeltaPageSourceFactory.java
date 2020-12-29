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
package io.prestosql.plugin.hive.orc;

import io.prestosql.orc.OrcReaderOptions;
import io.prestosql.plugin.hive.FileFormatDataSourceStats;
import io.prestosql.plugin.hive.HdfsEnvironment;
import io.prestosql.spi.connector.ConnectorPageSource;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import java.util.Optional;

import static io.prestosql.plugin.hive.orc.OrcDeleteDeltaPageSource.createOrcDeleteDeltaPageSource;
import static java.util.Objects.requireNonNull;

public class OrcDeleteDeltaPageSourceFactory
{
    private final OrcReaderOptions options;
    private final String sessionUser;
    private final Configuration configuration;
    private final HdfsEnvironment hdfsEnvironment;
    private final FileFormatDataSourceStats stats;

    public OrcDeleteDeltaPageSourceFactory(
            OrcReaderOptions options,
            String sessionUser,
            Configuration configuration,
            HdfsEnvironment hdfsEnvironment,
            FileFormatDataSourceStats stats)
    {
        this.options = requireNonNull(options, "options is null");
        this.sessionUser = requireNonNull(sessionUser, "sessionUser is null");
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
                sessionUser,
                configuration,
                hdfsEnvironment,
                stats);
    }
}
