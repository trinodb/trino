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

import io.trino.filesystem.TrinoInputFile;
import io.trino.orc.FileStatusInfo;
import io.trino.orc.OrcFileMetadataProvider;
import io.trino.orc.OrcReaderOptions;
import io.trino.plugin.hive.FileFormatDataSourceStats;
import io.trino.spi.connector.ConnectorPageSource;

import java.util.Optional;

import static io.trino.plugin.hive.orc.OrcDeleteDeltaPageSource.createOrcDeleteDeltaPageSource;
import static java.util.Objects.requireNonNull;

public class OrcDeleteDeltaPageSourceFactory
{
    private final Optional<FileStatusInfo> fileStatusInfo;
    private final OrcReaderOptions options;
    private final FileFormatDataSourceStats stats;
    private final OrcFileMetadataProvider orcFileMetadataProvider;

    public OrcDeleteDeltaPageSourceFactory(
            Optional<FileStatusInfo> fileStatusInfo,
            OrcReaderOptions options,
            FileFormatDataSourceStats stats,
            OrcFileMetadataProvider orcFileMetadataProvider)
    {
        this.fileStatusInfo = requireNonNull(fileStatusInfo, "fileStatusInfo is null");
        this.options = requireNonNull(options, "options is null");
        this.stats = requireNonNull(stats, "stats is null");
        this.orcFileMetadataProvider = requireNonNull(orcFileMetadataProvider, "orcFileMetadataProvider is null");
    }

    public Optional<ConnectorPageSource> createPageSource(TrinoInputFile inputFile)
    {
        return createOrcDeleteDeltaPageSource(
                fileStatusInfo,
                inputFile,
                options,
                stats,
                orcFileMetadataProvider);
    }
}
