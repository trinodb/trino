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

import io.trino.orc.OrcDataSource;
import io.trino.orc.OrcDataSourceId;
import io.trino.orc.OrcReader;
import io.trino.orc.OrcReaderOptions;
import io.trino.plugin.hive.FileFormatDataSourceStats;
import io.trino.plugin.hive.HdfsEnvironment;
import io.trino.spi.TrinoException;
import io.trino.spi.security.ConnectorIdentity;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.util.Collection;

import static io.trino.orc.OrcReader.createOrcReader;
import static io.trino.plugin.hive.AcidInfo.OriginalFileInfo;
import static io.trino.plugin.hive.HiveErrorCode.HIVE_CANNOT_OPEN_SPLIT;

public final class OriginalFilesUtils
{
    private OriginalFilesUtils() {}

    /**
     * Returns total number of rows present before the given original file in the same bucket.
     * example: if bucket-1 has original files
     * 000000_0 -> X0 rows
     * 000000_0_copy1 -> X1 rows
     * 000000_0_copy2 -> X2 rows
     * <p>
     * for 000000_0_copy2, it returns (X0+X1)
     */
    public static long getPrecedingRowCount(
            Collection<OriginalFileInfo> originalFileInfos,
            Path splitPath,
            HdfsEnvironment hdfsEnvironment,
            ConnectorIdentity identity,
            OrcReaderOptions options,
            Configuration configuration,
            FileFormatDataSourceStats stats)
    {
        long rowCount = 0;
        for (OriginalFileInfo originalFileInfo : originalFileInfos) {
            Path path = new Path(splitPath.getParent() + "/" + originalFileInfo.getName());
            if (path.compareTo(splitPath) < 0) {
                rowCount += getRowsInFile(path, hdfsEnvironment, identity, options, configuration, stats, originalFileInfo.getFileSize());
            }
        }

        return rowCount;
    }

    /**
     * Returns number of rows present in the file, based on the ORC footer.
     */
    private static Long getRowsInFile(
            Path splitPath,
            HdfsEnvironment hdfsEnvironment,
            ConnectorIdentity identity,
            OrcReaderOptions options,
            Configuration configuration,
            FileFormatDataSourceStats stats,
            long fileSize)
    {
        try {
            FileSystem fileSystem = hdfsEnvironment.getFileSystem(identity, splitPath, configuration);
            FSDataInputStream inputStream = hdfsEnvironment.doAs(identity, () -> fileSystem.open(splitPath));
            try (OrcDataSource orcDataSource = new HdfsOrcDataSource(
                    new OrcDataSourceId(splitPath.toString()),
                    fileSize,
                    options,
                    inputStream,
                    stats)) {
                OrcReader reader = createOrcReader(orcDataSource, options)
                        .orElseThrow(() -> new TrinoException(HIVE_CANNOT_OPEN_SPLIT, "Could not read ORC footer from empty file: " + splitPath));
                return reader.getFooter().getNumberOfRows();
            }
        }
        catch (TrinoException e) {
            throw e;
        }
        catch (Exception e) {
            throw new TrinoException(HIVE_CANNOT_OPEN_SPLIT, "Could not read ORC footer from file: " + splitPath, e);
        }
    }
}
