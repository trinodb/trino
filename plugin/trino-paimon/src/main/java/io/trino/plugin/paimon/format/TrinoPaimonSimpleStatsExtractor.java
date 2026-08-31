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
package io.trino.plugin.paimon.format;

import org.apache.paimon.format.SimpleColStats;
import org.apache.paimon.format.SimpleStatsExtractor;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.statistics.SimpleColStatsCollector;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.Pair;

import javax.annotation.Nullable;

import java.io.IOException;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

class TrinoPaimonSimpleStatsExtractor
        implements SimpleStatsExtractor
{
    private final SimpleColStatsCollector[] statsCollectors;

    TrinoPaimonSimpleStatsExtractor(RowType rowType, SimpleColStatsCollector.Factory[] statsCollectors)
    {
        requireNonNull(rowType, "rowType is null");
        requireNonNull(statsCollectors, "statsCollectors is null");
        checkArgument(
                rowType.getFieldCount() == statsCollectors.length,
                "field count %s does not match stats collector count %s",
                rowType.getFieldCount(),
                statsCollectors.length);
        this.statsCollectors = SimpleColStatsCollector.create(statsCollectors);
    }

    @Override
    public SimpleColStats[] extract(FileIO fileIO, Path path, long length)
    {
        throw new UnsupportedOperationException(
                "Trino Paimon file format can extract column stats only from writer metadata");
    }

    @Override
    public SimpleColStats[] extract(FileIO fileIO, Path path, long length, @Nullable Object writerMetadata)
            throws IOException
    {
        if (writerMetadata instanceof TrinoPaimonFormatWriter.WriterMetadata metadata) {
            SimpleColStats[] fullStats = metadata.simpleColStats();
            if (fullStats.length != statsCollectors.length) {
                throw new IOException(
                        "Trino Paimon writer metadata column stats count " + fullStats.length
                                + " does not match stats collector count " + statsCollectors.length);
            }
            SimpleColStats[] result = new SimpleColStats[fullStats.length];
            for (int i = 0; i < result.length; i++) {
                result[i] = statsCollectors[i].convert(fullStats[i]);
            }
            return result;
        }
        throw new UnsupportedOperationException(
                "Trino Paimon file format can extract column stats only from writer metadata");
    }

    @Override
    public Pair<SimpleColStats[], FileInfo> extractWithFileInfo(FileIO fileIO, Path path, long length)
    {
        throw new UnsupportedOperationException(
                "Trino Paimon file format can extract column stats only from writer metadata");
    }
}
