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
package io.trino.plugin.iceberg;

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import io.trino.plugin.hive.HiveCompressionCodec;
import org.apache.iceberg.FileFormat;

import javax.validation.constraints.NotNull;

import java.util.Optional;

import static io.trino.plugin.hive.HiveCompressionCodec.GZIP;
import static io.trino.plugin.iceberg.IcebergFileFormat.ORC;

public class IcebergConfig
{
    private IcebergFileFormat fileFormat = ORC;
    private HiveCompressionCodec compressionCodec = GZIP;
    // TODO: Change this to Optional.empty() along with other such Trino migration changes
    private Optional<String> alternateViewCodecTag = Optional.of("Presto");

    @NotNull
    public FileFormat getFileFormat()
    {
        return FileFormat.valueOf(fileFormat.name());
    }

    @Config("iceberg.file-format")
    public IcebergConfig setFileFormat(IcebergFileFormat fileFormat)
    {
        this.fileFormat = fileFormat;
        return this;
    }

    @NotNull
    public HiveCompressionCodec getCompressionCodec()
    {
        return compressionCodec;
    }

    @Config("iceberg.compression-codec")
    public IcebergConfig setCompressionCodec(HiveCompressionCodec compressionCodec)
    {
        this.compressionCodec = compressionCodec;
        return this;
    }

    public Optional<String> getAlternateViewCodecTag()
    {
        return alternateViewCodecTag;
    }

    @Deprecated
    @Config("iceberg.alternate-view-codec-tag")
    @ConfigDescription("Enable reading views encoded with a tag other than Trino")
    public IcebergConfig setAlternateViewCodecTag(String alternateViewCodecTag)
    {
        this.alternateViewCodecTag = Optional.ofNullable(alternateViewCodecTag);
        return this;
    }
}
