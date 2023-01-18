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
package io.trino.plugin.hive.util;

import io.trino.hive.orc.OrcConf;
import io.trino.plugin.hive.HiveCompressionCodec;
import org.apache.avro.mapred.AvroJob;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.parquet.hadoop.ParquetOutputFormat;

import static com.google.common.base.Preconditions.checkArgument;
import static org.apache.hadoop.hive.conf.HiveConf.ConfVars.COMPRESSRESULT;
import static org.apache.hadoop.io.SequenceFile.CompressionType.BLOCK;

public final class CompressionConfigUtil
{
    private static final String COMPRESSION_CONFIGURED_MARKER = "trino.compression.configured";

    private CompressionConfigUtil() {}

    public static void configureCompression(Configuration config, HiveCompressionCodec compressionCodec)
    {
        boolean compression = compressionCodec != HiveCompressionCodec.NONE;
        config.setBoolean(COMPRESSRESULT.varname, compression);
        config.setBoolean("mapred.output.compress", compression);
        config.setBoolean(FileOutputFormat.COMPRESS, compression);

        // For ORC
        OrcConf.COMPRESS.setString(config, compressionCodec.getOrcCompressionKind().name());

        // For RCFile and Text
        if (compressionCodec.getHiveCompressionKind().isPresent()) {
            config.set("mapred.output.compression.codec", compressionCodec.getHiveCompressionKind().get().getHadoopClassName());
            config.set(FileOutputFormat.COMPRESS_CODEC, compressionCodec.getHiveCompressionKind().get().getHadoopClassName());
        }
        else {
            config.unset("mapred.output.compression.codec");
            config.unset(FileOutputFormat.COMPRESS_CODEC);
        }

        // For Parquet
        config.set(ParquetOutputFormat.COMPRESSION, compressionCodec.getParquetCompressionCodec().name());

        // For Avro
        compressionCodec.getAvroCompressionCodec().ifPresent(codec -> config.set(AvroJob.OUTPUT_CODEC, codec));

        // For SequenceFile
        config.set(FileOutputFormat.COMPRESS_TYPE, BLOCK.toString());

        config.set(COMPRESSION_CONFIGURED_MARKER, "true");
    }

    public static void assertCompressionConfigured(Configuration config)
    {
        String markerValue = config.get(COMPRESSION_CONFIGURED_MARKER);
        checkArgument("true".equals(markerValue), "Compression should have been configured");
    }
}
