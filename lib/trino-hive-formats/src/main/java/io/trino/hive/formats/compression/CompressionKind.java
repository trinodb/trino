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
package io.trino.hive.formats.compression;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.compress.bzip2.BZip2HadoopStreams;
import io.airlift.compress.deflate.JdkDeflateHadoopStreams;
import io.airlift.compress.gzip.JdkGzipHadoopStreams;
import io.airlift.compress.hadoop.HadoopStreams;
import io.airlift.compress.lz4.Lz4HadoopStreams;
import io.airlift.compress.lzo.LzoHadoopStreams;
import io.airlift.compress.lzo.LzopHadoopStreams;
import io.airlift.compress.snappy.SnappyHadoopStreams;
import io.airlift.compress.zstd.ZstdHadoopStreams;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;

import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static java.util.Objects.requireNonNull;

public enum CompressionKind
{
    // These are in preference order
    ZSTD(new ZstdHadoopStreams()),
    LZ4(new Lz4HadoopStreams()),
    SNAPPY(new SnappyHadoopStreams()),
    GZIP(new JdkGzipHadoopStreams()),
    DEFLATE(new JdkDeflateHadoopStreams()),
    // These algorithms are only supported for backwards compatibility, and should be avoided at all costs
    BZIP2(new BZip2HadoopStreams()),
    LZO(new LzoHadoopStreams()),
    LZOP(new LzopHadoopStreams());

    private final HadoopStreams hadoopStreams;
    private final List<String> hadoopClassNames;
    private final String fileExtension;

    CompressionKind(HadoopStreams hadoopStreams)
    {
        this.hadoopStreams = requireNonNull(hadoopStreams, "hadoopStreams is null");
        this.hadoopClassNames = ImmutableList.copyOf(hadoopStreams.getHadoopCodecName());
        this.fileExtension = hadoopStreams.getDefaultFileExtension();
    }

    public String getHadoopClassName()
    {
        return hadoopClassNames.get(0);
    }

    public String getFileExtension()
    {
        return fileExtension;
    }

    public Codec createCodec()
    {
        return new Codec(hadoopStreams);
    }

    private static final Map<String, CompressionKind> CODECS_BY_HADOOP_CLASS_NAME;

    static {
        ImmutableMap.Builder<String, CompressionKind> builder = ImmutableMap.builder();
        for (CompressionKind codec : values()) {
            for (String hadoopClassNames : codec.hadoopClassNames) {
                builder.put(hadoopClassNames, codec);
            }
        }
        CODECS_BY_HADOOP_CLASS_NAME = builder.buildOrThrow();
    }

    public static CompressionKind fromHadoopClassName(String hadoopClassName)
    {
        return Optional.ofNullable(CODECS_BY_HADOOP_CLASS_NAME.get(hadoopClassName))
                .orElseThrow(() -> new IllegalArgumentException("Unknown codec: " + hadoopClassName));
    }

    private static final Map<String, CompressionKind> CODECS_BY_FILE_EXTENSION = Arrays.stream(values())
            .filter(codec -> codec.fileExtension != null)
            .collect(toImmutableMap(codec -> codec.fileExtension, Function.identity()));

    public static Optional<CompressionKind> forFile(String fileName)
    {
        int position = fileName.lastIndexOf('.');
        if (position < 0) {
            return Optional.empty();
        }
        return Optional.ofNullable(CODECS_BY_FILE_EXTENSION.get(fileName.substring(position)));
    }
}
