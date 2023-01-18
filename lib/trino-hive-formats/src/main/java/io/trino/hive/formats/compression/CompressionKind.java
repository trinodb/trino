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
import io.airlift.compress.gzip.JdkGzipCodec;
import io.airlift.compress.lz4.Lz4Codec;
import io.airlift.compress.lzo.LzoCodec;
import io.airlift.compress.snappy.SnappyCodec;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;

import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static io.trino.hadoop.ConfigurationInstantiator.newEmptyConfiguration;
import static java.util.Objects.requireNonNull;

public enum CompressionKind
{
    SNAPPY(".snappy", "org.apache.hadoop.io.compress.SnappyCodec") {
        @Override
        public Codec createCodec()
        {
            return new AircompressorCodec(new SnappyCodec());
        }
    },
    LZO(".lzo_deflate", "org.apache.hadoop.io.compress.LzoCodec", "com.hadoop.compression.lzo.LzoCodec") {
        @Override
        public Codec createCodec()
        {
            return new AircompressorCodec(new LzoCodec());
        }
    },
    LZ4(".lz4", "org.apache.hadoop.io.compress.Lz4Codec") {
        @Override
        public Codec createCodec()
        {
            return new AircompressorCodec(new Lz4Codec());
        }
    },
    GZIP(".gz", "org.apache.hadoop.io.compress.GzipCodec") {
        @Override
        public Codec createCodec()
        {
            return new AircompressorCodec(new JdkGzipCodec());
        }
    },
    ZSTD(".zst", "org.apache.hadoop.io.compress.ZStandardCodec") {
        @Override
        public Codec createCodec()
        {
            org.apache.hadoop.io.compress.ZStandardCodec codec = new org.apache.hadoop.io.compress.ZStandardCodec();
            codec.setConf(newEmptyConfiguration());
            return new HadoopCodec(codec);
        }
    },
    BZIP2(".bz2", "org.apache.hadoop.io.compress.BZip2Codec") {
        @Override
        public Codec createCodec()
        {
            org.apache.hadoop.io.compress.BZip2Codec codec = new org.apache.hadoop.io.compress.BZip2Codec();
            codec.setConf(newEmptyConfiguration());
            return new HadoopCodec(codec);
        }
    };

    private final List<String> hadoopClassNames;
    private final String fileExtension;

    CompressionKind(String fileExtension, String... hadoopClassNames)
    {
        this.hadoopClassNames = ImmutableList.copyOf(hadoopClassNames);
        this.fileExtension = requireNonNull(fileExtension, "fileExtension is null");
    }

    public String getHadoopClassName()
    {
        return hadoopClassNames.get(0);
    }

    public String getFileExtension()
    {
        return fileExtension;
    }

    public abstract Codec createCodec();

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

    public static Codec createCodecFromHadoopClassName(String hadoopClassName)
    {
        return Optional.ofNullable(CODECS_BY_HADOOP_CLASS_NAME.get(hadoopClassName))
                .orElseThrow(() -> new IllegalArgumentException("Unknown codec: " + hadoopClassName))
                .createCodec();
    }

    private static final Map<String, CompressionKind> CODECS_BY_FILE_EXTENSION = Arrays.stream(values())
            .filter(codec -> codec.fileExtension != null)
            .collect(toImmutableMap(codec -> codec.fileExtension, Function.identity()));

    public static Optional<Codec> createCodecFromExtension(String extension)
    {
        return Optional.ofNullable(CODECS_BY_FILE_EXTENSION.get(extension))
                .map(CompressionKind::createCodec);
    }
}
