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

import com.google.common.base.Splitter;
import io.airlift.compress.lzo.LzoCodec;
import io.airlift.compress.lzo.LzopCodec;
import io.trino.plugin.hive.HiveStorageFormat;
import io.trino.spi.TrinoException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.util.ReflectionUtils;

import java.util.List;
import java.util.Properties;

import static com.google.common.collect.Lists.newArrayList;
import static io.trino.hdfs.ConfigurationUtils.toJobConf;
import static io.trino.hive.thrift.metastore.hive_metastoreConstants.FILE_INPUT_FORMAT;
import static io.trino.plugin.hive.HiveErrorCode.HIVE_INVALID_METADATA;
import static io.trino.plugin.hive.HiveErrorCode.HIVE_UNSUPPORTED_FORMAT;
import static io.trino.plugin.hive.HiveStorageFormat.TEXTFILE;
import static io.trino.plugin.hive.util.HiveClassNames.MAPRED_PARQUET_INPUT_FORMAT_CLASS;
import static io.trino.plugin.hive.util.HiveClassNames.SYMLINK_TEXT_INPUT_FORMAT_CLASS;

public final class HiveReaderUtil
{
    private HiveReaderUtil() {}

    private static void configureCompressionCodecs(JobConf jobConf)
    {
        // add Airlift LZO and LZOP to head of codecs list to not override existing entries
        List<String> codecs = newArrayList(Splitter.on(",").trimResults().omitEmptyStrings().split(jobConf.get("io.compression.codecs", "")));
        if (!codecs.contains(LzoCodec.class.getName())) {
            codecs.add(0, LzoCodec.class.getName());
        }
        if (!codecs.contains(LzopCodec.class.getName())) {
            codecs.add(0, LzopCodec.class.getName());
        }
        jobConf.set("io.compression.codecs", String.join(",", codecs));
    }

    public static InputFormat<?, ?> getInputFormat(Configuration configuration, Properties schema)
    {
        String inputFormatName = HiveUtil.getInputFormatName(schema).orElseThrow(() ->
                new TrinoException(HIVE_INVALID_METADATA, "Table or partition is missing Hive input format property: " + FILE_INPUT_FORMAT));
        try {
            JobConf jobConf = toJobConf(configuration);
            configureCompressionCodecs(jobConf);

            Class<? extends InputFormat<?, ?>> inputFormatClass = getInputFormatClass(jobConf, inputFormatName);
            if (inputFormatClass.getName().equals(SYMLINK_TEXT_INPUT_FORMAT_CLASS)) {
                String serde = HiveUtil.getDeserializerClassName(schema);
                // LazySimpleSerDe is used by TEXTFILE and SEQUENCEFILE. Default to TEXTFILE
                // per Hive spec (https://hive.apache.org/javadocs/r2.1.1/api/org/apache/hadoop/hive/ql/io/SymlinkTextInputFormat.html)
                if (serde.equals(TEXTFILE.getSerde())) {
                    inputFormatClass = getInputFormatClass(jobConf, TEXTFILE.getInputFormat());
                    return ReflectionUtils.newInstance(inputFormatClass, jobConf);
                }
                for (HiveStorageFormat format : HiveStorageFormat.values()) {
                    if (serde.equals(format.getSerde())) {
                        inputFormatClass = getInputFormatClass(jobConf, format.getInputFormat());
                        return ReflectionUtils.newInstance(inputFormatClass, jobConf);
                    }
                }
                throw new TrinoException(HIVE_UNSUPPORTED_FORMAT, "Unknown SerDe for SymlinkTextInputFormat: " + serde);
            }

            return ReflectionUtils.newInstance(inputFormatClass, jobConf);
        }
        catch (ClassNotFoundException | RuntimeException e) {
            throw new TrinoException(HIVE_UNSUPPORTED_FORMAT, "Unable to create input format " + inputFormatName, e);
        }
    }

    @SuppressWarnings("unchecked")
    private static Class<? extends InputFormat<?, ?>> getInputFormatClass(JobConf conf, String inputFormatName)
            throws ClassNotFoundException
    {
        // legacy names for Parquet
        if ("parquet.hive.DeprecatedParquetInputFormat".equals(inputFormatName) ||
                "parquet.hive.MapredParquetInputFormat".equals(inputFormatName)) {
            inputFormatName = MAPRED_PARQUET_INPUT_FORMAT_CLASS;
        }

        Class<?> clazz = conf.getClassByName(inputFormatName);
        return (Class<? extends InputFormat<?, ?>>) clazz.asSubclass(InputFormat.class);
    }
}
