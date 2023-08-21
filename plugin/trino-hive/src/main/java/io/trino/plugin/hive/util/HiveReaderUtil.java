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

import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import io.airlift.compress.lzo.LzoCodec;
import io.airlift.compress.lzo.LzopCodec;
import io.trino.hadoop.TextLineLengthLimitExceededException;
import io.trino.plugin.hive.HiveColumnHandle;
import io.trino.plugin.hive.HiveStorageFormat;
import io.trino.plugin.hive.avro.TrinoAvroSerDe;
import io.trino.spi.TrinoException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.serde2.AbstractSerDe;
import org.apache.hadoop.hive.serde2.Deserializer;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.ReflectionUtils;

import java.io.IOException;
import java.util.List;
import java.util.Properties;

import static com.google.common.base.MoreObjects.firstNonNull;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.Lists.newArrayList;
import static io.trino.hdfs.ConfigurationUtils.copy;
import static io.trino.hdfs.ConfigurationUtils.toJobConf;
import static io.trino.hive.thrift.metastore.hive_metastoreConstants.FILE_INPUT_FORMAT;
import static io.trino.plugin.hive.HiveColumnHandle.ColumnType.REGULAR;
import static io.trino.plugin.hive.HiveErrorCode.HIVE_BAD_DATA;
import static io.trino.plugin.hive.HiveErrorCode.HIVE_CANNOT_OPEN_SPLIT;
import static io.trino.plugin.hive.HiveErrorCode.HIVE_INVALID_METADATA;
import static io.trino.plugin.hive.HiveErrorCode.HIVE_SERDE_NOT_FOUND;
import static io.trino.plugin.hive.HiveErrorCode.HIVE_UNSUPPORTED_FORMAT;
import static io.trino.plugin.hive.HiveStorageFormat.TEXTFILE;
import static io.trino.plugin.hive.util.HiveClassNames.AVRO_SERDE_CLASS;
import static io.trino.plugin.hive.util.HiveClassNames.LAZY_SIMPLE_SERDE_CLASS;
import static io.trino.plugin.hive.util.HiveClassNames.MAPRED_PARQUET_INPUT_FORMAT_CLASS;
import static io.trino.plugin.hive.util.HiveClassNames.SYMLINK_TEXT_INPUT_FORMAT_CLASS;
import static io.trino.plugin.hive.util.SerdeConstants.COLLECTION_DELIM;
import static org.apache.hadoop.hive.serde2.ColumnProjectionUtils.READ_ALL_COLUMNS;
import static org.apache.hadoop.hive.serde2.ColumnProjectionUtils.READ_COLUMN_IDS_CONF_STR;

public final class HiveReaderUtil
{
    private HiveReaderUtil() {}

    public static RecordReader<?, ?> createRecordReader(Configuration configuration, Path path, long start, long length, Properties schema, List<HiveColumnHandle> columns)
    {
        // determine which hive columns we will read
        List<HiveColumnHandle> readColumns = columns.stream()
                .filter(column -> column.getColumnType() == REGULAR)
                .collect(toImmutableList());

        // Projected columns are not supported here
        readColumns.forEach(readColumn -> checkArgument(readColumn.isBaseColumn(), "column %s is not a base column", readColumn.getName()));

        List<Integer> readHiveColumnIndexes = readColumns.stream()
                .map(HiveColumnHandle::getBaseHiveColumnIndex)
                .collect(toImmutableList());

        // Tell hive the columns we would like to read, this lets hive optimize reading column oriented files
        configuration = copy(configuration);
        setReadColumns(configuration, readHiveColumnIndexes);

        InputFormat<?, ?> inputFormat = getInputFormat(configuration, schema);
        JobConf jobConf = toJobConf(configuration);
        FileSplit fileSplit = new FileSplit(path, start, length, (String[]) null);

        // propagate serialization configuration to getRecordReader
        schema.stringPropertyNames().stream()
                .filter(name -> name.startsWith("serialization."))
                .forEach(name -> jobConf.set(name, schema.getProperty(name)));

        configureCompressionCodecs(jobConf);

        try {
            @SuppressWarnings("unchecked")
            RecordReader<? extends WritableComparable<?>, ? extends Writable> recordReader = (RecordReader<? extends WritableComparable<?>, ? extends Writable>)
                    inputFormat.getRecordReader(fileSplit, jobConf, Reporter.NULL);

            int headerCount = HiveUtil.getHeaderCount(schema);
            //  Only skip header rows when the split is at the beginning of the file
            if (start == 0 && headerCount > 0) {
                skipHeader(recordReader, headerCount);
            }

            int footerCount = HiveUtil.getFooterCount(schema);
            if (footerCount > 0) {
                recordReader = new FooterAwareRecordReader<>(recordReader, footerCount, jobConf);
            }

            return recordReader;
        }
        catch (IOException e) {
            if (e instanceof TextLineLengthLimitExceededException) {
                throw new TrinoException(HIVE_BAD_DATA, "Line too long in text file: " + path, e);
            }

            throw new TrinoException(HIVE_CANNOT_OPEN_SPLIT, String.format("Error opening Hive split %s (offset=%s, length=%s) using %s: %s",
                    path,
                    start,
                    length,
                    HiveUtil.getInputFormatName(schema).orElse(null),
                    firstNonNull(e.getMessage(), e.getClass().getName())),
                    e);
        }
    }

    private static <K, V> void skipHeader(RecordReader<K, V> reader, int headerCount)
            throws IOException
    {
        K key = reader.createKey();
        V value = reader.createValue();

        while (headerCount > 0) {
            if (!reader.next(key, value)) {
                return;
            }
            headerCount--;
        }
    }

    private static void setReadColumns(Configuration configuration, List<Integer> readHiveColumnIndexes)
    {
        configuration.set(READ_COLUMN_IDS_CONF_STR, Joiner.on(',').join(readHiveColumnIndexes));
        configuration.setBoolean(READ_ALL_COLUMNS, false);
    }

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

    public static StructObjectInspector getTableObjectInspector(Deserializer deserializer)
    {
        try {
            ObjectInspector inspector = deserializer.getObjectInspector();
            checkArgument(inspector.getCategory() == ObjectInspector.Category.STRUCT, "expected STRUCT: %s", inspector.getCategory());
            return (StructObjectInspector) inspector;
        }
        catch (SerDeException e) {
            throw new RuntimeException(e);
        }
    }

    public static Deserializer getDeserializer(Configuration configuration, Properties schema)
    {
        String name = HiveUtil.getDeserializerClassName(schema);

        // for collection delimiter, Hive 1.x, 2.x uses "colelction.delim" but Hive 3.x uses "collection.delim"
        // see also https://issues.apache.org/jira/browse/HIVE-16922
        if (name.equals(LAZY_SIMPLE_SERDE_CLASS)) {
            if (schema.containsKey("colelction.delim") && !schema.containsKey(COLLECTION_DELIM)) {
                schema.setProperty(COLLECTION_DELIM, schema.getProperty("colelction.delim"));
            }
        }

        Deserializer deserializer = createDeserializer(getDeserializerClass(name));
        initializeDeserializer(configuration, deserializer, schema);
        return deserializer;
    }

    private static Class<? extends Deserializer> getDeserializerClass(String name)
    {
        if (AVRO_SERDE_CLASS.equals(name)) {
            return TrinoAvroSerDe.class;
        }

        try {
            return Class.forName(name).asSubclass(Deserializer.class);
        }
        catch (ClassNotFoundException e) {
            throw new TrinoException(HIVE_SERDE_NOT_FOUND, "deserializer does not exist: " + name);
        }
        catch (ClassCastException e) {
            throw new RuntimeException("invalid deserializer class: " + name);
        }
    }

    private static Deserializer createDeserializer(Class<? extends Deserializer> clazz)
    {
        try {
            return clazz.getConstructor().newInstance();
        }
        catch (ReflectiveOperationException e) {
            throw new RuntimeException("error creating deserializer: " + clazz.getName(), e);
        }
    }

    private static void initializeDeserializer(Configuration configuration, Deserializer deserializer, Properties schema)
    {
        try {
            configuration = copy(configuration); // Some SerDes (e.g. Avro) modify passed configuration
            deserializer.initialize(configuration, schema);
            validate(deserializer);
        }
        catch (SerDeException | RuntimeException e) {
            throw new RuntimeException("error initializing deserializer: " + deserializer.getClass().getName(), e);
        }
    }

    private static void validate(Deserializer deserializer)
    {
        if (deserializer instanceof AbstractSerDe && !((AbstractSerDe) deserializer).getConfigurationErrors().isEmpty()) {
            throw new RuntimeException("There are configuration errors: " + ((AbstractSerDe) deserializer).getConfigurationErrors());
        }
    }
}
