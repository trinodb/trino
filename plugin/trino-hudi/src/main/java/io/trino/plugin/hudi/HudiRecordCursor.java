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

package io.trino.plugin.hudi;

import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import io.airlift.compress.lzo.LzoCodec;
import io.airlift.compress.lzo.LzopCodec;
import io.trino.hdfs.HdfsContext;
import io.trino.hdfs.HdfsEnvironment;
import io.trino.plugin.hive.HiveColumnHandle;
import io.trino.plugin.hive.util.HiveUtil;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.RecordCursor;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hudi.common.model.HoodieLogFile;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.hadoop.realtime.HoodieRealtimeFileSplit;

import java.io.IOException;
import java.util.List;
import java.util.Properties;
import java.util.function.Function;

import static com.google.common.base.MoreObjects.firstNonNull;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.Lists.newArrayList;
import static io.trino.plugin.hudi.HudiErrorCode.HUDI_CANNOT_OPEN_SPLIT;
import static io.trino.plugin.hudi.HudiUtil.getHudiBaseFile;
import static io.trino.plugin.hudi.query.HiveHudiRecordCursor.createRecordCursor;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;
import static org.apache.hadoop.hive.serde2.ColumnProjectionUtils.READ_ALL_COLUMNS;
import static org.apache.hadoop.hive.serde2.ColumnProjectionUtils.READ_COLUMN_NAMES_CONF_STR;

class HudiRecordCursor
{
    private HudiRecordCursor() {}

    public static RecordCursor createRealtimeRecordCursor(
            HdfsEnvironment hdfsEnvironment,
            ConnectorSession session,
            HudiSplit split,
            HudiTableHandle tableHandle,
            List<HiveColumnHandle> dataColumns)
    {
        requireNonNull(session, "session is null");
        checkArgument(dataColumns.stream().allMatch(HudiRecordCursor::isRegularColumn), "dataColumns contains non regular column");
        HudiFile baseFile = getHudiBaseFile(split);
        Path path = new Path(baseFile.getPath());
        Configuration configuration = hdfsEnvironment.getConfiguration(new HdfsContext(session), path);

        return hdfsEnvironment.doAs(session.getIdentity(), () -> {
            RecordReader<?, ?> recordReader = createRecordReader(configuration, tableHandle.getSchema(), split, dataColumns, tableHandle.getBasePath());
            @SuppressWarnings("unchecked") RecordReader<?, ? extends Writable> reader = (RecordReader<?, ? extends Writable>) recordReader;
            return createRecordCursor(configuration, path, reader, baseFile.getLength(), tableHandle.getSchema(), dataColumns);
        });
    }

    private static RecordReader<?, ?> createRecordReader(
            Configuration configuration,
            Properties schema,
            HudiSplit split,
            List<HiveColumnHandle> dataColumns,
            String basePath)
    {
        // update configuration
        JobConf jobConf = new JobConf(configuration);
        jobConf.setBoolean(READ_ALL_COLUMNS, false);
        //jobConf.set(READ_COLUMN_IDS_CONF_STR, join(dataColumns, HudiColumnHandle::getId));
        jobConf.set(READ_COLUMN_NAMES_CONF_STR, join(dataColumns, HiveColumnHandle::getName));
        schema.stringPropertyNames()
                .forEach(name -> jobConf.set(name, schema.getProperty(name)));
        refineCompressionCodecs(jobConf);

        // create input format
        String inputFormatName = HiveUtil.getInputFormatName(schema);
        InputFormat<?, ?> inputFormat = createInputFormat(jobConf, inputFormatName);

        // create record reader for split
        try {
            HudiFile baseFile = getHudiBaseFile(split);
            Path path = new Path(baseFile.getPath());
            FileSplit fileSplit = new FileSplit(path, baseFile.getStart(), baseFile.getLength(), (String[]) null);
            List<HoodieLogFile> logFiles = split.getLogFiles().stream().map(file -> new HoodieLogFile(file.getPath())).collect(toList());
            FileSplit hudiSplit = new HoodieRealtimeFileSplit(fileSplit, basePath, logFiles, split.getCommitTime(), false, Option.empty());
            return inputFormat.getRecordReader(hudiSplit, jobConf, Reporter.NULL);
        }
        catch (IOException e) {
            String msg = format("Error opening Hive split %s using %s: %s",
                    split,
                    inputFormatName,
                    firstNonNull(e.getMessage(), e.getClass().getName()));
            throw new TrinoException(HUDI_CANNOT_OPEN_SPLIT, msg, e);
        }
    }

    private static InputFormat<?, ?> createInputFormat(Configuration conf, String inputFormat)
    {
        try {
            Class<?> clazz = conf.getClassByName(inputFormat);
            @SuppressWarnings("unchecked") Class<? extends InputFormat<?, ?>> cls =
                    (Class<? extends InputFormat<?, ?>>) clazz.asSubclass(InputFormat.class);
            return ReflectionUtils.newInstance(cls, conf);
        }
        catch (ClassNotFoundException | RuntimeException e) {
            throw new TrinoException(HUDI_CANNOT_OPEN_SPLIT, "Unable to create input format " + inputFormat, e);
        }
    }

    private static void refineCompressionCodecs(Configuration conf)
    {
        List<String> codecs = newArrayList(Splitter.on(",").trimResults().omitEmptyStrings()
                .split(conf.get("io.compression.codecs", "")));
        if (!codecs.contains(LzoCodec.class.getName())) {
            codecs.add(0, LzoCodec.class.getName());
        }
        if (!codecs.contains(LzopCodec.class.getName())) {
            codecs.add(0, LzopCodec.class.getName());
        }
        conf.set("io.compression.codecs", String.join(",", codecs));
    }

    private static <T, V> String join(List<T> list, Function<T, V> extractor)
    {
        return Joiner.on(',').join(list.stream().map(extractor).iterator());
    }

    private static boolean isRegularColumn(HiveColumnHandle column)
    {
        return column.getColumnType() == HiveColumnHandle.ColumnType.REGULAR;
    }
}
