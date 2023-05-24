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

import io.trino.plugin.hive.RecordFileWriter.ExtendedRecordWriter;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BinaryComparable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Optional;
import java.util.Properties;

import static io.trino.plugin.hive.util.SerdeConstants.LINE_DELIM;
import static java.lang.Integer.parseInt;
import static org.apache.hadoop.hive.ql.exec.Utilities.createCompressedStream;

public class TextRecordWriter
        implements ExtendedRecordWriter
{
    private final FSDataOutputStream output;
    private final OutputStream compressedOutput;
    private final int rowSeparator;

    public TextRecordWriter(Path path, JobConf jobConf, Properties properties, boolean isCompressed, Optional<TextHeaderWriter> textHeaderWriter)
            throws IOException
    {
        String rowSeparatorString = properties.getProperty(LINE_DELIM, "\n");
        // same logic as HiveIgnoreKeyTextOutputFormat
        int rowSeparatorByte;
        try {
            rowSeparatorByte = Byte.parseByte(rowSeparatorString);
        }
        catch (NumberFormatException e) {
            rowSeparatorByte = rowSeparatorString.charAt(0);
        }
        rowSeparator = rowSeparatorByte;
        output = path.getFileSystem(jobConf).create(path, Reporter.NULL);
        compressedOutput = createCompressedStream(jobConf, output, isCompressed);

        Optional<String> skipHeaderLine = Optional.ofNullable(properties.getProperty("skip.header.line.count"));
        if (skipHeaderLine.isPresent()) {
            if (parseInt(skipHeaderLine.get()) == 1) {
                textHeaderWriter
                        .orElseThrow(() -> new IllegalArgumentException("TextHeaderWriter must not be empty when skip.header.line.count is set to 1"))
                        .write(compressedOutput, rowSeparator);
            }
        }
    }

    @Override
    public long getWrittenBytes()
    {
        return output.getPos();
    }

    @Override
    public void write(Writable writable)
            throws IOException
    {
        BinaryComparable binary = (BinaryComparable) writable;
        compressedOutput.write(binary.getBytes(), 0, binary.getLength());
        compressedOutput.write(rowSeparator);
    }

    @Override
    public void close(boolean abort)
            throws IOException
    {
        compressedOutput.close();
    }
}
