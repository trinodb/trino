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
package io.prestosql.plugin.hive.recordwriters;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Properties;

public class TextOutputFormatRecordWriter
        implements ExtendedRecordWriter
{
    private final FSDataOutputStream fsDataOutputStream;
    private final OutputStream wrappedOutputStream;
    private final int rowSeparator;

    public TextOutputFormatRecordWriter(Path path, JobConf jobConf, Properties properties, boolean isCompressed)
            throws IOException
    {
        String rowSeparatorString = properties.getProperty(serdeConstants.LINE_DELIM, "\n");
        int rowSeparatorByte;
        try {
            rowSeparatorByte = Byte.parseByte(rowSeparatorString);
        }
        catch (NumberFormatException e) {
            rowSeparatorByte = rowSeparatorString.charAt(0);
        }
        rowSeparator = rowSeparatorByte;
        fsDataOutputStream = path.getFileSystem(jobConf).create(path, Reporter.NULL);
        wrappedOutputStream = Utilities.createCompressedStream(jobConf, fsDataOutputStream, isCompressed);
    }

    @Override
    public long getWrittenBytes()
    {
        return fsDataOutputStream.getPos();
    }

    @Override
    public void write(Writable writable) throws IOException
    {
        if (writable instanceof Text) {
            Text tr = (Text) writable;
            wrappedOutputStream.write(tr.getBytes(), 0, tr.getLength());
            wrappedOutputStream.write(rowSeparator);
        }
        else {
            // DynamicSerDe always writes out BytesWritable
            BytesWritable bw = (BytesWritable) writable;
            wrappedOutputStream.write(bw.get(), 0, bw.getSize());
            wrappedOutputStream.write(rowSeparator);
        }
    }

    @Override
    public void close(boolean b) throws IOException
    {
        wrappedOutputStream.close();
    }
}
