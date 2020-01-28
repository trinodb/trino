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

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.SequenceFile.Writer;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;

public class SequenceFileFormatRecordWriter
        implements ExtendedRecordWriter
{
    private long finalWrittenBytes = -1;
    private final Writer sequenceFileWriter;
    private static final BytesWritable EMPTY_KEY = new BytesWritable();

    public SequenceFileFormatRecordWriter(Path path, JobConf jobConf, Class value, boolean isCompressed) throws IOException
    {
        sequenceFileWriter = Utilities.createSequenceWriter(jobConf, path.getFileSystem(jobConf), path, BytesWritable.class, value, isCompressed, Reporter.NULL);
    }

    @Override
    public long getWrittenBytes()
    {
        if (finalWrittenBytes != -1) {
            return finalWrittenBytes;
        }
        try {
            return sequenceFileWriter.getLength();
        }
        catch (IOException e) {
            return 0; // do nothing
        }
    }

    @Override
    public void write(Writable writable) throws IOException
    {
        sequenceFileWriter.append(EMPTY_KEY, writable);
    }

    @Override
    public void close(boolean b) throws IOException
    {
        if (finalWrittenBytes == -1) {
            sequenceFileWriter.hflush();
            finalWrittenBytes = sequenceFileWriter.getLength();
        }
        sequenceFileWriter.close();
    }
}
