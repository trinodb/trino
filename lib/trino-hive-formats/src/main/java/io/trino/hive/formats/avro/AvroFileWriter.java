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
package io.trino.hive.formats.avro;

import io.trino.spi.Page;
import io.trino.spi.type.Type;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;

import java.io.Closeable;
import java.io.IOException;
import java.io.OutputStream;
import java.util.List;
import java.util.Map;

import static com.google.common.base.Verify.verify;
import static io.airlift.slice.SizeOf.instanceSize;

public class AvroFileWriter
        implements Closeable
{
    private static final int INSTANCE_SIZE = instanceSize(AvroFileWriter.class);

    private final AvroPagePositionDataWriter pagePositionDataWriter;
    private final DataFileWriter<Integer> pagePositionFileWriter;

    public AvroFileWriter(
            OutputStream rawOutput,
            Schema schema,
            AvroTypeManager avroTypeManager,
            AvroCompressionKind compressionKind,
            Map<String, String> fileMetadata,
            List<String> names,
            List<Type> types)
            throws IOException, AvroTypeException
    {
        verify(compressionKind.isSupportedLocally(), "compression kind must be supported locally: %s", compressionKind);
        pagePositionDataWriter = new AvroPagePositionDataWriter(schema, avroTypeManager, names, types);
        try {
            DataFileWriter<Integer> fileWriter = new DataFileWriter<>(pagePositionDataWriter)
                    .setCodec(compressionKind.getCodecFactory());
            fileMetadata.forEach(fileWriter::setMeta);
            pagePositionFileWriter = fileWriter.create(schema, rawOutput);
        }
        catch (org.apache.avro.AvroTypeException e) {
            throw new AvroTypeException(e);
        }
        catch (org.apache.avro.AvroRuntimeException e) {
            throw new IOException(e);
        }
    }

    public void write(Page page)
            throws IOException
    {
        pagePositionDataWriter.setPage(page);
        for (int pos = 0; pos < page.getPositionCount(); pos++) {
            try {
                pagePositionFileWriter.append(pos);
            }
            catch (RuntimeException e) {
                throw new IOException("Error writing to avro file", e);
            }
        }
    }

    public long getRetainedSize()
    {
        // Avro library delegates to java.io.BufferedOutputStream.BufferedOutputStream(java.io.OutputStream)
        // which has a default buffer size of 8192
        return INSTANCE_SIZE + 8192;
    }

    @Override
    public void close()
            throws IOException
    {
        pagePositionFileWriter.close();
    }
}
