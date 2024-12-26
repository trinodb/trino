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
package io.trino.server.protocol.spooling.encoding.arrow;

import com.google.common.collect.ImmutableList;
import io.trino.server.protocol.OutputColumn;
import io.trino.spi.Page;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;

import java.io.IOException;
import java.util.List;

import static io.trino.server.protocol.spooling.encoding.arrow.VectorWriters.writerForVector;
import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;

public class PageWriter
        implements AutoCloseable
{
    private final VectorSchemaRoot schema;
    private final ArrowStreamWriter streamWriter;
    private final List<OutputColumn> columns;

    public PageWriter(ArrowStreamWriter streamWriter, VectorSchemaRoot schema, List<OutputColumn> columns)
    {
        this.streamWriter = requireNonNull(streamWriter, "streamWriter is null");
        this.schema = requireNonNull(schema, "schema is null");
        this.columns = ImmutableList.copyOf(requireNonNull(columns, "columns is null"));
    }

    public int writePages(List<Page> pages)
            throws IOException
    {
        for (Page page : pages) {
            schema.setRowCount(page.getPositionCount());
            for (int i = 0; i < columns.size(); i++) {
                schema.getVector(i).allocateNew();
                writerForVector(schema.getVector(i), columns.get(i).type())
                        .write(page.getBlock(columns.get(i).sourcePageChannel()));
            }
            streamWriter.writeBatch();
        }
        return toIntExact(streamWriter.bytesWritten());
    }

    @Override
    public void close()
            throws IOException
    {
        streamWriter.close();
        schema.close();
    }
}
