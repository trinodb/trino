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
package io.trino.cli;

import io.trino.client.Column;

import java.io.IOException;
import java.io.StringWriter;
import java.io.Writer;
import java.util.List;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;

public class AutoTablePrinter
        implements OutputPrinter
{
    private final int maxWidth;
    private OutputPrinter delegate;
    private final OutputPrinter fallback;

    private final StringWriter bufferWriter = new StringWriter();
    private final Writer writer;

    public AutoTablePrinter(List<Column> columns, Writer writer, int maxWidth)
    {
        requireNonNull(columns, "columns is null");
        this.writer = requireNonNull(writer, "writer is null");
        this.maxWidth = maxWidth;

        this.delegate = new AlignedTablePrinter(columns, bufferWriter);
        List<String> fieldNames = columns.stream()
                .map(Column::getName)
                .collect(toImmutableList());
        this.fallback = new VerticalRecordPrinter(fieldNames, bufferWriter);
    }

    @Override
    public void finish()
            throws IOException
    {
        delegate.finish();
        flush();
    }

    @Override
    public void printRows(List<List<?>> rows, boolean complete)
            throws IOException
    {
        delegate.printRows(rows, complete);
        if (!delegate.equals(fallback)) {
            if (bufferWriter.toString().indexOf("\n") > maxWidth) {
                delegate = fallback;
                bufferWriter.getBuffer().setLength(0);
                delegate.printRows(rows, complete);
            }
        }

        flush();
    }

    private void flush()
            throws IOException
    {
        writer.write(bufferWriter.toString());
        writer.flush();
        bufferWriter.getBuffer().setLength(0);
    }
}
