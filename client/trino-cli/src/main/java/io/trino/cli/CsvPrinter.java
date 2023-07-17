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

import au.com.bytecode.opencsv.CSVWriter;
import com.google.common.collect.ImmutableList;

import java.io.IOException;
import java.io.Writer;
import java.util.List;

import static io.trino.cli.FormatUtils.formatValue;
import static java.util.Objects.requireNonNull;

public class CsvPrinter
        implements OutputPrinter
{
    private final List<String> fieldNames;
    private final CSVWriter writer;

    private boolean needHeader;

    public enum CsvOutputFormat
    {
        STANDARD(true, true),
        NO_HEADER(false, true),
        NO_QUOTES(true, false),
        NO_HEADER_AND_QUOTES(false, false);

        private final boolean header;
        private final boolean quote;

        CsvOutputFormat(boolean header, boolean quote)
        {
            this.header = header;
            this.quote = quote;
        }

        public boolean showHeader()
        {
            return header;
        }

        public boolean isQuoted()
        {
            return quote;
        }
    }

    public CsvPrinter(List<String> fieldNames, Writer writer, CsvOutputFormat csvOutputFormat)
    {
        requireNonNull(fieldNames, "fieldNames is null");
        requireNonNull(writer, "writer is null");
        this.fieldNames = ImmutableList.copyOf(fieldNames);
        this.writer = csvOutputFormat.isQuoted() ? new CSVWriter(writer) : new CSVWriter(writer, CSVWriter.DEFAULT_SEPARATOR, CSVWriter.NO_QUOTE_CHARACTER);
        this.needHeader = csvOutputFormat.showHeader();
    }

    @Override
    public void printRows(List<List<?>> rows, boolean complete)
            throws IOException
    {
        String[] array = null;
        if (needHeader) {
            needHeader = false;
            array = toStrings(fieldNames, array);
            writer.writeNext(array);
        }

        for (List<?> row : rows) {
            array = toStrings(row, array);
            writer.writeNext(array);
            checkError();
        }
    }

    @Override
    public void finish()
            throws IOException
    {
        printRows(ImmutableList.of(), true);
        writer.flush();
        checkError();
    }

    private void checkError()
            throws IOException
    {
        if (writer.checkError()) {
            throw new IOException("error writing to output");
        }
    }

    private static String[] toStrings(List<?> values, String[] array)
    {
        int rowSize = values.size();
        if (array == null || rowSize != array.length) {
            array = new String[rowSize];
        }
        for (int i = 0; i < rowSize; i++) {
            array[i] = formatValue(values.get(i), "", -1);
        }
        return array;
    }
}
