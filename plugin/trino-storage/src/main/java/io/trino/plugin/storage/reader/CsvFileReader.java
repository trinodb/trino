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
package io.trino.plugin.storage.reader;

import com.fasterxml.jackson.databind.MappingIterator;
import com.fasterxml.jackson.dataformat.csv.CsvMapper;
import com.fasterxml.jackson.dataformat.csv.CsvSchema;
import com.google.common.collect.Streams;
import io.trino.plugin.storage.StorageColumnHandle;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Stream;

import static com.fasterxml.jackson.dataformat.csv.CsvParser.Feature.TRIM_SPACES;
import static com.fasterxml.jackson.dataformat.csv.CsvParser.Feature.WRAP_AS_ARRAY;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.spi.type.VarcharType.VARCHAR;

public class CsvFileReader
{
    private final CsvMapper mapper;
    private final CsvSchema schema;

    public CsvFileReader(char delimiter)
    {
        this.mapper = new CsvMapper();
        this.mapper.enable(WRAP_AS_ARRAY).enable(TRIM_SPACES);
        this.schema = CsvSchema.emptySchema().withColumnSeparator(delimiter);
    }

    public List<StorageColumnHandle> getColumns(String location, Function<String, InputStream> streamProvider)
    {
        try {
            // Read the first line and use the values as column names
            MappingIterator<List<String>> iterator = mapper.readerFor(List.class).with(schema).readValues(streamProvider.apply(location));
            List<String> fields = iterator.next();
            return fields.stream()
                    .map(field -> new StorageColumnHandle(field, VARCHAR))
                    .collect(toImmutableList());
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public Stream<List<?>> getRecordsIterator(String path, Function<String, InputStream> streamProvider)
    {
        try {
            // Read lines and skip the first one because that contains the column names
            MappingIterator<List<?>> iterator = mapper.readerFor(List.class).with(schema).readValues(streamProvider.apply(path));
            return Streams.stream(iterator).skip(1);
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
