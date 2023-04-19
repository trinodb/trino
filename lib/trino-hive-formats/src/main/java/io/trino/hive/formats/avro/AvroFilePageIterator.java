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
import org.apache.avro.AvroRuntimeException;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.SeekableInput;

import javax.annotation.Nullable;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Iterator;
import java.util.Optional;
import java.util.function.Function;

import static com.google.common.collect.ImmutableMap.toImmutableMap;

public class AvroFilePageIterator
        implements Iterator<Page>
{
    private final AvroPageDataReader dataReader;
    private final DataFileReader<Optional<Page>> fileReader;
    private Optional<Page> nextPage = Optional.empty();

    // Calling class responsible for closing input stream
    public AvroFilePageIterator(Schema readerSchema, AvroTypeManager typeManager, SeekableInput inputStream) throws IOException
    {
        dataReader = new AvroPageDataReader(readerSchema, typeManager);
        fileReader = new DataFileReader<>(inputStream, dataReader);
        typeManager.configure(fileReader.getMetaKeys().stream().collect(toImmutableMap(Function.identity(), fileReader::getMeta)));
    }

    @Override
    public boolean hasNext()
    {
        populateNextPage();
        return nextPage.isPresent();
    }

    @Override
    @Nullable
    public Page next()
    {
        populateNextPage();
        Page toReturn = nextPage.orElse(null);
        nextPage = Optional.empty();
        return toReturn;
    }

    private void populateNextPage()
    {
        while (fileReader.hasNext() && nextPage.isEmpty()) {
            try {
                nextPage = fileReader.next();
            }
            catch (AvroRuntimeException runtimeException) {
                // TODO determine best throw type
                throw new UncheckedIOException((IOException) runtimeException.getCause());
            }
        }
        if (nextPage.isEmpty()) {
            nextPage = dataReader.flush();
        }
    }
}
