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
package io.trino.plugin.kafka.schema.file;

import com.google.common.io.CharStreams;
import io.trino.plugin.kafka.schema.AbstractContentSchemaProvider;
import io.trino.spi.TrinoException;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkState;
import static io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Locale.ENGLISH;

public class FileReadContentSchemaProvider
        extends AbstractContentSchemaProvider
{
    @Override
    protected Optional<String> readSchema(Optional<String> dataSchemaLocation, Optional<String> subject)
    {
        if (dataSchemaLocation.isEmpty()) {
            return Optional.empty();
        }
        checkState(subject.isEmpty(), "Unexpected parameter: subject");
        try (InputStream inputStream = openSchemaLocation(dataSchemaLocation.get())) {
            return Optional.of(CharStreams.toString(new InputStreamReader(inputStream, UTF_8)));
        }
        catch (IOException e) {
            throw new TrinoException(GENERIC_INTERNAL_ERROR, "Could not parse the Avro schema at: " + dataSchemaLocation, e);
        }
    }

    private static InputStream openSchemaLocation(String dataSchemaLocation)
            throws IOException
    {
        if (isURI(dataSchemaLocation.trim().toLowerCase(ENGLISH))) {
            try {
                return new URL(dataSchemaLocation).openStream();
            }
            catch (MalformedURLException ignore) {
                // TODO probably should not be ignored
            }
        }

        return new FileInputStream(dataSchemaLocation);
    }

    private static boolean isURI(String location)
    {
        try {
            //noinspection ResultOfMethodCallIgnored
            URI.create(location);
        }
        catch (Exception e) {
            return false;
        }
        return true;
    }
}
