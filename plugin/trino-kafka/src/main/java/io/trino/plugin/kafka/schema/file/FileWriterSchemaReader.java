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

import io.trino.plugin.kafka.KafkaTableHandle;
import io.trino.plugin.kafka.schema.ContentSchemaReader;
import io.trino.spi.TrinoException;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Optional;

import static io.trino.plugin.kafka.KafkaErrorCode.KAFKA_SCHEMA_ERROR;
import static java.lang.String.format;

public class FileWriterSchemaReader
        implements ContentSchemaReader
{
    @Override
    public Optional<String> readKeyContentSchema(KafkaTableHandle tableHandle)
    {
        return getDataSchema(tableHandle.getKeyDataSchemaLocation());
    }

    @Override
    public Optional<String> readValueContentSchema(KafkaTableHandle tableHandle)
    {
        return getDataSchema(tableHandle.getMessageDataSchemaLocation());
    }

    private static Optional<String> getDataSchema(Optional<String> dataSchemaLocation)
    {
        return dataSchemaLocation.map(location -> {
            try {
                return Files.readString(Paths.get(location));
            }
            catch (IOException e) {
                throw new TrinoException(KAFKA_SCHEMA_ERROR, format("Unable to read data schema at '%s'", location), e);
            }
        });
    }
}
