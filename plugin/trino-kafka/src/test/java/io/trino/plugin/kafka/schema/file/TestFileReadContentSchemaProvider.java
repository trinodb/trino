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

import com.google.common.collect.ImmutableList;
import io.trino.plugin.kafka.KafkaTableHandle;
import io.trino.spi.TrinoException;
import io.trino.spi.predicate.TupleDomain;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Optional;

import static io.trino.plugin.kafka.KafkaErrorCode.KAFKA_SCHEMA_ERROR;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestFileReadContentSchemaProvider
{
    @Test
    public void testReadSchema(@TempDir Path directory)
            throws IOException
    {
        Path keySchema = Files.writeString(directory.resolve("key.proto"), "syntax = \"proto3\";");
        Path messageSchema = Files.writeString(directory.resolve("message.avsc"), "{\"type\": \"string\"}");
        FileReadContentSchemaProvider provider = new FileReadContentSchemaProvider();

        KafkaTableHandle tableHandle = tableHandle(Optional.of(keySchema.toString()), Optional.of(messageSchema.toUri().toString()));
        assertThat(provider.getKey(tableHandle)).contains("syntax = \"proto3\";");
        assertThat(provider.getMessage(tableHandle)).contains("{\"type\": \"string\"}");
        assertThat(provider.getKey(tableHandle(Optional.empty(), Optional.empty()))).isEmpty();
    }

    @Test
    public void testMissingSchema(@TempDir Path directory)
    {
        // the page sink reads the data schema of an INSERT through the provider as well, whatever its format
        String missing = directory.resolve("missing.proto").toString();
        assertThatThrownBy(() -> new FileReadContentSchemaProvider().getMessage(tableHandle(Optional.empty(), Optional.of(missing))))
                .isInstanceOfSatisfying(TrinoException.class, e -> assertThat(e.getErrorCode()).isEqualTo(KAFKA_SCHEMA_ERROR.toErrorCode()))
                .hasMessage("Unable to read data schema at '%s'", missing);
    }

    private static KafkaTableHandle tableHandle(Optional<String> keyDataSchemaLocation, Optional<String> messageDataSchemaLocation)
    {
        return new KafkaTableHandle("default", "topic", "topic", "protobuf", "avro", keyDataSchemaLocation, messageDataSchemaLocation, Optional.empty(), Optional.empty(), ImmutableList.of(), TupleDomain.all());
    }
}
