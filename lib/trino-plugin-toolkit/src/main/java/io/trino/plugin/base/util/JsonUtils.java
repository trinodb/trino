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
package io.trino.plugin.base.util;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import io.airlift.json.ObjectMapperProvider;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;

import static com.google.common.base.Preconditions.checkArgument;
import static java.lang.String.format;
import static java.nio.file.Files.exists;
import static java.nio.file.Files.isReadable;

public final class JsonUtils
{
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapperProvider().get()
            .enable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)
            .enable(MapperFeature.ACCEPT_CASE_INSENSITIVE_ENUMS);

    private JsonUtils() {}

    public static <T> T parseJson(Path path, Class<T> javaType)
    {
        if (!path.isAbsolute()) {
            path = path.toAbsolutePath();
        }

        checkArgument(exists(path), "File does not exist: %s", path);
        checkArgument(isReadable(path), "File is not readable: %s", path);

        try {
            byte[] json = Files.readAllBytes(path);
            return parseJson(json, javaType);
        }
        catch (IOException e) {
            throw new IllegalArgumentException(format("Invalid JSON file '%s' for '%s'", path, javaType), e);
        }
    }

    public static JsonNode parseJson(String json)
    {
        try {
            return OBJECT_MAPPER.readTree(json);
        }
        catch (IOException e) {
            throw new UncheckedIOException("Could not parse JSON node from given byte array", e);
        }
    }

    public static <T> T jsonTreeToValue(JsonNode treeNode, Class<T> javaType)
    {
        try {
            return OBJECT_MAPPER.treeToValue(treeNode, javaType);
        }
        catch (JsonProcessingException e) {
            throw new UncheckedIOException("Failed to parse JSON tree node", e);
        }
    }

    @VisibleForTesting
    static <T> T parseJson(byte[] jsonBytes, Class<T> javaType)
            throws IOException
    {
        return OBJECT_MAPPER.readValue(jsonBytes, javaType);
    }
}
