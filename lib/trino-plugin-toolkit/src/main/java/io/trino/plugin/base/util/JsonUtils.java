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

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonFactoryBuilder;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.StreamReadConstraints;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.airlift.json.ObjectMapperProvider;
import org.gaul.modernizer_maven_annotations.SuppressModernizer;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;

import static com.google.common.base.Preconditions.checkArgument;
import static java.lang.String.format;
import static java.nio.file.Files.exists;
import static java.nio.file.Files.isReadable;
import static java.util.Objects.requireNonNull;

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
        catch (IOException | RuntimeException e) {
            throw new IllegalArgumentException(format("Invalid JSON file '%s' for '%s'", path, javaType), e);
        }
    }

    public static <T> T parseJson(String json, Class<T> javaType)
    {
        return parseJson(OBJECT_MAPPER, json, javaType);
    }

    public static <T> T parseJson(ObjectMapper mapper, String json, Class<T> javaType)
    {
        return parseJson(mapper, ObjectMapper::createParser, json, javaType);
    }

    public static <T> T parseJson(byte[] jsonBytes, Class<T> javaType)
    {
        return parseJson(OBJECT_MAPPER, jsonBytes, javaType);
    }

    public static <T> T parseJson(ObjectMapper mapper, byte[] jsonBytes, Class<T> javaType)
    {
        return parseJson(mapper, ObjectMapper::createParser, jsonBytes, javaType);
    }

    public static <T> T parseJson(InputStream inputStream, Class<T> javaType)
    {
        return parseJson(OBJECT_MAPPER, inputStream, javaType);
    }

    public static <T> T parseJson(ObjectMapper mapper, InputStream inputStream, Class<T> javaType)
    {
        return parseJson(mapper, ObjectMapper::createParser, inputStream, javaType);
    }

    public static <T> T parseJson(URL url, Class<T> javaType)
    {
        return parseJson(OBJECT_MAPPER, url, javaType);
    }

    public static <T> T parseJson(ObjectMapper mapper, URL url, Class<T> javaType)
    {
        return parseJson(mapper, ObjectMapper::createParser, url, javaType);
    }

    private static <I, T> T parseJson(ObjectMapper mapper, ParserConstructor<I> parserConstructor, I input, Class<T> javaType)
    {
        requireNonNull(mapper, "mapper is null");
        requireNonNull(parserConstructor, "parserConstructor is null");
        requireNonNull(input, "input is null");
        requireNonNull(javaType, "javaType is null");
        try (JsonParser parser = parserConstructor.createParser(mapper, input)) {
            T value = mapper.readValue(parser, javaType);
            checkArgument(parser.nextToken() == null, "Found characters after the expected end of input");
            return value;
        }
        catch (IOException e) {
            throw new UncheckedIOException("Could not parse JSON", e);
        }
    }

    public static <T> T jsonTreeToValue(JsonNode treeNode, Class<T> javaType)
    {
        try {
            return OBJECT_MAPPER.treeToValue(treeNode, javaType);
        }
        catch (JsonProcessingException e) {
            throw new UncheckedIOException("Failed to convert JSON tree node", e);
        }
    }

    public static <T> T parseJson(String json, String jsonPointer, Class<T> javaType)
    {
        JsonNode node = parseJson(json, JsonNode.class);
        return parseJson(node, jsonPointer, javaType);
    }

    public static <T> T parseJson(Path path, String jsonPointer, Class<T> javaType)
    {
        JsonNode node = parseJson(path, JsonNode.class);
        return parseJson(node, jsonPointer, javaType);
    }

    private static <T> T parseJson(JsonNode node, String jsonPointer, Class<T> javaType)
    {
        JsonNode mappingsNode = node.at(jsonPointer);
        return jsonTreeToValue(mappingsNode, javaType);
    }

    public static JsonFactory jsonFactory()
    {
        return jsonFactoryBuilder().build();
    }

    @SuppressModernizer
    // JsonFactoryBuilder usage is intentional as we need to disable read constraints
    // due to the limits introduced by Jackson 2.15
    public static JsonFactoryBuilder jsonFactoryBuilder()
    {
        return new JsonFactoryBuilder()
                .streamReadConstraints(StreamReadConstraints.builder()
                        .maxStringLength(Integer.MAX_VALUE)
                        .maxNestingDepth(Integer.MAX_VALUE)
                        .maxNumberLength(Integer.MAX_VALUE)
                        .build());
    }

    private interface ParserConstructor<I>
    {
        JsonParser createParser(ObjectMapper mapper, I input)
                throws IOException;
    }
}
