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

import io.airlift.json.JsonMapperProvider;
import org.gaul.modernizer_maven_annotations.SuppressModernizer;
import tools.jackson.core.JsonParser;
import tools.jackson.core.StreamReadConstraints;
import tools.jackson.core.StreamReadFeature;
import tools.jackson.core.StreamWriteConstraints;
import tools.jackson.core.StreamWriteFeature;
import tools.jackson.core.json.JsonFactory;
import tools.jackson.core.json.JsonFactoryBuilder;
import tools.jackson.core.json.JsonWriteFeature;
import tools.jackson.core.util.JsonRecyclerPools;
import tools.jackson.databind.DeserializationContext;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ValueDeserializer;
import tools.jackson.databind.deser.DeserializationProblemHandler;
import tools.jackson.databind.exc.UnrecognizedPropertyException;
import tools.jackson.databind.json.JsonMapper;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collection;

import static com.google.common.base.Preconditions.checkArgument;
import static java.lang.String.format;
import static java.nio.file.Files.exists;
import static java.nio.file.Files.isReadable;
import static java.util.Objects.requireNonNull;
import static tools.jackson.databind.DeserializationFeature.FAIL_ON_NULL_FOR_PRIMITIVES;
import static tools.jackson.databind.DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES;
import static tools.jackson.databind.MapperFeature.ACCEPT_CASE_INSENSITIVE_ENUMS;

public final class JsonUtils
{
    /**
     * Reports unknown properties from the main parser so the exception carries the line and column.
     * Jackson 3 would otherwise build it from a replayed token buffer (e.g. for {@code @JsonCreator} classes), which has no location.
     */
    public static final DeserializationProblemHandler UNKNOWN_PROPERTY_LOCATION_HANDLER = new DeserializationProblemHandler()
    {
        @Override
        public boolean handleUnknownProperty(DeserializationContext context, JsonParser parser, ValueDeserializer<?> deserializer, Object beanOrClass, String propertyName)
        {
            if (!context.isEnabled(FAIL_ON_UNKNOWN_PROPERTIES)) {
                return false;
            }
            Collection<Object> knownProperties = deserializer == null ? null : deserializer.getKnownPropertyNames();
            throw UnrecognizedPropertyException.from(context.getParser(), beanOrClass, propertyName, knownProperties);
        }
    };

    private static final JsonMapper JSON_MAPPER = new JsonMapperProvider().get()
            .rebuild()
            .enable(FAIL_ON_UNKNOWN_PROPERTIES)
            .disable(FAIL_ON_NULL_FOR_PRIMITIVES)
            .enable(ACCEPT_CASE_INSENSITIVE_ENUMS)
            .addHandler(UNKNOWN_PROPERTY_LOCATION_HANDLER)
            .build();

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
        return parseJson(JSON_MAPPER, json, javaType);
    }

    public static <T> T parseJson(JsonMapper mapper, String json, Class<T> javaType)
    {
        return parseJson(mapper, JsonMapper::createParser, json, javaType);
    }

    public static <T> T parseJson(byte[] jsonBytes, Class<T> javaType)
    {
        return parseJson(JSON_MAPPER, jsonBytes, javaType);
    }

    public static <T> T parseJson(JsonMapper mapper, byte[] jsonBytes, Class<T> javaType)
    {
        return parseJson(mapper, JsonMapper::createParser, jsonBytes, javaType);
    }

    public static <T> T parseJson(JsonMapper mapper, InputStream inputStream, Class<T> javaType)
    {
        return parseJson(mapper, JsonMapper::createParser, inputStream, javaType);
    }

    private static <I, T> T parseJson(JsonMapper mapper, ParserConstructor<I> parserConstructor, I input, Class<T> javaType)
    {
        requireNonNull(mapper, "mapper is null");
        requireNonNull(parserConstructor, "parserConstructor is null");
        requireNonNull(input, "input is null");
        requireNonNull(javaType, "javaType is null");
        try (JsonParser parser = parserConstructor.createParser(mapper, input)) {
            return mapper.readValue(parser, javaType);
        }
    }

    public static <T> T jsonTreeToValue(JsonNode treeNode, Class<T> javaType)
    {
        return JSON_MAPPER.treeToValue(treeNode, javaType);
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
        // https://github.com/FasterXML/jackson-core/issues/1256
        return new JsonFactoryBuilder()
                .streamReadConstraints(StreamReadConstraints.builder()
                        .maxStringLength(Integer.MAX_VALUE)
                        .maxNestingDepth(Integer.MAX_VALUE)
                        .maxNumberLength(Integer.MAX_VALUE)
                        .build())
                .streamWriteConstraints(StreamWriteConstraints.builder()
                        .maxNestingDepth(1000)
                        .build())
                .enable(StreamReadFeature.USE_FAST_BIG_NUMBER_PARSER)
                .enable(StreamReadFeature.USE_FAST_DOUBLE_PARSER)
                .enable(StreamWriteFeature.USE_FAST_DOUBLE_WRITER)
                // Keep escaping surrogate pairs in UTF-8 output as Jackson 2 did
                .disable(JsonWriteFeature.COMBINE_UNICODE_SURROGATES_IN_UTF8)
                .recyclerPool(JsonRecyclerPools.threadLocalPool());
    }

    private interface ParserConstructor<I>
    {
        JsonParser createParser(JsonMapper mapper, I input);
    }
}
