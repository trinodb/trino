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
package io.trino.client;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.StreamReadConstraints;
import com.fasterxml.jackson.core.StreamReadFeature;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.lang.reflect.Type;
import java.util.function.Supplier;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public class TrinoJsonCodec<T>
{
    // copy of https://github.com/airlift/airlift/blob/master/json/src/main/java/io/airlift/json/ObjectMapperProvider.java
    static final Supplier<ObjectMapper> OBJECT_MAPPER_SUPPLIER = () -> {
        JsonFactory jsonFactory = JsonFactory.builder()
                .streamReadConstraints(StreamReadConstraints.builder()
                    .maxStringLength(Integer.MAX_VALUE)
                    .maxNestingDepth(Integer.MAX_VALUE)
                    .maxNumberLength(Integer.MAX_VALUE)
                    .build())
                .build();

        return JsonMapper.builder(jsonFactory)
                .enable(StreamReadFeature.USE_FAST_DOUBLE_PARSER)
                .enable(StreamReadFeature.USE_FAST_BIG_NUMBER_PARSER)
                .disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)
                .disable(MapperFeature.AUTO_DETECT_CREATORS)
                .disable(MapperFeature.AUTO_DETECT_FIELDS)
                .disable(MapperFeature.AUTO_DETECT_SETTERS)
                .disable(MapperFeature.AUTO_DETECT_GETTERS)
                .disable(MapperFeature.AUTO_DETECT_IS_GETTERS)
                .disable(MapperFeature.USE_GETTERS_AS_SETTERS)
                .disable(MapperFeature.CAN_OVERRIDE_ACCESS_MODIFIERS)
                .disable(MapperFeature.INFER_PROPERTY_MUTATORS)
                .disable(MapperFeature.ALLOW_FINAL_FIELDS_AS_MUTATORS)
                .addModule(new Jdk8Module())
                .addModule(new QueryDataClientJacksonModule())
                .build();
    };

    public static <T> TrinoJsonCodec<T> jsonCodec(Class<T> type)
    {
        return new TrinoJsonCodec<>(OBJECT_MAPPER_SUPPLIER.get(), type);
    }

    private final ObjectMapper mapper;
    private final Type type;
    private final JavaType javaType;

    private TrinoJsonCodec(ObjectMapper mapper, Type type)
    {
        this.mapper = requireNonNull(mapper, "mapper is null");
        this.type = requireNonNull(type, "type is null");
        this.javaType = mapper.getTypeFactory().constructType(type);
    }

    public Type getType()
    {
        return type;
    }

    public T fromJson(String json)
            throws JsonProcessingException
    {
        try (JsonParser parser = mapper.createParser(json)) {
            T value = mapper.readerFor(javaType).readValue(parser);
            checkArgument(parser.nextToken() == null, "Found characters after the expected end of input");
            return value;
        }
        catch (JsonProcessingException e) {
            throw e;
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public T fromJson(InputStream inputStream)
            throws IOException
    {
        try (JsonParser parser = mapper.createParser(inputStream)) {
            T value = mapper.readerFor(javaType).readValue(parser);
            checkArgument(parser.nextToken() == null, "Found characters after the expected end of input");
            return value;
        }
    }

    public String toJson(T instance)
    {
        try {
            return mapper.writerFor(javaType).writeValueAsString(instance);
        }
        catch (IOException exception) {
            throw new IllegalArgumentException(String.format("%s could not be converted to JSON", instance.getClass().getName()), exception);
        }
    }
}
