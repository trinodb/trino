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
package io.trino.hive.formats.line;

import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.trino.hive.formats.HiveFormatUtils;
import io.trino.hive.formats.line.openxjson.JsonString;
import io.trino.spi.TrinoException;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.type.CharType;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.VarbinaryType;
import io.trino.spi.type.VarcharType;

import java.math.BigDecimal;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static io.trino.hive.formats.line.openxjson.JsonWriter.canonicalizeJsonString;
import static io.trino.hive.formats.line.openxjson.JsonWriter.writeJsonArray;
import static io.trino.hive.formats.line.openxjson.JsonWriter.writeJsonObject;
import static io.trino.spi.StandardErrorCode.BAD_DATA;
import static io.trino.spi.type.Chars.truncateToLengthAndTrimSpaces;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.Varchars.truncateToLength;

public final class LineDeserializerUtils
{
    private LineDeserializerUtils() {}

    public static RuntimeException parseError(String message)
    {
        return parseError(message, Optional.empty());
    }

    public static RuntimeException parseError(String message, Optional<Throwable> throwable)
    {
        String errorPrefix = "Failed to parse value: ";
        return throwable.map(e -> new TrinoException(BAD_DATA, errorPrefix + message, e)).orElseGet(() -> new TrinoException(BAD_DATA, errorPrefix + message));
    }

    public static void writeDouble(BlockBuilder builder, Double value)
    {
        try {
            DOUBLE.writeDouble(builder, value);
        }
        catch (IllegalArgumentException | UnsupportedOperationException e) {
            throw parseError(e.getMessage(), Optional.ofNullable(e.getCause()));
        }
    }

    public static void writeSlice(VarcharType type, BlockBuilder builder, Slice value)
    {
        try {
            type.writeSlice(builder, value);
        }
        catch (IllegalArgumentException | UnsupportedOperationException e) {
            throw parseError(e.getMessage(), Optional.ofNullable(e.getCause()));
        }
    }

    public static void writeSlice(VarbinaryType type, BlockBuilder builder, Slice value)
    {
        try {
            type.writeSlice(builder, value);
        }
        catch (IllegalArgumentException | UnsupportedOperationException e) {
            throw parseError(e.getMessage(), Optional.ofNullable(e.getCause()));
        }
    }

    public static void writeSlice(CharType type, BlockBuilder builder, Slice value)
    {
        try {
            type.writeSlice(builder, value);
        }
        catch (IllegalArgumentException | UnsupportedOperationException e) {
            throw parseError(e.getMessage(), Optional.ofNullable(e.getCause()));
        }
    }

    public static void writeDecimal(DecimalType type, BlockBuilder builder, String value)
    {
        try {
            HiveFormatUtils.writeDecimal(value, type, builder);
        }
        catch (IllegalArgumentException | UnsupportedOperationException e) {
            throw parseError(e.getMessage(), Optional.ofNullable(e.getCause()));
        }
    }

    public static void writeDecimal(DecimalType type, BlockBuilder builder, String value, BigDecimal bigDecimal)
    {
        try {
            HiveFormatUtils.writeDecimal(value, type, builder, bigDecimal);
        }
        catch (IllegalArgumentException | UnsupportedOperationException e) {
            throw parseError(e.getMessage(), Optional.ofNullable(e.getCause()));
        }
    }

    public static void writeJson(VarcharType type, BlockBuilder builder, Object jsonValue)
    {
        try {
            String string;
            if (jsonValue instanceof Map<?, ?> jsonObject) {
                string = writeJsonObject(jsonObject);
            }
            else if (jsonValue instanceof List<?> jsonList) {
                string = writeJsonArray(jsonList);
            }
            else {
                JsonString jsonString = (JsonString) jsonValue;
                string = canonicalizeJsonString(jsonString);
            }
            Slice slice = truncateToLength(Slices.utf8Slice(string), type);
            type.writeSlice(builder, slice);
        }
        catch (IllegalArgumentException | UnsupportedOperationException e) {
            throw parseError(e.getMessage(), Optional.ofNullable(e.getCause()));
        }
    }

    public static void writeJson(CharType type, BlockBuilder builder, Object jsonValue)
    {
        try {
            String string;
            if (jsonValue instanceof Map<?, ?> jsonObject) {
                string = writeJsonObject(jsonObject);
            }
            else if (jsonValue instanceof List<?> jsonList) {
                string = writeJsonArray(jsonList);
            }
            else {
                JsonString jsonString = (JsonString) jsonValue;
                string = canonicalizeJsonString(jsonString);
            }
            Slice slice = truncateToLengthAndTrimSpaces(Slices.utf8Slice(string), type);
            type.writeSlice(builder, slice);
        }
        catch (IllegalArgumentException | UnsupportedOperationException e) {
            throw parseError(e.getMessage(), Optional.ofNullable(e.getCause()));
        }
    }
}
