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
package io.trino.hive.formats.line.grok;

import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slices;
import io.trino.hive.formats.line.Column;
import io.trino.hive.formats.line.LineBuffer;
import io.trino.hive.formats.line.LineDeserializer;
import io.trino.hive.formats.line.grok.exception.GrokException;
import io.trino.spi.PageBuilder;
import io.trino.spi.TrinoException;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.type.CharType;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarcharType;

import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.Chars.truncateToLengthAndTrimSpaces;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.TinyintType.TINYINT;
import static io.trino.spi.type.Varchars.truncateToLength;
import static java.lang.Float.floatToRawIntBits;

public class GrokDeserializer
        implements LineDeserializer
{
    private final List<Column> columns;
    private final Grok grokPattern;
    private final List<Type> types;

    public GrokDeserializer(List<Column> columns, String inputFormat, String inputGrokCustomPatterns)
    {
        this.columns = ImmutableList.copyOf(columns);
        this.types = columns.stream()
                .map(Column::type)
                .collect(toImmutableList());

        // Create a new grok instance using update create() method
        try {
            grokPattern = Grok.create();
        }
        catch (GrokException e) {
            throw new RuntimeException("Grok creation failure", e);
        }

        try {
            // Capture named expressions only and do not use auto conversion.
            grokPattern.setStrictMode(true);

            if (inputGrokCustomPatterns != null) {
                grokPattern.addPatternFromReader(new StringReader(inputGrokCustomPatterns));
            }

            grokPattern.compile(inputFormat, true);
        }
        catch (GrokException e) {
            throw new RuntimeException(String.format("Grok compilation failure: %s", e.getMessage()), e);
        }
    }

    @Override
    public List<? extends Type> getTypes()
    {
        return this.types;
    }

    @Override
    public void deserialize(LineBuffer lineBuffer, PageBuilder builder)
    {
        builder.declarePosition();

        Match match = grokPattern.match(new String(lineBuffer.getBuffer(), 0, lineBuffer.getLength(), java.nio.charset.StandardCharsets.UTF_8));

        // If line does not match grok pattern, return a row of nulls
        if (match.getMatch() == null) {
            for (int i = 0; i < columns.size(); i++) {
                builder.getBlockBuilder(i).appendNull();
            }
            return;
        }

        try {
            match.captures();
        }
        catch (GrokException e) {
            throw new RuntimeException("Grok capture failure.", e);
        }

        Map<String, Object> map = match.toMap();
        List<Object> row = new ArrayList<Object>(map.values());

        for (int i = 0; i < columns.size(); i++) {
            Column column = columns.get(i);
            BlockBuilder blockBuilder = builder.getBlockBuilder(i);
            String value = String.valueOf(row.get(i));
            if (value == null) {
                blockBuilder.appendNull();
                continue;
            }
            serializeValue(value, column, blockBuilder);
        }
    }

    private static void serializeValue(String value, Column column, BlockBuilder builder)
    {
        try {
            Type type = column.type();
            if (BOOLEAN.equals(type)) {
                type.writeBoolean(builder, Boolean.parseBoolean(value));
            }
            else if (BIGINT.equals(type)) {
                type.writeLong(builder, Long.parseLong(value));
            }
            else if (INTEGER.equals(type)) {
                type.writeLong(builder, Integer.parseInt(value));
            }
            else if (SMALLINT.equals(type)) {
                type.writeLong(builder, Short.parseShort(value));
            }
            else if (TINYINT.equals(type)) {
                type.writeLong(builder, Byte.parseByte(value));
            }
            else if (REAL.equals(type)) {
                type.writeLong(builder, floatToRawIntBits(Float.parseFloat(value)));
            }
            else if (DOUBLE.equals(type)) {
                type.writeDouble(builder, Double.parseDouble(value));
            }
            else if (type instanceof VarcharType varcharType) {
                type.writeSlice(builder, truncateToLength(Slices.utf8Slice(value), varcharType));
            }
            else if (type instanceof CharType charType) {
                type.writeSlice(builder, truncateToLengthAndTrimSpaces(Slices.utf8Slice(value), charType));
            }
            else {
                throw new TrinoException(NOT_SUPPORTED, "Unsupported column type: " + type);
            }
        }
        catch (TrinoException e) {
            throw e;
        }
        catch (RuntimeException e) {
            builder.appendNull();
        }
    }
}
