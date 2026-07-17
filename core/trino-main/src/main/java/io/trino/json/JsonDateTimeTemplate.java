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
package io.trino.json;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import io.trino.json.JsonDateTimeTemplateParser.Parsed;
import io.trino.json.ir.TypedValue;
import io.trino.spi.type.Type;

import java.util.List;
import java.util.Objects;

import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

/// Parsed SQL/JSON path `datetime(<template>)` format template per ISO/IEC 9075-2:2023 §9.46.
///
/// Represents an immutable, pre-validated template that the runtime can apply to a
/// value string via [#parseValue(String)].
public final class JsonDateTimeTemplate
{
    private final String template;
    private final Type type;
    private final int precision;
    private final List<Segment> segments;

    public static JsonDateTimeTemplate parse(String template)
    {
        Parsed parsed = JsonDateTimeTemplateParser.parse(template);
        return new JsonDateTimeTemplate(parsed.template(), parsed.type(), parsed.precision(), parsed.segments());
    }

    @JsonCreator
    public JsonDateTimeTemplate(
            @JsonProperty("template") String template,
            @JsonProperty("type") Type type,
            @JsonProperty("precision") int precision)
    {
        Parsed parsed = JsonDateTimeTemplateParser.parse(template);
        if (!parsed.type().equals(type)) {
            throw new IllegalArgumentException(format("datetime() template type mismatch: expected %s, found %s", parsed.type().getDisplayName(), type.getDisplayName()));
        }
        if (parsed.precision() != precision) {
            throw new IllegalArgumentException(format("datetime() template precision mismatch: expected %s, found %s", parsed.precision(), precision));
        }
        this.template = parsed.template();
        this.type = type;
        this.precision = precision;
        this.segments = parsed.segments();
    }

    private JsonDateTimeTemplate(String template, Type type, int precision, List<Segment> segments)
    {
        this.template = requireNonNull(template, "template is null");
        this.type = requireNonNull(type, "type is null");
        this.precision = precision;
        this.segments = ImmutableList.copyOf(segments);
    }

    /// The template in canonical form: field tokens upper-cased, literal text verbatim.
    /// Templates that differ only in the case of their field tokens are equal.
    @JsonProperty
    public String getTemplate()
    {
        return template;
    }

    @JsonProperty
    public Type getType()
    {
        return type;
    }

    @JsonProperty
    public int getPrecision()
    {
        return precision;
    }

    public TypedValue parseValue(String value)
    {
        return JsonDateTimeValueParser.parse(segments, type, value);
    }

    @Override
    public boolean equals(Object object)
    {
        if (this == object) {
            return true;
        }
        if (!(object instanceof JsonDateTimeTemplate other)) {
            return false;
        }
        return precision == other.precision &&
                template.equals(other.template) &&
                type.equals(other.type);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(template, type, precision);
    }

    @Override
    public String toString()
    {
        return template;
    }

    sealed interface Segment
            permits FieldSegment, LiteralSegment {}

    record LiteralSegment(String literal)
            implements Segment
    {
        LiteralSegment
        {
            requireNonNull(literal, "literal is null");
        }
    }

    record FieldSegment(Field field, int width, boolean delimited)
            implements Segment
    {
        FieldSegment
        {
            requireNonNull(field, "field is null");
        }
    }

    enum Field
    {
        YEAR,
        ROUNDED_YEAR,
        MONTH,
        DAY,
        DAY_OF_YEAR,
        HOUR12,
        HOUR24,
        MINUTE,
        SECOND,
        SECOND_OF_DAY,
        FRACTION,
        AM_PM,
        TIME_ZONE_HOUR,
        TIME_ZONE_MINUTE,
    }
}
