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
package io.prestosql.plugin.kafka.encoder.json.format;

import io.prestosql.spi.type.Type;

import java.util.Locale;
import java.util.Optional;
import java.util.function.Function;

import static java.util.Objects.requireNonNull;

public enum DateTimeFormat
{
    CUSTOM_DATE_TIME(CustomDateTimeFormatter::new, CustomDateTimeFormatter::isSupportedType),
    ISO8601(pattern -> new ISO8601DateTimeFormatter(), ISO8601DateTimeFormatter::isSupportedType),
    RFC2822(pattern -> new RFC2822DateTimeFormatter(), RFC2822DateTimeFormatter::isSupportedType),
    MILLISECONDS_SINCE_EPOCH(pattern -> new MillisecondsSinceEpochFormatter(), MillisecondsSinceEpochFormatter::isSupportedType),
    SECONDS_SINCE_EPOCH(pattern -> new SecondsSinceEpochFormatter(), SecondsSinceEpochFormatter::isSupportedType);

    private final Function<Optional<String>, JsonDateTimeFormatter> formatterConstructor;
    private final Function<Type, Boolean> isSupportedType;

    DateTimeFormat(Function<Optional<String>, JsonDateTimeFormatter> formatterConstructor, Function<Type, Boolean> isSupportedType)
    {
        this.formatterConstructor = requireNonNull(formatterConstructor, "formatterConstructor is null");
        this.isSupportedType = requireNonNull(isSupportedType, "isSupportedType is null");
    }

    public boolean isSupportedType(Type type)
    {
        return isSupportedType.apply(type);
    }

    public JsonDateTimeFormatter getFormatter(Optional<String> pattern)
    {
        return formatterConstructor.apply(pattern);
    }

    @Override
    public String toString()
    {
        return name().toLowerCase(Locale.ENGLISH).replaceAll("_", "-");
    }
}
