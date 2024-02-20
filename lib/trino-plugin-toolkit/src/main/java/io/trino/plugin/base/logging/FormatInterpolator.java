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
package io.trino.plugin.base.logging;

import com.google.common.collect.ImmutableList;

import java.util.Arrays;
import java.util.List;
import java.util.regex.MatchResult;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.google.common.base.MoreObjects.firstNonNull;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.joining;

public class FormatInterpolator<Context>
{
    private final String format;
    private final List<InterpolatedValue<Context>> values;

    public FormatInterpolator(String format, List<InterpolatedValue<Context>> values)
    {
        this.format = firstNonNull(format, "");
        this.values = ImmutableList.copyOf(requireNonNull(values, "values is null"));
    }

    @SafeVarargs
    public FormatInterpolator(String format, InterpolatedValue<Context>... values)
    {
        this(format, Arrays.stream(values).toList());
    }

    public String interpolate(Context context)
    {
        String result = format;
        for (InterpolatedValue<Context> value : values) {
            if (result.contains(value.getCode())) {
                result = result.replaceAll(value.getMatchCase(), value.value(context));
            }
        }
        return result;
    }

    public static boolean hasValidPlaceholders(String format, InterpolatedValue<?>... values)
    {
        return hasValidPlaceholders(format, Arrays.stream(values).toList());
    }

    public static boolean hasValidPlaceholders(String format, List<InterpolatedValue<?>> values)
    {
        List<String> matches = values.stream().map(InterpolatedValue::getMatchCase).toList();
        Pattern pattern = Pattern.compile("[\\w ,_\\-=]|" + String.join("|", matches));

        Matcher matcher = pattern.matcher(format);
        return matcher.results()
                .map(MatchResult::group)
                .collect(joining())
                .equals(format);
    }

    interface InterpolatedValue<Context>
    {
        String name();

        default String getCode()
        {
            return "$" + this.name();
        }

        default String getMatchCase()
        {
            return "\\$" + this.name();
        }

        String value(Context context);
    }
}
