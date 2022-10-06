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
package io.trino.plugin.jdbc.logging;

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import io.trino.plugin.jdbc.logging.FormatBasedRemoteQueryModifier.PredefinedValue;

import javax.validation.constraints.AssertTrue;

import java.util.Arrays;
import java.util.List;
import java.util.regex.MatchResult;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static java.util.stream.Collectors.joining;

public class FormatBasedRemoteQueryModifierConfig
{
    private static final List<String> PREDEFINED_MATCHES = Arrays.stream(PredefinedValue.values()).map(PredefinedValue::getMatchCase).toList();
    private static final Pattern VALIDATION_PATTERN = Pattern.compile("[\\w ,=]|" + String.join("|", PREDEFINED_MATCHES));
    private String format = "";

    @Config("query.comment-format")
    @ConfigDescription("Format in which logs about query execution context should be added as comments sent through jdbc driver.")
    public FormatBasedRemoteQueryModifierConfig setFormat(String format)
    {
        this.format = format;
        return this;
    }

    public String getFormat()
    {
        return format;
    }

    @AssertTrue(message = "Incorrect format it may consist of only letters, digits, underscores, commas, spaces, equal signs and predefined values")
    boolean isFormatValid()
    {
        Matcher matcher = VALIDATION_PATTERN.matcher(format);
        return matcher.results()
                .map(MatchResult::group)
                .collect(joining())
                .equals(format);
    }
}
