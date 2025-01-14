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
package io.trino.plugin.hive.ion;

import com.google.common.collect.ImmutableMap;
import io.trino.hive.formats.ion.IonDecoderConfig;

import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public final class IonReaderOptions
{
    public static final String STRICT_PATH_TYPING_PROPERTY = "ion.path_extractor.strict";
    public static final String STRICT_PATH_TYPING_DEFAULT = "false";
    public static final String PATH_EXTRACTOR_PROPERTY = "ion.(\\w+).path_extractor";
    public static final String PATH_EXTRACTION_CASE_SENSITIVITY = "ion.path_extractor.case_sensitive";
    public static final String PATH_EXTRACTION_CASE_SENSITIVITY_DEFAULT = "false";
    public static final String FAIL_ON_OVERFLOW_PROPERTY_WITH_COLUMN = "ion.\\w+.fail_on_overflow";
    public static final String FAIL_ON_OVERFLOW_PROPERTY = "ion.fail_on_overflow";
    public static final String FAIL_ON_OVERFLOW_PROPERTY_DEFAULT = "true";
    public static final String IGNORE_MALFORMED = "ion.ignore_malformed";
    public static final String IGNORE_MALFORMED_DEFAULT = "false";

    private static final Pattern pathExtractorPattern = Pattern.compile(PATH_EXTRACTOR_PROPERTY);

    private IonReaderOptions() {}

    public static IonDecoderConfig decoderConfigFor(Map<String, String> propertiesMap)
    {
        ImmutableMap.Builder<String, String> extractionsBuilder = ImmutableMap.builder();

        for (Map.Entry<String, String> property : propertiesMap.entrySet()) {
            Matcher matcher = pathExtractorPattern.matcher(property.getKey());
            if (matcher.matches()) {
                extractionsBuilder.put(matcher.group(1), property.getValue());
            }
        }

        Boolean strictTyping = Boolean.parseBoolean(
                propertiesMap.getOrDefault(STRICT_PATH_TYPING_PROPERTY, STRICT_PATH_TYPING_DEFAULT));
        Boolean caseSensitive = Boolean.parseBoolean(
                propertiesMap.getOrDefault(PATH_EXTRACTION_CASE_SENSITIVITY, PATH_EXTRACTION_CASE_SENSITIVITY_DEFAULT));

        // n.b.: the hive serde overwrote when there were duplicate extractors defined for a column
        return new IonDecoderConfig(extractionsBuilder.buildOrThrow(), strictTyping, caseSensitive);
    }
}
