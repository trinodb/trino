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

import com.google.common.collect.ImmutableSet;
import io.trino.hive.formats.line.Column;
import io.trino.hive.formats.line.LineDeserializer;
import io.trino.hive.formats.line.LineDeserializerFactory;

import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;
import static io.trino.hive.formats.HiveClassNames.GROK_SERDE_CLASS;

public class GrokDeserializerFactory
        implements LineDeserializerFactory
{
    static final String INPUT_FORMAT = "input.format";
    static final String INPUT_GROK_CUSTOM_PATTERNS = "input.grokCustomPatterns";
    static final String GROK_STRICT_MODE = "grok.strictMode";
    static final String GROK_NAMED_ONLY_MODE = "grok.namedOnlyMode";
    static final String GROK_NULL_ON_PARSE_ERROR = "grok.nullOnParseError";

    @Override
    public Set<String> getHiveSerDeClassNames()
    {
        return ImmutableSet.of(GROK_SERDE_CLASS);
    }

    @Override
    public LineDeserializer create(List<Column> columns, Map<String, String> serdeProperties)
    {
        String inputFormat = serdeProperties.get(INPUT_FORMAT);
        checkArgument(inputFormat != null, "Schema does not have required '%s' property", INPUT_FORMAT);
        String inputGrokCustomPatterns = serdeProperties.get(INPUT_GROK_CUSTOM_PATTERNS);
        boolean grokStrictMode = Boolean.parseBoolean(serdeProperties.getOrDefault(GROK_STRICT_MODE, "true"));
        boolean grokNamedOnlyMode = Boolean.parseBoolean(serdeProperties.getOrDefault(GROK_NAMED_ONLY_MODE, "true"));
        boolean grokNullOnParseError = Boolean.parseBoolean(serdeProperties.getOrDefault(GROK_NULL_ON_PARSE_ERROR, "false"));
        return new GrokDeserializer(columns, inputFormat, inputGrokCustomPatterns, grokStrictMode, grokNamedOnlyMode, grokNullOnParseError);
    }
}
