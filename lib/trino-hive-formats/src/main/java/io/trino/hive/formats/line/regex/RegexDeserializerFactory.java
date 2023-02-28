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
package io.trino.hive.formats.line.regex;

import com.google.common.collect.ImmutableList;
import io.trino.hive.formats.line.Column;
import io.trino.hive.formats.line.LineDeserializer;
import io.trino.hive.formats.line.LineDeserializerFactory;

import java.util.List;
import java.util.Map;

import static com.google.common.base.Preconditions.checkArgument;

public class RegexDeserializerFactory
        implements LineDeserializerFactory
{
    static final String REGEX_KEY = "input.regex";
    static final String REGEX_CASE_SENSITIVE_KEY = "input.regex.case.insensitive";

    @Override
    public List<String> getHiveSerDeClassNames()
    {
        return ImmutableList.of("org.apache.hadoop.hive.serde2.RegexSerDe");
    }

    @Override
    public LineDeserializer create(List<Column> columns, Map<String, String> serdeProperties)
    {
        String regex = serdeProperties.get(REGEX_KEY);
        checkArgument(regex != null, "Schema does not have required '%s' property", REGEX_KEY);
        boolean caseSensitive = Boolean.parseBoolean(serdeProperties.get(REGEX_CASE_SENSITIVE_KEY));
        return new RegexDeserializer(columns, regex, caseSensitive);
    }
}
