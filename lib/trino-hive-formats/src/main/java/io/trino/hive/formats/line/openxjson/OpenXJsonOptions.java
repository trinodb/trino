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
package io.trino.hive.formats.line.openxjson;

import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Map.Entry;

public class OpenXJsonOptions
{
    public static final OpenXJsonOptions DEFAULT_OPEN_X_JSON_OPTIONS = builder().build();

    static final ImmutableList<String> HIVE_SERDE_CLASS_NAMES = ImmutableList.of("org.openx.data.jsonserde.JsonSerDe");

    private static final String CASE_INSENSITIVE_KEY = "case.insensitive";
    private static final String FIELD_MAPPING_KEY_PREFIX = "mapping.";
    private static final String IGNORE_MALFORMED_JSON_KEY = "ignore.malformed.json";
    private static final String DOTS_IN_FIELD_NAMES_KEY = "dots.in.keys";
    private static final String EXPLICIT_NULL_KEY = "explicit.null";
    private static final String TIMESTAMP_FORMATS_KEY = "timestamp.formats";

    private final boolean ignoreMalformedJson;
    private final Map<String, String> fieldNameMappings;
    private final boolean caseInsensitive;
    private final boolean dotsInFieldNames;
    private final boolean explicitNull;
    private final List<String> timestampFormats;

    private OpenXJsonOptions(
            boolean ignoreMalformedJson,
            Map<String, String> fieldNameMappings,
            boolean caseInsensitive,
            boolean dotsInFieldNames,
            boolean explicitNull,
            List<String> timestampFormats)
    {
        this.ignoreMalformedJson = ignoreMalformedJson;
        this.fieldNameMappings = ImmutableMap.copyOf(fieldNameMappings);
        this.caseInsensitive = caseInsensitive;
        this.dotsInFieldNames = dotsInFieldNames;
        this.explicitNull = explicitNull;
        this.timestampFormats = ImmutableList.copyOf(timestampFormats);
    }

    public boolean isIgnoreMalformedJson()
    {
        return ignoreMalformedJson;
    }

    public Map<String, String> getFieldNameMappings()
    {
        return fieldNameMappings;
    }

    public boolean isCaseInsensitive()
    {
        return caseInsensitive;
    }

    public boolean isDotsInFieldNames()
    {
        return dotsInFieldNames;
    }

    public boolean isExplicitNull()
    {
        return explicitNull;
    }

    public List<String> getTimestampFormats()
    {
        return timestampFormats;
    }

    public Map<String, String> toSchema()
    {
        ImmutableMap.Builder<String, String> schema = ImmutableMap.builder();

        if (ignoreMalformedJson) {
            schema.put(IGNORE_MALFORMED_JSON_KEY, "true");
        }

        if (!caseInsensitive) {
            schema.put(CASE_INSENSITIVE_KEY, "false");
        }

        for (Entry<String, String> entry : fieldNameMappings.entrySet()) {
            schema.put(FIELD_MAPPING_KEY_PREFIX + entry.getKey(), entry.getValue());
        }

        if (dotsInFieldNames) {
            schema.put(DOTS_IN_FIELD_NAMES_KEY, "true");
        }

        if (explicitNull) {
            schema.put(EXPLICIT_NULL_KEY, "true");
        }

        if (!timestampFormats.isEmpty()) {
            schema.put(TIMESTAMP_FORMATS_KEY, String.join(",", timestampFormats));
        }
        return schema.buildOrThrow();
    }

    public static OpenXJsonOptions fromSchema(Map<String, String> serdeProperties)
    {
        Builder builder = builder();

        if ("true".equalsIgnoreCase(serdeProperties.get(IGNORE_MALFORMED_JSON_KEY))) {
            builder.ignoreMalformedJson();
        }

        boolean caseInsensitive = "true".equalsIgnoreCase(serdeProperties.getOrDefault(CASE_INSENSITIVE_KEY, "true"));
        if (!caseInsensitive) {
            builder.caseSensitive();
        }

        for (Entry<String, String> entry : serdeProperties.entrySet()) {
            String key = entry.getKey();
            if (key.startsWith(FIELD_MAPPING_KEY_PREFIX)) {
                String hiveField = key.substring(FIELD_MAPPING_KEY_PREFIX.length());
                String jsonField = caseInsensitive ? entry.getValue().toLowerCase(Locale.ROOT) : entry.getValue();
                builder.addFieldMapping(hiveField, jsonField);
            }
        }

        if ("true".equalsIgnoreCase(serdeProperties.get(DOTS_IN_FIELD_NAMES_KEY))) {
            builder.dotsInFieldNames();
        }

        if ("true".equalsIgnoreCase(serdeProperties.get(EXPLICIT_NULL_KEY))) {
            builder.explicitNull();
        }

        String timestampFormats = serdeProperties.get(TIMESTAMP_FORMATS_KEY);
        if (timestampFormats != null) {
            // Note there is no escaping for commas in timestamps
            builder.timestampFormats(Splitter.on(',').splitToList(timestampFormats));
        }

        return builder.build();
    }

    public static Builder builder()
    {
        return new Builder();
    }

    public static Builder builder(OpenXJsonOptions options)
    {
        return new Builder(options);
    }

    public static class Builder
    {
        private boolean ignoreMalformedJson;
        private boolean caseInsensitive = true;
        private final ImmutableMap.Builder<String, String> fieldNameMappings = ImmutableMap.builder();
        private boolean dotsInFieldNames;
        private boolean explicitNull;
        private List<String> timestampFormats = ImmutableList.of();

        public Builder() {}

        private Builder(OpenXJsonOptions options)
        {
            ignoreMalformedJson = options.isIgnoreMalformedJson();
            caseInsensitive = options.isCaseInsensitive();
            fieldNameMappings.putAll(options.getFieldNameMappings());
            dotsInFieldNames = options.isDotsInFieldNames();
            explicitNull = options.isExplicitNull();
            timestampFormats = options.getTimestampFormats();
        }

        public Builder ignoreMalformedJson()
        {
            this.ignoreMalformedJson = true;
            return this;
        }

        public Builder addFieldMapping(String hiveField, String jsonField)
        {
            this.fieldNameMappings.put(hiveField, jsonField);
            return this;
        }

        public Builder caseSensitive()
        {
            this.caseInsensitive = false;
            return this;
        }

        public Builder dotsInFieldNames()
        {
            this.dotsInFieldNames = true;
            return this;
        }

        public Builder explicitNull()
        {
            this.explicitNull = true;
            return this;
        }

        public Builder timestampFormats(String... timestampFormats)
        {
            return timestampFormats(ImmutableList.copyOf(timestampFormats));
        }

        public Builder timestampFormats(List<String> timestampFormats)
        {
            this.timestampFormats = timestampFormats;
            return this;
        }

        public OpenXJsonOptions build()
        {
            return new OpenXJsonOptions(
                    ignoreMalformedJson,
                    fieldNameMappings.buildOrThrow(),
                    caseInsensitive,
                    dotsInFieldNames,
                    explicitNull,
                    timestampFormats);
        }
    }
}
