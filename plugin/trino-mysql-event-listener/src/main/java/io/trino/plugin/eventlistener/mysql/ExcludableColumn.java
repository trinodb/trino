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
package io.trino.plugin.eventlistener.mysql;

import jakarta.annotation.Nullable;

import java.util.Map;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static java.util.Arrays.stream;
import static java.util.Locale.ENGLISH;

enum ExcludableColumn
{
    CLIENT_INFO("client_info", null),
    CLIENT_TAGS_JSON("client_tags_json", "[]"),
    FAILURE_MESSAGE("failure_message", null),
    FAILURES_JSON("failures_json", null),
    INPUTS_JSON("inputs_json", "[]"),
    OPERATOR_SUMMARIES_JSON("operator_summaries_json", "[]"),
    OUTPUT_JSON("output_json", null),
    PLAN("plan", null),
    PREPARED_QUERY("prepared_query", null),
    QUERY("query", ""),
    SESSION_PROPERTIES_JSON("session_properties_json", "{}"),
    STAGE_INFO_JSON("stage_info_json", null),
    WARNINGS_JSON("warnings_json", "[]");

    private static final Map<String, ExcludableColumn> COLUMNS = stream(values())
            .collect(toImmutableMap(ExcludableColumn::columnName, column -> column));

    private final String columnName;
    @Nullable
    private final String excludedValue;

    ExcludableColumn(String columnName, @Nullable String excludedValue)
    {
        this.columnName = columnName;
        this.excludedValue = excludedValue;
    }

    public String columnName()
    {
        return columnName;
    }

    public Optional<String> excludedValue()
    {
        return Optional.ofNullable(excludedValue);
    }

    public static boolean isSupported(String columnName)
    {
        return COLUMNS.containsKey(normalize(columnName));
    }

    public static ExcludableColumn fromColumnName(String columnName)
    {
        ExcludableColumn column = COLUMNS.get(normalize(columnName));
        checkArgument(column != null, "Unsupported MySQL event listener column: %s", columnName);
        return column;
    }

    public static String normalize(String columnName)
    {
        return columnName.trim().toLowerCase(ENGLISH);
    }
}
