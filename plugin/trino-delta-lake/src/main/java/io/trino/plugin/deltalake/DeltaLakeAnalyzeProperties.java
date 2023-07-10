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
package io.trino.plugin.deltalake;

import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import io.trino.spi.TrinoException;
import io.trino.spi.session.PropertyMetadata;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.SqlTimestampWithTimeZone;
import io.trino.spi.type.TimestampWithTimeZoneType;

import java.time.Instant;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.trino.plugin.deltalake.DeltaLakeAnalyzeProperties.AnalyzeMode.INCREMENTAL;
import static io.trino.spi.StandardErrorCode.INVALID_ANALYZE_PROPERTY;
import static io.trino.spi.session.PropertyMetadata.enumProperty;
import static io.trino.spi.type.TimeZoneKey.UTC_KEY;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.lang.String.format;

public class DeltaLakeAnalyzeProperties
{
    enum AnalyzeMode
    {
        INCREMENTAL,
        FULL_REFRESH,
    }

    public static final String FILES_MODIFIED_AFTER = "files_modified_after";
    public static final String COLUMNS_PROPERTY = "columns";
    public static final String MODE_PROPERTY = "mode";

    private final List<PropertyMetadata<?>> analyzeProperties;

    @Inject
    public DeltaLakeAnalyzeProperties()
    {
        analyzeProperties = ImmutableList.of(
                new PropertyMetadata<>(
                        FILES_MODIFIED_AFTER,
                        "Take into account only files modified after given timestamp",
                        TimestampWithTimeZoneType.TIMESTAMP_TZ_MILLIS,
                        Instant.class,
                        null,
                        false,
                        value -> ((SqlTimestampWithTimeZone) value).toZonedDateTime().toInstant(),
                        instant -> SqlTimestampWithTimeZone.fromInstant(3, instant, UTC_KEY.getZoneId())),
                new PropertyMetadata<>(
                        COLUMNS_PROPERTY,
                        "Columns to be analyzed",
                        new ArrayType(VARCHAR),
                        Set.class,
                        null,
                        false,
                        DeltaLakeAnalyzeProperties::decodeColumnNames,
                        value -> value),
                enumProperty(
                        MODE_PROPERTY,
                        "Analyze mode",
                        AnalyzeMode.class,
                        INCREMENTAL,
                        false));
    }

    public List<PropertyMetadata<?>> getAnalyzeProperties()
    {
        return analyzeProperties;
    }

    public static Optional<Instant> getFilesModifiedAfterProperty(Map<String, Object> properties)
    {
        return Optional.ofNullable((Instant) properties.get(FILES_MODIFIED_AFTER));
    }

    public static AnalyzeMode getRefreshMode(Map<String, Object> properties)
    {
        return (AnalyzeMode) properties.get(MODE_PROPERTY);
    }

    public static Optional<Set<String>> getColumnNames(Map<String, Object> properties)
    {
        @SuppressWarnings("unchecked")
        Set<String> columns = (Set<String>) properties.get(COLUMNS_PROPERTY);
        return Optional.ofNullable(columns);
    }

    private static Set<String> decodeColumnNames(Object object)
    {
        if (object == null) {
            return null;
        }

        Collection<?> columns = ((Collection<?>) object);
        return columns.stream()
                .peek(property -> throwIfNull(property, "columns"))
                .map(String.class::cast)
                .collect(toImmutableSet());
    }

    private static void throwIfNull(Object object, String propertyName)
    {
        if (object == null) {
            throw new TrinoException(INVALID_ANALYZE_PROPERTY, format("Invalid null value in analyze %s property", propertyName));
        }
    }
}
