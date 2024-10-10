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
package io.trino.plugin.spanner;

import com.google.common.collect.ImmutableList;
import io.trino.plugin.jdbc.TablePropertiesProvider;
import io.trino.spi.session.PropertyMetadata;
import io.trino.spi.type.ArrayType;

import javax.inject.Inject;

import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.stream.Collectors;

import static io.trino.spi.session.PropertyMetadata.booleanProperty;
import static io.trino.spi.session.PropertyMetadata.stringProperty;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.util.Objects.requireNonNull;

public class SpannerTableProperties
        implements TablePropertiesProvider
{
    public static final String PRIMARY_KEYS = "primary_keys";
    public static final String NOT_NULL_FIELDS = "not_null_fields";
    public static final String COMMIT_TIMESTAMP_FIELDS = "commit_timestamp_fields";
    public static final String INTERLEAVE_IN_PARENT = "interleave_in_parent";
    public static final String ON_DELETE_CASCADE = "on_delete_cascade";
    private final ImmutableList<PropertyMetadata<?>> sessionProperties;

    @Inject
    public SpannerTableProperties()
    {
        System.out.println("CALLED TABLE PROPERTIES ");
        sessionProperties = ImmutableList.<PropertyMetadata<?>>builder()
                .add(new PropertyMetadata<>(
                        PRIMARY_KEYS,
                        "Primary keys for the table being created",
                        new ArrayType(VARCHAR),
                        List.class,
                        ImmutableList.of(),
                        false,
                        value -> (List<?>) value,
                        value -> value))
                .add(new PropertyMetadata<>(
                        NOT_NULL_FIELDS,
                        "Array of fields that should have NOT NULL constraints set on them in Spanner",
                        new ArrayType(VARCHAR),
                        List.class,
                        ImmutableList.of(),
                        false,
                        value -> (List<?>) value,
                        value -> value))
                .add(new PropertyMetadata<>(
                        COMMIT_TIMESTAMP_FIELDS,
                        "Array of timestamp fields that should have 'OPTIONS (allow_commit_timestamp=true)' constraints set on them in Spanner",
                        new ArrayType(VARCHAR),
                        List.class,
                        ImmutableList.of(),
                        false,
                        value -> (List<?>) value,
                        value -> value))
                .add(stringProperty(INTERLEAVE_IN_PARENT,
                        "Table name which needs to be interleaved with this table", null, false))
                .add(booleanProperty(ON_DELETE_CASCADE,
                        "Boolean property to cascade on delete. ON DELETE CASCADE if set to true or ON DELETE NO ACTION if false",
                        false, false))
                .build();
    }

    public static List<String> getPrimaryKey(Map<String, Object> tableProperties)
    {
        requireNonNull(tableProperties, "tableProperties is null");
        return ImmutableList.copyOf(toUpperCase((List<String>) tableProperties.get(PRIMARY_KEYS)));
    }

    public static String getInterleaveInParent(Map<String, Object> tableProperties)
    {
        requireNonNull(tableProperties, "tableProperties is null");
        return (String) tableProperties.get(INTERLEAVE_IN_PARENT);
    }

    public static List<String> getNotNullFields(Map<String, Object> tableProperties)
    {
        requireNonNull(tableProperties, "tableProperties is null");
        return ImmutableList.copyOf(toUpperCase((List<String>) tableProperties.get(NOT_NULL_FIELDS)));
    }

    public static List<String> getCommitTimestampFields(Map<String, Object> tableProperties)
    {
        requireNonNull(tableProperties, "tableProperties is null");
        return ImmutableList.copyOf(toUpperCase((List<String>) tableProperties.get(COMMIT_TIMESTAMP_FIELDS)));
    }

    public static boolean getOnDeleteCascade(Map<String, Object> tableProperties)
    {
        requireNonNull(tableProperties, "tableProperties is null");
        return (boolean) tableProperties.get(ON_DELETE_CASCADE);
    }

    private static List<String> toUpperCase(List<String> collection)
    {
        return collection.stream().map(f -> f.toUpperCase(Locale.ENGLISH)).collect(Collectors.toList());
    }

    @Override
    public List<PropertyMetadata<?>> getTableProperties()
    {
        return sessionProperties;
    }
}
