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
package io.prestosql.plugin.redshift;

import com.google.common.collect.ImmutableList;
import io.prestosql.plugin.jdbc.TablePropertiesProvider;
import io.prestosql.spi.session.PropertyMetadata;
import io.prestosql.spi.type.TypeManager;

import javax.inject.Inject;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static io.prestosql.spi.session.PropertyMetadata.stringProperty;
import static java.util.Objects.requireNonNull;

/**
 * Class contains all table properties for the Redshift connector. Used when creating a table:
 * <p>
 * <pre>CREATE TABLE foo WITH (DISTKEY = 'col_one') as SELECT col_one, col_two FROM table;</pre>
 */
public final class RedshiftTableProperties
        implements TablePropertiesProvider
{
    private static final String DISTKEY = "distkey";
    private static final String COMPOUND_SORTKEYS = "compound_sortkeys";
    private static final String INTERLEAVED_SORTKEYS = "interleaved_sortkeys";

    private final List<PropertyMetadata<?>> tableProperties;

    @Inject
    public RedshiftTableProperties(TypeManager typeManager)
    {
        this.tableProperties = ImmutableList.of(
                stringProperty(
                        DISTKEY,
                        "The column name of the distkey column.",
                        null,
                        false),
                stringProperty(
                        COMPOUND_SORTKEYS,
                        "A column separated list of columns to use for a compound sortkey.",
                        null,
                        false),
                stringProperty(
                        INTERLEAVED_SORTKEYS,
                        "A column separated list of columns to use for an interleaved sortkey",
                        null,
                        false));
    }

    @Override
    public List<PropertyMetadata<?>> getTableProperties()
    {
        return tableProperties;
    }

    private static Optional<String> maybeGetString(Map<String, Object> tableProperties, String key)
    {
        requireNonNull(tableProperties);

        String value = (String) tableProperties.get(key);
        if (value == null) {
            return Optional.empty();
        }

        return Optional.of(value);
    }

    public static Optional<String> getDistKey(Map<String, Object> tableProperties)
    {
        return maybeGetString(tableProperties, DISTKEY);
    }

    public static Optional<String> getCompoundSortKeys(Map<String, Object> tableProperties)
    {
        return maybeGetString(tableProperties, COMPOUND_SORTKEYS);
    }

    public static Optional<String> getInterleavedSortKeys(Map<String, Object> tableProperties)
    {
        return maybeGetString(tableProperties, INTERLEAVED_SORTKEYS);
    }
}
