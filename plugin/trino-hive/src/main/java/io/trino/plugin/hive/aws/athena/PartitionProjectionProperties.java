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
package io.trino.plugin.hive.aws.athena;

import java.util.Map;
import java.util.Optional;
import java.util.function.Function;

import static io.trino.plugin.hive.aws.athena.projection.Projection.invalidProjectionException;
import static java.lang.String.format;

public final class PartitionProjectionProperties
{
    /**
     * General properties suffixes
     */
    static final String TABLE_PROJECTION_ENABLED_SUFFIX = "enabled";
    /**
     * Forces Trino to not use Athena table projection for a given table.
     * Kill switch to be used as a workaround if compatibility issues are found.
     */
    static final String TABLE_PROJECTION_IGNORE_SUFFIX = "ignore";
    static final String COLUMN_PROJECTION_TYPE_SUFFIX = "type";
    static final String COLUMN_PROJECTION_VALUES_SUFFIX = "values";
    static final String COLUMN_PROJECTION_RANGE_SUFFIX = "range";
    static final String COLUMN_PROJECTION_INTERVAL_SUFFIX = "interval";
    static final String COLUMN_PROJECTION_DIGITS_SUFFIX = "digits";
    static final String COLUMN_PROJECTION_FORMAT_SUFFIX = "format";

    /**
     * Metastore table properties
     */
    private static final String METASTORE_PROPERTY_SEPARATOR = ".";

    private static final String METASTORE_PROPERTY_PREFIX = "projection" + METASTORE_PROPERTY_SEPARATOR;

    static final String METASTORE_PROPERTY_PROJECTION_INTERVAL_UNIT_SUFFIX = "interval" + METASTORE_PROPERTY_SEPARATOR + "unit";

    static final String METASTORE_PROPERTY_PROJECTION_ENABLED = METASTORE_PROPERTY_PREFIX + TABLE_PROJECTION_ENABLED_SUFFIX;
    static final String METASTORE_PROPERTY_PROJECTION_LOCATION_TEMPLATE = "storage" + METASTORE_PROPERTY_SEPARATOR + "location" + METASTORE_PROPERTY_SEPARATOR + "template";
    /**
     * See {@link #TABLE_PROJECTION_IGNORE_SUFFIX } to understand duplication with enable property
     **/
    static final String METASTORE_PROPERTY_PROJECTION_IGNORE = "trino" + METASTORE_PROPERTY_SEPARATOR + "partition_projection" + METASTORE_PROPERTY_SEPARATOR + TABLE_PROJECTION_IGNORE_SUFFIX;

    /**
     * Trino table properties
     */
    private static final String PROPERTY_KEY_SEPARATOR = "_";

    static final String PROPERTY_KEY_PREFIX = "partition" + PROPERTY_KEY_SEPARATOR + "projection" + PROPERTY_KEY_SEPARATOR;

    private static final String PROPERTY_KEY_SUFFIX_COLUMN_PROJECTION_INTERVAL_UNIT = "interval" + PROPERTY_KEY_SEPARATOR + "unit";

    public static final String PARTITION_PROJECTION_ENABLED = PROPERTY_KEY_PREFIX + TABLE_PROJECTION_ENABLED_SUFFIX;
    public static final String PARTITION_PROJECTION_LOCATION_TEMPLATE = PROPERTY_KEY_PREFIX + "location" + PROPERTY_KEY_SEPARATOR + "template";
    /**
     * See {@link #TABLE_PROJECTION_IGNORE_SUFFIX } to understand duplication with enable property
     **/
    public static final String PARTITION_PROJECTION_IGNORE = PROPERTY_KEY_PREFIX + TABLE_PROJECTION_IGNORE_SUFFIX;

    public static final String COLUMN_PROJECTION_TYPE = PROPERTY_KEY_PREFIX + COLUMN_PROJECTION_TYPE_SUFFIX;
    public static final String COLUMN_PROJECTION_VALUES = PROPERTY_KEY_PREFIX + COLUMN_PROJECTION_VALUES_SUFFIX;
    public static final String COLUMN_PROJECTION_RANGE = PROPERTY_KEY_PREFIX + COLUMN_PROJECTION_RANGE_SUFFIX;
    public static final String COLUMN_PROJECTION_INTERVAL = PROPERTY_KEY_PREFIX + COLUMN_PROJECTION_INTERVAL_SUFFIX;
    public static final String COLUMN_PROJECTION_INTERVAL_UNIT = PROPERTY_KEY_PREFIX + PROPERTY_KEY_SUFFIX_COLUMN_PROJECTION_INTERVAL_UNIT;
    public static final String COLUMN_PROJECTION_DIGITS = PROPERTY_KEY_PREFIX + COLUMN_PROJECTION_DIGITS_SUFFIX;
    public static final String COLUMN_PROJECTION_FORMAT = PROPERTY_KEY_PREFIX + COLUMN_PROJECTION_FORMAT_SUFFIX;

    static String getMetastoreProjectionPropertyKey(String columnName, String propertyKeySuffix)
    {
        return METASTORE_PROPERTY_PREFIX + columnName + METASTORE_PROPERTY_SEPARATOR + propertyKeySuffix;
    }

    public static <T, I> T getProjectionPropertyRequiredValue(
            String columnName,
            Map<String, I> columnProjectionProperties,
            String propertyKey,
            Function<I, T> decoder)
    {
        return getProjectionPropertyValue(columnProjectionProperties, propertyKey, decoder)
                .orElseThrow(() -> invalidProjectionException(columnName, format("Missing required property: '%s'", propertyKey)));
    }

    public static <T, I> Optional<T> getProjectionPropertyValue(
            Map<String, I> columnProjectionProperties,
            String propertyKey,
            Function<I, T> decoder)
    {
        return Optional.ofNullable(
                        columnProjectionProperties.get(propertyKey))
                .map(value -> decoder.apply(value));
    }

    private PartitionProjectionProperties()
    {
    }
}
