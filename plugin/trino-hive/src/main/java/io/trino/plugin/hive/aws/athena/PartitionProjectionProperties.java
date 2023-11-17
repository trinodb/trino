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

import static io.trino.plugin.hive.aws.athena.Projection.invalidProjectionException;
import static java.lang.String.format;

public final class PartitionProjectionProperties
{
    static final String COLUMN_PROJECTION_TYPE_SUFFIX = "type";
    static final String COLUMN_PROJECTION_VALUES_SUFFIX = "values";
    static final String COLUMN_PROJECTION_RANGE_SUFFIX = "range";
    static final String COLUMN_PROJECTION_INTERVAL_SUFFIX = "interval";
    static final String COLUMN_PROJECTION_DIGITS_SUFFIX = "digits";
    static final String COLUMN_PROJECTION_FORMAT_SUFFIX = "format";

    static final String METASTORE_PROPERTY_PROJECTION_INTERVAL_UNIT_SUFFIX = "interval.unit";

    static final String METASTORE_PROPERTY_PROJECTION_ENABLED = "projection.enabled";
    static final String METASTORE_PROPERTY_PROJECTION_LOCATION_TEMPLATE = "storage.location.template";
    static final String METASTORE_PROPERTY_PROJECTION_IGNORE = "trino.partition_projection.ignore";

    static final String PROPERTY_KEY_PREFIX = "partition_projection_";

    public static final String PARTITION_PROJECTION_ENABLED = PROPERTY_KEY_PREFIX + "enabled";
    public static final String PARTITION_PROJECTION_LOCATION_TEMPLATE = PROPERTY_KEY_PREFIX + "location_template";
    public static final String PARTITION_PROJECTION_IGNORE = PROPERTY_KEY_PREFIX + "ignore";

    public static final String COLUMN_PROJECTION_TYPE = PROPERTY_KEY_PREFIX + COLUMN_PROJECTION_TYPE_SUFFIX;
    public static final String COLUMN_PROJECTION_VALUES = PROPERTY_KEY_PREFIX + COLUMN_PROJECTION_VALUES_SUFFIX;
    public static final String COLUMN_PROJECTION_RANGE = PROPERTY_KEY_PREFIX + COLUMN_PROJECTION_RANGE_SUFFIX;
    public static final String COLUMN_PROJECTION_INTERVAL = PROPERTY_KEY_PREFIX + COLUMN_PROJECTION_INTERVAL_SUFFIX;
    public static final String COLUMN_PROJECTION_INTERVAL_UNIT = PROPERTY_KEY_PREFIX + "interval_unit";
    public static final String COLUMN_PROJECTION_DIGITS = PROPERTY_KEY_PREFIX + COLUMN_PROJECTION_DIGITS_SUFFIX;
    public static final String COLUMN_PROJECTION_FORMAT = PROPERTY_KEY_PREFIX + COLUMN_PROJECTION_FORMAT_SUFFIX;

    static String getMetastoreProjectionPropertyKey(String columnName, String propertyKeySuffix)
    {
        return "projection" + "." + columnName + "." + propertyKeySuffix;
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
                .map(decoder);
    }

    private PartitionProjectionProperties() {}
}
