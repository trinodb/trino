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
package io.trino.plugin.hsqldb;

import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import io.trino.spi.session.PropertyMetadata;

import java.util.List;
import java.util.Map;

import static io.trino.spi.type.BooleanType.BOOLEAN;
import static java.util.Objects.requireNonNull;

public class HsqlDbColumnProperties
{
    private static final String AUTOINCREMENT_PROPERTY = "autoincrement";

    private final List<PropertyMetadata<?>> columnProperties;

    @Inject
    public HsqlDbColumnProperties()
    {
        columnProperties = ImmutableList.<PropertyMetadata<?>>builder()
                .add(new PropertyMetadata<>(
                        AUTOINCREMENT_PROPERTY,
                        "Is the column autoincrement",
                        BOOLEAN,
                        Boolean.class,
                        false,
                        false,
                        value -> (Boolean) value,
                        value -> value))

                .build();
    }

    public List<PropertyMetadata<?>> getColumnProperties()
    {
        return columnProperties;
    }

    @SuppressWarnings("unchecked")
    public static boolean isAutoIncrement(Map<String, Object> columnProperties)
    {
        requireNonNull(columnProperties, "columnProperties is null");
        return columnProperties.containsKey(AUTOINCREMENT_PROPERTY) && (Boolean) columnProperties.get(AUTOINCREMENT_PROPERTY);
    }
}
