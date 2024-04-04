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
package io.trino.plugin.hive;

import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import io.trino.spi.TrinoException;
import io.trino.spi.session.PropertyMetadata;
import io.trino.spi.type.MapType;
import io.trino.spi.type.TypeManager;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static io.trino.spi.StandardErrorCode.INVALID_VIEW_PROPERTY;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.lang.String.format;

public class HiveViewProperties
{
    public static final String EXTRA_PROPERTIES = "extra_properties";

    private final List<PropertyMetadata<?>> viewProperties;

    @Inject
    public HiveViewProperties(TypeManager typeManager)
    {
        viewProperties = ImmutableList.of(
                new PropertyMetadata<>(
                        EXTRA_PROPERTIES,
                        "Extra view properties",
                        new MapType(VARCHAR, VARCHAR, typeManager.getTypeOperators()),
                        Map.class,
                        null,
                        true, // currently not shown in SHOW CREATE VIEW
                        value -> {
                            Map<String, String> extraProperties = (Map<String, String>) value;
                            if (extraProperties.containsValue(null)) {
                                throw new TrinoException(INVALID_VIEW_PROPERTY, format("Extra view property value cannot be null '%s'", extraProperties));
                            }
                            if (extraProperties.containsKey(null)) {
                                throw new TrinoException(INVALID_VIEW_PROPERTY, format("Extra view property key cannot be null '%s'", extraProperties));
                            }
                            return extraProperties;
                        },
                        value -> value));
    }

    public List<PropertyMetadata<?>> getViewProperties()
    {
        return viewProperties;
    }

    public static Optional<Map<String, String>> getExtraProperties(Map<String, Object> tableProperties)
    {
        return Optional.ofNullable((Map<String, String>) tableProperties.get(EXTRA_PROPERTIES));
    }
}
