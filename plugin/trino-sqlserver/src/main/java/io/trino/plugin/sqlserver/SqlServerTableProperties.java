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
package io.trino.plugin.sqlserver;

import com.google.common.collect.ImmutableList;
import io.trino.plugin.jdbc.TablePropertiesProvider;
import io.trino.spi.session.PropertyMetadata;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static io.trino.spi.session.PropertyMetadata.enumProperty;

public class SqlServerTableProperties
        implements TablePropertiesProvider
{
    public static final String DATA_COMPRESSION = "data_compression";

    private final List<PropertyMetadata<?>> tableProperties = ImmutableList.of(
            enumProperty(
                    DATA_COMPRESSION,
                    "DataCompression type for table",
                    DataCompression.class,
                    null,
                    false));

    @Override
    public List<PropertyMetadata<?>> getTableProperties()
    {
        return tableProperties;
    }

    public static Optional<DataCompression> getDataCompression(Map<String, Object> tableProperties)
    {
        return Optional.ofNullable(tableProperties.get(DATA_COMPRESSION)).map(DataCompression.class::cast);
    }
}
