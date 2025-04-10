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
package io.trino.plugin.mysql;

import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import io.trino.plugin.jdbc.ColumnPropertiesProvider;
import io.trino.spi.session.PropertyMetadata;

import java.util.List;

import static io.trino.spi.session.PropertyMetadata.booleanProperty;

public final class MySqlColumnProperties
        implements ColumnPropertiesProvider
{
    public static final String AUTO_INCREMENT = "auto_increment";

    private final List<PropertyMetadata<?>> columnProperties;

    @Inject
    public MySqlColumnProperties()
    {
        columnProperties = ImmutableList.<PropertyMetadata<?>>builder()
                .add(booleanProperty(
                        AUTO_INCREMENT,
                        "Auto generate a unique identity for new rows",
                        null,
                        false))
                .build();
    }

    @Override
    public List<PropertyMetadata<?>> getColumnProperties()
    {
        return columnProperties;
    }
}
