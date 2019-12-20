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
package io.prestosql.plugin.phoenix;

import com.google.common.collect.ImmutableList;
import io.prestosql.spi.connector.ColumnMetadata;
import io.prestosql.spi.session.PropertyMetadata;

import javax.inject.Inject;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static io.prestosql.plugin.phoenix.PhoenixTableProperties.getRowkeys;
import static io.prestosql.spi.session.PropertyMetadata.booleanProperty;

public class PhoenixColumnProperties
{
    public static final String PRIMARY_KEY = "primary_key";

    private final List<PropertyMetadata<?>> columnProperties;

    @Inject
    public PhoenixColumnProperties()
    {
        columnProperties = ImmutableList.of(
                booleanProperty(
                        PRIMARY_KEY,
                        "True if the column is part of the primary key",
                        false,
                        false));
    }

    public List<PropertyMetadata<?>> getColumnProperties()
    {
        return columnProperties;
    }

    public static boolean isPrimaryKey(ColumnMetadata col, Map<String, Object> tableProperties)
    {
        Optional<List<String>> rowkeysTableProp = getRowkeys(tableProperties);
        if (rowkeysTableProp.isPresent()) {
            return rowkeysTableProp.get().stream().anyMatch(col.getName()::equalsIgnoreCase);
        }
        Boolean isPk = (Boolean) col.getProperties().get(PRIMARY_KEY);
        return isPk != null && isPk;
    }
}
