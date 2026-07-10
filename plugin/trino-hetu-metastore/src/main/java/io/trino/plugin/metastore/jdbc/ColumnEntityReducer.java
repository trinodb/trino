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
package io.trino.plugin.metastore.jdbc;

import io.trino.spi.metastore.model.ColumnEntity;
import org.jdbi.v3.core.result.LinkedHashMapRowReducer;
import org.jdbi.v3.core.result.RowView;

import java.util.Map;

/**
 * column entity reducer
 *
 * @since 2020-03-16
 */
public class ColumnEntityReducer
        implements LinkedHashMapRowReducer<Long, ColumnEntity>
{
    @Override
    public void accumulate(Map<Long, ColumnEntity> map, RowView rowView)
    {
        ColumnEntity column = map.computeIfAbsent(rowView.getColumn("c_id", Long.class),
                id -> rowView.getRow(ColumnEntity.class));

        if (rowView.getColumn("p_id", Long.class) != null) {
            PropertyEntity param = rowView.getRow(PropertyEntity.class);
            column.getParameters().put(param.getKey(), param.getValue());
        }
    }
}
