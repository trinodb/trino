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

import io.trino.spi.metastore.model.TableEntity;
import io.trino.spi.metastore.util.MetastoreUtil;
import org.jdbi.v3.core.result.RowReducer;
import org.jdbi.v3.core.result.RowView;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.stream.Stream;

/**
 * table entity reducer
 *
 * @since 2020-03-16
 */
public class TableEntityReducer
        implements RowReducer<Map<Long, TableEntity>, Map.Entry<Long, TableEntity>>
{
    @Override
    public Map<Long, TableEntity> container()
    {
        return new LinkedHashMap<>(MetastoreUtil.PARAMETER_NUMBER);
    }

    @Override
    public void accumulate(Map<Long, TableEntity> map, RowView rowView)
    {
        TableEntity table = map.computeIfAbsent(rowView.getColumn("t_id", Long.class),
                id -> rowView.getRow(TableEntity.class));

        if (rowView.getColumn("p_id", Long.class) != null) {
            PropertyEntity property = rowView.getRow(PropertyEntity.class);
            table.getParameters().put(property.getKey(), property.getValue());
        }
    }

    @Override
    public Stream<Map.Entry<Long, TableEntity>> stream(Map<Long, TableEntity> container)
    {
        return container.entrySet().stream();
    }
}
