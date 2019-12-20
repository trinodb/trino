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

package io.prestosql.plugin.influx;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.collect.ImmutableList;
import io.prestosql.spi.connector.RecordCursor;
import io.prestosql.spi.connector.RecordSet;
import io.prestosql.spi.type.DateTimeEncoding;
import io.prestosql.spi.type.TimeZoneKey;
import io.prestosql.spi.type.Type;

import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class InfluxRecordSet
    implements RecordSet
{

    private final List<InfluxColumn> columns;
    private final List<Type> columnTypes;
    private final List<Object[]> rows;

    public InfluxRecordSet(List<InfluxColumn> columns, JsonNode results)
    {
        this.columns = columns;
        ImmutableList.Builder<Type> columnTypes = new ImmutableList.Builder<>();
        Map<String, Integer> mapping = new HashMap<>();
        for (InfluxColumn column : columns) {
            columnTypes.add(column.getType());
            mapping.put(column.getInfluxName(), mapping.size());
        }
        this.columnTypes = columnTypes.build();
        this.rows = new ArrayList<>();
        final int IGNORE = -1;
        for (JsonNode series : results) {
            if (!series.has("values")) {
                continue;
            }
            // we can't push down group-bys so we have no tags to consider
            JsonNode header = series.get("columns");
            int[] fields = new int[header.size()];
            for (int i = 0; i < fields.length; i++) {
                fields[i] = mapping.getOrDefault(header.get(i).textValue(), IGNORE);
            }
            for (JsonNode values : series.get("values")) {
                Object[] row = new Object[columns.size()];
                for (int i = 0; i < fields.length; i++) {
                    int slot = fields[i];
                    if (slot != IGNORE) {
                        final Object value;
                        JsonNode node = values.get(i);
                        if (node.isNull()) {
                            value = null;
                        }
                        else {
                            switch (columns.get(slot).getInfluxType()) {
                                case "string":
                                    value = node.textValue();
                                    break;
                                case "boolean":
                                    value = node.booleanValue();
                                    break;
                                case "integer":
                                    value = node.longValue();
                                    break;
                                case "float":
                                    value = node.doubleValue();
                                    break;
                                case "time":
                                    Instant timestamp = Instant.parse(node.textValue());
                                    value = DateTimeEncoding.packDateTimeWithZone(timestamp.toEpochMilli(), TimeZoneKey.UTC_KEY);
                                    break;
                                default:
                                    InfluxError.GENERAL.fail("cannot map " + node + " to " + columns.get(slot));
                                    value = null;
                            }
                        }
                        row[slot] = value;
                    }
                }
                rows.add(row);
            }
        }
    }

    @Override
    public List<Type> getColumnTypes()
    {
        return columnTypes;
    }

    @Override
    public RecordCursor cursor()
    {
        return new InfluxRecordCursor(columns, rows);
    }
}
