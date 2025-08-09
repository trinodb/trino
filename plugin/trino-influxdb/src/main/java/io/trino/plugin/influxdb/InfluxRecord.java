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

package io.trino.plugin.influxdb;

import java.util.List;
import java.util.Objects;

public class InfluxRecord
{
    private final List<String> columns;
    private final List<List<Object>> values;

    public InfluxRecord(List<String> columns, List<List<Object>> values)
    {
        this.columns = columns;
        this.values = values;
    }

    public List<String> getColumns()
    {
        return columns;
    }

    public List<List<Object>> getValues()
    {
        return values;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        InfluxRecord that = (InfluxRecord) o;
        return Objects.equals(columns, that.columns) && Objects.equals(values, that.values);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(columns, values);
    }
}
