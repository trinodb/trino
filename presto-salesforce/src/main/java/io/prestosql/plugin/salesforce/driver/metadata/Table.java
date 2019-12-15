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
package io.prestosql.plugin.salesforce.driver.metadata;

import java.io.Serializable;
import java.util.List;

public class Table
        implements Serializable
{
    private String name;
    private String comments;
    private List<Column> columns;

    public Table(String name, String comments, List<Column> columns)
    {
        this.name = name;
        this.comments = comments;
        this.columns = columns;
        for (Column c : columns) {
            c.setTable(this);
        }
    }

    public String getName()
    {
        return name;
    }

    public String getComments()
    {
        return comments;
    }

    public List<Column> getColumns()
    {
        return columns;
    }

    public Column findColumn(String columnName)
    {
        return columns.stream().filter(column -> columnName.equals(column.getName())).findFirst().orElse(null);
    }
}
