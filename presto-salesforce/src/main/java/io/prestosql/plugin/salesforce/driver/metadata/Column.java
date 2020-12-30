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

public class Column
        implements Serializable
{
    private Table table;
    private String name;
    private String type;
    private String referencedTable;
    private String referencedColumn;

    private Integer length;
    private boolean nillable;
    private String comments;
    private boolean calculated;

    public Column(String name, String type)
    {
        this.name = name;
        this.type = type;
    }

    public Table getTable()
    {
        return table;
    }

    public void setTable(Table table)
    {
        this.table = table;
    }

    public String getName()
    {
        return name;
    }

    public String getType()
    {
        return type;
    }

    public String getReferencedTable()
    {
        return referencedTable;
    }

    public void setReferencedTable(String referencedTable)
    {
        this.referencedTable = referencedTable;
    }

    public String getReferencedColumn()
    {
        return referencedColumn;
    }

    public void setReferencedColumn(String referencedColumn)
    {
        this.referencedColumn = referencedColumn;
    }

    public Integer getLength()
    {
        return length;
    }

    public void setLength(Integer length)
    {
        this.length = length;
    }

    public boolean isNillable()
    {
        return nillable;
    }

    public void setNillable(boolean nillable)
    {
        this.nillable = nillable;
    }

    public String getComments()
    {
        return comments;
    }

    public void setComments(String comments)
    {
        this.comments = comments;
    }

    public boolean isCalculated()
    {
        return calculated;
    }

    public void setCalculated(boolean calculated)
    {
        this.calculated = calculated;
    }
}
