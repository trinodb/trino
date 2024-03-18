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
package io.trino.plugin.varada.dispatcher.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Locale;
import java.util.Objects;

import static java.util.Objects.requireNonNull;

public class RegularColumn
        implements VaradaColumn
{
    private final String columnName;
    private final String columnId;

    public RegularColumn(String columnName)
    {
        this(columnName, columnName);
    }

    @JsonCreator
    public RegularColumn(
            @JsonProperty(COLUMN_NAME) String columnName,
            @JsonProperty(COLUMN_ID) String columnId)
    {
        this.columnName = requireNonNull(columnName).toLowerCase(Locale.ENGLISH);
        this.columnId = requireNonNull(columnId).toLowerCase(Locale.ENGLISH);
    }

    @Override
    public String getName()
    {
        return columnName;
    }

    @Override
    public String getColumnId()
    {
        return columnId;
    }

    @Override
    public int getOrder()
    {
        return 3;
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
        RegularColumn that = (RegularColumn) o;
        return Objects.equals(columnId, that.columnId);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(columnId);
    }

    @Override
    public String toString()
    {
        return "RegularColumn{" +
                "columnName='" + columnName + '\'' +
                ", columnId='" + columnId + '\'' +
                '}';
    }
}
