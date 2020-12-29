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
package io.prestosql.plugin.hive.benchmark;

import com.google.common.collect.ImmutableList;
import io.prestosql.spi.Page;
import io.prestosql.spi.type.Type;

import java.util.List;

public class TestData
{
    private final List<String> columnNames;
    private final List<Type> columnTypes;

    private final List<Page> pages;

    private final int size;

    public TestData(List<String> columnNames, List<Type> columnTypes, List<Page> pages)
    {
        this.columnNames = ImmutableList.copyOf(columnNames);
        this.columnTypes = ImmutableList.copyOf(columnTypes);
        this.pages = ImmutableList.copyOf(pages);
        this.size = (int) pages.stream().mapToLong(Page::getSizeInBytes).sum();
    }

    public List<String> getColumnNames()
    {
        return columnNames;
    }

    public List<Type> getColumnTypes()
    {
        return columnTypes;
    }

    public List<Page> getPages()
    {
        return pages;
    }

    public int getSize()
    {
        return size;
    }
}
