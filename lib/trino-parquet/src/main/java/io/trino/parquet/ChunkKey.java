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
package io.trino.parquet;

import java.util.Objects;

import static java.lang.String.format;

public class ChunkKey
{
    private final int column;
    private final int rowGroup;

    public ChunkKey(int column, int rowGroup)
    {
        this.column = column;
        this.rowGroup = rowGroup;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(column, rowGroup);
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        ChunkKey other = (ChunkKey) obj;
        return Objects.equals(this.column, other.column)
                && Objects.equals(this.rowGroup, other.rowGroup);
    }

    @Override
    public String toString()
    {
        return format("[rowGroup=%s, column=%s]", rowGroup, column);
    }
}
