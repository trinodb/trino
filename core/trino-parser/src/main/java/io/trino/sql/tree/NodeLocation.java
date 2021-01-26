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
package io.trino.sql.tree;

import java.util.Objects;

import static com.google.common.base.Preconditions.checkArgument;

public final class NodeLocation
{
    private final int line;
    private final int column;

    public NodeLocation(int line, int column)
    {
        checkArgument(line >= 1, "line must be at least one, got: %s", line);
        checkArgument(column >= 1, "column must be at least one, got: %s", column);

        this.line = line;
        this.column = column;
    }

    public int getLineNumber()
    {
        return line;
    }

    public int getColumnNumber()
    {
        return column;
    }

    @Override
    public String toString()
    {
        return line + ":" + column;
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
        NodeLocation that = (NodeLocation) o;
        return line == that.line &&
                column == that.column;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(line, column);
    }
}
