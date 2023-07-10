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
package io.trino.spi;

import com.google.errorprone.annotations.Immutable;

@Immutable
public class Location
{
    private final int lineNumber;
    private final int columnNumber;

    public Location(int lineNumber, int columnNumber)
    {
        if (lineNumber < 1) {
            throw new IllegalArgumentException("lineNumber must be at least one");
        }

        if (columnNumber < 1) {
            throw new IllegalArgumentException("columnNumber must be at least one");
        }

        this.lineNumber = lineNumber;
        this.columnNumber = columnNumber;
    }

    public int getLineNumber()
    {
        return lineNumber;
    }

    public int getColumnNumber()
    {
        return columnNumber;
    }

    @Override
    public String toString()
    {
        return lineNumber + ":" + columnNumber;
    }
}
