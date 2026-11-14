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
package io.trino.cli;

/**
 * Limits applied by {@link AgentTablePrinter}. A value of zero or less means unlimited.
 */
public final class AgentLimits
{
    public static final int DEFAULT_MAX_ROWS = 1000;
    public static final int DEFAULT_MAX_BYTES = 10_000;
    public static final int DEFAULT_MAX_CELL_WIDTH = 500;

    public static final AgentLimits DEFAULT = new AgentLimits(DEFAULT_MAX_ROWS, DEFAULT_MAX_BYTES, DEFAULT_MAX_CELL_WIDTH);

    private final int maxRows;
    private final int maxBytes;
    private final int maxCellWidth;

    public AgentLimits(int maxRows, int maxBytes, int maxCellWidth)
    {
        this.maxRows = maxRows;
        this.maxBytes = maxBytes;
        this.maxCellWidth = maxCellWidth;
    }

    public int maxRows()
    {
        return maxRows;
    }

    public int maxBytes()
    {
        return maxBytes;
    }

    public int maxCellWidth()
    {
        return maxCellWidth;
    }
}
