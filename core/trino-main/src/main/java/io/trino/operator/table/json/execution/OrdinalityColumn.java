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
package io.trino.operator.table.json.execution;

import com.fasterxml.jackson.databind.JsonNode;
import io.trino.spi.Page;

public class OrdinalityColumn
        implements Column
{
    private final int outputIndex;

    public OrdinalityColumn(int outputIndex)
    {
        this.outputIndex = outputIndex;
    }

    @Override
    public Object evaluate(long sequentialNumber, JsonNode item, Page input, int position)
    {
        return sequentialNumber;
    }

    @Override
    public int getOutputIndex()
    {
        return outputIndex;
    }
}
