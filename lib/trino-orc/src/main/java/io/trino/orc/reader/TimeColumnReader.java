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
package io.trino.orc.reader;

import io.trino.memory.context.LocalMemoryContext;
import io.trino.orc.OrcColumn;
import io.trino.orc.OrcCorruptionException;
import io.trino.spi.type.Type;

import static io.trino.spi.type.Timestamps.PICOSECONDS_PER_MICROSECOND;

public class TimeColumnReader
        extends LongColumnReader
{
    public TimeColumnReader(Type type, OrcColumn column, LocalMemoryContext memoryContext)
            throws OrcCorruptionException
    {
        super(type, column, memoryContext);
    }

    @Override
    protected void maybeTransformValues(long[] values, int nextBatchSize)
    {
        for (int i = 0; i < nextBatchSize; i++) {
            values[i] *= PICOSECONDS_PER_MICROSECOND;
        }
    }
}
