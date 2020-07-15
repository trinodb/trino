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
package io.prestosql.orc.reader;

import io.prestosql.memory.context.LocalMemoryContext;
import io.prestosql.orc.OrcColumn;
import io.prestosql.orc.OrcCorruptionException;
import io.prestosql.spi.type.Type;

import static io.prestosql.spi.type.Timestamps.PICOSECONDS_PER_MICROSECOND;

public class TimeColumnReader
        extends LongColumnReader
{
    public TimeColumnReader(Type type, OrcColumn column, LocalMemoryContext systemMemoryContext)
            throws OrcCorruptionException
    {
        super(type, column, systemMemoryContext);
    }

    @Override
    protected void maybeTransformValues(long[] values, int nextBatchSize)
    {
        for (int i = 0; i < nextBatchSize; i++) {
            values[i] *= PICOSECONDS_PER_MICROSECOND;
        }
    }
}
