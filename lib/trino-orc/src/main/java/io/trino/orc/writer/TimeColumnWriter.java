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
package io.prestosql.orc.writer;

import io.prestosql.orc.metadata.CompressionKind;
import io.prestosql.orc.metadata.OrcColumnId;
import io.prestosql.orc.metadata.statistics.LongValueStatisticsBuilder;
import io.prestosql.spi.type.Type;

import java.util.function.Supplier;

import static io.prestosql.spi.type.Timestamps.PICOSECONDS_PER_MICROSECOND;

public class TimeColumnWriter
        extends LongColumnWriter
{
    public TimeColumnWriter(OrcColumnId columnId, Type type, CompressionKind compression, int bufferSize, Supplier<LongValueStatisticsBuilder> statisticsBuilderSupplier)
    {
        super(columnId, type, compression, bufferSize, statisticsBuilderSupplier);
    }

    @Override
    protected long transformValue(long value)
    {
        return value / PICOSECONDS_PER_MICROSECOND;
    }
}
