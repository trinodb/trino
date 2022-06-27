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
package io.trino.orc;

import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slices;
import io.trino.spi.type.LongTimestampWithTimeZone;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.TimestampWithTimeZoneType;
import org.joda.time.DateTimeZone;
import org.testng.annotations.Test;

import static com.google.common.io.Resources.getResource;
import static com.google.common.io.Resources.toByteArray;
import static io.trino.memory.context.AggregatedMemoryContext.newSimpleAggregatedMemoryContext;
import static io.trino.orc.OrcReader.INITIAL_BATCH_SIZE;
import static io.trino.spi.type.TimestampWithTimeZoneType.createTimestampWithTimeZoneType;
import static org.assertj.core.api.Assertions.assertThat;

public class TestTimestampTzMicros
{
    /**
     * Test for incorrect rounding when reading timestamp_tz_micros: https://github.com/trinodb/trino/pull/12804
     */
    @Test
    public void test()
            throws Exception
    {
        OrcReader orcReader = OrcReader.createOrcReader(new MemoryOrcDataSource(
                                new OrcDataSourceId("memory"),
                                Slices.wrappedBuffer(toByteArray(getResource("timestamp-tz-micros.orc")))),
                        new OrcReaderOptions())
                .orElseThrow(() -> new RuntimeException("File is empty"));

        TimestampWithTimeZoneType timestampTzType = createTimestampWithTimeZoneType(6);

        try (OrcRecordReader reader = orcReader.createRecordReader(
                orcReader.getRootColumn().getNestedColumns(),
                ImmutableList.of(timestampTzType, TimestampType.createTimestampType(6)),
                OrcPredicate.TRUE,
                DateTimeZone.UTC,
                newSimpleAggregatedMemoryContext(),
                INITIAL_BATCH_SIZE,
                RuntimeException::new)) {
            assertThat(timestampTzType.getObject(reader.nextPage().getBlock(0), 0))
                    .isEqualTo(LongTimestampWithTimeZone.fromEpochMillisAndFraction(1654809982754L, 0, (short) 0));
        }
    }
}
