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
package io.trino.operator.aggregation;

import com.google.common.collect.ImmutableList;
import io.trino.spi.type.LongTimestamp;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.Type;

import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

public class TestApproximateSetGenericLongTimestamp
        extends AbstractTestApproximateSetGeneric
{
    @Override
    protected Type getValueType()
    {
        return TimestampType.createTimestampType(9);
    }

    @Override
    protected Object randomValue()
    {
        return new LongTimestamp(ThreadLocalRandom.current().nextLong(), ThreadLocalRandom.current().nextInt(1000));
    }

    @Override
    protected List<Object> getResultStabilityTestSample()
    {
        return ImmutableList.of(
                new LongTimestamp(0, 0),
                new LongTimestamp(1000, 4434),
                new LongTimestamp(3323232332L, 343443),
                new LongTimestamp(43434, 4343),
                new LongTimestamp(2, 10),
                new LongTimestamp(60, 545));
    }

    @Override
    protected String getResultStabilityExpected()
    {
        // This value should not be changed; it is used to test result stability for $approx_set
        // Result stability is important because a produced value can be persisted by a connector.
        return "020C060026000000C0FB0651C2B3EE6640E0727B0138E2D1034E9CF2";
    }
}
