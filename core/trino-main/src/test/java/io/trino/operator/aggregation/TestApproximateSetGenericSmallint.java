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
import io.trino.spi.type.Type;

import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

import static io.trino.spi.type.SmallintType.SMALLINT;

public class TestApproximateSetGenericSmallint
        extends AbstractTestApproximateSetGeneric
{
    @Override
    protected Type getValueType()
    {
        return SMALLINT;
    }

    @Override
    protected Object randomValue()
    {
        return ThreadLocalRandom.current().nextLong(Short.MIN_VALUE, Short.MAX_VALUE + 1);
    }

    @Override
    protected List<Object> getResultStabilityTestSample()
    {
        return ImmutableList.of(
                (long) Short.MIN_VALUE,
                -30000L,
                -1L,
                0L,
                1L,
                30000L,
                (long) Short.MAX_VALUE);
    }

    @Override
    protected String getResultStabilityExpected()
    {
        // This value should not be changed; it is used to test result stability for $approx_set
        // Result stability is important because a produced value can be persisted by a connector.
        return "020C0700C26AC934420D105F001A4F738036D18501CB299F835E98ADC389AFE7";
    }
}
