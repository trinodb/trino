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

import static io.trino.spi.type.IntegerType.INTEGER;

public class TestApproximateSetGenericInteger
        extends AbstractTestApproximateSetGeneric
{
    @Override
    protected Type getValueType()
    {
        return INTEGER;
    }

    @Override
    protected Object randomValue()
    {
        return (long) ThreadLocalRandom.current().nextInt();
    }

    @Override
    protected List<Object> getResultStabilityTestSample()
    {
        return ImmutableList.of(
                (long) Integer.MIN_VALUE,
                -14343L,
                -1L,
                0L,
                1L,
                32133L,
                (long) Integer.MAX_VALUE);
    }

    @Override
    protected String getResultStabilityExpected()
    {
        // This value should not be changed; it is used to test result stability for $approx_set
        // Result stability is important because a produced value can be persisted by a connector.
        return "020C070002FEFB0AC26AC934401D8943402AD548C1D5D3538036D18501CB299F";
    }
}
