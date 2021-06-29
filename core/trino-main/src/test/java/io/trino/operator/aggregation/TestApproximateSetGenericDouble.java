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

import static io.trino.spi.type.DoubleType.DOUBLE;

public class TestApproximateSetGenericDouble
        extends AbstractTestApproximateSetGeneric
{
    @Override
    protected Type getValueType()
    {
        return DOUBLE;
    }

    @Override
    protected Object randomValue()
    {
        return ThreadLocalRandom.current().nextDouble();
    }

    @Override
    protected List<Object> getResultStabilityTestSample()
    {
        return ImmutableList.of(
                Double.MIN_VALUE,
                -1342434.33434,
                -15.3,
                -1.0,
                17.3,
                14343437.343434,
                Double.NaN,
                Double.MAX_VALUE,
                Double.NEGATIVE_INFINITY,
                Double.POSITIVE_INFINITY);
    }

    @Override
    protected String getResultStabilityExpected()
    {
        // This value should not be changed; it is used to test result stability for $approx_set
        // Result stability is important because a produced value can be persisted by a connector.
        return "020C0A0001D0270E02AF771D0245672BC03F8B324089747DC070307E01CB299FC36F2FA181B0ADE9409D3DFA";
    }
}
