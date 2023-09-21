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
import io.airlift.slice.Slices;
import io.trino.spi.type.Type;

import java.util.List;

import static io.trino.type.IpAddressType.IPADDRESS;
import static java.lang.Byte.MAX_VALUE;
import static java.lang.Byte.MIN_VALUE;

public class TestApproximateSetGenericIpAddress
        extends AbstractTestApproximateSetGeneric
{
    @Override
    protected Type getValueType()
    {
        return IPADDRESS;
    }

    @Override
    protected Object randomValue()
    {
        return Slices.random(16);
    }

    @Override
    protected List<Object> getResultStabilityTestSample()
    {
        return ImmutableList.of(
                Slices.wrappedBuffer(new byte[] {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15}),
                Slices.wrappedBuffer(new byte[] {10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 110, 111, 112, 113, 114, 115}),
                Slices.wrappedBuffer(new byte[] {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}),
                Slices.wrappedBuffer(MIN_VALUE, MIN_VALUE, MIN_VALUE, MIN_VALUE, MIN_VALUE, MIN_VALUE, MIN_VALUE, MIN_VALUE, MIN_VALUE, MIN_VALUE, MIN_VALUE, MIN_VALUE, MIN_VALUE, MIN_VALUE, MIN_VALUE, MIN_VALUE),
                Slices.wrappedBuffer(MAX_VALUE, MAX_VALUE, MAX_VALUE, MAX_VALUE, MAX_VALUE, MAX_VALUE, MAX_VALUE, MAX_VALUE, MAX_VALUE, MAX_VALUE, MAX_VALUE, MAX_VALUE, MAX_VALUE, MAX_VALUE, MAX_VALUE, MAX_VALUE));
    }

    @Override
    protected String getResultStabilityExpected()
    {
        // This value should not be changed; it is used to test result stability for $approx_set
        // Result stability is important because a produced value can be persisted by a connector.
        return "020C030026000000810D723B804A38DE";
    }
}
