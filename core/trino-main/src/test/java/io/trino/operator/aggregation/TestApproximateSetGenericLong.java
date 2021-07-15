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

import static io.trino.spi.type.BigintType.BIGINT;

public class TestApproximateSetGenericLong
        extends AbstractTestApproximateSetGeneric
{
    @Override
    protected Type getValueType()
    {
        return BIGINT;
    }

    @Override
    protected Object randomValue()
    {
        return ThreadLocalRandom.current().nextLong();
    }

    @Override
    protected List<Object> getResultStabilityTestSample()
    {
        return ImmutableList.of(
                Long.MIN_VALUE,
                -143434343433L,
                -1L,
                0L,
                1L,
                3213343434343L,
                Long.MAX_VALUE);
    }

    @Override
    protected String getResultStabilityExpected()
    {
        // This value should not be changed; it is used to test result stability for $approx_set
        // Result stability is important because a produced value can be persisted by a connector.
        return "020C07004041351CC26AC934805E423F40A5D37B8036D18501CB299F40CC70FF";
    }
}
