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
import com.google.common.primitives.Booleans;
import io.trino.spi.type.Type;
import org.junit.jupiter.api.Test;

import java.util.concurrent.ThreadLocalRandom;

import static io.trino.spi.type.BooleanType.BOOLEAN;

public class TestApproximateCountDistinctBoolean
        extends AbstractTestApproximateCountDistinct
{
    @Override
    protected Type getValueType()
    {
        return BOOLEAN;
    }

    @Override
    protected Object randomValue()
    {
        return ThreadLocalRandom.current().nextBoolean();
    }

    @Test
    public void testNonEmptyInputs()
    {
        assertCount(Booleans.asList(true), 0, 1);
        assertCount(Booleans.asList(false), 0, 1);
        assertCount(Booleans.asList(true, false), 0, 2);
        assertCount(Booleans.asList(true, true, true), 0, 1);
        assertCount(Booleans.asList(false, false, false), 0, 1);
        assertCount(Booleans.asList(true, false, true, false), 0, 2);
    }

    @Test
    public void testNoInput()
    {
        assertCount(ImmutableList.of(), 0, 0);
    }

    @Override
    protected int getUniqueValuesCount()
    {
        return 2;
    }
}
