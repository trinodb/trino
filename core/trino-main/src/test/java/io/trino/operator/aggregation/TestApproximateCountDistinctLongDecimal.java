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

import io.trino.spi.type.Int128;
import io.trino.spi.type.Type;

import java.util.concurrent.ThreadLocalRandom;

import static io.trino.spi.type.DecimalType.createDecimalType;
import static io.trino.spi.type.Decimals.MAX_PRECISION;

public class TestApproximateCountDistinctLongDecimal
        extends AbstractTestApproximateCountDistinct
{
    private static final Type LONG_DECIMAL = createDecimalType(MAX_PRECISION);

    @Override
    protected Type getValueType()
    {
        return LONG_DECIMAL;
    }

    @Override
    protected Object randomValue()
    {
        long low = ThreadLocalRandom.current().nextLong();
        long high = ThreadLocalRandom.current().nextLong();
        return Int128.valueOf(high, low);
    }
}
