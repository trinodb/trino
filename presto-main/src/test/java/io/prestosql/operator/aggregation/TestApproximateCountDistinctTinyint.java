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
package io.prestosql.operator.aggregation;

import io.prestosql.spi.type.Type;
import io.prestosql.sql.tree.QualifiedName;

import java.util.concurrent.ThreadLocalRandom;

import static io.prestosql.spi.type.DoubleType.DOUBLE;
import static io.prestosql.spi.type.TinyintType.TINYINT;
import static io.prestosql.sql.analyzer.TypeSignatureProvider.fromTypes;

public class TestApproximateCountDistinctTinyint
        extends AbstractTestApproximateCountDistinct
{
    @Override
    protected InternalAggregationFunction getAggregationFunction()
    {
        return metadata.getAggregateFunctionImplementation(metadata.resolveFunction(QualifiedName.of("approx_distinct"), fromTypes(TINYINT, DOUBLE)));
    }

    @Override
    protected Type getValueType()
    {
        return TINYINT;
    }

    @Override
    protected Object randomValue()
    {
        return ThreadLocalRandom.current().nextLong(Byte.MIN_VALUE, Byte.MAX_VALUE + 1);
    }

    @Override
    protected int getUniqueValuesCount()
    {
        return (Byte.MAX_VALUE - Byte.MIN_VALUE + 1) / 10;
    }
}
