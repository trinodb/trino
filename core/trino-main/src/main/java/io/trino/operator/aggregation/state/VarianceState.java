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
package io.trino.operator.aggregation.state;

import io.trino.spi.function.AccumulatorState;

import static com.google.common.base.Preconditions.checkArgument;

public interface VarianceState
        extends AccumulatorState
{
    long getCount();

    void setCount(long value);

    double getMean();

    void setMean(double value);

    double getM2();

    void setM2(double value);

    default void update(double value)
    {
        setCount(getCount() + 1);
        double delta = value - getMean();
        setMean(getMean() + delta / getCount());
        setM2(getM2() + delta * (value - getMean()));
    }

    default void merge(VarianceState otherState)
    {
        long count = otherState.getCount();
        double mean = otherState.getMean();
        double m2 = otherState.getM2();

        checkArgument(count >= 0, "count is negative");
        if (count == 0) {
            return;
        }
        long newCount = count + getCount();
        double newMean = ((count * mean) + (getCount() * getMean())) / (double) newCount;
        double delta = mean - getMean();
        setM2(getM2() + m2 + delta * delta * count * getCount() / (double) newCount);
        setCount(newCount);
        setMean(newMean);
    }
}
