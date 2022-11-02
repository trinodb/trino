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

public interface CovarianceState
        extends AccumulatorState
{
    long getCount();

    void setCount(long value);

    double getMeanX();

    void setMeanX(double value);

    double getMeanY();

    void setMeanY(double value);

    double getC2();

    void setC2(double value);

    default void update(double x, double y)
    {
        long n = getCount() + 1;
        setCount(n);
        double oldMeanX = getMeanX();
        setMeanX(oldMeanX + (x - oldMeanX) / n);
        double oldMeanY = getMeanY();
        double newMeanY = oldMeanY + (y - oldMeanY) / n;
        setMeanY(newMeanY);
        setC2(getC2() + (x - oldMeanX) * (y - newMeanY));
    }

    default void merge(CovarianceState otherState)
    {
        if (otherState.getCount() == 0) {
            return;
        }

        long na = getCount();
        long nb = otherState.getCount();
        long n = na + nb;
        setCount(n);
        double meanX = getMeanX();
        double meanY = getMeanY();
        double deltaX = otherState.getMeanX() - meanX;
        double deltaY = otherState.getMeanY() - meanY;
        setC2(getC2() + otherState.getC2() + deltaX * deltaY * na * nb / (double) n);
        setMeanX(meanX + deltaX * nb / (double) n);
        setMeanY(meanY + deltaY * nb / (double) n);
    }

    default double getCovarianceSample()
    {
        return getC2() / (getCount() - 1);
    }

    default double getCovariancePopulation()
    {
        return getC2() / getCount();
    }
}
