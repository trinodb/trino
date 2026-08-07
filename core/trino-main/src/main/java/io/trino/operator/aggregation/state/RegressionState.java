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

public interface RegressionState
        extends CovarianceState
{
    double getM2X();

    void setM2X(double value);

    @Override
    default void update(double x, double y)
    {
        double oldMeanX = getMeanX();
        CovarianceState.super.update(x, y);
        setM2X(getM2X() + (x - oldMeanX) * (x - getMeanX()));
    }

    default double getRegressionSlope()
    {
        double dividend = getC2();
        double divisor = getM2X();

        // divisor deliberately not checked for zero because the result can be Infty or NaN even if it is not zero
        return dividend / divisor;
    }

    default void merge(RegressionState otherState)
    {
        merge(otherState.getCount(), otherState.getMeanX(), otherState.getMeanY(), otherState.getC2(), otherState.getM2X());
    }

    default void merge(long count, double otherMeanX, double otherMeanY, double c2, double m2X)
    {
        if (count == 0) {
            return;
        }

        long na = getCount();
        long nb = count;
        setM2X(getM2X() + m2X + na * nb * Math.pow(getMeanX() - otherMeanX, 2) / (double) (na + nb));
        merge(count, otherMeanX, otherMeanY, c2);
    }

    default double getRegressionIntercept()
    {
        double slope = getRegressionSlope();
        double meanX = getMeanX();
        double meanY = getMeanY();

        return meanY - slope * meanX;
    }
}
