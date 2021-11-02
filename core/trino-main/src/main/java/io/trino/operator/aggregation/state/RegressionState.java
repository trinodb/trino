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
        if (otherState.getCount() == 0) {
            return;
        }

        long na = getCount();
        long nb = otherState.getCount();
        setM2X(getM2X() + otherState.getM2X() + na * nb * Math.pow(getMeanX() - otherState.getMeanX(), 2) / (double) (na + nb));
        CovarianceState.super.merge(otherState);
    }

    default double getRegressionIntercept()
    {
        double slope = getRegressionSlope();
        double meanX = getMeanX();
        double meanY = getMeanY();

        return meanY - slope * meanX;
    }
}
