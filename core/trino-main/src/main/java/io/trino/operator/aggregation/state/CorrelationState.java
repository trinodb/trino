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

public interface CorrelationState
        extends CovarianceState
{
    double getM2X();

    void setM2X(double value);

    double getM2Y();

    void setM2Y(double value);

    @Override
    default void update(double x, double y)
    {
        double oldMeanX = getMeanX();
        double oldMeanY = getMeanY();
        CovarianceState.super.update(x, y);
        setM2X(getM2X() + (x - oldMeanX) * (x - getMeanX()));
        setM2Y(getM2Y() + (y - oldMeanY) * (y - getMeanY()));
    }

    default void merge(CorrelationState otherState)
    {
        if (otherState.getCount() == 0) {
            return;
        }

        long na = getCount();
        long nb = otherState.getCount();
        setM2X(getM2X() + otherState.getM2X() + na * nb * Math.pow(getMeanX() - otherState.getMeanX(), 2) / (double) (na + nb));
        setM2Y(getM2Y() + otherState.getM2Y() + na * nb * Math.pow(getMeanY() - otherState.getMeanY(), 2) / (double) (na + nb));
        CovarianceState.super.merge(otherState);
    }

    default double getCorrelation()
    {
        // This is defined as covariance(x, y) / (stdev(x) * stdev(y))
        double covariance = getC2();
        double stdevX = Math.sqrt(getM2X());
        double stdevY = Math.sqrt(getM2Y());

        // stdevX and stdevY deliberately not checked for zero because the result can be Infinity or NaN even
        // if they are both not zero
        return covariance / stdevX / stdevY;
    }
}
