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

public interface CentralMomentsState
        extends AccumulatorState
{
    long getCount();

    void setCount(long value);

    double getM1();

    void setM1(double value);

    double getM2();

    void setM2(double value);

    double getM3();

    void setM3(double value);

    double getM4();

    void setM4(double value);

    default void update(double value)
    {
        long n1 = getCount();
        long n = n1 + 1;
        double m1 = getM1();
        double m2 = getM2();
        double m3 = getM3();
        double delta = value - m1;
        double deltaN = delta / n;
        double deltaN2 = deltaN * deltaN;
        double dm2 = delta * deltaN * n1;
        setCount(n);
        setM1(m1 + deltaN);
        setM2(m2 + dm2);
        setM3(m3 + dm2 * deltaN * (n - 2) - 3 * deltaN * m2);
        setM4(getM4() + dm2 * deltaN2 * (n * (double) n - 3 * n + 3) + 6 * deltaN2 * m2 - 4 * deltaN * m3);
    }

    default void merge(CentralMomentsState otherState)
    {
        long na = getCount();
        long nb = otherState.getCount();

        checkArgument(nb >= 0, "count is negative");
        if (nb == 0) {
            return;
        }

        double m1a = getM1();
        double m2a = getM2();
        double m3a = getM3();
        double m1b = otherState.getM1();
        double m2b = otherState.getM2();
        double m3b = otherState.getM3();
        double n = na + nb; // Use double as type of n to avoid integer overflow for n*n and n*n*n
        double delta = m1b - m1a;
        double delta2 = delta * delta;
        double delta3 = delta * delta2;
        double delta4 = delta2 * delta2;

        setCount((long) n);
        setM1((na * m1a + nb * m1b) / n);
        setM2(m2a + m2b + delta2 * na * nb / n);
        setM3(m3a + m3b
                + delta3 * na * nb * (na - nb) / (n * n)
                + 3 * delta * (na * m2b - nb * m2a) / n);
        setM4(getM4() + otherState.getM4()
                + delta4 * na * nb * (na * na - na * nb + nb * nb) / (n * n * n)
                + 6 * delta2 * (na * na * m2b + nb * nb * m2a) / (n * n)
                + 4 * delta * (na * m3b - nb * m3a) / n);
    }
}
