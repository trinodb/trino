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
package io.trino.operator.scalar;

import io.trino.spi.block.Block;
import io.trino.spi.function.Description;
import io.trino.spi.function.ScalarFunction;
import io.trino.spi.function.SqlNullable;
import io.trino.spi.function.SqlType;
import io.trino.spi.type.StandardTypes;

import static io.trino.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.util.Failures.checkCondition;

public final class ArrayVectorFunctions
{
    private ArrayVectorFunctions() {}

    @Description("Calculates the euclidean distance between two vectors")
    @ScalarFunction
    @SqlType(StandardTypes.DOUBLE)
    public static double euclideanDistance(@SqlType("array(double)") Block first, @SqlType("array(double)") Block second)
    {
        checkCondition(first.getPositionCount() == second.getPositionCount(), INVALID_FUNCTION_ARGUMENT, "The arguments must have the same length");

        double sum = 0.0;
        for (int i = 0; i < first.getPositionCount(); i++) {
            double diff = DOUBLE.getDouble(first, i) - DOUBLE.getDouble(second, i);
            sum += diff * diff;
        }
        return Math.sqrt(sum);
    }

    @Description("Calculates the dot product between two vectors")
    @ScalarFunction
    @SqlType(StandardTypes.DOUBLE)
    public static double dotProduct(@SqlType("array(double)") Block first, @SqlType("array(double)") Block second)
    {
        checkCondition(first.getPositionCount() == second.getPositionCount(), INVALID_FUNCTION_ARGUMENT, "The arguments must have the same length");

        double dotProduct = 0.0;
        for (int i = 0; i < first.getPositionCount(); i++) {
            dotProduct += DOUBLE.getDouble(first, i) * DOUBLE.getDouble(second, i);
        }
        return dotProduct;
    }

    @Description("Calculates the cosine similarity between two vectors")
    @ScalarFunction
    @SqlType(StandardTypes.DOUBLE)
    @SqlNullable
    public static Double cosineSimilarity(@SqlType("array(double)") Block first, @SqlType("array(double)") Block second)
    {
        checkCondition(first.getPositionCount() == second.getPositionCount(), INVALID_FUNCTION_ARGUMENT, "The arguments must have the same length");

        if (first.hasNull() || second.hasNull()) {
            return null;
        }

        double firstMagnitude = 0.0;
        double secondMagnitude = 0.0;
        double dotProduct = 0.0;
        for (int i = 0; i < first.getPositionCount(); i++) {
            double firstValue = DOUBLE.getDouble(first, i);
            double secondValue = DOUBLE.getDouble(second, i);
            firstMagnitude += firstValue * firstValue;
            secondMagnitude += secondValue * secondValue;
            dotProduct += firstValue * secondValue;
        }

        checkCondition(firstMagnitude != 0 && secondMagnitude != 0, INVALID_FUNCTION_ARGUMENT, "Vector magnitude cannot be zero");
        return dotProduct / Math.sqrt(firstMagnitude * secondMagnitude);
    }

    @Description("Calculates the cosine distance between two vectors")
    @ScalarFunction
    @SqlType(StandardTypes.DOUBLE)
    @SqlNullable
    public static Double cosineDistance(@SqlType("array(double)") Block first, @SqlType("array(double)") Block second)
    {
        Double cosineSimilarity = cosineSimilarity(first, second);
        if (cosineSimilarity == null) {
            return null;
        }
        return 1.0 - cosineSimilarity;
    }
}
