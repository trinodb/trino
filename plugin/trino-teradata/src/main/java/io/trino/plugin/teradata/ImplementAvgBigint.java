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

package io.trino.plugin.teradata;

import io.trino.plugin.jdbc.aggregation.BaseImplementAvgBigint;

/**
 * Implements the AVG aggregation for BIGINT columns in Teradata.
 * <p>
 * Teradata uses FLOAT for double precision floating-point arithmetic.
 * This class ensures that the AVG function casts BIGINT values to FLOAT,
 * providing correct decimal division semantics.
 * </p>
 */
public class ImplementAvgBigint
        extends BaseImplementAvgBigint
{
    /**
     * Returns the SQL expression format for rewriting AVG aggregation.
     * <p>
     * The expression casts the input to FLOAT before applying AVG,
     * ensuring proper floating-point division in Teradata.
     * </p>
     *
     * @return the SQL format string for AVG aggregation
     */
    @Override
    public String getRewriteFormatExpression()
    {
        // Teradata uses FLOAT for double precision floating-point
        // CAST to FLOAT ensures proper decimal division for AVG
        return "avg(CAST(%s AS FLOAT))";
    }
}

