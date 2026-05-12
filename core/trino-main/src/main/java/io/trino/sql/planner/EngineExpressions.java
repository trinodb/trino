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
package io.trino.sql.planner;

import io.trino.spi.expression.FunctionName;

/**
 * Utilities for the {@code $engine_expression} synthetic function, which wraps an engine IR
 * expression as an opaque {@link io.trino.spi.expression.ConnectorExpression} payload for
 * connectors that support the {@link io.trino.spi.connector.ConnectorExpressionEvaluator} SPI.
 */
public final class EngineExpressions
{
    public static final FunctionName ENGINE_EXPRESSION_FUNCTION_NAME = new FunctionName("$engine_expression");

    private EngineExpressions() {}
}
