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
package io.trino.plugin.varada.expression.rewrite.coordinator.connectortovarada;

import com.google.common.collect.SetMultimap;
import io.trino.plugin.base.expression.ConnectorExpressionRule;
import io.trino.plugin.varada.expression.VaradaExpression;
import io.trino.plugin.varada.metrics.MetricsManager;
import io.trino.spi.expression.Call;
import io.trino.spi.expression.FunctionName;

import java.util.Set;

public class ExperimentSupportedFunction
        extends SupportedFunctions
{
    private final SetMultimap<FunctionName, ConnectorExpressionRule<Call, VaradaExpression>> supportedFunctionsRules;

    public ExperimentSupportedFunction(MetricsManager metricsManager)
    {
        super(metricsManager);
        supportedFunctionsRules = getSupportedFunctionsRules();
        supportedFunctionsRules.put(new FunctionName("mod"), createGenericRewriter("mod(n, m)", false));
        supportedFunctionsRules.put(new FunctionName("lower"), createGenericRewriter("lower(x)", false));
    }

    @Override
    public Set<ConnectorExpressionRule<Call, VaradaExpression>> getRule(FunctionName functionName)
    {
        return supportedFunctionsRules.get(functionName);
    }
}
