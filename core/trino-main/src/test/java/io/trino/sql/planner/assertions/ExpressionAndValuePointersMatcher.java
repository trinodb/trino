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
package io.trino.sql.planner.assertions;

import io.trino.sql.planner.rowpattern.AggregationValuePointer;
import io.trino.sql.planner.rowpattern.ExpressionAndValuePointers;
import io.trino.sql.planner.rowpattern.ExpressionAndValuePointers.Assignment;
import io.trino.sql.planner.rowpattern.ScalarValuePointer;

class ExpressionAndValuePointersMatcher
{
    private ExpressionAndValuePointersMatcher() {}

    public static boolean matches(ExpressionAndValuePointers expected, ExpressionAndValuePointers actual, SymbolAliases aliases)
    {
        if (expected.getAssignments().size() != actual.getAssignments().size()) {
            return false;
        }

        SymbolAliases.Builder newAliases = SymbolAliases.builder();
        for (int i = 0; i < actual.getAssignments().size(); i++) {
            Assignment actualAssignment = actual.getAssignments().get(i);
            Assignment expectedAssignment = expected.getAssignments().get(i);

            boolean match = switch (actualAssignment.valuePointer()) {
                case ScalarValuePointer actualPointer when expectedAssignment.valuePointer() instanceof ScalarValuePointer expectedPointer ->
                        aliases.get(expectedPointer.getInputSymbol().getName()).getName().equals(actualPointer.getInputSymbol().getName());
                case AggregationValuePointer actualPointer when expectedAssignment.valuePointer() instanceof AggregationValuePointer expectedPointer -> {
                    if (!expectedPointer.getFunction().equals(actualPointer.getFunction()) ||
                            !expectedPointer.getSetDescriptor().equals(actualPointer.getSetDescriptor()) ||
                            !actualPointer.getMatchNumberSymbol().equals(expectedPointer.getMatchNumberSymbol().map(symbol -> aliases.getSymbol(symbol.getName()))) ||
                            !actualPointer.getClassifierSymbol().equals(expectedPointer.getClassifierSymbol().map(symbol -> aliases.getSymbol(symbol.getName()))) ||
                            expectedPointer.getArguments().size() != actualPointer.getArguments().size()) {
                        yield false;
                    }

                    for (int j = 0; j < expectedPointer.getArguments().size(); j++) {
                        if (!actualPointer.getArguments().get(i).equals(aliases.rewrite(expectedPointer.getArguments().get(i)))) {
                            yield false;
                        }
                    }

                    yield true;
                }
                default -> actualAssignment.valuePointer().equals(expectedAssignment.valuePointer());
            };

            if (!match) {
                return false;
            }

            newAliases.put(expectedAssignment.symbol().getName(), actualAssignment.symbol().toSymbolReference());
        }

        if (!actual.getExpression().equals(newAliases.build().rewrite(expected.getExpression()))) {
            return false;
        }

        return true;
    }
}
