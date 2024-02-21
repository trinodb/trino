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
package io.trino.sql.planner.rowpattern;

import com.google.common.collect.ImmutableMap;
import io.trino.sql.planner.Symbol;
import io.trino.sql.tree.Node;
import io.trino.sql.tree.SymbolReference;

import java.util.Map;
import java.util.function.BiFunction;

import static io.trino.sql.util.AstUtils.treeEqual;

public class ExpressionAndValuePointersEquivalence
{
    private ExpressionAndValuePointersEquivalence() {}

    public static boolean equivalent(ExpressionAndValuePointers left, ExpressionAndValuePointers right)
    {
        return equivalent(left, right, Symbol::equals);
    }

    public static boolean equivalent(ExpressionAndValuePointers left, ExpressionAndValuePointers right, BiFunction<Symbol, Symbol, Boolean> symbolEquivalence)
    {
        if (left.getAssignments().size() != right.getAssignments().size()) {
            return false;
        }

        for (int i = 0; i < left.getAssignments().size(); i++) {
            ValuePointer leftPointer = left.getAssignments().get(i).valuePointer();
            ValuePointer rightPointer = right.getAssignments().get(i).valuePointer();

            boolean equivalent = switch (leftPointer) {
                case ClassifierValuePointer pointer when rightPointer instanceof ClassifierValuePointer -> true;
                case MatchNumberValuePointer pointer when rightPointer instanceof MatchNumberValuePointer -> true;
                case ScalarValuePointer leftScalar
                        when rightPointer instanceof ScalarValuePointer rightScalar && symbolEquivalence.apply(leftScalar.getInputSymbol(), rightScalar.getInputSymbol()) -> true;
                case AggregationValuePointer leftAggregation
                        when rightPointer instanceof AggregationValuePointer rightAggregation && equivalent(leftAggregation, rightAggregation, symbolEquivalence) -> true;
                default -> false;
            };

            if (!equivalent) {
                return false;
            }
        }

        ImmutableMap.Builder<Symbol, Symbol> mapping = ImmutableMap.builder();
        for (int i = 0; i < left.getAssignments().size(); i++) {
            mapping.put(left.getAssignments().get(i).symbol(), right.getAssignments().get(i).symbol());
        }

        return treeEqual(left.getExpression(), right.getExpression(), mappingComparator(mapping.buildOrThrow()));
    }

    private static boolean equivalent(AggregationValuePointer left, AggregationValuePointer right, BiFunction<Symbol, Symbol, Boolean> symbolEquivalence)
    {
        if (!left.getFunction().equals(right.getFunction()) ||
                !left.getSetDescriptor().equals(right.getSetDescriptor()) ||
                left.getArguments().size() != right.getArguments().size()) {
            return false;
        }

        BiFunction<Node, Node, Boolean> comparator = subsetComparator(left.getClassifierSymbol(), left.getMatchNumberSymbol(), right.getClassifierSymbol(), right.getMatchNumberSymbol(), symbolEquivalence);
        for (int i = 0; i < left.getArguments().size(); i++) {
            if (!treeEqual(left.getArguments().get(i), right.getArguments().get(i), comparator)) {
                return false;
            }
        }

        return true;
    }

    private static BiFunction<Node, Node, Boolean> subsetComparator(
            Symbol leftClassifierSymbol,
            Symbol leftMatchNumberSymbol,
            Symbol rightClassifierSymbol,
            Symbol rightMatchNumberSymbol,
            BiFunction<Symbol, Symbol, Boolean> symbolEquivalence)
    {
        return (left, right) -> {
            if (left instanceof SymbolReference && right instanceof SymbolReference) {
                Symbol leftSymbol = Symbol.from((SymbolReference) left);
                Symbol rightSymbol = Symbol.from((SymbolReference) right);

                boolean leftIsClassifier = leftSymbol.equals(leftClassifierSymbol);
                boolean leftIsMatchNumber = leftSymbol.equals(leftMatchNumberSymbol);
                boolean rightIsClassifier = rightSymbol.equals(rightClassifierSymbol);
                boolean rightIsMatchNumber = rightSymbol.equals(rightMatchNumberSymbol);

                if (leftIsClassifier != rightIsClassifier || leftIsMatchNumber != rightIsMatchNumber) {
                    return false;
                }

                if (!leftIsClassifier && !leftIsMatchNumber) {
                    return symbolEquivalence.apply(leftSymbol, rightSymbol);
                }

                return true;
            }
            if (!left.shallowEquals(right)) {
                return false;
            }
            return null;
        };
    }

    private static BiFunction<Node, Node, Boolean> mappingComparator(Map<Symbol, Symbol> mapping)
    {
        return (left, right) -> {
            if (left instanceof SymbolReference && right instanceof SymbolReference) {
                Symbol leftSymbol = Symbol.from((SymbolReference) left);
                Symbol rightSymbol = Symbol.from((SymbolReference) right);
                return rightSymbol.equals(mapping.get(leftSymbol));
            }
            if (!left.shallowEquals(right)) {
                return false;
            }
            return null;
        };
    }
}
