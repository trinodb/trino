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
import io.trino.sql.planner.rowpattern.LogicalIndexExtractor.ExpressionAndValuePointers;
import io.trino.sql.tree.Node;
import io.trino.sql.tree.SymbolReference;

import java.util.Map;
import java.util.Set;
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
        if (left.getLayout().size() != right.getLayout().size()) {
            return false;
        }

        for (int i = 0; i < left.getLayout().size(); i++) {
            ValuePointer leftPointer = left.getValuePointers().get(i);
            ValuePointer rightPointer = right.getValuePointers().get(i);

            if (leftPointer.getClass() != rightPointer.getClass()) {
                return false;
            }

            if (leftPointer instanceof ScalarValuePointer) {
                if (!equivalent(
                        (ScalarValuePointer) leftPointer,
                        (ScalarValuePointer) rightPointer,
                        left.getClassifierSymbols(),
                        left.getMatchNumberSymbols(),
                        right.getClassifierSymbols(),
                        right.getMatchNumberSymbols(),
                        symbolEquivalence)) {
                    return false;
                }
            }
            else if (leftPointer instanceof AggregationValuePointer) {
                if (!equivalent((AggregationValuePointer) leftPointer, (AggregationValuePointer) rightPointer, symbolEquivalence)) {
                    return false;
                }
            }
            else {
                throw new UnsupportedOperationException("unexpected ValuePointer type: " + leftPointer.getClass().getSimpleName());
            }
        }

        ImmutableMap.Builder<Symbol, Symbol> mapping = ImmutableMap.builder();
        for (int i = 0; i < left.getLayout().size(); i++) {
            mapping.put(left.getLayout().get(i), right.getLayout().get(i));
        }

        return treeEqual(left.getExpression(), right.getExpression(), mappingComparator(mapping.buildOrThrow()));
    }

    private static boolean equivalent(
            ScalarValuePointer left,
            ScalarValuePointer right,
            Set<Symbol> leftClassifierSymbols,
            Set<Symbol> leftMatchNumberSymbols,
            Set<Symbol> rightClassifierSymbols,
            Set<Symbol> rightMatchNumberSymbols,
            BiFunction<Symbol, Symbol, Boolean> symbolEquivalence)
    {
        if (!left.getLogicalIndexPointer().equals(right.getLogicalIndexPointer())) {
            return false;
        }

        Symbol leftInputSymbol = left.getInputSymbol();
        Symbol rightInputSymbol = right.getInputSymbol();

        boolean leftIsClassifier = leftClassifierSymbols.contains(leftInputSymbol);
        boolean leftIsMatchNumber = leftMatchNumberSymbols.contains(leftInputSymbol);
        boolean rightIsClassifier = rightClassifierSymbols.contains(rightInputSymbol);
        boolean rightIsMatchNumber = rightMatchNumberSymbols.contains(rightInputSymbol);

        if (leftIsClassifier != rightIsClassifier || leftIsMatchNumber != rightIsMatchNumber) {
            return false;
        }

        if (!leftIsClassifier && !leftIsMatchNumber) {
            return symbolEquivalence.apply(leftInputSymbol, rightInputSymbol);
        }

        return true;
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
