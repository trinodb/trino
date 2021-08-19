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
            if (!left.getValuePointers().get(i).getLogicalIndexPointer().equals(right.getValuePointers().get(i).getLogicalIndexPointer())) {
                return false;
            }
        }

        ImmutableMap.Builder<Symbol, Symbol> mapping = ImmutableMap.builder();
        for (int i = 0; i < left.getLayout().size(); i++) {
            Symbol leftLayoutSymbol = left.getLayout().get(i);
            boolean leftIsClassifier = left.getClassifierSymbols().contains(leftLayoutSymbol);
            boolean leftIsMatchNumber = left.getMatchNumberSymbols().contains(leftLayoutSymbol);

            Symbol rightLayoutSymbol = right.getLayout().get(i);
            boolean rightIsClassifier = right.getClassifierSymbols().contains(rightLayoutSymbol);
            boolean rightIsMatchNumber = right.getMatchNumberSymbols().contains(rightLayoutSymbol);

            if (leftIsClassifier != rightIsClassifier || leftIsMatchNumber != rightIsMatchNumber) {
                return false;
            }

            if (!leftIsClassifier && !leftIsMatchNumber) {
                Symbol leftInputSymbol = left.getValuePointers().get(i).getInputSymbol();
                Symbol rightInputSymbol = right.getValuePointers().get(i).getInputSymbol();
                if (!symbolEquivalence.apply(leftInputSymbol, rightInputSymbol)) {
                    return false;
                }
            }

            mapping.put(leftLayoutSymbol, rightLayoutSymbol);
        }

        return treeEqual(left.getExpression(), right.getExpression(), mappingComparator(mapping.build()));
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
