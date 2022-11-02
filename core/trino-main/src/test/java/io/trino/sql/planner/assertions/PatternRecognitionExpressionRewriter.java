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

import com.google.common.collect.ImmutableList;
import io.trino.spi.type.Type;
import io.trino.sql.parser.ParsingOptions;
import io.trino.sql.parser.SqlParser;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.SymbolAllocator;
import io.trino.sql.planner.rowpattern.LogicalIndexExtractor;
import io.trino.sql.planner.rowpattern.LogicalIndexExtractor.ExpressionAndValuePointers;
import io.trino.sql.planner.rowpattern.ir.IrLabel;
import io.trino.sql.tree.DereferenceExpression;
import io.trino.sql.tree.Expression;
import io.trino.sql.tree.ExpressionRewriter;
import io.trino.sql.tree.ExpressionTreeRewriter;
import io.trino.sql.tree.GenericDataType;
import io.trino.sql.tree.Identifier;
import io.trino.sql.tree.LabelDereference;
import io.trino.sql.tree.LambdaExpression;
import io.trino.sql.tree.RowDataType;
import io.trino.sql.tree.SymbolReference;

import java.util.Map;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static io.trino.SessionTestUtils.TEST_SESSION;
import static io.trino.metadata.MetadataManager.createTestMetadataManager;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.sql.analyzer.ExpressionTreeUtils.extractExpressions;

public class PatternRecognitionExpressionRewriter
{
    private PatternRecognitionExpressionRewriter() {}

    public static ExpressionAndValuePointers rewrite(String definition, Map<IrLabel, Set<IrLabel>> subsets)
    {
        return rewrite(new SqlParser().createExpression(definition, new ParsingOptions()), subsets);
    }

    public static ExpressionAndValuePointers rewrite(Expression definition, Map<IrLabel, Set<IrLabel>> subsets)
    {
        Expression expression = rewriteIdentifiers(definition);
        Map<Symbol, Type> types = extractExpressions(ImmutableList.of(expression), SymbolReference.class).stream()
                .collect(toImmutableMap(Symbol::from, reference -> BIGINT));
        return LogicalIndexExtractor.rewrite(expression, subsets, new SymbolAllocator(types), TEST_SESSION, createTestMetadataManager());
    }

    private static Expression rewriteIdentifiers(Expression expression)
    {
        return ExpressionTreeRewriter.rewriteWith(new ExpressionRewriter<>()
        {
            @Override
            public Expression rewriteDereferenceExpression(DereferenceExpression node, Void context, ExpressionTreeRewriter<Void> treeRewriter)
            {
                checkArgument(node.getBase() instanceof Identifier, "chained dereferences not supported");
                return new LabelDereference(
                        ((Identifier) node.getBase()).getCanonicalValue(),
                        node.getField().map(identifier -> new SymbolReference(identifier.getValue())));
            }

            @Override
            public Expression rewriteIdentifier(Identifier node, Void context, ExpressionTreeRewriter<Void> treeRewriter)
            {
                return new SymbolReference(node.getValue());
            }

            @Override
            public Expression rewriteLambdaExpression(LambdaExpression node, Void context, ExpressionTreeRewriter<Void> treeRewriter)
            {
                return new LambdaExpression(node.getArguments(), treeRewriter.rewrite(node.getBody(), context));
            }

            @Override
            public Expression rewriteGenericDataType(GenericDataType node, Void context, ExpressionTreeRewriter<Void> treeRewriter)
            {
                // do not rewrite identifiers within type parameters
                return node;
            }

            @Override
            public Expression rewriteRowDataType(RowDataType node, Void context, ExpressionTreeRewriter<Void> treeRewriter)
            {
                // do not rewrite identifiers in field names
                return node;
            }
        }, expression);
    }
}
