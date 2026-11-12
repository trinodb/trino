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
package io.trino.sql.analyzer;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.trino.sql.tree.Expression;
import io.trino.sql.tree.Identifier;
import io.trino.sql.tree.LambdaArgumentDeclaration;
import io.trino.sql.tree.LambdaExpression;
import io.trino.sql.tree.Node;
import io.trino.sql.tree.NodeRef;
import io.trino.sql.tree.SubqueryExpression;

import java.util.List;
import java.util.Set;

import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.trino.sql.tree.AstTraversal.forEachChild;
import static java.util.Objects.requireNonNull;

/**
 * Extract expressions that are free (unbound) references to a lambda argument.
 */
public final class FreeLambdaReferenceExtractor
{
    private FreeLambdaReferenceExtractor() {}

    public static boolean hasFreeReferencesToLambdaArgument(Node node, Analysis analysis)
    {
        return !getFreeReferencesToLambdaArgument(node, analysis).isEmpty();
    }

    public static List<Expression> getFreeReferencesToLambdaArgument(Node node, Analysis analysis)
    {
        Extractor extractor = new Extractor(analysis);
        extractor.process(node, ImmutableSet.of());
        return extractor.getFreeReferencesToLambdaArgument();
    }

    private static class Extractor
    {
        private final Analysis analysis;
        private final ImmutableList.Builder<Expression> freeReferencesToLambdaArgument = ImmutableList.builder();

        private Extractor(Analysis analysis)
        {
            this.analysis = requireNonNull(analysis, "analysis is null");
        }

        List<Expression> getFreeReferencesToLambdaArgument()
        {
            return freeReferencesToLambdaArgument.build();
        }

        void process(Node node, Set<String> lambdaArgumentNames)
        {
            switch (node) {
                case SubqueryExpression _ -> {
                    // Don't traverse into subqueries within an expression
                }
                case Identifier identifier -> {
                    if (analysis.getLambdaArgumentReferences().containsKey(NodeRef.of(identifier)) && !lambdaArgumentNames.contains(identifier.getValue())) {
                        freeReferencesToLambdaArgument.add(identifier);
                    }
                }
                case LambdaExpression lambda -> process(lambda.getBody(), ImmutableSet.<String>builder()
                        .addAll(lambdaArgumentNames)
                        .addAll(lambda.getArguments().stream()
                                .map(LambdaArgumentDeclaration::getName)
                                .map(Identifier::getValue)
                                .collect(toImmutableSet()))
                        .build());
                default -> forEachChild(node, child -> process(child, lambdaArgumentNames));
            }
        }
    }
}
