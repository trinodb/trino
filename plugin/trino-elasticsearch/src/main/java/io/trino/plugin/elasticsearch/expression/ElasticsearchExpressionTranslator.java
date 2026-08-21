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
package io.trino.plugin.elasticsearch.expression;

import com.google.common.collect.ImmutableSet;
import io.trino.plugin.base.expression.ConnectorExpressionRewriter;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.expression.ConnectorExpression;

import java.util.Map;
import java.util.Optional;

/**
 * Registry for connector-expression to Elasticsearch predicate rules. Expression recognition belongs in independent
 * {@code ConnectorExpressionRule} implementations; metadata is responsible only for mode policy and lowering the
 * returned remote predicate representation into the table handle.
 */
public final class ElasticsearchExpressionTranslator
{
    private final ConnectorExpressionRewriter<ElasticsearchExpressionRewrite> rewriter;

    public ElasticsearchExpressionTranslator()
    {
        rewriter = new ConnectorExpressionRewriter<>(ImmutableSet.of(
                new RewriteAnalyzedTextLike()));
    }

    public Optional<ElasticsearchExpressionRewrite> rewrite(
            ConnectorSession session,
            ConnectorExpression expression,
            Map<String, ColumnHandle> assignments)
    {
        return rewriter.rewrite(session, expression, assignments);
    }
}
