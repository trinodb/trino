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

import com.google.common.base.VerifyException;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.sql.analyzer.Analysis;
import io.trino.sql.analyzer.Scope;
import io.trino.sql.ir.Constant;
import io.trino.sql.parser.SqlParser;
import io.trino.sql.tree.Expression;
import io.trino.sql.tree.NodeRef;
import io.trino.sql.tree.SubqueryExpression;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.sql.analyzer.QueryType.OTHERS;
import static io.trino.sql.planner.ScopeAware.scopeAwareKey;
import static io.trino.sql.planner.TestingPlannerContext.PLANNER_CONTEXT;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static io.trino.testing.assertions.TrinoExceptionAssert.assertTrinoExceptionThrownBy;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestTranslationMap
{
    @Test
    public void testLiteral()
    {
        Expression literal = new SqlParser().createExpression("1");
        Analysis analysis = new Analysis(null, ImmutableMap.of(), OTHERS);
        analysis.addTypes(ImmutableMap.of(NodeRef.of(literal), INTEGER));
        TranslationMap translationMap = createTranslationMap(analysis);

        assertThat(translationMap.rewrite(literal)).isEqualTo(new Constant(INTEGER, 1L));
    }

    @Test
    public void testMappedSubquery()
    {
        SubqueryExpression subquery = (SubqueryExpression) new SqlParser().createExpression("(SELECT 1)");
        Analysis analysis = new Analysis(subquery.getQuery(), ImmutableMap.of(), OTHERS);
        analysis.addTypes(ImmutableMap.of(NodeRef.of(subquery), INTEGER));
        TranslationMap translationMap = createTranslationMap(analysis);
        Symbol symbol = new Symbol(INTEGER, "subquery");
        TranslationMap mappedTranslationMap = translationMap.withAdditionalMappings(
                ImmutableMap.of(scopeAwareKey(subquery, analysis, translationMap.getScope()), symbol));

        assertThat(mappedTranslationMap.canTranslate(subquery)).isTrue();
        assertThat(mappedTranslationMap.rewrite(subquery)).isEqualTo(symbol.toSymbolReference());
    }

    @Test
    public void testUnanalyzedExpression()
    {
        Expression literal = new SqlParser().createExpression("1");
        TranslationMap translationMap = createTranslationMap(new Analysis(null, ImmutableMap.of(), OTHERS));

        assertThatThrownBy(() -> translationMap.rewrite(literal))
                .isInstanceOf(VerifyException.class)
                .hasMessage("Expression is not analyzed (%s): %s", literal.getClass().getName(), literal);
    }

    @Test
    public void testUnsupportedExpression()
    {
        SubqueryExpression subquery = (SubqueryExpression) new SqlParser().createExpression("(SELECT 1)");
        Analysis analysis = new Analysis(subquery.getQuery(), ImmutableMap.of(), OTHERS);
        analysis.addTypes(ImmutableMap.of(NodeRef.of(subquery), INTEGER));
        TranslationMap translationMap = createTranslationMap(analysis);

        assertTrinoExceptionThrownBy(() -> translationMap.rewrite(subquery))
                .hasErrorCode(NOT_SUPPORTED)
                .hasMessage("Unsupported expression (%s): %s", SubqueryExpression.class.getName(), subquery);
    }

    private static TranslationMap createTranslationMap(Analysis analysis)
    {
        return new TranslationMap(
                Optional.empty(),
                Scope.builder().build(),
                analysis,
                ImmutableMap.of(),
                ImmutableList.of(),
                testSessionBuilder().build(),
                PLANNER_CONTEXT,
                new SymbolAllocator(ImmutableList.of()));
    }
}
