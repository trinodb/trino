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
package io.trino.sql.gen.columnar;

import io.trino.operator.project.SelectedPositions;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.SourcePage;
import io.trino.sql.ir.SecureExpressions;

import static java.util.Objects.requireNonNull;

/**
 * Evaluates a secure filter while redacting evaluation failures, like the bytecode path does.
 */
public final class RedactingFilterEvaluator
        implements FilterEvaluator
{
    private final FilterEvaluator delegate;

    public RedactingFilterEvaluator(FilterEvaluator delegate)
    {
        this.delegate = requireNonNull(delegate, "delegate is null");
    }

    @Override
    public SelectionResult evaluate(ConnectorSession session, SelectedPositions activePositions, SourcePage page)
    {
        try {
            return delegate.evaluate(session, activePositions, page);
        }
        catch (RuntimeException e) {
            throw SecureExpressions.redactFailure(e);
        }
    }
}
