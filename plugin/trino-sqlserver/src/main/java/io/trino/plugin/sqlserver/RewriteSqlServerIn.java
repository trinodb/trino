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
package io.trino.plugin.sqlserver;

import io.trino.matching.Captures;
import io.trino.plugin.jdbc.expression.ParameterizedExpression;
import io.trino.plugin.jdbc.expression.RewriteIn;
import io.trino.spi.expression.Call;

import java.util.Optional;

import static io.trino.plugin.sqlserver.RemoteComparison.isSupportedComparison;

/**
 * Pushes {@code IN} down only when {@link RemoteComparison} allows comparing the values remotely.
 */
public class RewriteSqlServerIn
        extends RewriteIn
{
    @Override
    public Optional<ParameterizedExpression> rewrite(Call call, Captures captures, RewriteContext<ParameterizedExpression> context)
    {
        if (!isSupportedComparison(call, context)) {
            return Optional.empty();
        }
        return super.rewrite(call, captures, context);
    }
}
