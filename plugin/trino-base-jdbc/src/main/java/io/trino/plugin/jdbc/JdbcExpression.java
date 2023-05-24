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
package io.trino.plugin.jdbc;

import com.google.common.collect.ImmutableList;

import java.util.List;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public final class JdbcExpression
{
    private final String expression;
    private final List<QueryParameter> parameters;
    private final JdbcTypeHandle jdbcTypeHandle;

    public JdbcExpression(String expression, List<QueryParameter> parameters, JdbcTypeHandle jdbcTypeHandle)
    {
        this.expression = requireNonNull(expression, "expression is null");
        this.parameters = ImmutableList.copyOf(requireNonNull(parameters, "parameters is null"));
        this.jdbcTypeHandle = requireNonNull(jdbcTypeHandle, "jdbcTypeHandle is null");
    }

    public String getExpression()
    {
        return expression;
    }

    public List<QueryParameter> getParameters()
    {
        return parameters;
    }

    public JdbcTypeHandle getJdbcTypeHandle()
    {
        return jdbcTypeHandle;
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("expression", expression)
                .add("parameters", parameters)
                .add("jdbcTypeHandle", jdbcTypeHandle)
                .toString();
    }
}
