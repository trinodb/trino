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
package io.trino.plugin.base.logging;

import io.trino.plugin.base.logging.FormatInterpolator.InterpolatedValue;
import io.trino.spi.connector.ConnectorSession;

import java.util.function.Function;

public enum SessionInterpolatedValues
        implements InterpolatedValue<ConnectorSession>
{
    QUERY_ID(ConnectorSession::getQueryId),
    SOURCE(session -> session.getSource().orElse("")),
    USER(ConnectorSession::getUser),
    TRACE_TOKEN(session -> session.getTraceToken().orElse(""));

    private final Function<ConnectorSession, String> valueProvider;

    SessionInterpolatedValues(Function<ConnectorSession, String> valueProvider)
    {
        this.valueProvider = valueProvider;
    }

    @Override
    public String value(ConnectorSession session)
    {
        return valueProvider.apply(session);
    }
}
