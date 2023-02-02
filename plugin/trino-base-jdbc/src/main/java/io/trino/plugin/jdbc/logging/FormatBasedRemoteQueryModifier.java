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
package io.trino.plugin.jdbc.logging;

import com.google.inject.Inject;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ConnectorSession;

import java.util.function.Function;
import java.util.function.Predicate;
import java.util.regex.Pattern;

import static com.google.common.base.Preconditions.checkState;
import static io.trino.plugin.jdbc.JdbcErrorCode.JDBC_NON_TRANSIENT_ERROR;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class FormatBasedRemoteQueryModifier
        implements RemoteQueryModifier
{
    private final String commentFormat;

    @Inject
    public FormatBasedRemoteQueryModifier(FormatBasedRemoteQueryModifierConfig config)
    {
        this.commentFormat = requireNonNull(config, "config is null").getFormat();
        checkState(!commentFormat.isBlank(), "comment format is blank");
    }

    @Override
    public String apply(ConnectorSession session, String query)
    {
        String message = commentFormat;
        for (PredefinedValue predefinedValue : PredefinedValue.values()) {
            if (message.contains(predefinedValue.getPredefinedValueCode())) {
                message = message.replaceAll(predefinedValue.getMatchCase(), predefinedValue.value(session));
            }
        }
        return query + " /*" + message + "*/";
    }

    enum PredefinedValue
    {
        QUERY_ID(ConnectorSession::getQueryId),
        SOURCE(new SanitizedValuesProvider(session -> session.getSource().orElse(""), "$SOURCE")),
        USER(ConnectorSession::getUser),
        TRACE_TOKEN(new SanitizedValuesProvider(session -> session.getTraceToken().orElse(""), "$TRACE_TOKEN"));

        private final Function<ConnectorSession, String> valueProvider;

        PredefinedValue(Function<ConnectorSession, String> valueProvider)
        {
            this.valueProvider = valueProvider;
        }

        String getMatchCase()
        {
            return "\\$" + this.name();
        }

        String getPredefinedValueCode()
        {
            return "$" + this.name();
        }

        String value(ConnectorSession session)
        {
            return valueProvider.apply(session);
        }
    }

    private static class SanitizedValuesProvider
            implements Function<ConnectorSession, String>
    {
        private static final Predicate<String> VALIDATION_MATCHER = Pattern.compile("^[\\w_-]*$").asMatchPredicate();
        private final Function<ConnectorSession, String> valueProvider;
        private final String name;

        private SanitizedValuesProvider(Function<ConnectorSession, String> valueProvider, String name)
        {
            this.valueProvider = requireNonNull(valueProvider, "valueProvider is null");
            this.name = requireNonNull(name, "name is null");
        }

        @Override
        public String apply(ConnectorSession session)
        {
            String value = valueProvider.apply(session);
            if (VALIDATION_MATCHER.test(value)) {
                return value;
            }
            throw new TrinoException(JDBC_NON_TRANSIENT_ERROR, format("Passed value %s as %s does not meet security criteria. It can contain only letters, digits, underscores and hyphens", value, name));
        }
    }
}
