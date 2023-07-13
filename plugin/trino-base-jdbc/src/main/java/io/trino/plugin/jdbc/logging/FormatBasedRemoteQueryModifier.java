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
import io.trino.plugin.base.logging.FormatInterpolator;
import io.trino.plugin.base.logging.SessionInterpolatedValues;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ConnectorSession;

import static com.google.common.base.Preconditions.checkState;
import static io.trino.plugin.jdbc.JdbcErrorCode.JDBC_NON_TRANSIENT_ERROR;
import static java.util.Objects.requireNonNull;

public class FormatBasedRemoteQueryModifier
        implements RemoteQueryModifier
{
    private final FormatInterpolator<ConnectorSession> interpolator;

    @Inject
    public FormatBasedRemoteQueryModifier(FormatBasedRemoteQueryModifierConfig config)
    {
        String commentFormat = requireNonNull(config, "config is null").getFormat();
        checkState(!commentFormat.isBlank(), "comment format is blank");
        this.interpolator = new FormatInterpolator<>(commentFormat, SessionInterpolatedValues.values());
    }

    @Override
    public String apply(ConnectorSession session, String query)
    {
        try {
            return query + " /*" + interpolator.interpolate(session) + "*/";
        }
        catch (SecurityException e) {
            throw new TrinoException(JDBC_NON_TRANSIENT_ERROR, e.getMessage());
        }
    }
}
