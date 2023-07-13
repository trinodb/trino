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
package io.trino.plugin.bigquery;

import com.google.common.base.CharMatcher;
import io.trino.plugin.base.logging.FormatInterpolator;
import io.trino.spi.connector.ConnectorSession;

import java.util.Map;

import static com.google.common.base.Strings.isNullOrEmpty;
import static com.google.common.base.Verify.verify;
import static java.util.Objects.requireNonNull;

public class BigQueryLabelFactory
{
    private static final CharMatcher ALLOWED_CHARS = CharMatcher.inRange('a', 'z')
            .or(CharMatcher.inRange('0', '9'))
            .or(CharMatcher.anyOf("_-"))
            .precomputed();

    private static final int MAX_LABEL_VALUE_LENGTH = 63;
    private final String name;
    private final FormatInterpolator<ConnectorSession> interpolator;

    public BigQueryLabelFactory(String labelName, FormatInterpolator<ConnectorSession> interpolator)
    {
        this.name = labelName;
        this.interpolator = requireNonNull(interpolator, "interpolator is null");
    }

    public Map<String, String> getLabels(ConnectorSession session)
    {
        if (isNullOrEmpty(name)) {
            return Map.of();
        }

        String value = interpolator.interpolate(session).trim();
        if (isNullOrEmpty(value)) {
            return Map.of();
        }

        verifyLabelValue(name);
        verifyLabelValue(value);
        return Map.of(name, value);
    }

    private void verifyLabelValue(String value)
    {
        verify(value.length() <= MAX_LABEL_VALUE_LENGTH, "BigQuery label value cannot be longer than %s characters", MAX_LABEL_VALUE_LENGTH);
        verify(ALLOWED_CHARS.matchesAllOf(value), "BigQuery label value can contain only lowercase letters, numeric characters, underscores, and dashes");
    }
}
