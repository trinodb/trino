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
package io.trino.plugin.oracle;

import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import io.trino.plugin.base.session.SessionPropertiesProvider;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.session.PropertyMetadata;

import java.math.RoundingMode;
import java.util.List;
import java.util.Optional;

import static io.trino.spi.session.PropertyMetadata.enumProperty;
import static io.trino.spi.session.PropertyMetadata.integerProperty;

public final class OracleSessionProperties
        implements SessionPropertiesProvider
{
    public static final String NUMBER_ROUNDING_MODE = "number_rounding_mode";
    public static final String NUMBER_DEFAULT_SCALE = "number_default_scale";

    private final List<PropertyMetadata<?>> sessionProperties;

    @Inject
    public OracleSessionProperties(OracleConfig config)
    {
        sessionProperties = ImmutableList.<PropertyMetadata<?>>builder()
                .add(enumProperty(
                        NUMBER_ROUNDING_MODE,
                        "Rounding mode for Oracle NUMBER data type",
                        RoundingMode.class,
                        config.getNumberRoundingMode(),
                        false))
                .add(integerProperty(
                        NUMBER_DEFAULT_SCALE,
                        "Default scale for Oracle Number data type",
                        config.getDefaultNumberScale().orElse(null),
                        false))
                .build();
    }

    @Override
    public List<PropertyMetadata<?>> getSessionProperties()
    {
        return sessionProperties;
    }

    public static RoundingMode getNumberRoundingMode(ConnectorSession session)
    {
        return session.getProperty(NUMBER_ROUNDING_MODE, RoundingMode.class);
    }

    public static Optional<Integer> getNumberDefaultScale(ConnectorSession session)
    {
        return Optional.ofNullable(session.getProperty(NUMBER_DEFAULT_SCALE, Integer.class));
    }
}
