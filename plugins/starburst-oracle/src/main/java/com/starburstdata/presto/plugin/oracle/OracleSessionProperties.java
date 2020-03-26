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
package com.starburstdata.presto.plugin.oracle;

import com.google.common.collect.ImmutableList;
import io.prestosql.plugin.jdbc.SessionPropertiesProvider;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.session.PropertyMetadata;

import javax.inject.Inject;

import java.math.RoundingMode;
import java.util.List;
import java.util.Optional;

import static io.prestosql.spi.session.PropertyMetadata.enumProperty;
import static io.prestosql.spi.session.PropertyMetadata.integerProperty;

public final class OracleSessionProperties
        implements SessionPropertiesProvider
{
    public static final String NUMBER_ROUNDING_MODE = "number_rounding_mode";
    public static final String NUMBER_DEFAULT_SCALE = "number_default_scale";

    private final List<PropertyMetadata<?>> sessionProperties;

    @Inject
    public OracleSessionProperties(OracleConfig oracleConfig)
    {
        sessionProperties = ImmutableList.of(
                enumProperty(
                        NUMBER_ROUNDING_MODE,
                        "Rounding mode for Oracle NUMBER data type",
                        RoundingMode.class,
                        oracleConfig.getNumberRoundingMode(),
                        false),
                integerProperty(
                        NUMBER_DEFAULT_SCALE,
                        "Default scale for Oracle Number data type",
                        oracleConfig.getDefaultNumberScale().orElse(null),
                        false));
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
