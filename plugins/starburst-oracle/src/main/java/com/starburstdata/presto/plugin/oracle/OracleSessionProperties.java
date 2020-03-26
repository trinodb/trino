/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
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
