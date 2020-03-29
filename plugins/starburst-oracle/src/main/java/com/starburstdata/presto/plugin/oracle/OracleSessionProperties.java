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
    public static final String CONCURRENCY_TYPE = "concurrency_type";
    public static final String MAX_SPLITS_PER_SCAN = "max_splits_per_scan";

    private final List<PropertyMetadata<?>> sessionProperties;

    @Inject
    public OracleSessionProperties(OracleConfig oracleConfig)
    {
        sessionProperties = ImmutableList.<PropertyMetadata<?>>builder()
                .add(enumProperty(
                        NUMBER_ROUNDING_MODE,
                        "Rounding mode for Oracle NUMBER data type",
                        RoundingMode.class,
                        oracleConfig.getNumberRoundingMode(),
                        false))
                .add(integerProperty(
                        NUMBER_DEFAULT_SCALE,
                        "Default scale for Oracle Number data type",
                        oracleConfig.getDefaultNumberScale().orElse(null),
                        false))
                .add(enumProperty(
                        CONCURRENCY_TYPE,
                        "Concurrency strategy for reads",
                        OracleConcurrencyType.class,
                        oracleConfig.getConcurrencyType(),
                        false))
                .add(integerProperty(
                        MAX_SPLITS_PER_SCAN,
                        "Maximum number of splits for a table scan",
                        oracleConfig.getMaxSplitsPerScan(),
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

    public static OracleConcurrencyType getConcurrencyType(ConnectorSession session)
    {
        return session.getProperty(CONCURRENCY_TYPE, OracleConcurrencyType.class);
    }

    public static int getMaxSplitsPerScan(ConnectorSession session)
    {
        return session.getProperty(MAX_SPLITS_PER_SCAN, Integer.class);
    }
}
