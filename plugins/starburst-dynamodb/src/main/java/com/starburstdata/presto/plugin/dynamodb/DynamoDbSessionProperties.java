/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.presto.plugin.dynamodb;

import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import com.starburstdata.presto.plugin.dynamodb.DynamoDbConfig.GenerateSchemaFiles;
import io.trino.plugin.base.session.SessionPropertiesProvider;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.session.PropertyMetadata;

import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static io.trino.spi.StandardErrorCode.CONFIGURATION_INVALID;
import static io.trino.spi.session.PropertyMetadata.booleanProperty;
import static io.trino.spi.session.PropertyMetadata.enumProperty;
import static io.trino.spi.session.PropertyMetadata.integerProperty;

public class DynamoDbSessionProperties
        implements SessionPropertiesProvider
{
    public static final String GENERATE_SCHEMA_FILES = "generate_schema_files";
    public static final String FLATTEN_OBJECTS_ENABLED = "flatten_objects_enabled";
    public static final String FLATTEN_ARRAY_ELEMENT_COUNT = "flatten_arrays_element_count";

    private final List<PropertyMetadata<?>> sessionProperties;

    @Inject
    public DynamoDbSessionProperties(DynamoDbConfig dynamoDbConfig)
    {
        sessionProperties = ImmutableList.<PropertyMetadata<?>>builder()
                .add(enumProperty(
                        GENERATE_SCHEMA_FILES,
                        "Set to Never, OnUse, or OnStart to dictate how the connector generates schema files. See the connector documentation for details",
                        GenerateSchemaFiles.class,
                        dynamoDbConfig.getGenerateSchemaFiles(),
                        false))
                .add(booleanProperty(
                        FLATTEN_OBJECTS_ENABLED,
                        "True to enable flattening nested attributes into columns of their own. False returns nested attributes as a VARCHAR type containing a JSON string",
                        dynamoDbConfig.isFlattenObjectsEnabled(),
                        false))
                .add(integerProperty(
                        FLATTEN_ARRAY_ELEMENT_COUNT,
                        "Set to an integer value between 1 and 256 to flatten nested arrays into the given number of columns. A value of 0 returns nested attributes as a VARCHAR type containing a JSON string",
                        dynamoDbConfig.getFlattenArrayElementCount(),
                        value -> {
                            try {
                                checkArgument(value >= 0 && value <= 256);
                            }
                            catch (IllegalArgumentException e) {
                                throw new TrinoException(CONFIGURATION_INVALID, FLATTEN_ARRAY_ELEMENT_COUNT + " must be between 0 and 256");
                            }
                        },
                        false))
                .build();
    }

    @Override
    public List<PropertyMetadata<?>> getSessionProperties()
    {
        return sessionProperties;
    }

    public static GenerateSchemaFiles getGenerateSchemaFiles(ConnectorSession session)
    {
        return session.getProperty(GENERATE_SCHEMA_FILES, GenerateSchemaFiles.class);
    }

    public static boolean isFlattenObjectsEnabled(ConnectorSession session)
    {
        return session.getProperty(FLATTEN_OBJECTS_ENABLED, Boolean.class);
    }

    public static int getFlattenArrayElementCount(ConnectorSession session)
    {
        return session.getProperty(FLATTEN_ARRAY_ELEMENT_COUNT, Integer.class);
    }
}
