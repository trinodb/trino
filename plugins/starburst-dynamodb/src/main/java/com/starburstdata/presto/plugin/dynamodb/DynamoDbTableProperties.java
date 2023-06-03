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
import io.trino.plugin.jdbc.TablePropertiesProvider;
import io.trino.spi.session.PropertyMetadata;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static io.trino.spi.session.PropertyMetadata.integerProperty;
import static io.trino.spi.session.PropertyMetadata.stringProperty;
import static java.util.Objects.requireNonNull;

public class DynamoDbTableProperties
        implements TablePropertiesProvider
{
    public static final String PARTITION_KEY_ATTRIBUTE = "partition_key_attribute";
    public static final String SORT_KEY_ATTRIBUTE = "sort_key_attribute";
    public static final String READ_CAPACITY_UNITS = "read_capacity_units";
    public static final String WRITE_CAPACITY_UNITS = "write_capacity_units";

    private final List<PropertyMetadata<?>> tableProperties;

    @Inject
    public DynamoDbTableProperties()
    {
        tableProperties = ImmutableList.<PropertyMetadata<?>>builder()
                .add(stringProperty(
                        PARTITION_KEY_ATTRIBUTE,
                        "Attribute name for the partition key. Required if dynamodb.first-column-as-primary-key-enabled is false",
                        null,
                        value -> requireNonNull(value, PARTITION_KEY_ATTRIBUTE + " must be set if dynamodb.first-column-as-primary-key-enabled is false"),
                        false))
                .add(stringProperty(
                        SORT_KEY_ATTRIBUTE,
                        "Attribute name for the optional sort key. Must set sort_key_type if set",
                        null,
                        false))
                .add(integerProperty(
                        READ_CAPACITY_UNITS,
                        "Provisioned throughput read capacity units. Default 10",
                        10,
                        value -> checkArgument(value >= 1, "read_capacity_units must be greater than or equal to 1"),
                        false))
                .add(integerProperty(
                        WRITE_CAPACITY_UNITS,
                        "Provisioned throughput write capacity units. Default 5",
                        5,
                        value -> checkArgument(value >= 1, "write_capacity_units must be greater than or equal to 1"),
                        false))
                .build();
    }

    @Override
    public List<PropertyMetadata<?>> getTableProperties()
    {
        return tableProperties;
    }

    public static Optional<String> getPartitionKeyAttribute(Map<String, Object> tableProperties)
    {
        return Optional.ofNullable((String) tableProperties.get(PARTITION_KEY_ATTRIBUTE));
    }

    public static Optional<String> getSortKeyAttribute(Map<String, Object> tableProperties)
    {
        return Optional.ofNullable((String) tableProperties.get(SORT_KEY_ATTRIBUTE));
    }

    public static int getReadCapacityUnits(Map<String, Object> tableProperties)
    {
        return (int) tableProperties.get(READ_CAPACITY_UNITS);
    }

    public static int getWriteCapacityUnits(Map<String, Object> tableProperties)
    {
        return (int) tableProperties.get(WRITE_CAPACITY_UNITS);
    }
}
