/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package io.starburst.schema.discovery.trino.system.table;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.airlift.slice.Slice;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.security.ConnectorIdentity;

import java.util.Optional;

import static io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static io.trino.spi.StandardErrorCode.INVALID_ARGUMENTS;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.util.Objects.requireNonNull;

sealed class DiscoverySystemTableBase
        permits SchemaDiscoverySystemTable, ShallowDiscoverySystemTable
{
    private final ObjectMapper objectMapper;
    private final DiscoveryLocationAccessControlAdapter locationAccessControl;

    DiscoverySystemTableBase(ObjectMapper objectMapper, DiscoveryLocationAccessControlAdapter locationAccessControl)
    {
        this.objectMapper = requireNonNull(objectMapper, "objectMapper is null");
        this.locationAccessControl = requireNonNull(locationAccessControl, "locationAccessControl is null");
    }

    protected void validateLocationAccess(ConnectorIdentity identity, String location)
    {
        locationAccessControl.checkCanUseLocation(identity, location);
    }

    // copied from Trino io.trino.connector.system.jdbc.FilterUtil
    protected static <T> Optional<String> tryGetSingleVarcharValue(TupleDomain<T> constraint, T index)
    {
        if (constraint.isNone()) {
            return Optional.empty();
        }

        Domain domain = constraint.getDomains().orElseThrow(() -> new TrinoException(INVALID_ARGUMENTS, "No domain constraint")).get(index);
        if ((domain == null)) {
            return Optional.empty();
        }

        if (!domain.isSingleValue()) {
            throw new TrinoException(INVALID_ARGUMENTS, "Only single value is acceptable");
        }

        Object value = domain.getSingleValue();
        return Optional.of(((Slice) value).toStringUtf8());
    }

    protected static ColumnMetadata buildColumn(String name, String description)
    {
        return ColumnMetadata.builder()
                .setName(name)
                .setType(VARCHAR)
                .setComment(Optional.of(description))
                .build();
    }

    protected String toJson(Object obj)
    {
        try {
            return objectMapper.writeValueAsString(obj);
        }
        catch (JsonProcessingException e) {
            throw new TrinoException(GENERIC_INTERNAL_ERROR, "Could not generate JSON for: " + obj.getClass().getSimpleName(), e);
        }
    }

    protected ObjectMapper objectMapper()
    {
        return objectMapper;
    }
}
