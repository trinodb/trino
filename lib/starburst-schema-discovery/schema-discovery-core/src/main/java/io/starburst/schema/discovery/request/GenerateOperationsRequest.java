/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package io.starburst.schema.discovery.request;

import io.starburst.schema.discovery.models.DiscoveredSchema;

import static java.util.Objects.requireNonNull;

public record GenerateOperationsRequest(DiscoveredSchema schema, GenerateOptions generateOptions)
{
    public GenerateOperationsRequest
    {
        requireNonNull(schema, "schema cannot be null");
        requireNonNull(generateOptions, "generateOptions cannot be null");
    }
}
