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

import com.google.common.collect.ImmutableList;
import io.starburst.schema.discovery.models.Operation;

import java.util.List;

import static java.util.Objects.requireNonNull;

public record GenerateSqlRequest(List<Operation> operations, GenerateOptions generateOptions)
{
    public GenerateSqlRequest
    {
        operations = ImmutableList.copyOf(operations);
        requireNonNull(generateOptions, "generateOptions cannot be null");
    }
}
