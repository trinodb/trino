/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package io.starburst.schema.discovery.models;

import com.google.common.collect.ImmutableList;

import java.util.List;

public record GeneratedOperations(List<String> sql, List<Operation> operations)
{
    public GeneratedOperations
    {
        sql = ImmutableList.copyOf(sql);
        operations = ImmutableList.copyOf(operations);
    }
}
