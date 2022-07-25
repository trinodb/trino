/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.presto.plugin.snowflake.distributed;

import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.inject.Inject;

import javax.annotation.PreDestroy;

import java.util.concurrent.ExecutorService;

import static java.util.Objects.requireNonNull;

public class ExecutorCleanup
{
    private final ExecutorService executor;

    @Inject
    public ExecutorCleanup(ListeningExecutorService executor)
    {
        this.executor = requireNonNull(executor, "executor is null");
    }

    @PreDestroy
    public void shutdown()
    {
        executor.shutdownNow();
    }
}
