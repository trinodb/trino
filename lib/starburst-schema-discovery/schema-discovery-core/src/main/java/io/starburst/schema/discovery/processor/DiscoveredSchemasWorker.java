/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package io.starburst.schema.discovery.processor;

import com.google.common.util.concurrent.ForwardingListenableFuture;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import io.trino.filesystem.Location;

import java.util.List;
import java.util.concurrent.Executor;

// shim to watch for the list of format guess schemas to complete
class DiscoveredSchemasWorker
        extends ForwardingListenableFuture<Processor.ProcessorDiscoveredSchemas>
{
    private final SettableFuture<Processor.ProcessorDiscoveredSchemas> result = SettableFuture.create();

    DiscoveredSchemasWorker(Location parent, ListenableFuture<List<Processor.ProcessorFormatGuessSchema>> formatGuessSchemas, Executor executor)
    {
        Futures.addCallback(formatGuessSchemas, new FutureCallback<>()
        {
            @Override
            public void onSuccess(List<Processor.ProcessorFormatGuessSchema> list)
            {
                result.set(new Processor.ProcessorDiscoveredSchemas(parent, list));
            }

            @Override
            public void onFailure(Throwable t)
            {
                result.setException(t);
            }
        }, executor);
    }

    @Override
    protected ListenableFuture<? extends Processor.ProcessorDiscoveredSchemas> delegate()
    {
        return result;
    }
}
