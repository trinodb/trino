package io.trino.plugin.jdbc.longrunning;

import io.airlift.log.Logger;
import io.trino.spi.eventlistener.EventListener;
import io.trino.spi.eventlistener.QueryCompletedEvent;
import io.trino.spi.eventlistener.QueryCreatedEvent;
import io.trino.spi.eventlistener.SplitCompletedEvent;

public class LongRunningEventListener
        implements EventListener
{
    private static final Logger log = Logger.get(LongRunningEventListener.class);
    private static final long MILLIS_WAIT_TIME = 100L;

    private final String name;

    public LongRunningEventListener(String name)
    {
        this.name = name;
    }

    @Override
    public void queryCreated(QueryCreatedEvent queryCreatedEvent)
    {
        try {
            log.info("running for a long time: query created");
            Thread.sleep(MILLIS_WAIT_TIME);
            log.info("finished running for a long time: query created");
        }
        catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void queryCompleted(QueryCompletedEvent queryCompletedEvent)
    {
        try {
            log.info("running for a long time: query completed");
            Thread.sleep(MILLIS_WAIT_TIME);
            log.info("finished running for a long time: query completed");
        }
        catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void splitCompleted(SplitCompletedEvent splitCompletedEvent)
    {
        try {
            log.info("running for a long time: split completed");
            Thread.sleep(MILLIS_WAIT_TIME);
            log.info("finished running for a long time: split completed");
        }
        catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public String getName()
    {
        return name;
    }
}
