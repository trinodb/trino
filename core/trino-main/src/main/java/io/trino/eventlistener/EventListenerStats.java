package io.trino.eventlistener;

import io.airlift.stats.CounterStat;
import io.airlift.stats.TimeStat;
import io.trino.spi.eventlistener.EventListener;
import io.trino.spi.eventlistener.QueryCompletedEvent;
import io.trino.spi.eventlistener.QueryCreatedEvent;
import io.trino.spi.eventlistener.SplitCompletedEvent;
import org.weakref.jmx.Managed;
import org.weakref.jmx.Nested;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

public class EventListenerStats
{
    private final TimeStat queryCreatedTime = new TimeStat(MILLISECONDS);

    private final TimeStat queryCompletedTime = new TimeStat(MILLISECONDS);

    private final TimeStat splitCompletedTime = new TimeStat(MILLISECONDS);

    private final TimeStat artificialTime = new TimeStat(MILLISECONDS);

    private final CounterStat count = new CounterStat();

    public void artificialCount()
    {
        TimeStat.BlockTimer blockTimer = artificialTime.time();
        try {
            Thread.sleep(100);
        }
        catch (InterruptedException e) {
            count.update(100);
        }
        finally {
            blockTimer.close();
        }
    }

    public void queryCreated(EventListener eventListener, QueryCreatedEvent event)
    {
        try (TimeStat.BlockTimer ignored = queryCreatedTime.time()) {
            try {
                eventListener.queryCreated(event);
                count.update(1);
            }
            catch (Throwable t) {
                count.update(1);
                throw t;
            }
        }
    }

    public void queryCompleted(EventListener eventListener, QueryCompletedEvent event)
    {
        try (TimeStat.BlockTimer ignored = queryCompletedTime.time()) {
            try {
                eventListener.queryCompleted(event);
                count.update(1);
            }
            catch (Throwable t) {
                count.update(1);
                throw t;
            }
        }
    }

    public void splitCompleted(EventListener eventListener, SplitCompletedEvent event)
    {
        try (TimeStat.BlockTimer ignored = splitCompletedTime.time()) {
            try {
                eventListener.splitCompleted(event);
                count.update(1);
            }
            catch (Throwable t) {
                count.update(1);
                throw t;
            }
        }
    }

    @Managed
    @Nested
    public TimeStat getQueryCreatedTime()
    {
        return queryCreatedTime;
    }

    @Managed
    @Nested
    public TimeStat getQueryCompletedTime()
    {
        return queryCompletedTime;
    }

    @Managed
    @Nested
    public TimeStat getSplitCompletedTime()
    {
        return splitCompletedTime;
    }

    @Managed
    @Nested
    public TimeStat getArtificialTime()
    {
        return artificialTime;
    }

    @Managed
    @Nested
    public CounterStat getCount()
    {
        return count;
    }
}
