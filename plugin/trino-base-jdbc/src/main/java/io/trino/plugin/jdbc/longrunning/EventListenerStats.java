package io.trino.plugin.jdbc.longrunning;

import io.airlift.stats.CounterStat;
import io.airlift.stats.TimeStat;
import io.trino.spi.eventlistener.EventListener;
import io.trino.spi.eventlistener.QueryCreatedEvent;
import org.weakref.jmx.Managed;
import org.weakref.jmx.Nested;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

public class EventListenerStats
{
    private final TimeStat time = new TimeStat(MILLISECONDS);

    private final CounterStat count = new CounterStat();

    public void artificialCount()
    {
        TimeStat.BlockTimer blockTimer = time.time();
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
        try (TimeStat.BlockTimer ignored = time.time()) {
            try {
                eventListener.queryCreated(event);
            }
            catch (Throwable t) {
                count.update(1);
                throw t;
            }
        }
    }

    @Managed
    @Nested
    public TimeStat getTime()
    {
        return time;
    }

    @Managed
    @Nested
    public CounterStat getCount()
    {
        return count;
    }
}
