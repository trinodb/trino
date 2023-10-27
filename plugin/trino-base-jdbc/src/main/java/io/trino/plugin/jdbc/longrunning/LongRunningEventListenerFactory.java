package io.trino.plugin.jdbc.longrunning;

import io.trino.spi.eventlistener.EventListener;
import io.trino.spi.eventlistener.EventListenerFactory;

import java.util.Map;

public class LongRunningEventListenerFactory
        implements EventListenerFactory
{
    @Override
    public String getName()
    {
        return "longrunning";
    }

    @Override
    public EventListener create(Map<String, String> config)
    {
        return new LongRunningEventListener(getName());
    }
}
