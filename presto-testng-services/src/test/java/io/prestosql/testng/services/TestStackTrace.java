package io.prestosql.testng.services;

import io.airlift.log.Logger;
import org.testng.annotations.Test;

import java.util.Arrays;

import static java.lang.management.ManagementFactory.getThreadMXBean;
import static java.util.stream.Collectors.joining;

public class TestStackTrace
{
    private static final Logger LOG = Logger.get(TestStackTrace.class);

    @Test
    public void test()
    {
        LOG.warn("%s\n\nFull Thread Dump\n%s", "This is just a test",
                Arrays.stream(getThreadMXBean().dumpAllThreads(true, true))
                        .map(StackTraceUtil::getFullStackTrace)
                        .collect(joining("\n")));
    }
}
