/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.qubole.presto.kinesis;

import com.facebook.presto.spi.ConnectorSession;
import io.airlift.log.Logger;

import java.text.ParsePosition;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;

/**
 * Define session variables supported in the connector and an accessor.
 * <p>
 * Note that we default these properties to what is in the configuration,
 * so there should always be a value.
 */
public class SessionVariables
{
    private static final Logger log = Logger.get(SessionVariables.class);

    private SessionVariables() {}

    public static final String PRESTO_TIMESTAMP_FORMAT = "yyyy-MM-dd HH:mm:ss.SSS";
    public static final String UNSET_TIMESTAMP = "2000-01-01 00:00:00.000";

    public static final String CHECKPOINT_ENABLED = "checkpoint-enabled"; // boolean
    public static final String ITERATION_NUMBER = "iteration_number"; // int
    public static final String CHECKPOINT_LOGICAL_NAME = "checkpoint_logical_name"; // string
    public static final String MAX_BATCHES = "max_batches"; // int
    public static final String BATCH_SIZE = "batch_size"; // int
    public static final String ITER_FROM_TIMESTAMP = "iter_from_timestamp"; // boolean
    public static final String ITER_OFFSET_SECONDS = "iter_offset_seconds"; // long
    public static final String ITER_START_TIMESTAMP = "iter_start_timestamp"; // string timestamp format

    public static boolean getCheckpointEnabled(ConnectorSession session)
    {
        boolean value = session.getProperty(CHECKPOINT_ENABLED, Boolean.class);
        return value;
    }

    public static int getBatchSize(ConnectorSession session)
    {
        int value = session.getProperty(BATCH_SIZE, Integer.class);
        return value;
    }

    public static int getMaxBatches(ConnectorSession session)
    {
        int value = session.getProperty(MAX_BATCHES, Integer.class);
        return value;
    }

    public static boolean getIterFromTimestamp(ConnectorSession session)
    {
        boolean value = session.getProperty(ITER_FROM_TIMESTAMP, Boolean.class);
        return value;
    }

    public static long getIterOffsetSeconds(ConnectorSession session)
    {
        long value = session.getProperty(ITER_OFFSET_SECONDS, Long.class);
        return value;
    }

    public static long getIterStartTimestamp(ConnectorSession session)
    {
        // Return 0 to indicate this wasn't set, indicates to ignore this and use
        // other settings.
        String value = getSessionProperty(session, ITER_START_TIMESTAMP);
        if (value.equals(UNSET_TIMESTAMP)) {
            return 0;
        }
        else {
            return getTimestampAsLong(value, session);
        }
    }

    public static int getIntSessionProperty(ConnectorSession session, String key)
    {
        Integer value = session.getProperty(key, Integer.class);
        if (value == null) {
            return 0;
        }
        else {
            return value;
        }
    }

    public static String getSessionProperty(ConnectorSession session, String key)
    {
        String value = session.getProperty(key, String.class);
        if (value == null) {
            return "";
        }
        else {
            return value;
        }
    }

    public static long getTimestampAsLong(String tsValue, ConnectorSession session)
    {
        // Parse this as a date and return the long timestamp value (2016-07-10 17:03:56.124).
        // They will be entering timestamps in their session's timezone.  Use session.getTimeZoneKey().
        SimpleDateFormat frmt = new SimpleDateFormat(PRESTO_TIMESTAMP_FORMAT);

        if (!session.getTimeZoneKey().getId().equals(TimeZone.getDefault().getID())) {
            TimeZone sessionTz = TimeZone.getTimeZone(session.getTimeZoneKey().getId());
            frmt.setTimeZone(sessionTz);
        }

        Date result = frmt.parse(tsValue, new ParsePosition(0));
        long res = result.getTime();
        return res;
    }
}
