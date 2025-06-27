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
package io.trino.filesystem.tracking;

import com.google.common.base.Joiner;
import io.airlift.log.Logger;
import io.trino.filesystem.Location;

import java.io.Closeable;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;

public class TrackingState
        implements Runnable
{
    private static final Logger LOG = Logger.get(TrackingState.class);
    private static final Joiner JOINER = Joiner.on("\n\t\t");

    private final Location location;
    private final Closeable closeable;
    private final List<StackTraceElement> stacktrace;
    private boolean closed;

    public TrackingState(Closeable closeable, Location location)
    {
        this.location = requireNonNull(location, "location is null");
        this.closeable = requireNonNull(closeable, "closeable is null");
        StackTraceElement[] stackTrace = Thread.currentThread().getStackTrace();
        this.stacktrace = Arrays.stream(stackTrace)
                .filter(TrackingState::isNotTrackingFrame)
                .collect(toImmutableList());
    }

    public void markClosed()
    {
        this.closed = true;
    }

    @Override
    public void run()
    {
        // This is invoked by cleaner when associated closeable is phantom reachable.
        // If markClosed was not called prior to the invocation, resource is considered leaked.
        if (!closed) {
            LOG.error("%s with location '%s' was not closed properly and leaked. Created by: %s", closeable.getClass().getSimpleName(), location, JOINER.join(stacktrace));

            try {
                closeable.close();
            }
            catch (IOException e) {
                LOG.error(e, "Failed to close %s with location '%s'", closeable.getClass().getSimpleName(), location);
            }
        }
    }

    private static boolean isNotTrackingFrame(StackTraceElement stackTraceElement)
    {
        if (stackTraceElement.getClassName().startsWith(TrackingState.class.getPackageName())) {
            return false;
        }
        return !stackTraceElement.getClassName().contains("java.lang.Thread");
    }
}
