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
package io.trino.jvm;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;

import java.lang.management.LockInfo;
import java.lang.management.MonitorInfo;
import java.lang.management.ThreadInfo;
import java.util.Arrays;
import java.util.Iterator;
import java.util.function.Function;

public final class Threads
{
    private Threads() {}

    /**
     * Returns representation of {@link ThreadInfo}. Similar to {@link ThreadInfo#toString()} but
     * without a hard-coded limit on number or returned stack frames.
     */
    public static String fullToString(ThreadInfo thread)
    {
        StringBuilder sb = new StringBuilder();

        sb.append("\"%s\"%s prio=%s Id=%s %s%s%s%s%s\n".formatted(
                thread.getThreadName(),
                thread.isDaemon() ? " daemon" : "",
                thread.getPriority(),
                thread.getThreadId(),
                thread.getThreadState(),
                thread.getLockName() != null ? " on " + thread.getLockName() : "",
                thread.getLockOwnerName() != null ? " owned by " + thread.getLockOwnerName() : "",
                thread.isSuspended() ? " (suspended)" : "",
                thread.isInNative() ? " (in native)" : ""));

        Iterator<StackTraceElement> stackTrace = Arrays.asList(thread.getStackTrace()).iterator();

        Multimap<Integer, MonitorInfo> lockedMonitors = Arrays.stream(thread.getLockedMonitors())
                .collect(Multimaps.toMultimap(MonitorInfo::getLockedStackDepth, Function.identity(), ArrayListMultimap::create));

        int depth = 0;
        if (stackTrace.hasNext()) {
            StackTraceElement first = stackTrace.next();
            sb.append("\tat %s\n".formatted(first));
            LockInfo lockInfo = thread.getLockInfo();
            if (lockInfo != null) {
                String lockVerb = switch (thread.getThreadState()) {
                    case BLOCKED -> "blocked";
                    case WAITING, TIMED_WAITING -> "waiting";
                    default -> "(unexpected lock info)";
                };
                sb.append("\t-  %s on %s\n".formatted(lockVerb, lockInfo));
            }

            lockedMonitors.get(depth).forEach(monitor -> sb.append("\t-  locked %s\n".formatted(monitor)));
        }

        while (stackTrace.hasNext()) {
            StackTraceElement element = stackTrace.next();
            depth++;
            sb.append("\tat %s\n".formatted(element));
            lockedMonitors.get(depth).forEach(monitor -> sb.append("\t-  locked %s\n".formatted(monitor)));
        }

        LockInfo[] lockedSynchronizers = thread.getLockedSynchronizers();
        if (lockedSynchronizers.length != 0) {
            sb.append("\n");
            sb.append("\tlocked synchronizers:\n");
            for (LockInfo locked : lockedSynchronizers) {
                sb.append("\t- %s\n".formatted(locked));
            }
        }

        sb.append("\n");

        return sb.toString();
    }
}
