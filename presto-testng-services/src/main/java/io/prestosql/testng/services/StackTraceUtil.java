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

package io.prestosql.testng.services;

import java.lang.management.LockInfo;
import java.lang.management.MonitorInfo;
import java.lang.management.ThreadInfo;

public final class StackTraceUtil
{
    private StackTraceUtil() {}

    public static String getFullStackTrace(ThreadInfo threadInfo)
    {
        StringBuilder sb = getHeader(threadInfo);


        StackTraceElement[] stackTrace = threadInfo.getStackTrace();
        if (stackTrace.length > 0) {
            sb.append("\n\tStack Trace:\n");
        } else {
            sb.append("\n\tStack Trace: NONE");
            sb.append("\n\t\tThread State: N/A");
            sb.append("\n\t\tLocked Monitors: N/A");
        }

        for (int i = 0; i < stackTrace.length; i++) {
            StackTraceElement ste = stackTrace[i];
            sb.append("\tat " + ste.toString());
            sb.append('\n');

            appendThreadStateInfo(sb, threadInfo, i);
            appendLockedMonitors(sb, threadInfo.getLockedMonitors(), i);
        }

        appendLockedSynchronizers(sb, threadInfo.getLockedSynchronizers());
        return sb.toString();
    }

    private static StringBuilder getHeader(ThreadInfo threadInfo)
    {
        StringBuilder sb = new StringBuilder("Thread: [ " + threadInfo.getThreadName() + " ]");
        sb.append("\n\tID: " + threadInfo.getThreadId());
        sb.append("\n\tSTATE: " + threadInfo.getThreadState());
        sb.append("\n\tSUSPENDED: " + threadInfo.isSuspended());
        sb.append("\n\tIN NATIVE: " + threadInfo.isInNative());
        sb.append("\n\tIS DAEMON: " + threadInfo.isDaemon());

        if (threadInfo.getLockName() != null) {
            sb.append("\n\n\tLOCK: ");
            sb.append(threadInfo.getLockName());

            if (threadInfo.getLockOwnerName() != null) {
                sb.append("\n\t\tOWNER: " + threadInfo.getLockOwnerName());
                sb.append("\n\t\tOWNER ID: " + threadInfo.getLockOwnerId());
            }
        } else {
            sb.append("\n\tLOCK: NONE");
        }



        sb.append('\n');

        return sb;
    }

    private static StringBuilder appendThreadStateInfo(StringBuilder sb, ThreadInfo threadInfo, int stackTracePos)
    {
        if (stackTracePos != 0 || threadInfo == null) {
            return sb;
        }

        Thread.State ts = threadInfo.getThreadState();
        switch (ts) {
            case BLOCKED:
                sb.append("\t-  BLOCKED on " + threadInfo.getLockInfo() + " for " + threadInfo.getBlockedTime() + "ms\n");
                break;
            case WAITING:
            case TIMED_WAITING:
                sb.append("\t-  WAITING on " + threadInfo.getLockInfo() + " for " + threadInfo.getWaitedTime() + "ms\n");
                break;
            default:
        }

        return sb;
    }

    private static StringBuilder appendLockedSynchronizers(StringBuilder sb, LockInfo[] locks)
    {
        sb.append("\n\tLOCKED Synchronizers: " + locks.length);
        for (LockInfo li : locks) {
            sb.append("\n\t\tClass Name: " + li.getClassName());
            sb.append("\n\t\tHash Code: " + li.getIdentityHashCode());
            sb.append('\n');
        }

        return sb.append('\n');
    }

    private static StringBuilder appendLockedMonitors(StringBuilder sb, MonitorInfo[] miList, int stackTracePos)
    {
        if (miList.length <= 0) {
            return sb;
        }

        for (MonitorInfo mi : miList) {
            // skip if we are not on the locked thread
            if (stackTracePos == mi.getLockedStackDepth()) {
                continue;
            }

            sb.append("\t\tLocked Monitor - " + mi);
            sb.append("\n");
        }

        return sb;
    }
}
