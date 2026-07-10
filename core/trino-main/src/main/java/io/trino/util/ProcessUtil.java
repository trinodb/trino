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
package io.trino.util;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.lang.management.ManagementFactory;
import java.lang.management.RuntimeMXBean;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public final class ProcessUtil
{
    private ProcessUtil() {}

    public static boolean isRootProcess()
    {
        String ppid = getPPID(getPID());
        if (null == ppid) {
            return false;
        }

        return "1".equals(ppid.trim());
    }

    private static String getPID()
    {
        RuntimeMXBean bean = ManagementFactory.getRuntimeMXBean();
        return bean.getName().split("@")[0];
    }

    private static String getPPID(String pid)
    {
        try {
            ProcessBuilder psBuilder = new ProcessBuilder(partitionCommandLine("/usr/bin/ps -A -opid,ppid"));
            psBuilder.redirectErrorStream(true);
            ProcessBuilder awkBuilder = new ProcessBuilder(
                    partitionCommandLine(String.format("awk '$1==%s {print $2}'", pid)));
            awkBuilder.redirectErrorStream(true);

            List<ProcessBuilder> builders = Arrays.asList(psBuilder, awkBuilder);
            List<Process> processes = ProcessBuilder.startPipeline(builders);
            Process lastPrecess = processes.get(processes.size() - 1);
            String result = consumeInputStream(lastPrecess.getInputStream());
            int retCode = lastPrecess.waitFor();
            if (0 == retCode) {
                return result;
            }

            return "0";
        }
        catch (Exception e) {
            return "0";
        }
    }

    private static String consumeInputStream(InputStream is) throws IOException
    {
        try (InputStreamReader isr = new InputStreamReader(is); BufferedReader br = new BufferedReader(isr);) {
            StringBuilder sb = new StringBuilder();
            String s = br.readLine();
            while (null != s) {
                sb.append(s);
                s = br.readLine();
            }

            return sb.toString();
        }
    }

    /**
     * process command
     */
    private static String[] partitionCommandLine(final String command)
    {
        final ArrayList<String> commands = new ArrayList<>();
        int index = 0;
        StringBuilder buffer = new StringBuilder(command.length());
        boolean isApos = false;
        boolean isQuote = false;
        while (index < command.length()) {
            final char c = command.charAt(index);
            switch (c) {
                case ' ':
                    if (!isQuote && !isApos) {
                        final String arg = buffer.toString();
                        buffer = new StringBuilder(command.length() - index);
                        if (arg.length() > 0) {
                            commands.add(arg);
                        }
                    }
                    else {
                        buffer.append(c);
                    }
                    break;
                case '\'':
                    if (!isQuote) {
                        isApos = !isApos;
                    }
                    else {
                        buffer.append(c);
                    }
                    break;
                case '"':
                    if (!isApos) {
                        isQuote = !isQuote;
                    }
                    else {
                        buffer.append(c);
                    }
                    break;
                default:
                    buffer.append(c);
            }

            index++;
        }

        if (buffer.length() > 0) {
            final String arg = buffer.toString();
            commands.add(arg);
        }

        return commands.toArray(new String[commands.size()]);
    }
}
