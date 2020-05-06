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
package io.presto.eventlog.conf;

import io.airlift.configuration.Config;

public class EventLogConfig
{
    private boolean enableEventLog;
    private String eventLogDir;
    private String eventLogKeytab;
    private String eventLogPrincipal;
    private String hadoopConfPath;

    @Config("event-log.enabled")
    public EventLogConfig setEnableEventLog(boolean enable)
    {
        this.enableEventLog = enable;
        return this;
    }

    @Config("event-log.dir")
    public EventLogConfig setEventLogDir(String dir)
    {
        this.eventLogDir = dir;
        return this;
    }

    @Config("event-log.principal")
    public void setEventLogPrincipal(String eventLogPrincipal)
    {
        this.eventLogPrincipal = eventLogPrincipal;
    }

    @Config("event-log.keytab")
    public void setEventLogKeytab(String eventLogKeytab)
    {
        this.eventLogKeytab = eventLogKeytab;
    }

    @Config("event-log.hadoop.conf.path")
    public void setHadoopConfPath(String hadoopConfPath)
    {
        this.hadoopConfPath = hadoopConfPath;
    }

    public String getHadoopConfPath()
    {
        return hadoopConfPath;
    }

    public String getEventLogKeytab()
    {
        return eventLogKeytab;
    }

    public String getEventLogPrincipal()
    {
        return eventLogPrincipal;
    }

    public boolean isEnableEventLog()
    {
        return enableEventLog;
    }

    public String getEventLogDir()
    {
        return eventLogDir;
    }
}
