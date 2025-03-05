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
package io.trino.plugin.spanner;

import io.airlift.configuration.Config;

public class SpannerConfig
{
    private String credentialsFile;
    private String projectId;

    public String getHost()
    {
        return host;
    }
    @Config("spanner.emulated.host")
    public void setHost(String host)
    {
        this.host = host;
    }

    private String host = "";

    public boolean isEmulator()
    {
        return isEmulator;
    }

    @Config("spanner.emulated")
    public void setEmulator(boolean emulator)
    {
        isEmulator = emulator;
    }

    private boolean isEmulator = false;
    private String instanceId;
    private String database;

    public String getProjectId()
    {
        return projectId;
    }

    @Config("spanner.projectId")
    public void setProjectId(String projectId)
    {
        this.projectId = projectId;
    }


    public String getInstanceId()
    {
        return instanceId;
    }
    @Config("spanner.instanceId")
    public void setInstanceId(String instanceId)
    {
        this.instanceId = instanceId;
    }

    public String getDatabase()
    {
        return database;
    }

    @Config("spanner.database")
    public void setDatabase(String database)
    {
        this.database = database;
    }
    /*private int minSessions = 100;
    private int maxSessions = 400;
    private int numChannels = 4;
    private boolean retryAbortsInternally = true;*/

    public String getCredentialsFile()
    {
        return credentialsFile;
    }

    @Config("spanner.credentials.file")
    public void setCredentialsFile(String credentialsFile)
    {
        this.credentialsFile = credentialsFile;
    }
}
