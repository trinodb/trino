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
package io.trino.plugin.hive;

import io.airlift.configuration.Config;
import io.airlift.units.Duration;
import jakarta.validation.constraints.NotNull;

import static java.util.concurrent.TimeUnit.MINUTES;

public class RecordingMetastoreConfig
{
    private String recordingPath;
    private boolean replay;
    private Duration recordingDuration = new Duration(10, MINUTES);

    @Config("hive.metastore-recording-path")
    public RecordingMetastoreConfig setRecordingPath(String recordingPath)
    {
        this.recordingPath = recordingPath;
        return this;
    }

    public String getRecordingPath()
    {
        return recordingPath;
    }

    @Config("hive.replay-metastore-recording")
    public RecordingMetastoreConfig setReplay(boolean replay)
    {
        this.replay = replay;
        return this;
    }

    public boolean isReplay()
    {
        return replay;
    }

    @Config("hive.metastore-recording-duration")
    public RecordingMetastoreConfig setRecordingDuration(Duration recordingDuration)
    {
        this.recordingDuration = recordingDuration;
        return this;
    }

    @NotNull
    public Duration getRecordingDuration()
    {
        return recordingDuration;
    }
}
