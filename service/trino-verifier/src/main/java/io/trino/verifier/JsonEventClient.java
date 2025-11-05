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
package io.trino.verifier;

import com.google.inject.Inject;
import io.airlift.json.JsonCodec;

import java.io.FileNotFoundException;
import java.io.PrintStream;

import static java.util.Objects.requireNonNull;

public class JsonEventClient
        implements EventConsumer
{
    private final JsonCodec<VerifierQueryEvent> serializer = JsonCodec.jsonCodec(VerifierQueryEvent.class);
    private final PrintStream out;

    @Inject
    public JsonEventClient(VerifierConfig config)
            throws FileNotFoundException
    {
        requireNonNull(config.getEventLogFile(), "event log file path is null");
        this.out = new PrintStream(config.getEventLogFile());
    }

    @Override
    public void postEvent(VerifierQueryEvent event)
    {
        out.println(serializer.toJson(event));
    }

    @Override
    public void close()
    {
    }
}
