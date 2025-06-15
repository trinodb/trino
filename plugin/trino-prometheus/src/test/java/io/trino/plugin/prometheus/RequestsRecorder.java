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
package io.trino.plugin.prometheus;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.testcontainers.containers.output.OutputFrame;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Consumer;

public class RequestsRecorder
        implements Consumer<OutputFrame>
{
    private final ObjectMapper objectMapper = new ObjectMapper();
    public final List<LoggedRequest> requests = new CopyOnWriteArrayList<>();

    private List<LoggedRequest> requestsWithoutDiscovery;

    public List<LoggedRequest> getRequestsWithoutDiscovery()
    {
        if (requestsWithoutDiscovery != null) {
            return requestsWithoutDiscovery;
        }
        requestsWithoutDiscovery = requests.stream()
                .filter(request -> !request.uri.equals("/api/v1/label/__name__/values"))
                .toList();
        return requestsWithoutDiscovery;
    }

    @Override
    public void accept(OutputFrame outputFrame)
    {
        String text = outputFrame.getUtf8String().trim();
        try {
            LoggedRequest req = objectMapper.readValue(text, LoggedRequest.class);
            requests.add(req);
        }
        catch (Exception e) {
            // ignore non-JSON lines
        }
    }
}
