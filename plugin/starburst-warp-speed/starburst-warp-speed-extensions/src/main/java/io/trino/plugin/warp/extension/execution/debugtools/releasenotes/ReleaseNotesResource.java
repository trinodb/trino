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
package io.trino.plugin.warp.extension.execution.debugtools.releasenotes;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.inject.Inject;
import io.airlift.json.ObjectMapperProvider;
import io.airlift.log.Logger;
import io.trino.plugin.warp.extension.execution.TaskResource;
import io.trino.plugin.warp.extension.execution.TaskResourceMarker;
import io.varada.annotation.Audit;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;

import java.io.IOException;
import java.net.URL;
import java.util.List;

import static io.trino.plugin.warp.extension.execution.debugtools.releasenotes.ReleaseNotesResource.RELEASE_NOTES_PATH;
import static java.util.Objects.requireNonNull;

@TaskResourceMarker(worker = false)
//@Api(value = "Release Notes", tags = "Release notes")
@Path(RELEASE_NOTES_PATH)
@Consumes(MediaType.APPLICATION_JSON)
@Produces(MediaType.APPLICATION_JSON)
public class ReleaseNotesResource
        implements TaskResource
{
    public static final String RELEASE_NOTES_PATH = "release-notes";
    private static final Logger logger = Logger.get(ReleaseNotesResource.class);

    private final ObjectMapperProvider objectMapperProvider;

    @Inject
    public ReleaseNotesResource(ObjectMapperProvider objectMapperProvider)
    {
        this.objectMapperProvider = requireNonNull(objectMapperProvider);
    }

    @Audit
    @POST
    //@ApiOperation(value = "release-notes", extensions = {@Extension(properties = @ExtensionProperty(name = "exposing-level", value = "DEBUG"))})
    public List<ReleaseNoteVersionData> getReleaseNotes(ReleaseNotesRequestData requestData)
    {
        URL resource = this.getClass().getClassLoader().getResource("release-notes.txt");
        ObjectMapper om = objectMapperProvider.get();
        List<ReleaseNoteVersionData> ret = null;
        try {
            List<ReleaseNoteVersionData> list = om.readValue(resource, new TypeReference<>() {});
            ret = switch (requestData.type()) {
                case LATEST -> List.of(list.get(0));
                case ALL -> list;
                default -> null;
            };
        }
        catch (IOException e) {
            logger.warn("failed to read release notes file.");
        }
        return ret;
    }
}
