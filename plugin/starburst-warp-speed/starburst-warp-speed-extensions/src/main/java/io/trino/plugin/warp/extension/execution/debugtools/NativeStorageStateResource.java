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
package io.trino.plugin.warp.extension.execution.debugtools;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import io.airlift.log.Logger;
import io.trino.plugin.varada.storage.engine.nativeimpl.NativeStorageStateHandler;
import io.trino.plugin.warp.extension.execution.TaskResource;
import io.trino.plugin.warp.extension.execution.TaskResourceMarker;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;

import static java.util.Objects.requireNonNull;

@Singleton
@Path(NativeStorageStateResource.PATH)
@Consumes(MediaType.APPLICATION_JSON)
@Produces(MediaType.APPLICATION_JSON)
@TaskResourceMarker
public class NativeStorageStateResource
        implements TaskResource
{
    public static final String PATH = "native_storage_state";

    private static final Logger logger = Logger.get(NativeStorageStateResource.class);

    private final NativeStorageStateHandler handler;

    @Inject
    public NativeStorageStateResource(NativeStorageStateHandler handler)
    {
        this.handler = requireNonNull(handler);
    }

    @POST
    public void set(NativeStorageState state)
    {
        handler.setStorageDisableState(state.storagePermanentException, state.storageTemporaryException);
    }

    @GET
    public NativeStorageState get()
    {
        NativeStorageState state = new NativeStorageState(
                handler.isStorageDisabledPermanently(),
                handler.isStorageDisabledTemporarily());
        logger.info("state=%s", state);
        return state;
    }

    public record NativeStorageState(boolean storagePermanentException, boolean storageTemporaryException) {}
}
