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
package io.trino.metadata;

import io.airlift.log.Logger;
import io.trino.server.security.ResourceSecurity;

import javax.inject.Inject;
import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;

import java.util.List;
import java.util.Map;

import static io.trino.server.security.ResourceSecurity.AccessType.PUBLIC;

@Path("/v1/catalog")
public class CatalogResource
{
    private static final Logger log = Logger.get(CatalogResource.class);
    private final DynamicCatalogStore dynamicCatalogStore;

    @Inject
    public CatalogResource(DynamicCatalogStore dynamicCatalogStore)
    {
        this.dynamicCatalogStore = dynamicCatalogStore;
    }

    @ResourceSecurity(PUBLIC)
    @Path("/catalogs")
    @GET
    @Produces(MediaType.APPLICATION_JSON)
    public List<String> getCatalogs()
    {
        return dynamicCatalogStore.getCatalogs();
    }

    @ResourceSecurity(PUBLIC)
    @Path("/register")
    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    public void registerCatalog(@QueryParam("name") String catalogName, Map<String, String> catalogProperties)
    {
        log.info(catalogProperties.toString());
        dynamicCatalogStore.registerCatalog(catalogName, catalogProperties);
    }

    @ResourceSecurity(PUBLIC)
    @Path("/remove")
    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    public void removeCatalog(@QueryParam("name") String catalogName)
    {
        dynamicCatalogStore.removeCatalog(catalogName);
    }

    @ResourceSecurity(PUBLIC)
    @Path("/update")
    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    public void updateCatalog(@QueryParam("name") String catalogName, Map<String, String> catalogProperties)
    {
        log.info(catalogProperties.toString());
        dynamicCatalogStore.updateCatalog(catalogName, catalogProperties);
    }
}
