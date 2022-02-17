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

import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableMap;
import io.airlift.discovery.client.Announcer;
import io.airlift.discovery.client.ServiceAnnouncement;
import io.airlift.log.Logger;
import io.trino.connector.ConnectorManager;

import javax.inject.Inject;

import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkState;
import static io.airlift.discovery.client.ServiceAnnouncement.serviceAnnouncement;

public class DynamicCatalogStore
{
    private static final Logger log = Logger.get(DynamicCatalogStore.class);
    private final ConnectorManager connectorManager;
    private final CatalogManager catalogManager;
    private final Announcer announcer;

    @Inject
    public DynamicCatalogStore(ConnectorManager connectorManager, CatalogManager catalogManager, Announcer announcer)
    {
        this.connectorManager = connectorManager;
        this.catalogManager = catalogManager;
        this.announcer = announcer;
    }

    public List<String> getCatalogs()
    {
        List<Catalog> catalogs = catalogManager.getCatalogs();
        return catalogs.stream().map(catalog -> catalog.getCatalogName()).collect(Collectors.toList());
    }

    public void registerCatalog(String catalogName, Map<String, String> properties)
    {
        log.info("-- Loading catalog %s --", catalogName);
        String connectorName = properties.remove("connector.name");
        checkState(connectorName != null, "Catalog configuration does not contain connector.name");
        connectorManager.createCatalog(catalogName, connectorName, ImmutableMap.copyOf(properties));
        updateConnectorIds(announcer, catalogName, CatalogAction.ADD);
        log.info("-- Added catalog %s using connector %s --", catalogName, connectorName);
    }

    public void removeCatalog(String catalogName)
    {
        log.info("-- Removing catalog %s --", catalogName);
        connectorManager.dropConnection(catalogName);
        updateConnectorIds(announcer, catalogName, CatalogAction.DELETE);
        log.info("-- Removed catalog %s --", catalogName);
    }

    public void updateCatalog(String catalogName, Map<String, String> properties)
    {
        log.info("-- Updating catalog %s --", catalogName);
        removeCatalog(catalogName);
        registerCatalog(catalogName, properties);
        log.info("-- Updated catalog %s --", catalogName);
    }

    private void updateConnectorIds(Announcer announcer, String catalogName, CatalogAction action)
    {
        // get existing announcement
        ServiceAnnouncement announcement = getTrinoAnnouncement(announcer.getServiceAnnouncements());
        String property = announcement.getProperties().get("connectorIds");

        List<String> values = Splitter.on(',').trimResults().omitEmptyStrings().splitToList(property);
        Set<String> connectorIds = new LinkedHashSet<>(values);
        switch (action) {
            case ADD:
                connectorIds.add(catalogName);
                break;
            case DELETE:
                connectorIds.remove(catalogName);
                break;
        }

        // build announcement with updated sources
        ServiceAnnouncement.ServiceAnnouncementBuilder builder = serviceAnnouncement(announcement.getType());
        for (Map.Entry<String, String> entry : announcement.getProperties().entrySet()) {
            if (!entry.getKey().equals("connectorIds")) {
                builder.addProperty(entry.getKey(), entry.getValue());
            }
        }
        builder.addProperty("connectorIds", Joiner.on(',').join(connectorIds));

        // update announcement
        announcer.removeServiceAnnouncement(announcement.getId());
        announcer.addServiceAnnouncement(builder.build());
    }

    private ServiceAnnouncement getTrinoAnnouncement(Set<ServiceAnnouncement> announcements)
    {
        for (ServiceAnnouncement announcement : announcements) {
            if (announcement.getType().equals("trino")) {
                return announcement;
            }
        }
        throw new IllegalArgumentException("Trino announcement not found: " + announcements);
    }

    public enum CatalogAction
    {
        ADD, DELETE
    }
}
