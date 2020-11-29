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
package io.prestosql.metadata;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import io.airlift.discovery.client.Announcer;
import io.airlift.discovery.client.ServiceAnnouncement;
import io.airlift.log.Logger;
import io.prestosql.connector.ConnectorManager;

import java.sql.*;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

import static com.google.common.base.Preconditions.checkState;

public class DynamicCatalogStore
{
    private static final Logger log = Logger.get(DynamicCatalogStore.class);

    static final String JDBC_DRIVER = "com.mysql.jdbc.Driver";
    static final String DB_URL = "jdbc:mysql://localhost:3306/DataplayGround";
    static final String USER = "dpuser";
    static final String PASS = "dpuser";

    private static DynamicCatalogStore dynamicCatalogStore = new DynamicCatalogStore();

    private Connection dynamicCatalogDBStore = null;

    private ConnectorManager connectorManager;
    private Announcer announcer;

    public void init(ConnectorManager connectorManager, Announcer announcer) throws ClassNotFoundException, SQLException {
        this.connectorManager = connectorManager;
        this.announcer = announcer;
        this.initDynamicCatalogStoreConnection();
    }

    private void initDynamicCatalogStoreConnection() throws ClassNotFoundException, SQLException {
        Class.forName("com.mysql.jdbc.Driver");
        this.dynamicCatalogDBStore = DriverManager.getConnection(DB_URL,USER,PASS);
    }

    public static DynamicCatalogStore getInstance() {
        return dynamicCatalogStore;
    }

    public boolean loadCatalogIfExists(String catalogName) {
        log.info("-- Loading catalog %s --", catalogName);

        if(this.connectorManager != null && this.dynamicCatalogDBStore != null) {
            //String connectorName = "hive-hadoop2";
            ImmutableMap.Builder<String, String> connectorProperties = ImmutableMap.builder();

            try {
                String connectorName = this.getDynamicCatalogIfExists(catalogName, connectorProperties);
                checkState(connectorName != null, "Configuration for catalog %s does not contain connector.name", catalogName);

                connectorManager.createCatalog(catalogName, connectorName, connectorProperties.build());
                updateConnectorIds(this.announcer,catalogName);
                log.info("-- Added catalog %s using connector %s --", catalogName, connectorName);
                return true;
            } catch (SQLException throwables) {
                throwables.printStackTrace();
            }
        }
        return false;
    }

    private String getDynamicCatalogIfExists(String catalogName, ImmutableMap.Builder<String, String> connectorProperties) throws SQLException {

        String connectorId = "";
        String sql = String.format("select connector_id, properties from DP_Catalog where id = '%s'", catalogName);

        Statement stmt = null;
        ResultSet rs = null;
        try {
            stmt = this.dynamicCatalogDBStore.createStatement();
            rs = stmt.executeQuery(sql);

            if(rs.next()) {
                connectorId = rs.getString("connector_id");
                String properties = rs.getString("properties");

                ObjectMapper mapper = new ObjectMapper();
                TypeReference<HashMap<String, String>> typeRef = new TypeReference<>() {};
                Map<String, String> map = mapper.readValue(properties, typeRef);
                if(map != null) {
                    for (Map.Entry<String,String> entry : map.entrySet()) {
                        connectorProperties.put(entry.getKey(), entry.getValue());
                    }
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
        } catch (JsonMappingException e) {
            e.printStackTrace();
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        } finally {
            if(rs != null) {
                rs.close();
            }
            if(stmt != null) {
                stmt.close();
            }

        }

        return connectorId;
    }

    private static void updateConnectorIds(Announcer announcer, String connectorName)
    {
        // get existing announcement
        ServiceAnnouncement announcement = getPrestoAnnouncement(announcer.getServiceAnnouncements());

        // update announcement and thrift port property
        Map<String, String> properties = new LinkedHashMap<>(announcement.getProperties());
        String connectorIds = properties.getOrDefault("connectorIds","");
        if(connectorIds != "") {
            connectorIds += ",";
        }
        connectorIds += connectorName;

        properties.put("connectorIds", connectorIds);
        announcer.removeServiceAnnouncement(announcement.getId());
        announcer.addServiceAnnouncement(ServiceAnnouncement.serviceAnnouncement(announcement.getType()).addProperties(properties).build());

        announcer.forceAnnounce();
    }

    private static ServiceAnnouncement getPrestoAnnouncement(Set<ServiceAnnouncement> announcements)
    {
        for (ServiceAnnouncement announcement : announcements) {
            if (announcement.getType().equals("presto")) {
                return announcement;
            }
        }
        throw new IllegalArgumentException("Presto announcement not found: " + announcements);
    }

}
