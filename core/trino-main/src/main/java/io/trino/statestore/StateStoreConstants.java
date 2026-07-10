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
package io.trino.statestore;

/**
 * Class that holds constant values for state store
 *
 * @since 2020-02-06
 */

public class StateStoreConstants
{
    private StateStoreConstants() {}

    public static final String MERGEMAP = "merged";

    /**
     * State store config path
     */
    public static final String STATE_STORE_CONFIGURATION_PATH = "etc/state-store.properties";

    /**
     * State store type
     */
    public static final String STATE_STORE_TYPE_PROPERTY_NAME = "state-store.type";

    /**
     * State store name
     */
    public static final String STATE_STORE_NAME_PROPERTY_NAME = "state-store.name";

    /**
     * State store cluster
     */
    public static final String STATE_STORE_CLUSTER_PROPERTY_NAME = "state-store.cluster";

    /**
     * Query state collection name
     */
    public static final String QUERY_STATE_COLLECTION_NAME = "query";

    /**
     * Finished query state collection name
     */
    public static final String FINISHED_QUERY_STATE_COLLECTION_NAME = "finished-query";

    /**
     * OOM Query state collection name
     */
    public static final String OOM_QUERY_STATE_COLLECTION_NAME = "oom-query";

    /**
     * Resource group state collection name
     */
    public static final String RESOURCE_GROUP_STATE_COLLECTION_NAME = "resourceGroup";

    /**
     * Cluster CPU usage state collection name
     */
    public static final String CPU_USAGE_STATE_COLLECTION_NAME = "cpu-usage";

    /**
     * Transaction state collection name
     */
    public static final String TRANSACTION_STATE_COLLECTION_NAME = "transaction";

    /**
     * Discovery service state collection name
     */
    public static final String DISCOVERY_SERVICE_COLLECTION_NAME = "discovery-service";

    /**
     * Split cache metadata collection
     */
    public static final String SPLIT_CACHE_METADATA_NAME = "split-cache-metadata-map";

    /**
     * Lock name for submitting new query
     */
    public static final String SUBMIT_QUERY_LOCK_NAME = "submit-query-lock";

    /**
     * Lock name for handling expired query
     */
    public static final String HANDLE_EXPIRED_QUERY_LOCK_NAME = "handle-expired-query-lock";

    /**
     * Lock name for handling OOM query
     */
    public static final String HANDLE_OOM_QUERY_LOCK_NAME = "handle-oom-query-lock";

    /**
     * Lock name for updating cluster CPU usage
     */
    public static final String UPDATE_CPU_USAGE_LOCK_NAME = "update-cpu-usage-lock";

    /**
     * Default time in milliseconds for acquiring a lock
     */
    public static final long DEFAULT_ACQUIRED_LOCK_TIME_MS = 1000L;

    /**
     * Hazelcast
     */
    public static final String HAZELCAST = "hazelcast";

    /**
     * Hazelcast discovery port property name
     */
    public static final String HAZELCAST_DISCOVERY_PORT_PROPERTY_NAME = "hazelcast.discovery.port";

    /**
     * Hazelcast default discovery port
     */
    public static final String DEFAULT_HAZELCAST_DISCOVERY_PORT = "5701";

    /**
     * Hazelcast discovery mode property name
     */
    public static final String DISCOVERY_MODE_PROPERTY_NAME = "hazelcast.discovery.mode";

    /**
     * Hazelcast discovery mode tcp-ip
     */
    public static final String DISCOVERY_MODE_TCPIP = "tcp-ip";

    /**
     * Cross region dynamic filter collection suffix
     */
    public static final String CROSS_REGION_DYNAMIC_FILTER_COLLECTION = "-dynamic-filters";

    /**
     * cross region dynamic filter collections
     */
    public static final String CROSS_REGION_DYNAMIC_FILTERS = "cross-region-dynamic-filters";

    /**
     * query column names to output symbols mapping
     */
    public static final String QUERY_COLUMN_NAME_TO_SYMBOL_MAPPING = "-query-column-name-to-symbol-mapping";

    /**
     * cross layer dynamic filter
     */
    public static final String CROSS_LAYER_DYNAMIC_FILTER = "-cross-layer-dynamic-filter";

    /**
     * Hazelcast tcp-ip members
     */
    public static final String HAZELCAST_DISCOVERY_TCPIP_SEEDS = "hazelcast.discovery.tcp-ip.seeds";

    /**
     * Hazelcast tcp-ip profile
     */
    public static final String HAZELCAST_DISCOVERY_TCPIP_PROFILE = "hazelcast.discovery.tcp-ip.profile";
}
