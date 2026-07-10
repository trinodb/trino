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
package io.trino.spi.seedstore;

import java.io.IOException;
import java.io.Serializable;

/**
 * Seed that contains information of a seed node
 *
 * @since 2020-03-04
 */
public interface Seed
        extends Serializable
{
    /**
     * Constant property name for seed node location
     */
    String LOCATION_PROPERTY_NAME = "location";

    /**
     * Constant property name for seed node info timestamp
     */
    String TIMESTAMP_PROPERTY_NAME = "timestamp";

    /**
     * Internal state store URI (currently only used in FileBasedSeedOnYarn)
     */
    String INTERNAL_STATE_STORE_URI_PROPERTY_NAME = "internal-state-store-uri";

    /**
     * Get location of seed
     *
     * @return location of seed
     */
    String getLocation();

    /**
     * St location of seed
     *
     * @return void
     */
    void setLocation(String location);

    /**
     * Get timestamp of seed
     *
     * @return timestamp of seed
     */
    long getTimestamp();

    /**
     * Set timestamp of seed
     *
     * @return void
     */
    void setTimestamp(long timestamp);

    /**
     * Get attribute of seed
     *
     * @return value of attribute
     */
    String getUniqueInstanceId();

    /**
     * Serialize seed object to string and return
     *
     * @return serialized string
     * @throws IOException exception when failed to serialize
     */
    String serialize()
            throws IOException;
}
