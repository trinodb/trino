/**
 * Licensed to the Airtel International LLP (AILLP) under one
 * or more contributor license agreements.
 * The AILLP licenses this file to you under the AA License, Version 1.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * @author Akash Roy
 * @department Big Data Analytics Airtel Africa
 * @since Wed, 09-02-2022
 */
package io.trino.plugin.hive;

import com.airtel.africa.canvas.common.db.DBConfig;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import com.google.gson.stream.JsonReader;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ConnectorSession;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Map;

import static io.trino.spi.StandardErrorCode.INVALID_SESSION_PROPERTY;

/**
 * Singleton to store the connection details of the RDBMS containing stylus's metadata
 */
public class StylusMetadataConfig {

    private static final Gson gson = new Gson();
    private static volatile DBConfig.Properties properties = null;

    public static void initializeStylusMetadataConfig(HdfsEnvironment hdfsEnvironment, ConnectorSession session, String configLocation){

        if(properties == null) {
            synchronized (StylusMetadataConfig.class) {
                if(properties == null) {
                    try (FileSystem fileSystem = hdfsEnvironment.getFileSystem(new HdfsEnvironment.HdfsContext(session), new Path(configLocation));
                         FSDataInputStream dataInputStream = fileSystem.open(new Path(configLocation));
                         BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(dataInputStream));
                         JsonReader jsonReader = new JsonReader(bufferedReader)) {

                        Map<String, DBConfig.Properties> map = gson.fromJson(jsonReader, new TypeToken<Map<String, DBConfig.Properties>>() {
                        }.getType());
                        properties = map.get(DBConfig.Database.POSTGRES.getDatabase());

                    } catch (IOException e) {
                        throw new TrinoException(INVALID_SESSION_PROPERTY, "Invalid location URI: " + configLocation, e);
                    }
                }
            }
        }
    }

    public static DBConfig.Properties getStylusMetadataStoreConnectionDetails(){
        return properties;
    }

    private StylusMetadataConfig(){}

}
