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
 * @since Fri, 11-02-2022
 */
package io.trino.plugin.hive.procedure;

import com.airtel.africa.canvas.tesseract.querymanager.manager.CubeMetadataCacheManager;
import com.google.common.collect.ImmutableList;
import io.trino.plugin.hive.HdfsEnvironment;
import io.trino.plugin.hive.HiveSessionProperties;
import io.trino.plugin.hive.StylusMetadataConfig;
import io.trino.spi.TrinoException;
import io.trino.spi.classloader.ThreadContextClassLoader;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.procedure.Procedure;

import javax.inject.Inject;
import javax.inject.Provider;

import java.lang.invoke.MethodHandle;
import java.util.Optional;

import static io.trino.spi.StandardErrorCode.GENERIC_USER_ERROR;
import static io.trino.spi.StandardErrorCode.INVALID_SESSION_PROPERTY;
import static io.trino.spi.block.MethodHandleUtil.methodHandle;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.util.Objects.requireNonNull;

/**
 * API to refresh the tesseract cache for a table
 */
public class TesseractCacheRefreshProcedure
        implements Provider<Procedure>
{
    private static final MethodHandle TESSERACT_REFRESH_CACHE = methodHandle(
            TesseractCacheRefreshProcedure.class,
            "refreshTesseractCache",
            ConnectorSession.class,
            String.class,
            String.class,
            String.class);

    private final HdfsEnvironment hdfsEnvironment;

    @Inject
    public TesseractCacheRefreshProcedure(HdfsEnvironment hdfsEnvironment)
    {
        this.hdfsEnvironment = requireNonNull(hdfsEnvironment, "hdfsEnvironment is null");
    }

    @Override
    public Procedure get()
    {
        return new Procedure(
                "system",
                "refresh_tesseract_cache",
                ImmutableList.of(new Procedure.Argument("schema_name", VARCHAR), new Procedure.Argument("table_name", VARCHAR),
                        new Procedure.Argument("action", VARCHAR)),
                TESSERACT_REFRESH_CACHE.bindTo(this));
    }

    public void refreshTesseractCache(ConnectorSession session, String schemaName, String tableName, String action)
    {
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(getClass().getClassLoader())) {
            doRefreshTesseractCache(session, schemaName, tableName, Integer.valueOf(action));
        }
    }

    private void doRefreshTesseractCache(ConnectorSession session, String schemaName, String tableName, Integer action)
    {

        initializeTesseractMetadataConfig(session);
        try {
            CubeMetadataCacheManager.refreshTesseractCubeMetadata(String.join(".", schemaName, tableName),
                    action,
                    StylusMetadataConfig.getStylusMetadataStoreConnectionDetails());
        }
        catch (Exception e) {
            throw new TrinoException(GENERIC_USER_ERROR, String.format("Error while refreshing tesseract table : %s for action : %s", String.join(".", schemaName, tableName), action));
        }
    }

    public void initializeTesseractMetadataConfig(ConnectorSession session)
    {
        Optional<String> configLocation = HiveSessionProperties.getStylusMetadataConfigLocation(session);
        if (configLocation.isEmpty()) {
            throw new TrinoException(INVALID_SESSION_PROPERTY, "Tesseract metadata config location not specified in hive catalog");
        }
        StylusMetadataConfig.initializeStylusMetadataConfig(hdfsEnvironment, session, configLocation.get());
    }
}
