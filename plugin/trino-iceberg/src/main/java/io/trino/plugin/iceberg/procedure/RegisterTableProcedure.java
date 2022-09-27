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
package io.trino.plugin.iceberg.procedure;

import com.google.common.collect.ImmutableList;
import io.trino.plugin.iceberg.catalog.TrinoCatalog;
import io.trino.plugin.iceberg.catalog.TrinoCatalogFactory;
import io.trino.spi.TrinoException;
import io.trino.spi.classloader.ThreadContextClassLoader;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.procedure.Procedure;

import javax.inject.Inject;
import javax.inject.Provider;

import java.lang.invoke.MethodHandle;
import java.util.Optional;

import static io.trino.spi.StandardErrorCode.INVALID_ARGUMENTS;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.lang.String.format;
import static java.lang.invoke.MethodHandles.lookup;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;

public class RegisterTableProcedure
        implements Provider<Procedure>
{
    private static final MethodHandle REGISTER_TABLE;

    private static final String PROCEDURE_NAME = "register_table";
    private static final String SYSTEM_SCHEMA = "system";

    private static final String SCHEMA_NAME = "SCHEMA_NAME";
    private static final String TABLE_NAME = "TABLE_NAME";
    private static final String TABLE_LOCATION = "TABLE_LOCATION";
    private static final String METADATA_FILE_NAME = "METADATA_FILE_NAME";

    private static final String PROCEDURE_USAGE_EXAMPLES = format(
            "Valid usages:%n" +
                    " - %1$s(%2$s => ..., %3$s => ..., %4$s => ...)%n" +
                    " - %1$s(%2$s => ..., %3$s => ..., %4$s => ..., %5$s => ...)",
            PROCEDURE_NAME,
            // Use lowercase parameter names per convention. In the usage example the names are not delimited.
            SCHEMA_NAME.toLowerCase(ENGLISH),
            TABLE_NAME.toLowerCase(ENGLISH),
            TABLE_LOCATION.toLowerCase(ENGLISH),
            METADATA_FILE_NAME.toLowerCase(ENGLISH));

    static {
        try {
            REGISTER_TABLE = lookup().unreflect(RegisterTableProcedure.class.getMethod("registerTable", ConnectorSession.class, String.class, String.class, String.class, String.class));
        }
        catch (ReflectiveOperationException e) {
            throw new AssertionError(e);
        }
    }

    private final TrinoCatalogFactory catalogFactory;
    private final ClassLoader classLoader;

    @Inject
    public RegisterTableProcedure(TrinoCatalogFactory catalogFactory)
    {
        this.catalogFactory = requireNonNull(catalogFactory, "catalogFactory is null");
        // this class is loaded by PluginClassLoader, and we need its reference to be stored
        this.classLoader = getClass().getClassLoader();
    }

    @Override
    public Procedure get()
    {
        return new Procedure(
                SYSTEM_SCHEMA,
                PROCEDURE_NAME,
                ImmutableList.of(
                        new Procedure.Argument(SCHEMA_NAME, VARCHAR),
                        new Procedure.Argument(TABLE_NAME, VARCHAR),
                        new Procedure.Argument(TABLE_LOCATION, VARCHAR),
                        new Procedure.Argument(METADATA_FILE_NAME, VARCHAR, false, null)),
                REGISTER_TABLE.bindTo(this));
    }

    public void registerTable(ConnectorSession clientSession, String schemaName, String tableName, String tableLocation, String metadataFileName)
    {
        // this line guarantees that classLoader that we stored in the field will be used inside try/catch
        // as we captured reference to PluginClassLoader during initialization of this class
        // we can use it now to correctly execute the procedure
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(classLoader)) {
            doRegisterTable(
                    clientSession,
                    schemaName,
                    tableName,
                    tableLocation,
                    Optional.ofNullable(metadataFileName));
        }
    }

    private void doRegisterTable(ConnectorSession clientSession, String schemaName, String tableName, String tableLocation, Optional<String> metadataFileName)
    {
        if (schemaName == null || tableName == null || tableLocation == null) {
            throw new TrinoException(INVALID_ARGUMENTS, "Illegal parameter set passed. " + PROCEDURE_USAGE_EXAMPLES);
        }
        SchemaTableName schemaTableName = new SchemaTableName(schemaName, tableName);
        TrinoCatalog catalog = catalogFactory.create(clientSession.getIdentity());
        catalog.registerTable(clientSession, schemaTableName, tableLocation, metadataFileName);
    }
}
