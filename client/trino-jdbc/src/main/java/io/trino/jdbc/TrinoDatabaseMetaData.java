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
package io.trino.jdbc;

import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import io.trino.client.ClientStandardTypes;
import io.trino.client.ClientTypeSignature;
import io.trino.client.ClientTypeSignatureParameter;
import io.trino.client.Column;
import jakarta.annotation.Nullable;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.RowIdLifetime;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.Lists.newArrayList;
import static io.trino.client.ClientTypeSignature.VARCHAR_UNBOUNDED_LENGTH;
import static io.trino.jdbc.DriverInfo.DRIVER_NAME;
import static io.trino.jdbc.DriverInfo.DRIVER_VERSION;
import static io.trino.jdbc.DriverInfo.DRIVER_VERSION_MAJOR;
import static io.trino.jdbc.DriverInfo.DRIVER_VERSION_MINOR;
import static java.lang.Integer.parseInt;
import static java.util.Objects.requireNonNull;

public class TrinoDatabaseMetaData
        implements DatabaseMetaData
{
    private static final String SEARCH_STRING_ESCAPE = "\\";

    private final TrinoConnection connection;
    private final boolean assumeLiteralNamesInMetadataCallsForNonConformingClients;
    private final boolean assumeLiteralUnderscoreInMetadataCallsForNonConformingClients;

    TrinoDatabaseMetaData(
            TrinoConnection connection,
            boolean assumeLiteralNamesInMetadataCallsForNonConformingClients,
            boolean assumeLiteralUnderscoreInMetadataCallsForNonConformingClients)
    {
        this.connection = requireNonNull(connection, "connection is null");
        this.assumeLiteralNamesInMetadataCallsForNonConformingClients = assumeLiteralNamesInMetadataCallsForNonConformingClients;
        this.assumeLiteralUnderscoreInMetadataCallsForNonConformingClients = assumeLiteralUnderscoreInMetadataCallsForNonConformingClients;
    }

    @Override
    public boolean allProceduresAreCallable()
            throws SQLException
    {
        return false;
    }

    @Override
    public boolean allTablesAreSelectable()
            throws SQLException
    {
        return false;
    }

    @Override
    public String getURL()
            throws SQLException
    {
        return "jdbc:" + connection.getURI().toString();
    }

    @Override
    public String getUserName()
            throws SQLException
    {
        try (ResultSet rs = select("SELECT current_user")) {
            rs.next();
            return rs.getString(1);
        }
    }

    @Override
    public boolean isReadOnly()
            throws SQLException
    {
        return connection.isReadOnly();
    }

    @Override
    public boolean nullsAreSortedHigh()
            throws SQLException
    {
        return false;
    }

    @Override
    public boolean nullsAreSortedLow()
            throws SQLException
    {
        return false;
    }

    @Override
    public boolean nullsAreSortedAtStart()
            throws SQLException
    {
        return false;
    }

    @Override
    public boolean nullsAreSortedAtEnd()
            throws SQLException
    {
        return true;
    }

    @Override
    public String getDatabaseProductName()
            throws SQLException
    {
        return "Trino";
    }

    @Override
    public String getDatabaseProductVersion()
            throws SQLException
    {
        Optional<String> serverVersion = connection.getServerVersion();
        if (serverVersion.isPresent()) {
            return serverVersion.orElseThrow();
        }

        try (ResultSet rs = select("SELECT version()")) {
            rs.next();
            return rs.getString(1);
        }
    }

    @Override
    public String getDriverName()
            throws SQLException
    {
        return DRIVER_NAME;
    }

    @Override
    public String getDriverVersion()
            throws SQLException
    {
        return DRIVER_VERSION;
    }

    @Override
    public int getDriverMajorVersion()
    {
        return DRIVER_VERSION_MAJOR;
    }

    @Override
    public int getDriverMinorVersion()
    {
        return DRIVER_VERSION_MINOR;
    }

    @Override
    public boolean usesLocalFiles()
            throws SQLException
    {
        return false;
    }

    @Override
    public boolean usesLocalFilePerTable()
            throws SQLException
    {
        return false;
    }

    @Override
    public boolean supportsMixedCaseIdentifiers()
            throws SQLException
    {
        return true;
    }

    @Override
    public boolean storesUpperCaseIdentifiers()
            throws SQLException
    {
        return false;
    }

    @Override
    public boolean storesLowerCaseIdentifiers()
            throws SQLException
    {
        return false;
    }

    @Override
    public boolean storesMixedCaseIdentifiers()
            throws SQLException
    {
        return true;
    }

    @Override
    public boolean supportsMixedCaseQuotedIdentifiers()
            throws SQLException
    {
        // TODO: support quoted identifiers properly
        return true;
    }

    @Override
    public boolean storesUpperCaseQuotedIdentifiers()
            throws SQLException
    {
        return false;
    }

    @Override
    public boolean storesLowerCaseQuotedIdentifiers()
            throws SQLException
    {
        // TODO: support quoted identifiers properly
        return false;
    }

    @Override
    public boolean storesMixedCaseQuotedIdentifiers()
            throws SQLException
    {
        // TODO: support quoted identifiers properly
        return true;
    }

    @Override
    public String getIdentifierQuoteString()
            throws SQLException
    {
        return "\"";
    }

    @Override
    public String getSQLKeywords()
            throws SQLException
    {
        return "LIMIT";
    }

    @Override
    public String getNumericFunctions()
            throws SQLException
    {
        return "";
    }

    @Override
    public String getStringFunctions()
            throws SQLException
    {
        return "";
    }

    @Override
    public String getSystemFunctions()
            throws SQLException
    {
        return "";
    }

    @Override
    public String getTimeDateFunctions()
            throws SQLException
    {
        return "";
    }

    @Override
    public String getSearchStringEscape()
            throws SQLException
    {
        return SEARCH_STRING_ESCAPE;
    }

    @Override
    public String getExtraNameCharacters()
            throws SQLException
    {
        return "";
    }

    @Override
    public boolean supportsAlterTableWithAddColumn()
            throws SQLException
    {
        return false;
    }

    @Override
    public boolean supportsAlterTableWithDropColumn()
            throws SQLException
    {
        return false;
    }

    @Override
    public boolean supportsColumnAliasing()
            throws SQLException
    {
        return true;
    }

    @Override
    public boolean nullPlusNonNullIsNull()
            throws SQLException
    {
        return true;
    }

    @Override
    public boolean supportsConvert()
            throws SQLException
    {
        // TODO: support convert
        return false;
    }

    @Override
    public boolean supportsConvert(int fromType, int toType)
            throws SQLException
    {
        // TODO: support convert
        return false;
    }

    @Override
    public boolean supportsTableCorrelationNames()
            throws SQLException
    {
        return true;
    }

    @Override
    public boolean supportsDifferentTableCorrelationNames()
            throws SQLException
    {
        return false;
    }

    @Override
    public boolean supportsExpressionsInOrderBy()
            throws SQLException
    {
        return true;
    }

    @Override
    public boolean supportsOrderByUnrelated()
            throws SQLException
    {
        return true;
    }

    @Override
    public boolean supportsGroupBy()
            throws SQLException
    {
        return true;
    }

    @Override
    public boolean supportsGroupByUnrelated()
            throws SQLException
    {
        return true;
    }

    @Override
    public boolean supportsGroupByBeyondSelect()
            throws SQLException
    {
        return true;
    }

    @Override
    public boolean supportsLikeEscapeClause()
            throws SQLException
    {
        return true;
    }

    @Override
    public boolean supportsMultipleResultSets()
            throws SQLException
    {
        return false;
    }

    @Override
    public boolean supportsMultipleTransactions()
            throws SQLException
    {
        return true;
    }

    @Override
    public boolean supportsNonNullableColumns()
            throws SQLException
    {
        return true;
    }

    @Override
    public boolean supportsMinimumSQLGrammar()
            throws SQLException
    {
        return true;
    }

    @Override
    public boolean supportsCoreSQLGrammar()
            throws SQLException
    {
        // TODO: support this
        return false;
    }

    @Override
    public boolean supportsExtendedSQLGrammar()
            throws SQLException
    {
        // TODO: support this
        return false;
    }

    @Override
    public boolean supportsANSI92EntryLevelSQL()
            throws SQLException
    {
        // TODO: verify this
        return true;
    }

    @Override
    public boolean supportsANSI92IntermediateSQL()
            throws SQLException
    {
        // TODO: support this
        return false;
    }

    @Override
    public boolean supportsANSI92FullSQL()
            throws SQLException
    {
        // TODO: support this
        return false;
    }

    @Override
    public boolean supportsIntegrityEnhancementFacility()
            throws SQLException
    {
        return false;
    }

    @Override
    public boolean supportsOuterJoins()
            throws SQLException
    {
        return true;
    }

    @Override
    public boolean supportsFullOuterJoins()
            throws SQLException
    {
        return true;
    }

    @Override
    public boolean supportsLimitedOuterJoins()
            throws SQLException
    {
        return true;
    }

    @Override
    public String getSchemaTerm()
            throws SQLException
    {
        return "schema";
    }

    @Override
    public String getProcedureTerm()
            throws SQLException
    {
        return "procedure";
    }

    @Override
    public String getCatalogTerm()
            throws SQLException
    {
        return "catalog";
    }

    @Override
    public boolean isCatalogAtStart()
            throws SQLException
    {
        return true;
    }

    @Override
    public String getCatalogSeparator()
            throws SQLException
    {
        return ".";
    }

    @Override
    public boolean supportsSchemasInDataManipulation()
            throws SQLException
    {
        return true;
    }

    @Override
    public boolean supportsSchemasInProcedureCalls()
            throws SQLException
    {
        return true;
    }

    @Override
    public boolean supportsSchemasInTableDefinitions()
            throws SQLException
    {
        return true;
    }

    @Override
    public boolean supportsSchemasInIndexDefinitions()
            throws SQLException
    {
        return true;
    }

    @Override
    public boolean supportsSchemasInPrivilegeDefinitions()
            throws SQLException
    {
        return true;
    }

    @Override
    public boolean supportsCatalogsInDataManipulation()
            throws SQLException
    {
        return true;
    }

    @Override
    public boolean supportsCatalogsInProcedureCalls()
            throws SQLException
    {
        return true;
    }

    @Override
    public boolean supportsCatalogsInTableDefinitions()
            throws SQLException
    {
        return true;
    }

    @Override
    public boolean supportsCatalogsInIndexDefinitions()
            throws SQLException
    {
        return true;
    }

    @Override
    public boolean supportsCatalogsInPrivilegeDefinitions()
            throws SQLException
    {
        return true;
    }

    @Override
    public boolean supportsPositionedDelete()
            throws SQLException
    {
        return false;
    }

    @Override
    public boolean supportsPositionedUpdate()
            throws SQLException
    {
        return false;
    }

    @Override
    public boolean supportsSelectForUpdate()
            throws SQLException
    {
        return false;
    }

    @Override
    public boolean supportsStoredProcedures()
            throws SQLException
    {
        // TODO: support stored procedure escape syntax
        return false;
    }

    @Override
    public boolean supportsSubqueriesInComparisons()
            throws SQLException
    {
        return true;
    }

    @Override
    public boolean supportsSubqueriesInExists()
            throws SQLException
    {
        return true;
    }

    @Override
    public boolean supportsSubqueriesInIns()
            throws SQLException
    {
        return true;
    }

    @Override
    public boolean supportsSubqueriesInQuantifieds()
            throws SQLException
    {
        return true;
    }

    @Override
    public boolean supportsCorrelatedSubqueries()
            throws SQLException
    {
        return true;
    }

    @Override
    public boolean supportsUnion()
            throws SQLException
    {
        return true;
    }

    @Override
    public boolean supportsUnionAll()
            throws SQLException
    {
        return true;
    }

    @Override
    public boolean supportsOpenCursorsAcrossCommit()
            throws SQLException
    {
        return false;
    }

    @Override
    public boolean supportsOpenCursorsAcrossRollback()
            throws SQLException
    {
        return false;
    }

    @Override
    public boolean supportsOpenStatementsAcrossCommit()
            throws SQLException
    {
        return false;
    }

    @Override
    public boolean supportsOpenStatementsAcrossRollback()
            throws SQLException
    {
        return false;
    }

    @Override
    public int getMaxBinaryLiteralLength()
            throws SQLException
    {
        return 0;
    }

    @Override
    public int getMaxCharLiteralLength()
            throws SQLException
    {
        return 0;
    }

    @Override
    public int getMaxColumnNameLength()
            throws SQLException
    {
        // TODO: define max identifier length
        return 0;
    }

    @Override
    public int getMaxColumnsInGroupBy()
            throws SQLException
    {
        return 0;
    }

    @Override
    public int getMaxColumnsInIndex()
            throws SQLException
    {
        return 0;
    }

    @Override
    public int getMaxColumnsInOrderBy()
            throws SQLException
    {
        return 0;
    }

    @Override
    public int getMaxColumnsInSelect()
            throws SQLException
    {
        return 0;
    }

    @Override
    public int getMaxColumnsInTable()
            throws SQLException
    {
        return 0;
    }

    @Override
    public int getMaxConnections()
            throws SQLException
    {
        return 0;
    }

    @Override
    public int getMaxCursorNameLength()
            throws SQLException
    {
        return 0;
    }

    @Override
    public int getMaxIndexLength()
            throws SQLException
    {
        return 0;
    }

    @Override
    public int getMaxSchemaNameLength()
            throws SQLException
    {
        // TODO: define max identifier length
        return 0;
    }

    @Override
    public int getMaxProcedureNameLength()
            throws SQLException
    {
        // TODO: define max identifier length
        return 0;
    }

    @Override
    public int getMaxCatalogNameLength()
            throws SQLException
    {
        // TODO: define max identifier length
        return 0;
    }

    @Override
    public int getMaxRowSize()
            throws SQLException
    {
        return 0;
    }

    @Override
    public boolean doesMaxRowSizeIncludeBlobs()
            throws SQLException
    {
        return true;
    }

    @Override
    public int getMaxStatementLength()
            throws SQLException
    {
        return 0;
    }

    @Override
    public int getMaxStatements()
            throws SQLException
    {
        return 0;
    }

    @Override
    public int getMaxTableNameLength()
            throws SQLException
    {
        // TODO: define max identifier length
        return 0;
    }

    @Override
    public int getMaxTablesInSelect()
            throws SQLException
    {
        return 0;
    }

    @Override
    public int getMaxUserNameLength()
            throws SQLException
    {
        // TODO: define max identifier length
        return 0;
    }

    @Override
    public int getDefaultTransactionIsolation()
            throws SQLException
    {
        return Connection.TRANSACTION_READ_UNCOMMITTED;
    }

    @Override
    public boolean supportsTransactions()
            throws SQLException
    {
        return true;
    }

    @Override
    public boolean supportsTransactionIsolationLevel(int level)
            throws SQLException
    {
        return true;
    }

    @Override
    public boolean supportsDataDefinitionAndDataManipulationTransactions()
            throws SQLException
    {
        return true;
    }

    @Override
    public boolean supportsDataManipulationTransactionsOnly()
            throws SQLException
    {
        return false;
    }

    @Override
    public boolean dataDefinitionCausesTransactionCommit()
            throws SQLException
    {
        return false;
    }

    @Override
    public boolean dataDefinitionIgnoredInTransactions()
            throws SQLException
    {
        return false;
    }

    @Override
    public ResultSet getProcedures(String catalog, String schemaPattern, String procedureNamePattern)
            throws SQLException
    {
        schemaPattern = escapeIfNecessary(schemaPattern);
        procedureNamePattern = escapeIfNecessary(procedureNamePattern);
        String procedureCat = quoted("PROCEDURE_CAT");
        String procedureSchem = quoted("PROCEDURE_SCHEM");
        String procedureName = quoted("PROCEDURE_NAME");
        String specificName = quoted("SPECIFIC_NAME");
        String query = "SELECT " + procedureCat + ", " + procedureSchem + ", " + procedureName + ",\n " +
                "NULL, NULL, NULL, " + quoted("REMARKS") + ", " + quoted("PROCEDURE_TYPE") + ", " + specificName + "\n" +
                "FROM system.jdbc.procedures\n" +
                "ORDER BY " + procedureCat + ", " + procedureSchem + ", " + procedureName + ", " + specificName;
        return selectEmpty(query);
    }

    @Override
    public ResultSet getProcedureColumns(String catalog, String schemaPattern, String procedureNamePattern, String columnNamePattern)
            throws SQLException
    {
        schemaPattern = escapeIfNecessary(schemaPattern);
        procedureNamePattern = escapeIfNecessary(procedureNamePattern);
        columnNamePattern = escapeIfNecessary(columnNamePattern);
        String procedureCat = quoted("PROCEDURE_CAT");
        String procedureSchem = quoted("PROCEDURE_SCHEM");
        String procedureName = quoted("PROCEDURE_NAME");
        String specificName = quoted("SPECIFIC_NAME");
        String columnName = quoted("COLUMN_NAME");

        String query = "SELECT " + procedureCat + ", " + procedureSchem + ", " + procedureName + ", " + columnName + ", " +
                quoted("COLUMN_TYPE") + ", " + quoted("DATA_TYPE") + ", " + quoted("TYPE_NAME") + ",\n" +
                quoted("PRECISION") + ", " + quoted("LENGTH") + ", " + quoted("SCALE") + ", " + quoted("RADIX") + ",\n" +
                quoted("NULLABLE") + ", " + quoted("REMARKS") + ", " + quoted("COLUMN_DEF") + ", " + quoted("SQL_DATA_TYPE") + ", " + quoted("SQL_DATETIME_SUB") + ",\n" +
                quoted("CHAR_OCTET_LENGTH") + ", " + quoted("ORDINAL_POSITION") + ", " + quoted("IS_NULLABLE") + ", " + specificName + "\n" +
                "FROM system.jdbc.procedure_columns\n" +
                "ORDER BY " + procedureCat + ", " + procedureSchem + ", " + procedureName + ", " + specificName + ", " + columnName;
        return selectEmpty(query);
    }

    @Override
    public ResultSet getTables(String catalog, String schemaPattern, String tableNamePattern, String[] types)
            throws SQLException
    {
        schemaPattern = escapeIfNecessary(schemaPattern);
        tableNamePattern = escapeIfNecessary(tableNamePattern);

        String tableCat = quoted("TABLE_CAT");
        String tableSchem = quoted("TABLE_SCHEM");
        String tableName = quoted("TABLE_NAME");
        String tableType = quoted("TABLE_TYPE");
        StringBuilder query = new StringBuilder("SELECT ");
        query.append(tableCat).append(", ");
        query.append(tableSchem).append(", ");
        query.append(tableName).append(", ");
        query.append(tableType).append(", ");
        query.append(quoted("REMARKS")).append(",\n");
        query.append(quoted("TYPE_CAT")).append(", ");
        query.append(quoted("TYPE_SCHEM")).append(", ");
        query.append(quoted("TYPE_NAME")).append(", ");
        query.append(quoted("SELF_REFERENCING_COL_NAME")).append(", ");
        query.append(quoted("REF_GENERATION")).append("\n");
        query.append("FROM system.jdbc.tables");

        List<String> filters = new ArrayList<>();
        emptyStringEqualsFilter(filters, tableCat, effectiveCatalog(catalog));
        emptyStringLikeFilter(filters, tableSchem, schemaPattern);
        optionalStringLikeFilter(filters, tableName, tableNamePattern);
        optionalStringInFilter(filters, tableType, types);
        buildFilters(query, filters);

        query.append("\nORDER BY ");
        query.append(tableType).append(", ");
        query.append(tableCat).append(", ");
        query.append(tableSchem).append(", ");
        query.append(tableName);

        return select(query.toString());
    }

    @Override
    public ResultSet getSchemas()
            throws SQLException
    {
        String tableSchem = quoted("TABLE_SCHEM");
        String tableCatalog = quoted("TABLE_CATALOG");
        String query = "SELECT " +
                tableSchem + ", " +
                tableCatalog + "\n" +
                "FROM system.jdbc.schemas\n" +
                "ORDER BY " +
                tableCatalog + ", " +
                tableSchem;
        return select(query);
    }

    @Override
    public ResultSet getCatalogs()
            throws SQLException
    {
        String tableCat = quoted("TABLE_CAT");
        String query = "SELECT " + tableCat + "\n" +
                "FROM system.jdbc.catalogs\n" +
                "ORDER BY " + tableCat;
        return select(query);
    }

    @Override
    public ResultSet getTableTypes()
            throws SQLException
    {
        String tableType = quoted("TABLE_TYPE");
        String query = "SELECT " + tableType + "\n" +
                "FROM system.jdbc.table_types\n" +
                "ORDER BY " + tableType;
        return select(query);
    }

    @Override
    public ResultSet getColumns(String catalog, String schemaPattern, String tableNamePattern, String columnNamePattern)
            throws SQLException
    {
        schemaPattern = escapeIfNecessary(schemaPattern);
        tableNamePattern = escapeIfNecessary(tableNamePattern);
        columnNamePattern = escapeIfNecessary(columnNamePattern);

        String tableCat = quoted("TABLE_CAT");
        String tableSchem = quoted("TABLE_SCHEM");
        String tableName = quoted("TABLE_NAME");
        String columnName = quoted("COLUMN_NAME");
        String ordinalPosition = quoted("ORDINAL_POSITION");
        StringBuilder query = new StringBuilder("SELECT " + tableCat + ", " + tableSchem + ", " +
                tableName + ", " + columnName + ", " + quoted("DATA_TYPE") + ",\n" +
                quoted("TYPE_NAME") + ", " + quoted("COLUMN_SIZE") + ", " + quoted("BUFFER_LENGTH") + ", " +
                quoted("DECIMAL_DIGITS") + ", " + quoted("NUM_PREC_RADIX") + ",\n" +
                quoted("NULLABLE") + ", " + quoted("REMARKS") + ", " + quoted("COLUMN_DEF") + ", " +
                quoted("SQL_DATA_TYPE") + ", " + quoted("SQL_DATETIME_SUB") + ",\n" +
                quoted("CHAR_OCTET_LENGTH") + ", " + ordinalPosition + ", " + quoted("IS_NULLABLE") + ",\n" +
                quoted("SCOPE_CATALOG") + ", " + quoted("SCOPE_SCHEMA") + ", " + quoted("SCOPE_TABLE") + ",\n" +
                quoted("SOURCE_DATA_TYPE") + ", " + quoted("IS_AUTOINCREMENT") + ", " + quoted("IS_GENERATEDCOLUMN") + "\n" +
                "FROM system.jdbc.columns");

        List<String> filters = new ArrayList<>();
        emptyStringEqualsFilter(filters, tableCat, effectiveCatalog(catalog));
        emptyStringLikeFilter(filters, tableSchem, schemaPattern);
        optionalStringLikeFilter(filters, tableName, tableNamePattern);
        optionalStringLikeFilter(filters, columnName, columnNamePattern);
        buildFilters(query, filters);

        query.append("\nORDER BY ").append(tableCat).append(", ").append(tableSchem)
                .append(", ").append(tableName).append(", ").append(ordinalPosition);

        return select(query.toString());
    }

    @Override
    public ResultSet getColumnPrivileges(String catalog, String schema, String table, String columnNamePattern)
            throws SQLException
    {
        columnNamePattern = escapeIfNecessary(columnNamePattern);
        throw new SQLFeatureNotSupportedException("privileges not supported");
    }

    @Override
    public ResultSet getTablePrivileges(String catalog, String schemaPattern, String tableNamePattern)
            throws SQLException
    {
        schemaPattern = escapeIfNecessary(schemaPattern);
        tableNamePattern = escapeIfNecessary(tableNamePattern);
        throw new SQLFeatureNotSupportedException("privileges not supported");
    }

    @Override
    public ResultSet getBestRowIdentifier(String catalog, String schema, String table, int scope, boolean nullable)
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("row identifiers not supported");
    }

    @Override
    public ResultSet getVersionColumns(String catalog, String schema, String table)
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("version columns not supported");
    }

    @Override
    public ResultSet getPrimaryKeys(String catalog, String schema, String table)
            throws SQLException
    {
        String query = "SELECT " +
                " CAST(NULL AS varchar) " + quoted("TABLE_CAT") + ", " +
                " CAST(NULL AS varchar) " + quoted("TABLE_SCHEM") + ", " +
                " CAST(NULL AS varchar) " + quoted("TABLE_NAME") + ", " +
                " CAST(NULL AS varchar) " + quoted("COLUMN_NAME") + ", " +
                " CAST(NULL AS smallint) " + quoted("KEY_SEQ") + ", " +
                " CAST(NULL AS varchar) " + quoted("PK_NAME") +
                "WHERE false";
        return select(query);
    }

    @Override
    public ResultSet getImportedKeys(String catalog, String schema, String table)
            throws SQLException
    {
        String query = "SELECT " +
                " CAST(NULL AS varchar) " + quoted("PKTABLE_CAT") + ", " +
                " CAST(NULL AS varchar) " + quoted("PKTABLE_SCHEM") + ", " +
                " CAST(NULL AS varchar) " + quoted("PKTABLE_NAME") + ", " +
                " CAST(NULL AS varchar) " + quoted("PKCOLUMN_NAME") + ", " +
                " CAST(NULL AS varchar) " + quoted("FKTABLE_CAT") + ", " +
                " CAST(NULL AS varchar) " + quoted("FKTABLE_SCHEM") + ", " +
                " CAST(NULL AS varchar) " + quoted("FKTABLE_NAME") + ", " +
                " CAST(NULL AS varchar) " + quoted("FKCOLUMN_NAME") + ", " +
                " CAST(NULL AS smallint) " + quoted("KEY_SEQ") + ", " +
                " CAST(NULL AS smallint) " + quoted("UPDATE_RULE") + ", " +
                " CAST(NULL AS smallint) " + quoted("DELETE_RULE") + ", " +
                " CAST(NULL AS varchar) " + quoted("FK_NAME") + ", " +
                " CAST(NULL AS varchar) " + quoted("PK_NAME") + ", " +
                " CAST(NULL AS smallint) " + quoted("DEFERRABILITY") +
                " WHERE false";
        return select(query);
    }

    @Override
    public ResultSet getExportedKeys(String catalog, String schema, String table)
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("exported keys not supported");
    }

    @Override
    public ResultSet getCrossReference(String parentCatalog, String parentSchema, String parentTable, String foreignCatalog, String foreignSchema, String foreignTable)
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("cross reference not supported");
    }

    @Override
    public ResultSet getTypeInfo()
            throws SQLException
    {
        String dataType = quoted("DATA_TYPE");
        String query = "SELECT " + quoted("TYPE_NAME") + "," +
                dataType + ", " + quoted("PRECISION") + ", " +
                quoted("LITERAL_PREFIX") + ", " + quoted("LITERAL_SUFFIX") + ",\n" +
                quoted("CREATE_PARAMS") + ", " + quoted("NULLABLE") + ", " + quoted("CASE_SENSITIVE") + ", " +
                quoted("SEARCHABLE") + ", " + quoted("UNSIGNED_ATTRIBUTE") + ",\n" +
                quoted("FIXED_PREC_SCALE") + ", " + quoted("AUTO_INCREMENT") + ", " + quoted("LOCAL_TYPE_NAME") + ", " +
                quoted("MINIMUM_SCALE") + ", " + quoted("MAXIMUM_SCALE") + ",\n" +
                quoted("SQL_DATA_TYPE") + ", " + quoted("SQL_DATETIME_SUB") + ", " + quoted("NUM_PREC_RADIX") + "\n" +
                "FROM system.jdbc.types\n" +
                "ORDER BY " + dataType;
        return select(query);
    }

    @Override
    public ResultSet getIndexInfo(String catalog, String schema, String table, boolean unique, boolean approximate)
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("indexes not supported");
    }

    @Override
    public boolean supportsResultSetType(int type)
            throws SQLException
    {
        return type == ResultSet.TYPE_FORWARD_ONLY;
    }

    @Override
    public boolean supportsResultSetConcurrency(int type, int concurrency)
            throws SQLException
    {
        return (type == ResultSet.TYPE_FORWARD_ONLY) &&
                (concurrency == ResultSet.CONCUR_READ_ONLY);
    }

    @Override
    public boolean ownUpdatesAreVisible(int type)
            throws SQLException
    {
        return false;
    }

    @Override
    public boolean ownDeletesAreVisible(int type)
            throws SQLException
    {
        return false;
    }

    @Override
    public boolean ownInsertsAreVisible(int type)
            throws SQLException
    {
        return false;
    }

    @Override
    public boolean othersUpdatesAreVisible(int type)
            throws SQLException
    {
        return false;
    }

    @Override
    public boolean othersDeletesAreVisible(int type)
            throws SQLException
    {
        return false;
    }

    @Override
    public boolean othersInsertsAreVisible(int type)
            throws SQLException
    {
        return false;
    }

    @Override
    public boolean updatesAreDetected(int type)
            throws SQLException
    {
        return false;
    }

    @Override
    public boolean deletesAreDetected(int type)
            throws SQLException
    {
        return false;
    }

    @Override
    public boolean insertsAreDetected(int type)
            throws SQLException
    {
        return false;
    }

    @Override
    public boolean supportsBatchUpdates()
            throws SQLException
    {
        return true;
    }

    @Override
    public ResultSet getUDTs(String catalog, String schemaPattern, String typeNamePattern, int[] types)
            throws SQLException
    {
        schemaPattern = escapeIfNecessary(schemaPattern);
        typeNamePattern = escapeIfNecessary(typeNamePattern);
        String typeCat = quoted("TYPE_CAT");
        String typeSchem = quoted("TYPE_SCHEM");
        String typeName = quoted("TYPE_NAME");
        String dataType = quoted("DATA_TYPE");
        String query = "SELECT " +
                typeCat + ", " + typeSchem + ", " + typeName + ",\n" +
                quoted("CLASS_NAME") + ", " + dataType + ", " +
                quoted("REMARKS") + ", " + quoted("BASE_TYPE") + "\n" +
                "FROM system.jdbc.udts\n" +
                "ORDER BY " + dataType + ", " + typeCat + ", " + typeSchem + ", " + typeName;
        return selectEmpty(query);
    }

    @Override
    public Connection getConnection()
            throws SQLException
    {
        return connection;
    }

    @Override
    public boolean supportsSavepoints()
            throws SQLException
    {
        return false;
    }

    @Override
    public boolean supportsNamedParameters()
            throws SQLException
    {
        return true;
    }

    @Override
    public boolean supportsMultipleOpenResults()
            throws SQLException
    {
        return false;
    }

    @Override
    public boolean supportsGetGeneratedKeys()
            throws SQLException
    {
        return false;
    }

    @Override
    public ResultSet getSuperTypes(String catalog, String schemaPattern, String typeNamePattern)
            throws SQLException
    {
        schemaPattern = escapeIfNecessary(schemaPattern);
        typeNamePattern = escapeIfNecessary(typeNamePattern);
        String typeCat = quoted("TYPE_CAT");
        String typeSchem = quoted("TYPE_SCHEM");
        String typeName = quoted("TYPE_NAME");
        String query = "SELECT " + typeCat + ", " + typeSchem + ", " + typeName + ",\n" +
                quoted("SUPERTYPE_CAT") + ", " + quoted("SUPERTYPE_SCHEM") + ", " + quoted("SUPERTYPE_NAME") + "\n" +
                "FROM system.jdbc.super_types\n" +
                "ORDER BY " + typeCat + ", " + typeSchem + ", " + typeName;
        return selectEmpty(query);
    }

    @Override
    public ResultSet getSuperTables(String catalog, String schemaPattern, String tableNamePattern)
            throws SQLException
    {
        schemaPattern = escapeIfNecessary(schemaPattern);
        tableNamePattern = escapeIfNecessary(tableNamePattern);
        String tableCat = quoted("TABLE_CAT");
        String tableSchem = quoted("TABLE_SCHEM");
        String tableName = quoted("TABLE_NAME");
        String query = "SELECT " + tableCat + ", " + tableSchem + ", " + tableName + ", " + quoted("SUPERTABLE_NAME") + "\n" +
                "FROM system.jdbc.super_tables\n" +
                "ORDER BY " + tableCat + ", " + tableSchem + ", " + tableName;
        return selectEmpty(query);
    }

    @Override
    public ResultSet getAttributes(String catalog, String schemaPattern, String typeNamePattern, String attributeNamePattern)
            throws SQLException
    {
        schemaPattern = escapeIfNecessary(schemaPattern);
        typeNamePattern = escapeIfNecessary(typeNamePattern);
        attributeNamePattern = escapeIfNecessary(attributeNamePattern);
        String typeCat = quoted("TYPE_CAT");
        String typeSchem = quoted("TYPE_SCHEM");
        String typeName = quoted("TYPE_NAME");
        String ordinalPosition = quoted("ORDINAL_POSITION");
        String query = "SELECT " + typeCat + ", " + typeSchem + ", " + typeName + ", " + quoted("ATTR_NAME") + ", " + quoted("DATA_TYPE") + ",\n" +
                quoted("ATTR_TYPE_NAME") + ", " + quoted("ATTR_SIZE") + ", " + quoted("DECIMAL_DIGITS") + ", " + quoted("NUM_PREC_RADIX") + ", " + quoted("NULLABLE") + ",\n" +
                quoted("REMARKS") + ", " + quoted("ATTR_DEF") + ", " + quoted("SQL_DATA_TYPE") + ", " + quoted("SQL_DATETIME_SUB") + ", " + quoted("CHAR_OCTET_LENGTH") + ",\n" +
                ordinalPosition + ", " + quoted("IS_NULLABLE") + ", " + quoted("SCOPE_CATALOG") + ", " + quoted("SCOPE_SCHEMA") + ", " + quoted("SCOPE_TABLE") + ",\n" +
                quoted("SOURCE_DATA_TYPE") + "\n" +
                "FROM system.jdbc.attributes\n" +
                "ORDER BY " + typeCat + ", " + typeSchem + ", " + typeName + ", " + ordinalPosition;
        return selectEmpty(query);
    }

    @Override
    public boolean supportsResultSetHoldability(int holdability)
            throws SQLException
    {
        return holdability == ResultSet.HOLD_CURSORS_OVER_COMMIT;
    }

    @Override
    public int getResultSetHoldability()
            throws SQLException
    {
        return ResultSet.HOLD_CURSORS_OVER_COMMIT;
    }

    @Override
    public int getDatabaseMajorVersion()
            throws SQLException
    {
        return getDatabaseVersionPart(0);
    }

    @Override
    public int getDatabaseMinorVersion()
            throws SQLException
    {
        return getDatabaseVersionPart(1);
    }

    private int getDatabaseVersionPart(int part)
            throws SQLException
    {
        String version = getDatabaseProductVersion();
        List<String> parts = Splitter.on('.').limit(3).splitToList(version);
        try {
            return parseInt(parts.get(part));
        }
        catch (IndexOutOfBoundsException | NumberFormatException e) {
            return 0;
        }
    }

    @Override
    public int getJDBCMajorVersion()
            throws SQLException
    {
        return 4;
    }

    @Override
    public int getJDBCMinorVersion()
            throws SQLException
    {
        return 2;
    }

    @Override
    public int getSQLStateType()
            throws SQLException
    {
        return DatabaseMetaData.sqlStateSQL;
    }

    @Override
    public boolean locatorsUpdateCopy()
            throws SQLException
    {
        return true;
    }

    @Override
    public boolean supportsStatementPooling()
            throws SQLException
    {
        return false;
    }

    @Override
    public RowIdLifetime getRowIdLifetime()
            throws SQLException
    {
        return RowIdLifetime.ROWID_UNSUPPORTED;
    }

    @Override
    public ResultSet getSchemas(String catalog, String schemaPattern)
            throws SQLException
    {
        String tableSchem = quoted("TABLE_SCHEM");
        String tableCatalog = quoted("TABLE_CATALOG");
        schemaPattern = escapeIfNecessary(schemaPattern);

        StringBuilder query = new StringBuilder();
        query.append("SELECT ").append(tableSchem).append(", ").append(tableCatalog);
        query.append("\nFROM system.jdbc.schemas");

        List<String> filters = new ArrayList<>();
        emptyStringEqualsFilter(filters, tableCatalog, effectiveCatalog(catalog));
        optionalStringLikeFilter(filters, tableSchem, schemaPattern);
        buildFilters(query, filters);

        query.append("\nORDER BY ").append(tableCatalog).append(", ").append(tableSchem);

        return select(query.toString());
    }

    @Override
    public boolean supportsStoredFunctionsUsingCallSyntax()
            throws SQLException
    {
        return false;
    }

    @Override
    public boolean autoCommitFailureClosesAllResultSets()
            throws SQLException
    {
        return false;
    }

    @Override
    public ResultSet getClientInfoProperties()
            throws SQLException
    {
        ClientTypeSignature varchar = new ClientTypeSignature("varchar", ImmutableList.of(ClientTypeSignatureParameter.ofLong(VARCHAR_UNBOUNDED_LENGTH)));

        ImmutableList.Builder<Column> columns = ImmutableList.builder();
        columns.add(new Column("NAME", "varchar", varchar));
        columns.add(new Column("MAX_LEN", "integer", new ClientTypeSignature(ClientStandardTypes.INTEGER)));
        columns.add(new Column("DEFAULT_VALUE", "varchar", varchar));
        columns.add(new Column("DESCRIPTION", "varchar", varchar));

        ImmutableList.Builder<List<Object>> results = ImmutableList.builder();

        Stream.of(ClientInfoProperty.values())
                .sorted(Comparator.comparing(ClientInfoProperty::getPropertyName))
                .forEach(clientInfoProperty -> results.add(newArrayList(
                        clientInfoProperty.getPropertyName(),
                        VARCHAR_UNBOUNDED_LENGTH,
                        null,
                        null)));

        return new InMemoryTrinoResultSet(columns.build(), results.build());
    }

    @Override
    public ResultSet getFunctions(String catalog, String schemaPattern, String functionNamePattern)
            throws SQLException
    {
        schemaPattern = escapeIfNecessary(schemaPattern);
        functionNamePattern = escapeIfNecessary(functionNamePattern);
        // TODO: implement this
        throw new NotImplementedException("DatabaseMetaData", "getFunctions");
    }

    @Override
    public ResultSet getFunctionColumns(String catalog, String schemaPattern, String functionNamePattern, String columnNamePattern)
            throws SQLException
    {
        schemaPattern = escapeIfNecessary(schemaPattern);
        functionNamePattern = escapeIfNecessary(functionNamePattern);
        columnNamePattern = escapeIfNecessary(columnNamePattern);
        // TODO: implement this
        throw new NotImplementedException("DatabaseMetaData", "getFunctionColumns");
    }

    @Override
    public ResultSet getPseudoColumns(String catalog, String schemaPattern, String tableNamePattern, String columnNamePattern)
            throws SQLException
    {
        schemaPattern = escapeIfNecessary(schemaPattern);
        tableNamePattern = escapeIfNecessary(tableNamePattern);
        columnNamePattern = escapeIfNecessary(columnNamePattern);
        String tableCat = quoted("TABLE_CAT");
        String tableSchem = quoted("TABLE_SCHEM");
        String tableName = quoted("TABLE_NAME");
        String columnName = quoted("COLUMN_NAME");
        String query = "SELECT " + tableCat + ", " + tableSchem + ", " + tableName + ", " +
                columnName + ", " + quoted("DATA_TYPE") + ",\n" +
                quoted("COLUMN_SIZE") + ", " + quoted("DECIMAL_DIGITS") + ", " +
                quoted("NUM_PREC_RADIX") + ", " + quoted("COLUMN_USAGE") + ", " + quoted("REMARKS") + ",\n" +
                quoted("CHAR_OCTET_LENGTH") + ", " + quoted("IS_NULLABLE") + "\n" +
                "FROM system.jdbc.pseudo_columns\n" +
                "ORDER BY " + tableCat + ", " + tableSchem + ", " + tableName + ", " + columnName;
        return selectEmpty(query);
    }

    @Override
    public boolean generatedKeyAlwaysReturned()
            throws SQLException
    {
        return false;
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T> T unwrap(Class<T> iface)
            throws SQLException
    {
        if (isWrapperFor(iface)) {
            return (T) this;
        }
        throw new SQLException("No wrapper for " + iface);
    }

    @Override
    public boolean isWrapperFor(Class<?> iface)
            throws SQLException
    {
        return iface.isInstance(this);
    }

    private ResultSet selectEmpty(String sql)
            throws SQLException
    {
        return select(sql + " LIMIT 0");
    }

    private ResultSet select(String sql)
            throws SQLException
    {
        Statement statement = getConnection().createStatement();
        TrinoResultSet resultSet;
        try {
            resultSet = (TrinoResultSet) statement.executeQuery(sql);
            resultSet.setCloseStatementOnClose();
        }
        catch (Throwable e) {
            try {
                statement.close();
            }
            catch (Throwable closeException) {
                if (closeException != e) {
                    e.addSuppressed(closeException);
                }
            }
            throw e;
        }
        return resultSet;
    }

    private static void buildFilters(StringBuilder out, List<String> filters)
    {
        if (!filters.isEmpty()) {
            out.append("\nWHERE ");
            Joiner.on(" AND ").appendTo(out, filters);
        }
    }

    private static void optionalStringInFilter(List<String> filters, String columnName, String[] values)
    {
        if (values == null) {
            return;
        }

        if (values.length == 0) {
            filters.add("false");
            return;
        }

        StringBuilder filter = new StringBuilder();
        filter.append(columnName).append(" IN (");

        for (int i = 0; i < values.length; i++) {
            if (i > 0) {
                filter.append(", ");
            }
            quoteStringLiteral(filter, values[i]);
        }

        filter.append(")");
        filters.add(filter.toString());
    }

    @Nullable
    private String escapeIfNecessary(@Nullable String namePattern)
            throws SQLException
    {
        return escapeIfNecessary(assumeLiteralNamesInMetadataCallsForNonConformingClients, assumeLiteralUnderscoreInMetadataCallsForNonConformingClients, namePattern);
    }

    @Nullable
    static String escapeIfNecessary(
            boolean assumeLiteralNamesInMetadataCallsForNonConformingClients,
            boolean assumeLiteralUnderscoreInMetadataCallsForNonConformingClients,
            @Nullable String namePattern)
    {
        checkArgument(
                !assumeLiteralNamesInMetadataCallsForNonConformingClients || !assumeLiteralUnderscoreInMetadataCallsForNonConformingClients,
                "assumeLiteralNamesInMetadataCallsForNonConformingClients and assumeLiteralUnderscoreInMetadataCallsForNonConformingClients cannot be both true");
        if (namePattern == null || (!assumeLiteralNamesInMetadataCallsForNonConformingClients && !assumeLiteralUnderscoreInMetadataCallsForNonConformingClients)) {
            return namePattern;
        }
        //noinspection ConstantConditions
        verify(SEARCH_STRING_ESCAPE.equals("\\"));
        return namePattern.replaceAll(assumeLiteralNamesInMetadataCallsForNonConformingClients ? "[_%\\\\]" : "[_\\\\]", "\\\\$0");
    }

    private static void optionalStringLikeFilter(List<String> filters, String columnName, String value)
    {
        if (value != null) {
            filters.add(stringColumnLike(columnName, value));
        }
    }

    private static void emptyStringEqualsFilter(List<String> filters, String columnName, String value)
    {
        if (value != null) {
            if (value.isEmpty()) {
                filters.add(columnName + " IS NULL");
            }
            else {
                filters.add(stringColumnEquals(columnName, value));
            }
        }
    }

    private static void emptyStringLikeFilter(List<String> filters, String columnName, String value)
    {
        if (value != null) {
            if (value.isEmpty()) {
                filters.add(columnName + " IS NULL");
            }
            else {
                filters.add(stringColumnLike(columnName, value));
            }
        }
    }

    private static String stringColumnEquals(String columnName, String value)
    {
        StringBuilder filter = new StringBuilder();
        filter.append(columnName).append(" = ");
        quoteStringLiteral(filter, value);
        return filter.toString();
    }

    private static String stringColumnLike(String columnName, String pattern)
    {
        StringBuilder filter = new StringBuilder();
        filter.append(columnName).append(" LIKE ");
        quoteStringLiteral(filter, pattern);
        filter.append(" ESCAPE ");
        quoteStringLiteral(filter, SEARCH_STRING_ESCAPE);
        return filter.toString();
    }

    private static void quoteStringLiteral(StringBuilder out, String value)
    {
        out.append('\'');
        for (int i = 0; i < value.length(); i++) {
            char c = value.charAt(i);
            out.append(c);
            if (c == '\'') {
                out.append('\'');
            }
        }
        out.append('\'');
    }

    private static String quoted(String columnName)
    {
        return '"' + columnName + '"';
    }

    private String effectiveCatalog(String catalog)
            throws SQLException
    {
        if (connection.getAssumeNullCatalogMeansCurrentCatalog() && catalog == null) {
            return connection.getCatalog();
        }
        return catalog;
    }
}
