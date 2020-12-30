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
package io.prestosql.plugin.salesforce.driver.metadata;

import com.google.common.collect.Lists;
import io.prestosql.plugin.salesforce.driver.connection.ForceConnection;
import io.prestosql.plugin.salesforce.driver.delegates.PartnerService;
import io.prestosql.plugin.salesforce.driver.resultset.ForceResultSet;

import javax.inject.Inject;
import javax.inject.Singleton;

import java.io.Serializable;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.RowIdLifetime;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

@Singleton
public class ForceDatabaseMetaData
        implements DatabaseMetaData, Serializable
{
    private static final String DEFAULT_SCHEMA = "Salesforce";
    private static final TypeInfo OTHER_TYPE_INFO = new TypeInfo("other", Types.OTHER, 0x7fffffff, 0, 0, 0);
    private static final TypeInfo[] TYPE_INFO_DATA = {
            new TypeInfo("id", Types.VARCHAR, 0x7fffffff, 0, 0, 0), new TypeInfo("masterrecord", Types.VARCHAR, 0x7fffffff, 0, 0, 0),
            new TypeInfo("reference", Types.VARCHAR, 0x7fffffff, 0, 0, 0), new TypeInfo("string", Types.VARCHAR, 0x7fffffff, 0, 0, 0),
            new TypeInfo("encryptedstring", Types.VARCHAR, 0x7fffffff, 0, 0, 0), new TypeInfo("email", Types.VARCHAR, 0x7fffffff, 0, 0, 0),
            new TypeInfo("phone", Types.VARCHAR, 0x7fffffff, 0, 0, 0), new TypeInfo("url", Types.VARCHAR, 0x7fffffff, 0, 0, 0),
            new TypeInfo("textarea", Types.LONGVARCHAR, 0x7fffffff, 0, 0, 0), new TypeInfo("base64", Types.BLOB, 0x7fffffff, 0, 0, 0),
            new TypeInfo("boolean", Types.BOOLEAN, 1, 0, 0, 0), new TypeInfo("_boolean", Types.BOOLEAN, 1, 0, 0, 0), new TypeInfo("byte", Types.VARBINARY, 10, 0, 0, 10),
            new TypeInfo("_byte", Types.VARBINARY, 10, 0, 0, 10), new TypeInfo("int", Types.INTEGER, 10, 0, 0, 10), new TypeInfo("_int", Types.INTEGER, 10, 0, 0, 10),
            new TypeInfo("decimal", Types.DECIMAL, 17, -324, 306, 10), new TypeInfo("double", Types.DOUBLE, 17, -324, 306, 10),
            new TypeInfo("_double", Types.DOUBLE, 17, -324, 306, 10), new TypeInfo("percent", Types.DOUBLE, 17, -324, 306, 10),
            new TypeInfo("currency", Types.DOUBLE, 17, -324, 306, 10), new TypeInfo("date", Types.DATE, 10, 0, 0, 0), new TypeInfo("time", Types.TIME, 10, 0, 0, 0),
            new TypeInfo("datetime", Types.TIMESTAMP, 10, 0, 0, 0), new TypeInfo("picklist", Types.VARCHAR, 0, 0, 0, 0), new TypeInfo("multipicklist", Types.ARRAY, 0, 0, 0, 0),
            new TypeInfo("combobox", Types.ARRAY, 0, 0, 0, 0), new TypeInfo("anyType", Types.OTHER, 0x7fffffff, 0, 0, 0)};
    private transient PartnerService partnerService;
    private transient ForceConnection connection;
    private Map<String, Table> tablesCache;
    private int counter;

    @Inject
    public ForceDatabaseMetaData()
    {
    }

    public static TypeInfo lookupTypeInfo(String forceTypeName)
    {
        String typeName = forceTypeName.replaceFirst("\\A_+", "");
        return Arrays.stream(TYPE_INFO_DATA).filter(entry -> typeName.equals(entry.typeName)).findAny().orElse(OTHER_TYPE_INFO);
    }

    @Override
    public ResultSet getTables(String catalog, String schemaPattern, String tableNamePattern, String[] types)
    {
        List<Table> tables;

        if (tableNamePattern != null) {
            tables = Lists.newArrayList(getTables().get(tableNamePattern));
        }
        else {
            tables = Lists.newArrayList(getTables().values());
        }

        List<ColumnMap<String, Object>> maps = new ArrayList<>();
        for (Table table : tables) {
            ColumnMap<String, Object> map = new ColumnMap<>();
            map.put("TABLE_CAT", null);
            map.put("TABLE_SCHEM", null);
            map.put("TABLE_NAME", table.getName());
            map.put("TABLE_TYPE", "TABLE");
            map.put("REMARKS", table.getComments());
            map.put("TYPE_CAT", null);
            map.put("TYPE_SCHEM", null);
            map.put("TYPE_NAME", null);
            map.put("SELF_REFERENCING_COL_NAME", null);
            map.put("REF_GENERATION", null);
            maps.add(map);
        }
        return new ForceResultSet(maps);
    }

    private Map<String, Table> getTables()
    {
        if (tablesCache == null) {
            tablesCache = partnerService.getTables().stream().collect(Collectors.toMap(x -> x.getName().toLowerCase(Locale.getDefault()), x -> x));
        }
        return tablesCache;
    }

    @Override
    public ResultSet getColumns(String catalog, String schemaPattern, String tableNamePattern, String columnNamePattern)
    {
        List<Table> tables;

        if (tableNamePattern != null) {
            tables = Lists.newArrayList(getTables().get(tableNamePattern.toLowerCase(Locale.getDefault())));
        }
        else {
            tables = Lists.newArrayList(getTables().values());
        }

        AtomicInteger ordinal = new AtomicInteger(1);

        return new ForceResultSet(tables.stream().filter(table -> tableNamePattern == null || table.getName().equals(tableNamePattern)).flatMap(table -> table.getColumns().stream()).filter(column -> columnNamePattern == null || column.getName().equals(columnNamePattern)).map(column -> new ColumnMap<String, Object>()
        {{
                TypeInfo typeInfo = lookupTypeInfo(column.getType());
                put("TABLE_CAT", null);
                put("TABLE_SCHEM", null);
                put("TABLE_NAME", column.getTable().getName());
                put("COLUMN_NAME", column.getName());
                put("DATA_TYPE", typeInfo != null ? typeInfo.sqlDataType : Types.OTHER);
                put("TYPE_NAME", column.getType());
                put("COLUMN_SIZE", column.getLength());
                put("BUFFER_LENGTH", 0);
                put("DECIMAL_DIGITS", 0);
                put("NUM_PREC_RADIX", typeInfo != null ? typeInfo.radix : 10);
                put("NULLABLE", 0);
                put("REMARKS", column.getComments());
                put("COLUMN_DEF", null);
                put("SQL_DATA_TYPE", null);
                put("SQL_DATETIME_SUB", null);
                put("CHAR_OCTET_LENGTH", 0);
                put("ORDINAL_POSITION", ordinal.getAndIncrement());
                put("IS_NULLABLE", "");
                put("SCOPE_CATLOG", null);
                put("SCOPE_SCHEMA", null);
                put("SCOPE_TABLE", null);
                put("SOURCE_DATA_TYPE", column.getType());
                put("NULLABLE", column.isNillable() ? DatabaseMetaData.columnNullable : DatabaseMetaData.columnNoNulls);
            }}).collect(Collectors.toList()));
    }

    @Override
    public ResultSet getSchemas()
            throws SQLException
    {
        ColumnMap<String, Object> row = new ColumnMap<>();
        row.put("TABLE_SCHEM", DEFAULT_SCHEMA);
        row.put("TABLE_CATALOG", null);
        row.put("IS_DEFAULT", true);
        return new ForceResultSet(Collections.singletonList(row));
    }

    @Override
    public ResultSet getPrimaryKeys(String catalog, String schema, String tableName)
            throws SQLException
    {
        List<ColumnMap<String, Object>> maps = new ArrayList<>();
        for (Table table : getTables().values()) {
            if (table.getName().equals(tableName)) {
                for (Column column : table.getColumns()) {
                    if (column.getName().equalsIgnoreCase("Id")) {
                        ColumnMap<String, Object> map = new ColumnMap<>();
                        map.put("TABLE_CAT", null);
                        map.put("TABLE_SCHEM", null);
                        map.put("TABLE_NAME", table.getName());
                        map.put("COLUMN_NAME", "" + column.getName());
                        map.put("KEY_SEQ", 0);
                        map.put("PK_NAME", "FakePK" + counter);
                        maps.add(map);
                    }
                }
            }
        }
        return new ForceResultSet(maps);
    }

    @Override
    public ResultSet getImportedKeys(String catalog, String schema, String tableName)
            throws SQLException
    {
        List<ColumnMap<String, Object>> maps = new ArrayList<>();
        for (Table table : getTables().values()) {
            if (table.getName().equals(tableName)) {
                for (Column column : table.getColumns()) {
                    if (column.getReferencedTable() != null && column.getReferencedColumn() != null) {
                        ColumnMap<String, Object> map = new ColumnMap<>();
                        map.put("PKTABLE_CAT", null);
                        map.put("PKTABLE_SCHEM", null);
                        map.put("PKTABLE_NAME", column.getReferencedTable());
                        map.put("PKCOLUMN_NAME", column.getReferencedColumn());
                        map.put("FKTABLE_CAT", null);
                        map.put("FKTABLE_SCHEM", null);
                        map.put("FKTABLE_NAME", tableName);
                        map.put("FKCOLUMN_NAME", column.getName());
                        map.put("KEY_SEQ", counter);
                        map.put("UPDATE_RULE", 0);
                        map.put("DELETE_RULE", 0);
                        map.put("FK_NAME", "FakeFK" + counter);
                        map.put("PK_NAME", "FakePK" + counter);
                        map.put("DEFERRABILITY", 0);
                        counter++;
                        maps.add(map);
                    }
                }
            }
        }
        return new ForceResultSet(maps);
    }

    @Override
    public ResultSet getIndexInfo(String catalog, String schema, String tableName, boolean unique, boolean approximate)
    {
        List<ColumnMap<String, Object>> maps = new ArrayList<>();
        for (Table table : getTables().values()) {
            if (table.getName().equals(tableName)) {
                for (Column column : table.getColumns()) {
                    if (column.getName().equalsIgnoreCase("Id")) {
                        ColumnMap<String, Object> map = new ColumnMap<>();
                        map.put("TABLE_CAT", null);
                        map.put("TABLE_SCHEM", null);
                        map.put("TABLE_NAME", table.getName());
                        map.put("NON_UNIQUE", true);
                        map.put("INDEX_QUALIFIER", null);
                        map.put("INDEX_NAME", "FakeIndex" + counter++);
                        map.put("TYPE", DatabaseMetaData.tableIndexOther);
                        map.put("ORDINAL_POSITION", counter);
                        map.put("COLUMN_NAME", "Id");
                        map.put("ASC_OR_DESC", "A");
                        map.put("CARDINALITY", 1);
                        map.put("PAGES", 1);
                        map.put("FILTER_CONDITION", null);

                        maps.add(map);
                    }
                }
            }
        }
        return new ForceResultSet(maps);
    }

    @SuppressWarnings("unchecked")
    @Override
    public ResultSet getCatalogs()
            throws SQLException
    {
        return new ForceResultSet(Collections.emptyList());
    }

    @Override
    public ResultSet getTypeInfo()
            throws SQLException
    {
        List<ColumnMap<String, Object>> rows = new ArrayList<>();
        for (TypeInfo typeInfo : TYPE_INFO_DATA) {
            ColumnMap<String, Object> row = new ColumnMap<>();
            row.put("TYPE_NAME", typeInfo.typeName);
            row.put("DATA_TYPE", typeInfo.sqlDataType);
            row.put("PRECISION", typeInfo.precision);
            row.put("LITERAL_PREFIX", null);
            row.put("LITERAL_SUFFIX", null);
            row.put("CREATE_PARAMS", null);
            row.put("NULLABLE", 1);
            row.put("CASE_SENSITIVE", 0);
            row.put("SEARCHABLE", 3);
            row.put("UNSIGNED_ATTRIBUTE", false);
            row.put("FIXED_PREC_SCALE", false);
            row.put("AUTO_INCREMENT", false);
            row.put("LOCAL_TYPE_NAME", typeInfo.typeName);
            row.put("MINIMUM_SCALE", typeInfo.minScale);
            row.put("MAXIMUM_SCALE", typeInfo.maxScale);
            row.put("SQL_DATA_TYPE", typeInfo.sqlDataType);
            row.put("SQL_DATETIME_SUB", null);
            row.put("NUM_PREC_RADIX", typeInfo.radix);
            row.put("TYPE_SUB", 1);

            rows.add(row);
        }
        return new ForceResultSet(rows);
    }

    @Override
    public <T> T unwrap(Class<T> iface)
            throws SQLException
    {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public boolean isWrapperFor(Class<?> iface)
            throws SQLException
    {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public boolean allProceduresAreCallable()
            throws SQLException
    {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public boolean allTablesAreSelectable()
            throws SQLException
    {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public String getURL()
            throws SQLException
    {
        // TODO Auto-generated method stub
        return "";
    }

    @Override
    public String getUserName()
            throws SQLException
    {
        // TODO Auto-generated method stub
        return "";
    }

    @Override
    public boolean isReadOnly()
            throws SQLException
    {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public boolean nullsAreSortedHigh()
            throws SQLException
    {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public boolean nullsAreSortedLow()
            throws SQLException
    {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public boolean nullsAreSortedAtStart()
            throws SQLException
    {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public boolean nullsAreSortedAtEnd()
            throws SQLException
    {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public String getDatabaseProductName()
            throws SQLException
    {
        return "Ascendix JDBC driver for Salesforce";
    }

    @Override
    public String getDatabaseProductVersion()
            throws SQLException
    {
        return "39";
    }

    @Override
    public String getDriverName()
            throws SQLException
    {
        return "Ascendix JDBC driver for Salesforce";
    }

    @Override
    public String getDriverVersion()
            throws SQLException
    {
        return "1.1";
    }

    @Override
    public int getDriverMajorVersion()
    {
        return 1;
    }

    @Override
    public int getDriverMinorVersion()
    {
        return 0;
    }

    @Override
    public boolean usesLocalFiles()
            throws SQLException
    {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public boolean usesLocalFilePerTable()
            throws SQLException
    {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public boolean supportsMixedCaseIdentifiers()
            throws SQLException
    {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public boolean storesUpperCaseIdentifiers()
            throws SQLException
    {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public boolean storesLowerCaseIdentifiers()
            throws SQLException
    {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public boolean storesMixedCaseIdentifiers()
            throws SQLException
    {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public boolean supportsMixedCaseQuotedIdentifiers()
            throws SQLException
    {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public boolean storesUpperCaseQuotedIdentifiers()
            throws SQLException
    {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public boolean storesLowerCaseQuotedIdentifiers()
            throws SQLException
    {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public boolean storesMixedCaseQuotedIdentifiers()
            throws SQLException
    {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public String getIdentifierQuoteString()
            throws SQLException
    {
        // TODO Auto-generated method stub
        return "";
    }

    @Override
    public String getSQLKeywords()
            throws SQLException
    {
        // TODO Auto-generated method stub
        return "";
    }

    @Override
    public String getNumericFunctions()
            throws SQLException
    {
        // TODO Auto-generated method stub
        return "";
    }

    @Override
    public String getStringFunctions()
            throws SQLException
    {
        // TODO Auto-generated method stub
        return "";
    }

    @Override
    public String getSystemFunctions()
            throws SQLException
    {
        // TODO Auto-generated method stub
        return "";
    }

    @Override
    public String getTimeDateFunctions()
            throws SQLException
    {
        // TODO Auto-generated method stub
        return "";
    }

    @Override
    public String getSearchStringEscape()
            throws SQLException
    {
        // TODO Auto-generated method stub
        return "";
    }

    @Override
    public String getExtraNameCharacters()
            throws SQLException
    {
        // TODO Auto-generated method stub
        return "";
    }

    @Override
    public boolean supportsAlterTableWithAddColumn()
            throws SQLException
    {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public boolean supportsAlterTableWithDropColumn()
            throws SQLException
    {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public boolean supportsColumnAliasing()
            throws SQLException
    {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public boolean nullPlusNonNullIsNull()
            throws SQLException
    {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public boolean supportsConvert()
            throws SQLException
    {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public boolean supportsConvert(int fromType, int toType)
            throws SQLException
    {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public boolean supportsTableCorrelationNames()
            throws SQLException
    {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public boolean supportsDifferentTableCorrelationNames()
            throws SQLException
    {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public boolean supportsExpressionsInOrderBy()
            throws SQLException
    {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public boolean supportsOrderByUnrelated()
            throws SQLException
    {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public boolean supportsGroupBy()
            throws SQLException
    {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public boolean supportsGroupByUnrelated()
            throws SQLException
    {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public boolean supportsGroupByBeyondSelect()
            throws SQLException
    {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public boolean supportsLikeEscapeClause()
            throws SQLException
    {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public boolean supportsMultipleResultSets()
            throws SQLException
    {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public boolean supportsMultipleTransactions()
            throws SQLException
    {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public boolean supportsNonNullableColumns()
            throws SQLException
    {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public boolean supportsMinimumSQLGrammar()
            throws SQLException
    {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public boolean supportsCoreSQLGrammar()
            throws SQLException
    {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public boolean supportsExtendedSQLGrammar()
            throws SQLException
    {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public boolean supportsANSI92EntryLevelSQL()
            throws SQLException
    {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public boolean supportsANSI92IntermediateSQL()
            throws SQLException
    {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public boolean supportsANSI92FullSQL()
            throws SQLException
    {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public boolean supportsIntegrityEnhancementFacility()
            throws SQLException
    {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public boolean supportsOuterJoins()
            throws SQLException
    {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public boolean supportsFullOuterJoins()
            throws SQLException
    {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public boolean supportsLimitedOuterJoins()
            throws SQLException
    {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public String getSchemaTerm()
            throws SQLException
    {
        // TODO Auto-generated method stub
        return "";
    }

    @Override
    public String getProcedureTerm()
            throws SQLException
    {
        // TODO Auto-generated method stub
        return "";
    }

    @Override
    public String getCatalogTerm()
            throws SQLException
    {
        // TODO Auto-generated method stub
        return "";
    }

    @Override
    public boolean isCatalogAtStart()
            throws SQLException
    {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public String getCatalogSeparator()
            throws SQLException
    {
        // TODO Auto-generated method stub
        return "";
    }

    @Override
    public boolean supportsSchemasInDataManipulation()
            throws SQLException
    {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public boolean supportsSchemasInProcedureCalls()
            throws SQLException
    {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public boolean supportsSchemasInTableDefinitions()
            throws SQLException
    {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public boolean supportsSchemasInIndexDefinitions()
            throws SQLException
    {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public boolean supportsSchemasInPrivilegeDefinitions()
            throws SQLException
    {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public boolean supportsCatalogsInDataManipulation()
            throws SQLException
    {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public boolean supportsCatalogsInProcedureCalls()
            throws SQLException
    {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public boolean supportsCatalogsInTableDefinitions()
            throws SQLException
    {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public boolean supportsCatalogsInIndexDefinitions()
            throws SQLException
    {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public boolean supportsCatalogsInPrivilegeDefinitions()
            throws SQLException
    {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public boolean supportsPositionedDelete()
            throws SQLException
    {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public boolean supportsPositionedUpdate()
            throws SQLException
    {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public boolean supportsSelectForUpdate()
            throws SQLException
    {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public boolean supportsStoredProcedures()
            throws SQLException
    {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public boolean supportsSubqueriesInComparisons()
            throws SQLException
    {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public boolean supportsSubqueriesInExists()
            throws SQLException
    {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public boolean supportsSubqueriesInIns()
            throws SQLException
    {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public boolean supportsSubqueriesInQuantifieds()
            throws SQLException
    {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public boolean supportsCorrelatedSubqueries()
            throws SQLException
    {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public boolean supportsUnion()
            throws SQLException
    {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public boolean supportsUnionAll()
            throws SQLException
    {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public boolean supportsOpenCursorsAcrossCommit()
            throws SQLException
    {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public boolean supportsOpenCursorsAcrossRollback()
            throws SQLException
    {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public boolean supportsOpenStatementsAcrossCommit()
            throws SQLException
    {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public boolean supportsOpenStatementsAcrossRollback()
            throws SQLException
    {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public int getMaxBinaryLiteralLength()
            throws SQLException
    {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public int getMaxCharLiteralLength()
            throws SQLException
    {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public int getMaxColumnNameLength()
            throws SQLException
    {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public int getMaxColumnsInGroupBy()
            throws SQLException
    {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public int getMaxColumnsInIndex()
            throws SQLException
    {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public int getMaxColumnsInOrderBy()
            throws SQLException
    {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public int getMaxColumnsInSelect()
            throws SQLException
    {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public int getMaxColumnsInTable()
            throws SQLException
    {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public int getMaxConnections()
            throws SQLException
    {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public int getMaxCursorNameLength()
            throws SQLException
    {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public int getMaxIndexLength()
            throws SQLException
    {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public int getMaxSchemaNameLength()
            throws SQLException
    {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public int getMaxProcedureNameLength()
            throws SQLException
    {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public int getMaxCatalogNameLength()
            throws SQLException
    {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public int getMaxRowSize()
            throws SQLException
    {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public boolean doesMaxRowSizeIncludeBlobs()
            throws SQLException
    {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public int getMaxStatementLength()
            throws SQLException
    {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public int getMaxStatements()
            throws SQLException
    {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public int getMaxTableNameLength()
            throws SQLException
    {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public int getMaxTablesInSelect()
            throws SQLException
    {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public int getMaxUserNameLength()
            throws SQLException
    {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public int getDefaultTransactionIsolation()
            throws SQLException
    {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public boolean supportsTransactions()
            throws SQLException
    {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public boolean supportsTransactionIsolationLevel(int level)
            throws SQLException
    {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public boolean supportsDataDefinitionAndDataManipulationTransactions()
            throws SQLException
    {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public boolean supportsDataManipulationTransactionsOnly()
            throws SQLException
    {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public boolean dataDefinitionCausesTransactionCommit()
            throws SQLException
    {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public boolean dataDefinitionIgnoredInTransactions()
            throws SQLException
    {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public ResultSet getProcedures(String catalog, String schemaPattern, String procedureNamePattern)
            throws SQLException
    {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public ResultSet getProcedureColumns(String catalog, String schemaPattern, String procedureNamePattern, String columnNamePattern)
            throws SQLException
    {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public ResultSet getTableTypes()
            throws SQLException
    {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public ResultSet getColumnPrivileges(String catalog, String schema, String table, String columnNamePattern)
            throws SQLException
    {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public ResultSet getTablePrivileges(String catalog, String schemaPattern, String tableNamePattern)
            throws SQLException
    {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public ResultSet getBestRowIdentifier(String catalog, String schema, String table, int scope, boolean nullable)
            throws SQLException
    {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public ResultSet getVersionColumns(String catalog, String schema, String table)
            throws SQLException
    {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public ResultSet getExportedKeys(String catalog, String schema, String table)
            throws SQLException
    {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public ResultSet getCrossReference(String parentCatalog, String parentSchema, String parentTable, String foreignCatalog, String foreignSchema, String foreignTable)
            throws SQLException
    {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public boolean supportsResultSetType(int type)
            throws SQLException
    {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public boolean supportsResultSetConcurrency(int type, int concurrency)
            throws SQLException
    {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public boolean ownUpdatesAreVisible(int type)
            throws SQLException
    {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public boolean ownDeletesAreVisible(int type)
            throws SQLException
    {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public boolean ownInsertsAreVisible(int type)
            throws SQLException
    {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public boolean othersUpdatesAreVisible(int type)
            throws SQLException
    {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public boolean othersDeletesAreVisible(int type)
            throws SQLException
    {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public boolean othersInsertsAreVisible(int type)
            throws SQLException
    {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public boolean updatesAreDetected(int type)
            throws SQLException
    {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public boolean deletesAreDetected(int type)
            throws SQLException
    {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public boolean insertsAreDetected(int type)
            throws SQLException
    {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public boolean supportsBatchUpdates()
            throws SQLException
    {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public ResultSet getUDTs(String catalog, String schemaPattern, String typeNamePattern, int[] types)
            throws SQLException
    {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Connection getConnection()
            throws SQLException
    {
        // TODO Auto-generated method stub
        return connection;
    }

    public void setConnection(ForceConnection connection)
    {
        this.connection = connection;
        this.partnerService = new PartnerService(connection.getPartnerConnection());
    }

    @Override
    public boolean supportsSavepoints()
            throws SQLException
    {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public boolean supportsNamedParameters()
            throws SQLException
    {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public boolean supportsMultipleOpenResults()
            throws SQLException
    {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public boolean supportsGetGeneratedKeys()
            throws SQLException
    {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public ResultSet getSuperTypes(String catalog, String schemaPattern, String typeNamePattern)
            throws SQLException
    {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public ResultSet getSuperTables(String catalog, String schemaPattern, String tableNamePattern)
            throws SQLException
    {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public ResultSet getAttributes(String catalog, String schemaPattern, String typeNamePattern, String attributeNamePattern)
            throws SQLException
    {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public boolean supportsResultSetHoldability(int holdability)
            throws SQLException
    {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public int getResultSetHoldability()
            throws SQLException
    {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public int getDatabaseMajorVersion()
            throws SQLException
    {
        return 39;
    }

    @Override
    public int getDatabaseMinorVersion()
            throws SQLException
    {
        return 0;
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
        return 0;
    }

    @Override
    public int getSQLStateType()
            throws SQLException
    {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public boolean locatorsUpdateCopy()
            throws SQLException
    {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public boolean supportsStatementPooling()
            throws SQLException
    {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public RowIdLifetime getRowIdLifetime()
            throws SQLException
    {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public ResultSet getSchemas(String catalog, String schemaPattern)
            throws SQLException
    {
        return getSchemas();
    }

    @Override
    public boolean supportsStoredFunctionsUsingCallSyntax()
            throws SQLException
    {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public boolean autoCommitFailureClosesAllResultSets()
            throws SQLException
    {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public ResultSet getClientInfoProperties()
            throws SQLException
    {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public ResultSet getFunctions(String catalog, String schemaPattern, String functionNamePattern)
            throws SQLException
    {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public ResultSet getFunctionColumns(String catalog, String schemaPattern, String functionNamePattern, String columnNamePattern)
            throws SQLException
    {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public ResultSet getPseudoColumns(String catalog, String schemaPattern, String tableNamePattern, String columnNamePattern)
            throws SQLException
    {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public boolean generatedKeyAlwaysReturned()
            throws SQLException
    {
        // TODO Auto-generated method stub
        return false;
    }

    public static class TypeInfo
    {
        public String typeName;
        public int sqlDataType;
        public int precision;
        public int minScale;
        public int maxScale;
        public int radix;

        public TypeInfo(String typeName, int sqlDataType, int precision, int minScale, int maxScale, int radix)
        {
            this.typeName = typeName;
            this.sqlDataType = sqlDataType;
            this.precision = precision;
            this.minScale = minScale;
            this.maxScale = maxScale;
            this.radix = radix;
        }
    }
}
