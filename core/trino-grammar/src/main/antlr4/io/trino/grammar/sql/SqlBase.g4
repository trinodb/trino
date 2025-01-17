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

grammar SqlBase;

options { caseInsensitive = true; }

tokens {
    DELIMITER
}

singleStatement
    : statement EOF
    ;

standaloneExpression
    : expression EOF
    ;

standalonePathSpecification
    : pathSpecification EOF
    ;

standaloneType
    : type EOF
    ;

standaloneRowPattern
    : rowPattern EOF
    ;

standaloneFunctionSpecification
    : functionSpecification EOF
    ;

statement
    : rootQuery                                                        #statementDefault
    | USE schema=identifier                                            #use
    | USE catalog=identifier '.' schema=identifier                     #use
    | CREATE CATALOG (IF NOT EXISTS)? catalog=identifier
         USING connectorName=identifier
         (COMMENT string)?
         (AUTHORIZATION principal)?
         (WITH properties)?                                            #createCatalog
    | DROP CATALOG (IF EXISTS)? catalog=identifier
         (CASCADE | RESTRICT)?                                         #dropCatalog
    | CREATE SCHEMA (IF NOT EXISTS)? qualifiedName
        (AUTHORIZATION principal)?
        (WITH properties)?                                             #createSchema
    | DROP SCHEMA (IF EXISTS)? qualifiedName (CASCADE | RESTRICT)?     #dropSchema
    | ALTER SCHEMA qualifiedName RENAME TO identifier                  #renameSchema
    | ALTER SCHEMA qualifiedName SET AUTHORIZATION principal           #setSchemaAuthorization
    | CREATE (OR REPLACE)? TABLE (IF NOT EXISTS)? qualifiedName
        columnAliases?
        (COMMENT string)?
        (WITH properties)? AS (rootQuery | '('rootQuery')')
        (WITH (NO)? DATA)?                                             #createTableAsSelect
    | CREATE (OR REPLACE)? TABLE (IF NOT EXISTS)? qualifiedName
        '(' tableElement (',' tableElement)* ')'
         (COMMENT string)?
         (WITH properties)?                                            #createTable
    | DROP TABLE (IF EXISTS)? qualifiedName                            #dropTable
    | INSERT INTO qualifiedName columnAliases? rootQuery               #insertInto
    | DELETE FROM qualifiedName (WHERE booleanExpression)?             #delete
    | TRUNCATE TABLE qualifiedName                                     #truncateTable
    | COMMENT ON TABLE qualifiedName IS (string | NULL)                #commentTable
    | COMMENT ON VIEW qualifiedName IS (string | NULL)                 #commentView
    | COMMENT ON COLUMN qualifiedName IS (string | NULL)               #commentColumn
    | ALTER TABLE (IF EXISTS)? from=qualifiedName
        RENAME TO to=qualifiedName                                     #renameTable
    | ALTER TABLE (IF EXISTS)? tableName=qualifiedName
        ADD COLUMN (IF NOT EXISTS)? column=columnDefinition
        (FIRST | LAST | AFTER after=identifier)?                       #addColumn
    | ALTER TABLE (IF EXISTS)? tableName=qualifiedName
        RENAME COLUMN (IF EXISTS)? from=qualifiedName TO to=identifier #renameColumn
    | ALTER TABLE (IF EXISTS)? tableName=qualifiedName
        DROP COLUMN (IF EXISTS)? column=qualifiedName                  #dropColumn
    | ALTER TABLE (IF EXISTS)? tableName=qualifiedName
        ALTER COLUMN columnName=qualifiedName SET DATA TYPE type       #setColumnType
    | ALTER TABLE (IF EXISTS)? tableName=qualifiedName
        ALTER COLUMN columnName=identifier DROP NOT NULL               #dropNotNullConstraint
    | ALTER TABLE tableName=qualifiedName SET AUTHORIZATION principal  #setTableAuthorization
    | ALTER TABLE tableName=qualifiedName
        SET PROPERTIES propertyAssignments                             #setTableProperties
    | ALTER TABLE tableName=qualifiedName
        EXECUTE procedureName=identifier
        ('(' (callArgument (',' callArgument)*)? ')')?
        (WHERE where=booleanExpression)?                               #tableExecute
    | ANALYZE qualifiedName (WITH properties)?                         #analyze
    | CREATE (OR REPLACE)? MATERIALIZED VIEW
        (IF NOT EXISTS)? qualifiedName
        (GRACE PERIOD interval)?
        (COMMENT string)?
        (WITH properties)? AS rootQuery                                #createMaterializedView
    | CREATE (OR REPLACE)? VIEW qualifiedName
        (COMMENT string)?
        (SECURITY (DEFINER | INVOKER))?
        (WITH properties)? AS rootQuery                                #createView
    | REFRESH MATERIALIZED VIEW qualifiedName                          #refreshMaterializedView
    | DROP MATERIALIZED VIEW (IF EXISTS)? qualifiedName                #dropMaterializedView
    | ALTER MATERIALIZED VIEW (IF EXISTS)? from=qualifiedName
        RENAME TO to=qualifiedName                                     #renameMaterializedView
    | ALTER MATERIALIZED VIEW qualifiedName
        SET PROPERTIES propertyAssignments                             #setMaterializedViewProperties
    | DROP VIEW (IF EXISTS)? qualifiedName                             #dropView
    | ALTER VIEW from=qualifiedName RENAME TO to=qualifiedName         #renameView
    | ALTER VIEW from=qualifiedName SET AUTHORIZATION principal        #setViewAuthorization
    | CALL qualifiedName '(' (callArgument (',' callArgument)*)? ')'   #call
    | CREATE (OR REPLACE)? functionSpecification                       #createFunction
    | DROP FUNCTION (IF EXISTS)? functionDeclaration                   #dropFunction
    | CREATE ROLE name=identifier
        (WITH ADMIN grantor)?
        (IN catalog=identifier)?                                       #createRole
    | DROP ROLE (IF EXISTS)? name=identifier (IN catalog=identifier)?  #dropRole
    | GRANT
        privilegeOrRole (',' privilegeOrRole)*
        TO principal (',' principal)*
        (WITH ADMIN OPTION)?
        (GRANTED BY grantor)?
        (IN catalog=identifier)?                                       #grantRoles
    | GRANT
        ((privilegeOrRole (',' privilegeOrRole)*) | ALL PRIVILEGES)
        ON grantObject
        TO principal
        (WITH GRANT OPTION)?                                           #grantPrivileges
    | REVOKE
        (ADMIN OPTION FOR)?
        privilegeOrRole (',' privilegeOrRole)*
        FROM principal (',' principal)*
        (GRANTED BY grantor)?
        (IN catalog=identifier)?                                       #revokeRoles
    | REVOKE
        (GRANT OPTION FOR)?
        ((privilegeOrRole (',' privilegeOrRole)*) | ALL PRIVILEGES)
        ON grantObject
        FROM grantee=principal                                         #revokePrivileges
    | DENY
        (privilege (',' privilege)* | ALL PRIVILEGES)
        ON grantObject
        TO grantee=principal                                           #deny
    | SET ROLE (ALL | NONE | role=identifier)
        (IN catalog=identifier)?                                       #setRole
    | SHOW GRANTS (ON grantObject)?                                    #showGrants
    | EXPLAIN ('(' explainOption (',' explainOption)* ')')? statement  #explain
    | EXPLAIN ANALYZE VERBOSE? statement                               #explainAnalyze
    | SHOW CREATE TABLE qualifiedName                                  #showCreateTable
    | SHOW CREATE SCHEMA qualifiedName                                 #showCreateSchema
    | SHOW CREATE VIEW qualifiedName                                   #showCreateView
    | SHOW CREATE MATERIALIZED VIEW qualifiedName                      #showCreateMaterializedView
    | SHOW CREATE FUNCTION qualifiedName                               #showCreateFunction
    | SHOW TABLES ((FROM | IN) qualifiedName)?
        (LIKE pattern=string (ESCAPE escape=string)?)?                 #showTables
    | SHOW SCHEMAS ((FROM | IN) identifier)?
        (LIKE pattern=string (ESCAPE escape=string)?)?                 #showSchemas
    | SHOW CATALOGS
        (LIKE pattern=string (ESCAPE escape=string)?)?                 #showCatalogs
    | SHOW COLUMNS (FROM | IN) qualifiedName
        (LIKE pattern=string (ESCAPE escape=string)?)?                 #showColumns
    | SHOW STATS FOR qualifiedName                                     #showStats
    | SHOW STATS FOR '(' rootQuery ')'                                 #showStatsForQuery
    | SHOW CURRENT? ROLES ((FROM | IN) identifier)?                    #showRoles
    | SHOW ROLE GRANTS ((FROM | IN) identifier)?                       #showRoleGrants
    | DESCRIBE qualifiedName                                           #showColumns
    | DESC qualifiedName                                               #showColumns
    | SHOW FUNCTIONS ((FROM | IN) qualifiedName)?
        (LIKE pattern=string (ESCAPE escape=string)?)?                 #showFunctions
    | SHOW SESSION
        (LIKE pattern=string (ESCAPE escape=string)?)?                 #showSession
    | SET SESSION AUTHORIZATION authorizationUser                      #setSessionAuthorization
    | RESET SESSION AUTHORIZATION                                      #resetSessionAuthorization
    | SET SESSION qualifiedName EQ expression                          #setSession
    | RESET SESSION qualifiedName                                      #resetSession
    | START TRANSACTION (transactionMode (',' transactionMode)*)?      #startTransaction
    | COMMIT WORK?                                                     #commit
    | ROLLBACK WORK?                                                   #rollback
    | PREPARE identifier FROM statement                                #prepare
    | DEALLOCATE PREPARE identifier                                    #deallocate
    | EXECUTE identifier (USING expression (',' expression)*)?         #execute
    | EXECUTE IMMEDIATE string (USING expression (',' expression)*)?   #executeImmediate
    | DESCRIBE INPUT identifier                                        #describeInput
    | DESCRIBE OUTPUT identifier                                       #describeOutput
    | SET PATH pathSpecification                                       #setPath
    | SET TIME ZONE (LOCAL | expression)                               #setTimeZone
    | UPDATE qualifiedName
        SET updateAssignment (',' updateAssignment)*
        (WHERE where=booleanExpression)?                               #update
    | MERGE INTO qualifiedName (AS? identifier)?
        USING relation ON expression mergeCase+                        #merge
    ;

rootQuery
    : withFunction? query
    ;

withFunction
    : WITH functionSpecification (',' functionSpecification)*
    ;

query
    : with? queryNoWith
    ;

with
    : WITH RECURSIVE? namedQuery (',' namedQuery)*
    ;

tableElement
    : columnDefinition
    | likeClause
    ;

columnDefinition
    : qualifiedName type (NOT NULL)? (COMMENT string)? (WITH properties)?
    ;

likeClause
    : LIKE qualifiedName (optionType=(INCLUDING | EXCLUDING) PROPERTIES)?
    ;

properties
    : '(' propertyAssignments ')'
    ;

propertyAssignments
    : property (',' property)*
    ;

property
    : identifier EQ propertyValue
    ;

propertyValue
    : DEFAULT       #defaultPropertyValue
    | expression    #nonDefaultPropertyValue
    ;

queryNoWith
    : queryTerm
      (ORDER BY sortItem (',' sortItem)*)?
      (OFFSET offset=rowCount (ROW | ROWS)?)?
      ( (LIMIT limit=limitRowCount)
      | (FETCH (FIRST | NEXT) (fetchFirst=rowCount)? (ROW | ROWS) (ONLY | WITH TIES))
      )?
    ;

limitRowCount
    : ALL
    | rowCount
    ;

rowCount
    : INTEGER_VALUE
    | QUESTION_MARK
    ;

queryTerm
    : queryPrimary                                                             #queryTermDefault
    | left=queryTerm operator=INTERSECT setQuantifier? right=queryTerm         #setOperation
    | left=queryTerm operator=(UNION | EXCEPT) setQuantifier? right=queryTerm  #setOperation
    ;

queryPrimary
    : querySpecification                   #queryPrimaryDefault
    | TABLE qualifiedName                  #table
    | VALUES expression (',' expression)*  #inlineTable
    | '(' queryNoWith ')'                  #subquery
    ;

sortItem
    : expression ordering=(ASC | DESC)? (NULLS nullOrdering=(FIRST | LAST))?
    ;

querySpecification
    : SELECT setQuantifier? selectItem (',' selectItem)*
      (FROM relation (',' relation)*)?
      (WHERE where=booleanExpression)?
      (GROUP BY groupBy)?
      (HAVING having=booleanExpression)?
      (WINDOW windowDefinition (',' windowDefinition)*)?
    ;

groupBy
    : setQuantifier? groupingElement (',' groupingElement)*
    ;

groupingElement
    : groupingSet                                            #singleGroupingSet
    | ROLLUP '(' (groupingSet (',' groupingSet)*)? ')'       #rollup
    | CUBE '(' (groupingSet (',' groupingSet)*)? ')'         #cube
    | GROUPING SETS '(' groupingSet (',' groupingSet)* ')'   #multipleGroupingSets
    ;

groupingSet
    : '(' (expression (',' expression)*)? ')'
    | expression
    ;

windowDefinition
    : name=identifier AS '(' windowSpecification ')'
    ;

windowSpecification
    : (existingWindowName=identifier)?
      (PARTITION BY partition+=expression (',' partition+=expression)*)?
      (ORDER BY sortItem (',' sortItem)*)?
      windowFrame?
    ;

namedQuery
    : name=identifier (columnAliases)? AS '(' query ')'
    ;

setQuantifier
    : DISTINCT
    | ALL
    ;

selectItem
    : expression (AS? identifier)?                          #selectSingle
    | primaryExpression '.' ASTERISK (AS columnAliases)?    #selectAll
    | ASTERISK                                              #selectAll
    ;

relation
    : left=relation
      ( CROSS JOIN right=sampledRelation
      | joinType JOIN rightRelation=relation joinCriteria
      | NATURAL joinType JOIN right=sampledRelation
      )                                                     #joinRelation
    | sampledRelation                                       #relationDefault
    ;

joinType
    : INNER?
    | LEFT OUTER?
    | RIGHT OUTER?
    | FULL OUTER?
    ;

joinCriteria
    : ON booleanExpression
    | USING '(' identifier (',' identifier)* ')'
    ;

sampledRelation
    : patternRecognition (
        TABLESAMPLE sampleType '(' percentage=expression ')'
      )?
    ;

sampleType
    : BERNOULLI
    | SYSTEM
    ;

trimsSpecification
    : LEADING
    | TRAILING
    | BOTH
    ;

listAggOverflowBehavior
    : ERROR
    | TRUNCATE string? listaggCountIndication
    ;

listaggCountIndication
    : WITH COUNT
    | WITHOUT COUNT
    ;

patternRecognition
    : aliasedRelation (
        MATCH_RECOGNIZE '('
          (PARTITION BY partition+=expression (',' partition+=expression)*)?
          (ORDER BY sortItem (',' sortItem)*)?
          (MEASURES measureDefinition (',' measureDefinition)*)?
          rowsPerMatch?
          (AFTER MATCH skipTo)?
          (INITIAL | SEEK)?
          PATTERN '(' rowPattern ')'
          (SUBSET subsetDefinition (',' subsetDefinition)*)?
          DEFINE variableDefinition (',' variableDefinition)*
        ')'
        (AS? identifier columnAliases?)?
      )?
    ;

measureDefinition
    : expression AS identifier
    ;

rowsPerMatch
    : ONE ROW PER MATCH
    | ALL ROWS PER MATCH emptyMatchHandling?
    ;

emptyMatchHandling
    : SHOW EMPTY MATCHES
    | OMIT EMPTY MATCHES
    | WITH UNMATCHED ROWS
    ;

skipTo
    : 'SKIP' TO NEXT ROW
    | 'SKIP' PAST LAST ROW
    | 'SKIP' TO FIRST identifier
    | 'SKIP' TO LAST identifier
    | 'SKIP' TO identifier
    ;

subsetDefinition
    : name=identifier EQ '(' union+=identifier (',' union+=identifier)* ')'
    ;

variableDefinition
    : identifier AS expression
    ;

aliasedRelation
    : relationPrimary (AS? identifier columnAliases?)?
    ;

columnAliases
    : '(' identifier (',' identifier)* ')'
    ;

relationPrimary
    : qualifiedName queryPeriod?                                      #tableName
    | '(' query ')'                                                   #subqueryRelation
    | UNNEST '(' expression (',' expression)* ')' (WITH ORDINALITY)?  #unnest
    | LATERAL '(' query ')'                                           #lateral
    | TABLE '(' tableFunctionCall ')'                                 #tableFunctionInvocation
    | '(' relation ')'                                                #parenthesizedRelation
    | JSON_TABLE '('
        jsonPathInvocation
        COLUMNS '(' jsonTableColumn (',' jsonTableColumn)* ')'
        (PLAN '(' jsonTableSpecificPlan ')'
        | PLAN DEFAULT '(' jsonTableDefaultPlan ')'
        )?
        ((ERROR | EMPTY) ON ERROR)?
      ')'                                                             #jsonTable
    ;

jsonTableColumn
    : identifier FOR ORDINALITY                                     #ordinalityColumn
    | identifier type
        (PATH string)?
        (emptyBehavior=jsonValueBehavior ON EMPTY)?
        (errorBehavior=jsonValueBehavior ON ERROR)?                 #valueColumn
    | identifier type FORMAT jsonRepresentation
        (PATH string)?
        (jsonQueryWrapperBehavior WRAPPER)?
        ((KEEP | OMIT) QUOTES (ON SCALAR TEXT_STRING)?)?
        (emptyBehavior=jsonQueryBehavior ON EMPTY)?
        (errorBehavior=jsonQueryBehavior ON ERROR)?                 #queryColumn
    | NESTED PATH? string (AS identifier)?
        COLUMNS '(' jsonTableColumn (',' jsonTableColumn)* ')'      #nestedColumns
    ;

jsonTableSpecificPlan
    : jsonTablePathName                                         #leafPlan
    | jsonTablePathName (OUTER | INNER) planPrimary             #joinPlan
    | planPrimary UNION planPrimary (UNION planPrimary)*        #unionPlan
    | planPrimary CROSS planPrimary (CROSS planPrimary)*        #crossPlan
    ;

jsonTablePathName
    : identifier
    ;

planPrimary
    : jsonTablePathName
    | '(' jsonTableSpecificPlan ')'
    ;

jsonTableDefaultPlan
    : (OUTER | INNER) (',' (UNION | CROSS))?
    | (UNION | CROSS) (',' (OUTER | INNER))?
    ;

tableFunctionCall
    : qualifiedName '(' (tableFunctionArgument (',' tableFunctionArgument)*)?
      (COPARTITION copartitionTables (',' copartitionTables)*)? ')'
    ;

tableFunctionArgument
    : (identifier '=>')? (tableArgument | descriptorArgument | expression) // descriptor before expression to avoid parsing descriptor as a function call
    ;

tableArgument
    : tableArgumentRelation
        (PARTITION BY ('(' (expression (',' expression)*)? ')' | expression))?
        (PRUNE WHEN EMPTY | KEEP WHEN EMPTY)?
        (ORDER BY ('(' sortItem (',' sortItem)* ')' | sortItem))?
    ;

tableArgumentRelation
    : TABLE '(' qualifiedName ')' (AS? identifier columnAliases?)?  #tableArgumentTable
    | TABLE '(' query ')' (AS? identifier columnAliases?)?          #tableArgumentQuery
    ;

descriptorArgument
    : DESCRIPTOR '(' descriptorField (',' descriptorField)* ')'
    | CAST '(' NULL AS DESCRIPTOR ')'
    ;

descriptorField
    : identifier type?
    ;

copartitionTables
    : '(' qualifiedName ',' qualifiedName (',' qualifiedName)* ')'
    ;

expression
    : booleanExpression
    ;

booleanExpression
    : valueExpression predicate[$valueExpression.ctx]?  #predicated
    | NOT booleanExpression                             #logicalNot
    | booleanExpression AND booleanExpression           #and
    | booleanExpression OR booleanExpression            #or
    ;

// workaround for https://github.com/antlr/antlr4/issues/780
predicate[ParserRuleContext value]
    : comparisonOperator right=valueExpression                            #comparison
    | comparisonOperator comparisonQuantifier '(' query ')'               #quantifiedComparison
    | NOT? BETWEEN lower=valueExpression AND upper=valueExpression        #between
    | NOT? IN '(' expression (',' expression)* ')'                        #inList
    | NOT? IN '(' query ')'                                               #inSubquery
    | NOT? LIKE pattern=valueExpression (ESCAPE escape=valueExpression)?  #like
    | IS NOT? NULL                                                        #nullPredicate
    | IS NOT? DISTINCT FROM right=valueExpression                         #distinctFrom
    ;

valueExpression
    : primaryExpression                                                                 #valueExpressionDefault
    | valueExpression AT timeZoneSpecifier                                              #atTimeZone
    | operator=(MINUS | PLUS) valueExpression                                           #arithmeticUnary
    | left=valueExpression operator=(ASTERISK | SLASH | PERCENT) right=valueExpression  #arithmeticBinary
    | left=valueExpression operator=(PLUS | MINUS) right=valueExpression                #arithmeticBinary
    | left=valueExpression CONCAT right=valueExpression                                 #concatenation
    ;

primaryExpression
    : NULL                                                                                #nullLiteral
    | interval                                                                            #intervalLiteral
    | identifier string                                                                   #typeConstructor
    | DOUBLE PRECISION string                                                             #typeConstructor
    | number                                                                              #numericLiteral
    | booleanValue                                                                        #booleanLiteral
    | string                                                                              #stringLiteral
    | BINARY_LITERAL                                                                      #binaryLiteral
    | QUESTION_MARK                                                                       #parameter
    | POSITION '(' valueExpression IN valueExpression ')'                                 #position
    | '(' expression (',' expression)+ ')'                                                #rowConstructor
    | ROW '(' expression (',' expression)* ')'                                            #rowConstructor
    | name=LISTAGG '(' setQuantifier? expression (',' string)?
        (ON OVERFLOW listAggOverflowBehavior)? ')'
        (WITHIN GROUP '(' ORDER BY sortItem (',' sortItem)* ')')
        filter? over?                                                                     #listagg
    | processingMode? qualifiedName '(' (label=identifier '.')? ASTERISK ')'
        filter? over?                                                                     #functionCall
    | processingMode? qualifiedName '(' (setQuantifier? expression (',' expression)*)?
        (ORDER BY sortItem (',' sortItem)*)? ')' filter? (nullTreatment? over)?           #functionCall
    | identifier over                                                                     #measure
    | identifier '->' expression                                                          #lambda
    | '(' (identifier (',' identifier)*)? ')' '->' expression                             #lambda
    | '(' query ')'                                                                       #subqueryExpression
    // This is an extension to ANSI SQL, which considers EXISTS to be a <boolean expression>
    | EXISTS '(' query ')'                                                                #exists
    | CASE operand=expression whenClause+ (ELSE elseExpression=expression)? END           #simpleCase
    | CASE whenClause+ (ELSE elseExpression=expression)? END                              #searchedCase
    | CAST '(' expression AS type ')'                                                     #cast
    | TRY_CAST '(' expression AS type ')'                                                 #cast
    | ARRAY '[' (expression (',' expression)*)? ']'                                       #arrayConstructor
    | value=primaryExpression '[' index=valueExpression ']'                               #subscript
    | identifier                                                                          #columnReference
    | base=primaryExpression '.' fieldName=identifier                                     #dereference
    | name=CURRENT_DATE                                                                   #currentDate
    | name=CURRENT_TIME ('(' precision=INTEGER_VALUE ')')?                                #currentTime
    | name=CURRENT_TIMESTAMP ('(' precision=INTEGER_VALUE ')')?                           #currentTimestamp
    | name=LOCALTIME ('(' precision=INTEGER_VALUE ')')?                                   #localTime
    | name=LOCALTIMESTAMP ('(' precision=INTEGER_VALUE ')')?                              #localTimestamp
    | name=CURRENT_USER                                                                   #currentUser
    | name=CURRENT_CATALOG                                                                #currentCatalog
    | name=CURRENT_SCHEMA                                                                 #currentSchema
    | name=CURRENT_PATH                                                                   #currentPath
    | TRIM '(' (trimsSpecification? trimChar=valueExpression? FROM)?
        trimSource=valueExpression ')'                                                    #trim
    | TRIM '(' trimSource=valueExpression ',' trimChar=valueExpression ')'                #trim
    | SUBSTRING '(' valueExpression FROM valueExpression (FOR valueExpression)? ')'       #substring
    | NORMALIZE '(' valueExpression (',' normalForm)? ')'                                 #normalize
    | EXTRACT '(' identifier FROM valueExpression ')'                                     #extract
    | '(' expression ')'                                                                  #parenthesizedExpression
    | GROUPING '(' (qualifiedName (',' qualifiedName)*)? ')'                              #groupingOperation
    | JSON_EXISTS '(' jsonPathInvocation (jsonExistsErrorBehavior ON ERROR)? ')'          #jsonExists
    | JSON_VALUE '('
        jsonPathInvocation
        (RETURNING type)?
        (emptyBehavior=jsonValueBehavior ON EMPTY)?
        (errorBehavior=jsonValueBehavior ON ERROR)?
      ')'                                                                                 #jsonValue
    | JSON_QUERY '('
        jsonPathInvocation
        (RETURNING type (FORMAT jsonRepresentation)?)?
        (jsonQueryWrapperBehavior WRAPPER)?
        ((KEEP | OMIT) QUOTES (ON SCALAR TEXT_STRING)?)?
        (emptyBehavior=jsonQueryBehavior ON EMPTY)?
        (errorBehavior=jsonQueryBehavior ON ERROR)?
      ')'                                                                                 #jsonQuery
    | JSON_OBJECT '('
        (
          jsonObjectMember (',' jsonObjectMember)*
          (NULL ON NULL | ABSENT ON NULL)?
          (WITH UNIQUE KEYS? | WITHOUT UNIQUE KEYS?)?
        )?
        (RETURNING type (FORMAT jsonRepresentation)?)?
      ')'                                                                                 #jsonObject
    | JSON_ARRAY '('
        (
          jsonValueExpression (',' jsonValueExpression)*
          (NULL ON NULL | ABSENT ON NULL)?
        )?
        (RETURNING type (FORMAT jsonRepresentation)?)?
     ')'                                                                                  #jsonArray
    ;

jsonPathInvocation
    : jsonValueExpression ',' path=string
        (AS pathName=identifier)?
        (PASSING jsonArgument (',' jsonArgument)*)?
    ;

jsonValueExpression
    : expression (FORMAT jsonRepresentation)?
    ;

jsonRepresentation
    : JSON (ENCODING (UTF8 | UTF16 | UTF32))? // TODO add implementation-defined JSON representation option
    ;

jsonArgument
    : jsonValueExpression AS identifier
    ;

jsonExistsErrorBehavior
    : TRUE
    | FALSE
    | UNKNOWN
    | ERROR
    ;

jsonValueBehavior
    : ERROR
    | NULL
    | DEFAULT expression
    ;

jsonQueryWrapperBehavior
    : WITHOUT ARRAY?
    | WITH (CONDITIONAL | UNCONDITIONAL)? ARRAY?
    ;

jsonQueryBehavior
    : ERROR
    | NULL
    | EMPTY ARRAY
    | EMPTY OBJECT
    ;

jsonObjectMember
    : KEY? expression VALUE jsonValueExpression
    | expression ':' jsonValueExpression
    ;

processingMode
    : RUNNING
    | FINAL
    ;

nullTreatment
    : IGNORE NULLS
    | RESPECT NULLS
    ;

string
    : STRING                                #basicStringLiteral
    | UNICODE_STRING (UESCAPE STRING)?      #unicodeStringLiteral
    ;

timeZoneSpecifier
    : TIME ZONE interval  #timeZoneInterval
    | TIME ZONE string    #timeZoneString
    ;

comparisonOperator
    : EQ | NEQ | LT | LTE | GT | GTE
    ;

comparisonQuantifier
    : ALL | SOME | ANY
    ;

booleanValue
    : TRUE | FALSE
    ;

interval
    : INTERVAL sign=(PLUS | MINUS)? string from=intervalField (TO to=intervalField)?
    ;

intervalField
    : YEAR | MONTH | DAY | HOUR | MINUTE | SECOND
    ;

normalForm
    : NFD | NFC | NFKD | NFKC
    ;

type
    : ROW '(' rowField (',' rowField)* ')'                                         #rowType
    | INTERVAL from=intervalField (TO to=intervalField)?                           #intervalType
    | base=TIMESTAMP ('(' precision = typeParameter ')')? (WITHOUT TIME ZONE)?     #dateTimeType
    | base=TIMESTAMP ('(' precision = typeParameter ')')? WITH TIME ZONE           #dateTimeType
    | base=TIME ('(' precision = typeParameter ')')? (WITHOUT TIME ZONE)?          #dateTimeType
    | base=TIME ('(' precision = typeParameter ')')? WITH TIME ZONE                #dateTimeType
    | DOUBLE PRECISION                                                             #doublePrecisionType
    | ARRAY '<' type '>'                                                           #legacyArrayType
    | MAP '<' keyType=type ',' valueType=type '>'                                  #legacyMapType
    | type ARRAY ('[' INTEGER_VALUE ']')?                                          #arrayType
    | identifier ('(' typeParameter (',' typeParameter)* ')')?                     #genericType
    ;

rowField
    : type
    | identifier type;

typeParameter
    : INTEGER_VALUE | type
    ;

whenClause
    : WHEN condition=expression THEN result=expression
    ;

filter
    : FILTER '(' WHERE booleanExpression ')'
    ;

mergeCase
    : WHEN MATCHED (AND condition=expression)? THEN
        UPDATE SET targets+=identifier EQ values+=expression
          (',' targets+=identifier EQ values+=expression)*                  #mergeUpdate
    | WHEN MATCHED (AND condition=expression)? THEN DELETE                  #mergeDelete
    | WHEN NOT MATCHED (AND condition=expression)? THEN
        INSERT ('(' targets+=identifier (',' targets+=identifier)* ')')?
        VALUES '(' values+=expression (',' values+=expression)* ')'         #mergeInsert
    ;

over
    : OVER (windowName=identifier | '(' windowSpecification ')')
    ;

windowFrame
    : (MEASURES measureDefinition (',' measureDefinition)*)?
      frameExtent
      (AFTER MATCH skipTo)?
      (INITIAL | SEEK)?
      (PATTERN '(' rowPattern ')')?
      (SUBSET subsetDefinition (',' subsetDefinition)*)?
      (DEFINE variableDefinition (',' variableDefinition)*)?
    ;

frameExtent
    : frameType=RANGE start=frameBound
    | frameType=ROWS start=frameBound
    | frameType=GROUPS start=frameBound
    | frameType=RANGE BETWEEN start=frameBound AND end=frameBound
    | frameType=ROWS BETWEEN start=frameBound AND end=frameBound
    | frameType=GROUPS BETWEEN start=frameBound AND end=frameBound
    ;

frameBound
    : UNBOUNDED boundType=PRECEDING                 #unboundedFrame
    | UNBOUNDED boundType=FOLLOWING                 #unboundedFrame
    | CURRENT ROW                                   #currentRowBound
    | expression boundType=(PRECEDING | FOLLOWING)  #boundedFrame
    ;

rowPattern
    : patternPrimary patternQuantifier?                 #quantifiedPrimary
    | rowPattern rowPattern                             #patternConcatenation
    | rowPattern '|' rowPattern                         #patternAlternation
    ;

patternPrimary
    : identifier                                        #patternVariable
    | '(' ')'                                           #emptyPattern
    | PERMUTE '(' rowPattern (',' rowPattern)* ')'      #patternPermutation
    | '(' rowPattern ')'                                #groupedPattern
    | '^'                                               #partitionStartAnchor
    | '$'                                               #partitionEndAnchor
    | '{-' rowPattern '-}'                              #excludedPattern
    ;

patternQuantifier
    : ASTERISK (reluctant=QUESTION_MARK)?                                                       #zeroOrMoreQuantifier
    | PLUS (reluctant=QUESTION_MARK)?                                                           #oneOrMoreQuantifier
    | QUESTION_MARK (reluctant=QUESTION_MARK)?                                                  #zeroOrOneQuantifier
    | '{' exactly=INTEGER_VALUE '}' (reluctant=QUESTION_MARK)?                                  #rangeQuantifier
    | '{' (atLeast=INTEGER_VALUE)? ',' (atMost=INTEGER_VALUE)? '}' (reluctant=QUESTION_MARK)?   #rangeQuantifier
    ;

updateAssignment
    : identifier EQ expression
    ;

explainOption
    : FORMAT value=(TEXT | GRAPHVIZ | JSON)                 #explainFormat
    | TYPE value=(LOGICAL | DISTRIBUTED | VALIDATE | IO)    #explainType
    ;

transactionMode
    : ISOLATION LEVEL levelOfIsolation    #isolationLevel
    | READ accessMode=(ONLY | WRITE)      #transactionAccessMode
    ;

levelOfIsolation
    : READ UNCOMMITTED                    #readUncommitted
    | READ COMMITTED                      #readCommitted
    | REPEATABLE READ                     #repeatableRead
    | SERIALIZABLE                        #serializable
    ;

callArgument
    : expression                    #positionalArgument
    | identifier '=>' expression    #namedArgument
    ;

pathElement
    : identifier '.' identifier     #qualifiedArgument
    | identifier                    #unqualifiedArgument
    ;

pathSpecification
    : pathElement (',' pathElement)*
    ;

functionSpecification
    : FUNCTION functionDeclaration returnsClause routineCharacteristic*
        (controlStatement | AS functionDefinition)
    ;

functionDefinition
    : DOLLAR_STRING
    ;

functionDeclaration
    : qualifiedName '(' (parameterDeclaration (',' parameterDeclaration)*)? ')'
    ;

parameterDeclaration
    : identifier? type
    ;

returnsClause
    : RETURNS type
    ;

routineCharacteristic
    : LANGUAGE identifier               #languageCharacteristic
    | NOT? DETERMINISTIC                #deterministicCharacteristic
    | RETURNS NULL ON NULL INPUT        #returnsNullOnNullInputCharacteristic
    | CALLED ON NULL INPUT              #calledOnNullInputCharacteristic
    | SECURITY (DEFINER | INVOKER)      #securityCharacteristic
    | COMMENT string                    #commentCharacteristic
    | (WITH properties)                 #propertiesCharacteristic
    ;

controlStatement
    : RETURN valueExpression                                                        #returnStatement
    | SET identifier EQ expression                                                  #assignmentStatement
    | CASE expression caseStatementWhenClause+ elseClause? END CASE                 #simpleCaseStatement
    | CASE caseStatementWhenClause+ elseClause? END CASE                            #searchedCaseStatement
    | IF expression THEN sqlStatementList elseIfClause* elseClause? END IF          #ifStatement
    | ITERATE identifier                                                            #iterateStatement
    | LEAVE identifier                                                              #leaveStatement
    | BEGIN (variableDeclaration SEMICOLON)* sqlStatementList? END                  #compoundStatement
    | (label=identifier ':')? LOOP sqlStatementList END LOOP                        #loopStatement
    | (label=identifier ':')? WHILE expression DO sqlStatementList END WHILE        #whileStatement
    | (label=identifier ':')? REPEAT sqlStatementList UNTIL expression END REPEAT   #repeatStatement
    ;

caseStatementWhenClause
    : WHEN expression THEN sqlStatementList
    ;

elseIfClause
    : ELSEIF expression THEN sqlStatementList
    ;

elseClause
    : ELSE sqlStatementList
    ;

variableDeclaration
    : DECLARE identifier (',' identifier)* type (DEFAULT valueExpression)?
    ;

sqlStatementList
    : (controlStatement SEMICOLON)+
    ;

privilege
    : CREATE | SELECT | DELETE | INSERT | UPDATE | identifier
    ;

entityKind
    : TABLE | SCHEMA | identifier
    ;

grantObject
    : entityKind? qualifiedName
    ;

qualifiedName
    : identifier ('.' identifier)*
    ;

queryPeriod
    : FOR rangeType AS OF end=valueExpression
    ;

rangeType
    : TIMESTAMP
    | VERSION
    ;

grantor
    : principal             #specifiedPrincipal
    | CURRENT_USER          #currentUserGrantor
    | CURRENT_ROLE          #currentRoleGrantor
    ;

principal
    : identifier            #unspecifiedPrincipal
    | USER identifier       #userPrincipal
    | ROLE identifier       #rolePrincipal
    ;

roles
    : identifier (',' identifier)*
    ;

privilegeOrRole
    : CREATE | SELECT | DELETE | INSERT | UPDATE | identifier
    ;

identifier
    : IDENTIFIER             #unquotedIdentifier
    | QUOTED_IDENTIFIER      #quotedIdentifier
    | nonReserved            #unquotedIdentifier
    | BACKQUOTED_IDENTIFIER  #backQuotedIdentifier
    | DIGIT_IDENTIFIER       #digitIdentifier
    ;

number
    : MINUS? DECIMAL_VALUE  #decimalLiteral
    | MINUS? DOUBLE_VALUE   #doubleLiteral
    | MINUS? INTEGER_VALUE  #integerLiteral
    ;

authorizationUser
    : identifier            #identifierUser
    | string                #stringUser
    ;

nonReserved
    // IMPORTANT: this rule must only contain tokens. Nested rules are not supported. See SqlParser.exitNonReserved
    : ABSENT | ADD | ADMIN | AFTER | ALL | ANALYZE | ANY | ARRAY | ASC | AT | AUTHORIZATION
    | BEGIN | BERNOULLI | BOTH
    | CALL | CALLED | CASCADE | CATALOG | CATALOGS | COLUMN | COLUMNS | COMMENT | COMMIT | COMMITTED | CONDITIONAL | COPARTITION | COUNT | CURRENT
    | DATA | DATE | DAY | DECLARE | DEFAULT | DEFINE | DEFINER | DENY | DESC | DESCRIPTOR | DETERMINISTIC | DISTRIBUTED | DO | DOUBLE
    | ELSEIF | EMPTY | ENCODING | ERROR | EXCLUDING | EXECUTE | EXPLAIN
    | FETCH | FILTER | FINAL | FIRST | FOLLOWING | FORMAT | FUNCTION | FUNCTIONS
    | GRACE | GRANT | GRANTED | GRANTS | GRAPHVIZ | GROUPS
    | HOUR
    | IF | IGNORE | IMMEDIATE | INCLUDING | INITIAL | INPUT | INTERVAL | INVOKER | IO | ITERATE | ISOLATION
    | JSON
    | KEEP | KEY | KEYS
    | LANGUAGE | LAST | LATERAL | LEADING | LEAVE | LEVEL | LIMIT | LOCAL | LOGICAL | LOOP
    | MAP | MATCH | MATCHED | MATCHES | MATCH_RECOGNIZE | MATERIALIZED | MEASURES | MERGE | MINUTE | MONTH
    | NESTED | NEXT | NFC | NFD | NFKC | NFKD | NO | NONE | NULLIF | NULLS
    | OBJECT | OF | OFFSET | OMIT | ONE | ONLY | OPTION | ORDINALITY | OUTPUT | OVER | OVERFLOW
    | PARTITION | PARTITIONS | PASSING | PAST | PATH | PATTERN | PER | PERIOD | PERMUTE | PLAN | POSITION | PRECEDING | PRECISION | PRIVILEGES | PROPERTIES | PRUNE
    | QUOTES
    | RANGE | READ | REFRESH | RENAME | REPEAT  | REPEATABLE | REPLACE | RESET | RESPECT | RESTRICT | RETURN | RETURNING | RETURNS | REVOKE | ROLE | ROLES | ROLLBACK | ROW | ROWS | RUNNING
    | SCALAR | SCHEMA | SCHEMAS | SECOND | SECURITY | SEEK | SERIALIZABLE | SESSION | SET | SETS
    | SHOW | SOME | START | STATS | SUBSET | SUBSTRING | SYSTEM
    | TABLES | TABLESAMPLE | TEXT | TEXT_STRING | TIES | TIME | TIMESTAMP | TO | TRAILING | TRANSACTION | TRUNCATE | TRY_CAST | TYPE
    | UNBOUNDED | UNCOMMITTED | UNCONDITIONAL | UNIQUE | UNKNOWN | UNMATCHED | UNTIL | UPDATE | USE | USER | UTF16 | UTF32 | UTF8
    | VALIDATE | VALUE | VERBOSE | VERSION | VIEW
    | WHILE | WINDOW | WITHIN | WITHOUT | WORK | WRAPPER | WRITE
    | YEAR
    | ZONE
    ;

ABSENT: 'ABSENT';
ADD: 'ADD';
ADMIN: 'ADMIN';
AFTER: 'AFTER';
ALL: 'ALL';
ALTER: 'ALTER';
ANALYZE: 'ANALYZE';
AND: 'AND';
ANY: 'ANY';
ARRAY: 'ARRAY';
AS: 'AS';
ASC: 'ASC';
AT: 'AT';
AUTHORIZATION: 'AUTHORIZATION';
BEGIN: 'BEGIN';
BERNOULLI: 'BERNOULLI';
BETWEEN: 'BETWEEN';
BOTH: 'BOTH';
BY: 'BY';
CALL: 'CALL';
CALLED: 'CALLED';
CASCADE: 'CASCADE';
CASE: 'CASE';
CAST: 'CAST';
CATALOG: 'CATALOG';
CATALOGS: 'CATALOGS';
COLUMN: 'COLUMN';
COLUMNS: 'COLUMNS';
COMMENT: 'COMMENT';
COMMIT: 'COMMIT';
COMMITTED: 'COMMITTED';
CONDITIONAL: 'CONDITIONAL';
CONSTRAINT: 'CONSTRAINT';
COUNT: 'COUNT';
COPARTITION: 'COPARTITION';
CREATE: 'CREATE';
CROSS: 'CROSS';
CUBE: 'CUBE';
CURRENT: 'CURRENT';
CURRENT_CATALOG: 'CURRENT_CATALOG';
CURRENT_DATE: 'CURRENT_DATE';
CURRENT_PATH: 'CURRENT_PATH';
CURRENT_ROLE: 'CURRENT_ROLE';
CURRENT_SCHEMA: 'CURRENT_SCHEMA';
CURRENT_TIME: 'CURRENT_TIME';
CURRENT_TIMESTAMP: 'CURRENT_TIMESTAMP';
CURRENT_USER: 'CURRENT_USER';
DATA: 'DATA';
DATE: 'DATE';
DAY: 'DAY';
DEALLOCATE: 'DEALLOCATE';
DECLARE: 'DECLARE';
DEFAULT: 'DEFAULT';
DEFINE: 'DEFINE';
DEFINER: 'DEFINER';
DELETE: 'DELETE';
DENY: 'DENY';
DESC: 'DESC';
DESCRIBE: 'DESCRIBE';
DESCRIPTOR: 'DESCRIPTOR';
DETERMINISTIC: 'DETERMINISTIC';
DISTINCT: 'DISTINCT';
DISTRIBUTED: 'DISTRIBUTED';
DO: 'DO';
DOUBLE: 'DOUBLE';
DROP: 'DROP';
ELSE: 'ELSE';
EMPTY: 'EMPTY';
ELSEIF: 'ELSEIF';
ENCODING: 'ENCODING';
END: 'END';
ERROR: 'ERROR';
ESCAPE: 'ESCAPE';
EXCEPT: 'EXCEPT';
EXCLUDING: 'EXCLUDING';
EXECUTE: 'EXECUTE';
EXISTS: 'EXISTS';
EXPLAIN: 'EXPLAIN';
EXTRACT: 'EXTRACT';
FALSE: 'FALSE';
FETCH: 'FETCH';
FILTER: 'FILTER';
FINAL: 'FINAL';
FIRST: 'FIRST';
FOLLOWING: 'FOLLOWING';
FOR: 'FOR';
FORMAT: 'FORMAT';
FROM: 'FROM';
FULL: 'FULL';
FUNCTION: 'FUNCTION';
FUNCTIONS: 'FUNCTIONS';
GRACE: 'GRACE';
GRANT: 'GRANT';
GRANTED: 'GRANTED';
GRANTS: 'GRANTS';
GRAPHVIZ: 'GRAPHVIZ';
GROUP: 'GROUP';
GROUPING: 'GROUPING';
GROUPS: 'GROUPS';
HAVING: 'HAVING';
HOUR: 'HOUR';
IF: 'IF';
IGNORE: 'IGNORE';
IMMEDIATE: 'IMMEDIATE';
IN: 'IN';
INCLUDING: 'INCLUDING';
INITIAL: 'INITIAL';
INNER: 'INNER';
INPUT: 'INPUT';
INSERT: 'INSERT';
INTERSECT: 'INTERSECT';
INTERVAL: 'INTERVAL';
INTO: 'INTO';
INVOKER: 'INVOKER';
IO: 'IO';
IS: 'IS';
ISOLATION: 'ISOLATION';
ITERATE: 'ITERATE';
JOIN: 'JOIN';
JSON: 'JSON';
JSON_ARRAY: 'JSON_ARRAY';
JSON_EXISTS: 'JSON_EXISTS';
JSON_OBJECT: 'JSON_OBJECT';
JSON_QUERY: 'JSON_QUERY';
JSON_TABLE: 'JSON_TABLE';
JSON_VALUE: 'JSON_VALUE';
KEEP: 'KEEP';
KEY: 'KEY';
KEYS: 'KEYS';
LANGUAGE: 'LANGUAGE';
LAST: 'LAST';
LATERAL: 'LATERAL';
LEADING: 'LEADING';
LEAVE: 'LEAVE';
LEFT: 'LEFT';
LEVEL: 'LEVEL';
LIKE: 'LIKE';
LIMIT: 'LIMIT';
LISTAGG: 'LISTAGG';
LOCAL: 'LOCAL';
LOCALTIME: 'LOCALTIME';
LOCALTIMESTAMP: 'LOCALTIMESTAMP';
LOGICAL: 'LOGICAL';
LOOP: 'LOOP';
MAP: 'MAP';
MATCH: 'MATCH';
MATCHED: 'MATCHED';
MATCHES: 'MATCHES';
MATCH_RECOGNIZE: 'MATCH_RECOGNIZE';
MATERIALIZED: 'MATERIALIZED';
MEASURES: 'MEASURES';
MERGE: 'MERGE';
MINUTE: 'MINUTE';
MONTH: 'MONTH';
NATURAL: 'NATURAL';
NESTED: 'NESTED';
NEXT: 'NEXT';
NFC : 'NFC';
NFD : 'NFD';
NFKC : 'NFKC';
NFKD : 'NFKD';
NO: 'NO';
NONE: 'NONE';
NORMALIZE: 'NORMALIZE';
NOT: 'NOT';
NULL: 'NULL';
NULLIF: 'NULLIF';
NULLS: 'NULLS';
OBJECT: 'OBJECT';
OF: 'OF';
OFFSET: 'OFFSET';
OMIT: 'OMIT';
ON: 'ON';
ONE: 'ONE';
ONLY: 'ONLY';
OPTION: 'OPTION';
OR: 'OR';
ORDER: 'ORDER';
ORDINALITY: 'ORDINALITY';
OUTER: 'OUTER';
OUTPUT: 'OUTPUT';
OVER: 'OVER';
OVERFLOW: 'OVERFLOW';
PARTITION: 'PARTITION';
PARTITIONS: 'PARTITIONS';
PASSING: 'PASSING';
PAST: 'PAST';
PATH: 'PATH';
PATTERN: 'PATTERN';
PER: 'PER';
PERIOD: 'PERIOD';
PERMUTE: 'PERMUTE';
PLAN : 'PLAN';
POSITION: 'POSITION';
PRECEDING: 'PRECEDING';
PRECISION: 'PRECISION';
PREPARE: 'PREPARE';
PRIVILEGES: 'PRIVILEGES';
PROPERTIES: 'PROPERTIES';
PRUNE: 'PRUNE';
QUOTES: 'QUOTES';
RANGE: 'RANGE';
READ: 'READ';
RECURSIVE: 'RECURSIVE';
REFRESH: 'REFRESH';
RENAME: 'RENAME';
REPEAT: 'REPEAT';
REPEATABLE: 'REPEATABLE';
REPLACE: 'REPLACE';
RESET: 'RESET';
RESPECT: 'RESPECT';
RESTRICT: 'RESTRICT';
RETURN: 'RETURN';
RETURNING: 'RETURNING';
RETURNS: 'RETURNS';
REVOKE: 'REVOKE';
RIGHT: 'RIGHT';
ROLE: 'ROLE';
ROLES: 'ROLES';
ROLLBACK: 'ROLLBACK';
ROLLUP: 'ROLLUP';
ROW: 'ROW';
ROWS: 'ROWS';
RUNNING: 'RUNNING';
SCALAR: 'SCALAR';
SCHEMA: 'SCHEMA';
SCHEMAS: 'SCHEMAS';
SECOND: 'SECOND';
SECURITY: 'SECURITY';
SEEK: 'SEEK';
SELECT: 'SELECT';
SERIALIZABLE: 'SERIALIZABLE';
SESSION: 'SESSION';
SET: 'SET';
SETS: 'SETS';
SHOW: 'SHOW';
SOME: 'SOME';
START: 'START';
STATS: 'STATS';
SUBSET: 'SUBSET';
SUBSTRING: 'SUBSTRING';
SYSTEM: 'SYSTEM';
TABLE: 'TABLE';
TABLES: 'TABLES';
TABLESAMPLE: 'TABLESAMPLE';
TEXT: 'TEXT';
TEXT_STRING: 'STRING';
THEN: 'THEN';
TIES: 'TIES';
TIME: 'TIME';
TIMESTAMP: 'TIMESTAMP';
TO: 'TO';
TRAILING: 'TRAILING';
TRANSACTION: 'TRANSACTION';
TRIM: 'TRIM';
TRUE: 'TRUE';
TRUNCATE: 'TRUNCATE';
TRY_CAST: 'TRY_CAST';
TYPE: 'TYPE';
UESCAPE: 'UESCAPE';
UNBOUNDED: 'UNBOUNDED';
UNCOMMITTED: 'UNCOMMITTED';
UNCONDITIONAL: 'UNCONDITIONAL';
UNION: 'UNION';
UNIQUE: 'UNIQUE';
UNKNOWN: 'UNKNOWN';
UNMATCHED: 'UNMATCHED';
UNNEST: 'UNNEST';
UNTIL: 'UNTIL';
UPDATE: 'UPDATE';
USE: 'USE';
USER: 'USER';
USING: 'USING';
UTF16: 'UTF16';
UTF32: 'UTF32';
UTF8: 'UTF8';
VALIDATE: 'VALIDATE';
VALUE: 'VALUE';
VALUES: 'VALUES';
VERBOSE: 'VERBOSE';
VERSION: 'VERSION';
VIEW: 'VIEW';
WHEN: 'WHEN';
WHERE: 'WHERE';
WHILE: 'WHILE';
WINDOW: 'WINDOW';
WITH: 'WITH';
WITHIN: 'WITHIN';
WITHOUT: 'WITHOUT';
WORK: 'WORK';
WRAPPER: 'WRAPPER';
WRITE: 'WRITE';
YEAR: 'YEAR';
ZONE: 'ZONE';

EQ: '=';
NEQ: '<>' | '!=';
LT: '<';
LTE: '<=';
GT: '>';
GTE: '>=';

PLUS: '+';
MINUS: '-';
ASTERISK: '*';
SLASH: '/';
PERCENT: '%';
CONCAT: '||';
QUESTION_MARK: '?';
SEMICOLON: ';';

STRING
    : '\'' ( ~'\'' | '\'\'' )* '\''
    ;

UNICODE_STRING
    : 'U&\'' ( ~'\'' | '\'\'' )* '\''
    ;

DOLLAR_STRING
    : '$$' .*? '$$'
    ;

// Note: we allow any character inside the binary literal and validate
// its a correct literal when the AST is being constructed. This
// allows us to provide more meaningful error messages to the user
BINARY_LITERAL
    : 'X\'' (~'\'')* '\''
    ;

INTEGER_VALUE
    : DECIMAL_INTEGER
    | HEXADECIMAL_INTEGER
    | OCTAL_INTEGER
    | BINARY_INTEGER
    ;

DECIMAL_VALUE
    : DECIMAL_INTEGER '.' DECIMAL_INTEGER?
    | '.' DECIMAL_INTEGER
    ;

DOUBLE_VALUE
    : DIGIT+ ('.' DIGIT*)? EXPONENT
    | '.' DIGIT+ EXPONENT
    ;

IDENTIFIER
    : (LETTER | '_') (LETTER | DIGIT | '_')*
    ;

DIGIT_IDENTIFIER
    : DIGIT (LETTER | DIGIT | '_')+
    ;

QUOTED_IDENTIFIER
    : '"' ( ~'"' | '""' )* '"'
    ;

BACKQUOTED_IDENTIFIER
    : '`' ( ~'`' | '``' )* '`'
    ;

fragment DECIMAL_INTEGER
    : DIGIT ('_'? DIGIT)*
    ;

fragment HEXADECIMAL_INTEGER
    : '0X' ('_'? (DIGIT | [A-F]))+
    ;

fragment OCTAL_INTEGER
    : '0O' ('_'? [0-7])+
    ;

fragment BINARY_INTEGER
    : '0B' ('_'? [01])+
    ;

fragment EXPONENT
    : 'E' [+-]? DIGIT+
    ;

fragment DIGIT
    : [0-9]
    ;

fragment LETTER
    : [A-Z]
    ;

SIMPLE_COMMENT
    : '--' ~[\r\n]* '\r'? '\n'? -> channel(HIDDEN)
    ;

BRACKETED_COMMENT
    : '/*' .*? '*/' -> channel(HIDDEN)
    ;

WS
    : [ \r\n\t]+ -> channel(HIDDEN)
    ;

// Catch-all for anything we can't recognize.
// We use this to be able to ignore and recover all the text
// when splitting statements with DelimiterLexer
UNRECOGNIZED
    : .
    ;
