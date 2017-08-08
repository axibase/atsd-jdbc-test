package com.axibase.tsd.driver.jdbc.metadata;

import com.axibase.tsd.driver.jdbc.data.AbstractDataTest;
import lombok.extern.slf4j.Slf4j;
import org.hamcrest.core.Is;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.sql.DatabaseMetaData;
import java.sql.SQLException;

import static org.hamcrest.MatcherAssert.assertThat;

@Slf4j
public class AtsdDatabaseMetaDataTest extends AbstractDataTest {
    private static DatabaseMetaData meta;
    
    @Rule
    public final ExpectedException expectedException = ExpectedException.none();

    @BeforeClass
    public static void prepareMetadata() throws SQLException {
        meta = connection.getMetaData();
    }
    
    @Test
    public void supportsTransactions() throws SQLException {
        final boolean supportsTransactions = meta.supportsTransactions();
        log.debug("[Supports Transactions] {}", supportsTransactions);
        assertThat(supportsTransactions, Is.is(false));
    }

    @Test
    public void getSchemasString() throws SQLException {
        log.debug("[Schemas String] " + meta.getSchemas(null, ""));
    }

    @Test
    public void getSchemas() throws SQLException {
        log.debug("[Schemas] " + meta.getSchemas());
    }

    @Test
    public void getCatalogs() throws SQLException {
        log.debug("[Catalogs] " + meta.getCatalogs());
    }

    @Test
    public void getTableTypes() throws SQLException {
        log.debug("[Table Types] " + meta.getTableTypes());
    }

    @Test
    public void getTypeInfo() throws SQLException {
        log.debug("Type Info] " + meta.getTypeInfo());
    }

    @Test
    public void getSQLKeywords() throws SQLException {
        log.debug("[SQL Keywords] " + meta.getSQLKeywords());
    }

    @Test
    public void getNumericFunctions() throws SQLException {
        log.debug(meta.getNumericFunctions());
    }

    @Test
    public void getStringFunctions() throws SQLException {
        log.debug(meta.getStringFunctions());
    }

    @Test
    public void getSystemFunctions() throws SQLException {
        log.debug(meta.getSystemFunctions());
    }

    @Test
    public void getTimeDateFunctions() throws SQLException {
        log.debug("[Time Date Functions] " + meta.getTimeDateFunctions());
    }

    @Test
    public void getDriverVersionProperties() throws SQLException {
        String driverVersion = meta.getDriverVersion();
        int driverMajorVersion = meta.getDriverMajorVersion();
        int driverMinorVersion = meta.getDriverMinorVersion();

//        assertFalse(driverMajorVersion == 1 && driverMinorVersion == 0);

        log.debug("[Driver Version] " + driverVersion);
        log.debug("[Driver Major Version] " + driverMajorVersion);
        log.debug("[Driver Minor Version] " + driverMinorVersion);
    }

    @Test
    public void getCommonProperties() throws SQLException {
        log.debug("[Database Product Name] " + meta.getDatabaseProductName());
        log.debug("[Database Product Version] " + meta.getDatabaseProductVersion());
        log.debug("[Driver Name] " + meta.getDriverName());
        log.debug("[All Procedures Are Callable] " + meta.allProceduresAreCallable());
        log.debug("[All Tables Are Selectable] " + meta.allTablesAreSelectable());
        log.debug("[Auto Commit Failure Closes All Result Sets] " + meta.autoCommitFailureClosesAllResultSets());
        log.debug("[Catalog Separator] " + meta.getCatalogSeparator());
        log.debug("[Catalog Term] " + meta.getCatalogTerm());
        log.debug("[Database Major Version] " + meta.getDatabaseMajorVersion());
        log.debug("[Database Minor Version] " + meta.getDatabaseMinorVersion());
        log.debug("[Data Definition Causes Transaction Commit] " + meta.dataDefinitionCausesTransactionCommit());
        log.debug("[Data Definition Ignored In Transactions] " + meta.dataDefinitionIgnoredInTransactions());
        log.debug("[Default Transaction Isolation] " + meta.getDefaultTransactionIsolation());
        log.debug("[Does Max Row Size Include Blobs] " + meta.doesMaxRowSizeIncludeBlobs());
        log.debug("[Extra Name Characters] " + meta.getExtraNameCharacters());
        log.debug("[Generated Key Always Returned] " + meta.generatedKeyAlwaysReturned());
        log.debug("[Identifier Quote String] " + meta.getIdentifierQuoteString());
        log.debug("[Is Catalog At Start] " + meta.isCatalogAtStart());
        log.debug("[Is Read Only] " + meta.isReadOnly());
        log.debug("[JDBCMajor Version] " + meta.getJDBCMajorVersion());
        log.debug("[JDBCMinor Version] " + meta.getJDBCMinorVersion());
        log.debug("[Locators Update Copy] " + meta.locatorsUpdateCopy());
        log.debug("[Null Plus Non Null Is Null] " + meta.nullPlusNonNullIsNull());
        log.debug("[Nulls Are Sorted At End] " + meta.nullsAreSortedAtEnd());
        log.debug("[Nulls Are Sorted At Start] " + meta.nullsAreSortedAtStart());
        log.debug("[Nulls Are Sorted High] " + meta.nullsAreSortedHigh());
        log.debug("[Nulls Are Sorted Low] " + meta.nullsAreSortedLow());
        log.debug("[Procedure Term] " + meta.getProcedureTerm());
        log.debug("[Result Set Holdability] " + meta.getResultSetHoldability());
        log.debug("[Schema Term] " + meta.getSchemaTerm());
        log.debug("[Search String Escape] " + meta.getSearchStringEscape());
        log.debug("[SQL State Type] " + meta.getSQLStateType());
        log.debug("[Stores Lower Case Identifiers] " + meta.storesLowerCaseIdentifiers());
        log.debug("[Stores Lower Case Quoted Identifiers] " + meta.storesLowerCaseQuotedIdentifiers());
        log.debug("[Stores Mixed Case Identifiers] " + meta.storesMixedCaseIdentifiers());
        log.debug("[Stores Mixed Case Quoted Identifiers] " + meta.storesMixedCaseQuotedIdentifiers());
        log.debug("[Stores Upper Case Identifiers] " + meta.storesUpperCaseIdentifiers());
        log.debug("[Stores Upper Case Quoted Identifiers] " + meta.storesUpperCaseQuotedIdentifiers());
        log.debug("[URL] " + meta.getURL());
        log.debug("[User Name] " + meta.getUserName());
        log.debug("[Uses Local File Per Table] " + meta.usesLocalFilePerTable());
        log.debug("[Uses Local Files] " + meta.usesLocalFiles());
    }

    @Test
    public void getMaxProperties() throws SQLException {
        {
            log.debug("[Max Binary Literal Length] " + meta.getMaxBinaryLiteralLength());
            log.debug("[Max Catalog Name Length] " + meta.getMaxCatalogNameLength());
            log.debug("[Max Char Literal Length] " + meta.getMaxCharLiteralLength());
            log.debug("[Max Column Name Length] " + meta.getMaxColumnNameLength());
            log.debug("[Max Columns In Group By] " + meta.getMaxColumnsInGroupBy());
            log.debug("[Max Columns In Index] " + meta.getMaxColumnsInIndex());
            log.debug("[Max Columns In Order By] " + meta.getMaxColumnsInOrderBy());
            log.debug("[Max Columns In Select] " + meta.getMaxColumnsInSelect());
            log.debug("[Max Columns In Table] " + meta.getMaxColumnsInTable());
            log.debug("[Max Connections] " + meta.getMaxConnections());
            log.debug("[Max Cursor Name Length] " + meta.getMaxCursorNameLength());
            log.debug("[Max Index Length] " + meta.getMaxIndexLength());
            // logger.debug("[Max Logical Lob Size] " +
            // meta.getMaxLogicalLobSize());
            log.debug("[Max Procedure Name Length] " + meta.getMaxProcedureNameLength());
            log.debug("[Max Row Size] " + meta.getMaxRowSize());
            log.debug("[Max Schema Name Length] " + meta.getMaxSchemaNameLength());
            log.debug("[Max Statement Length] " + meta.getMaxStatementLength());
            log.debug("[Max Statements] " + meta.getMaxStatements());
            log.debug("[Max Table Name Length] " + meta.getMaxTableNameLength());
            log.debug("[Max Tables In Select] " + meta.getMaxTablesInSelect());
            log.debug("[Max User Name Length] " + meta.getMaxUserNameLength());
        }
    }

    @Test
    public void getSupportProperties() throws SQLException {
        {
            log.debug("[Supports Alter Table With Add Column] " + meta.supportsAlterTableWithAddColumn());
            log.debug("[Supports Alter Table With Drop Column] " + meta.supportsAlterTableWithDropColumn());
            log.debug("[Supports ANSI92 Entry Level SQL] " + meta.supportsANSI92EntryLevelSQL());
            log.debug("[Supports ANSI92 Full SQL] " + meta.supportsANSI92FullSQL());
            log.debug("[Supports ANSI92 Intermediate SQL] " + meta.supportsANSI92IntermediateSQL());
            log.debug("[Supports Batch Updates] " + meta.supportsBatchUpdates());
            log.debug("[Supports Catalogs In Data Manipulation] " + meta.supportsCatalogsInDataManipulation());
            log.debug("[Supports Catalogs In Index Definitions] " + meta.supportsCatalogsInIndexDefinitions());
            log.debug("[Supports Catalogs In Privilege Definitions] " + meta.supportsCatalogsInPrivilegeDefinitions());
            log.debug("[Supports Catalogs In Procedure Calls] " + meta.supportsCatalogsInProcedureCalls());
            log.debug("[Supports Catalogs In Table Definitions] " + meta.supportsCatalogsInTableDefinitions());
            log.debug("[Supports Column Aliasing] " + meta.supportsColumnAliasing());
            log.debug("[Supports Convert] " + meta.supportsConvert());
            log.debug("[Supports Core SQLGrammar] " + meta.supportsCoreSQLGrammar());
            log.debug("[Supports Correlated Subqueries] " + meta.supportsCorrelatedSubqueries());
            log.debug("[Supports Data Definition And Data Manipulation Transactions] "
                    + meta.supportsDataDefinitionAndDataManipulationTransactions());
            log.debug("[Supports Data Manipulation Transactions Only] " + meta.supportsDataManipulationTransactionsOnly());
            log.debug("[Supports Different Table Correlation Names] " + meta.supportsDifferentTableCorrelationNames());
            log.debug("[Supports Expressions In Order By] " + meta.supportsExpressionsInOrderBy());
            log.debug("[Supports Extended SQLGrammar] " + meta.supportsExtendedSQLGrammar());
            log.debug("[Supports Full Outer Joins] " + meta.supportsFullOuterJoins());
            log.debug("[Supports Get Generated Keys] " + meta.supportsGetGeneratedKeys());
            log.debug("[Supports Group By] " + meta.supportsGroupBy());
            log.debug("[Supports Group By Beyond Select] " + meta.supportsGroupByBeyondSelect());
            log.debug("[Supports Group By Unrelated] " + meta.supportsGroupByUnrelated());
            log.debug("[Supports Integrity Enhancement Facility] " + meta.supportsIntegrityEnhancementFacility());
            log.debug("[Supports Like Escape Clause] " + meta.supportsLikeEscapeClause());
            log.debug("[Supports Limited Outer Joins] " + meta.supportsLimitedOuterJoins());
            log.debug("[Supports Minimum SQLGrammar] " + meta.supportsMinimumSQLGrammar());
            log.debug("[Supports Mixed Case Identifiers] " + meta.supportsMixedCaseIdentifiers());
            log.debug("[Supports Mixed Case Quoted Identifiers] " + meta.supportsMixedCaseQuotedIdentifiers());
            log.debug("[Supports Multiple Open Results] " + meta.supportsMultipleOpenResults());
            log.debug("[Supports Multiple Result Sets] " + meta.supportsMultipleResultSets());
            log.debug("[Supports Multiple Transactions] " + meta.supportsMultipleTransactions());
            log.debug("[Supports Named Parameters] " + meta.supportsNamedParameters());
            log.debug("[Supports Non Nullable Columns] " + meta.supportsNonNullableColumns());
            log.debug("[Supports Open Cursors Across Commit] " + meta.supportsOpenCursorsAcrossCommit());
            log.debug("[Supports Open Cursors Across Rollback] " + meta.supportsOpenCursorsAcrossRollback());
            log.debug("[Supports Open Statements Across Commit] " + meta.supportsOpenStatementsAcrossCommit());
            log.debug("[Supports Open Statements Across Rollback] " + meta.supportsOpenStatementsAcrossRollback());
            log.debug("[Supports Order By Unrelated] " + meta.supportsOrderByUnrelated());
            log.debug("[Supports Outer Joins] " + meta.supportsOuterJoins());
            log.debug("[Supports Positioned Delete] " + meta.supportsPositionedDelete());
            log.debug("[Supports Positioned Update] " + meta.supportsPositionedUpdate());
            // logger.debug("[Supports Ref Cursors] " + meta.supportsRefCursors());
            log.debug("[Supports Savepoints] " + meta.supportsSavepoints());
            log.debug("[Supports Schemas In Data Manipulation] " + meta.supportsSchemasInDataManipulation());
            log.debug("[Supports Schemas In Index Definitions] " + meta.supportsSchemasInIndexDefinitions());
            log.debug("[Supports Schemas In Privilege Definitions] " + meta.supportsSchemasInPrivilegeDefinitions());
            log.debug("[Supports Schemas In Procedure Calls] " + meta.supportsSchemasInProcedureCalls());
            log.debug("[Supports Schemas In Table Definitions] " + meta.supportsSchemasInTableDefinitions());
            log.debug("[Supports Select For Update] " + meta.supportsSelectForUpdate());
            log.debug("[Supports Statement Pooling] " + meta.supportsStatementPooling());
            log.debug("[Supports Stored Functions Using Call Syntax] " + meta.supportsStoredFunctionsUsingCallSyntax());
            log.debug("[Supports Stored Procedures] " + meta.supportsStoredProcedures());
            log.debug("[Supports Subqueries In Comparisons] " + meta.supportsSubqueriesInComparisons());
            log.debug("[Supports Subqueries In Exists] " + meta.supportsSubqueriesInExists());
            log.debug("[Supports Subqueries In Ins] " + meta.supportsSubqueriesInIns());
            log.debug("[Supports Subqueries In Quantifieds] " + meta.supportsSubqueriesInQuantifieds());
            log.debug("[Supports Table Correlation Names] " + meta.supportsTableCorrelationNames());
            log.debug("[Supports Transactions] " + meta.supportsTransactions());
            log.debug("[Supports Union] " + meta.supportsUnion());
            log.debug("[Supports Union All] " + meta.supportsUnionAll());
        }
    }

}
