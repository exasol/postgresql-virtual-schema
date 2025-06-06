package com.exasol.adapter.dialects.postgresql;

import static com.exasol.matcher.ResultSetStructureMatcher.table;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assumptions.assumeFalse;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.sql.*;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Collections;
import java.util.Map;

import org.hamcrest.Matcher;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import com.exasol.closeafterall.CloseAfterAll;
import com.exasol.closeafterall.CloseAfterAllExtension;
import com.exasol.containers.ExasolDockerImageReference;
import com.exasol.dbbuilder.dialects.DatabaseObjectException;
import com.exasol.dbbuilder.dialects.exasol.VirtualSchema;
import com.exasol.matcher.TypeMatchMode;

@Tag("integration")
@ExtendWith({ CloseAfterAllExtension.class })
class PostgreSQLSqlDialectIT {
    @CloseAfterAll
    private static final PostgresVirtualSchemaIntegrationTestSetup SETUP = new PostgresVirtualSchemaIntegrationTestSetup();
    private static final String SCHEMA_POSTGRES = "schema_postgres";
    private static final String SCHEMA_POSTGRES_UPPERCASE_TABLE = "schema_postgres_upper";
    private static final String TABLE_POSTGRES_SIMPLE = "table_postgres_simple";
    private static final String TABLE_POSTGRES_MIXED_CASE = "Table_Postgres_Mixed_Case";
    private static final String TABLE_POSTGRES_LOWER_CASE = "table_postgres_lower_case";
    private static final String TABLE_POSTGRES_ALL_DATA_TYPES = "table_postgres_all_data_types";
    private static VirtualSchema virtualSchemaPostgres;
    private static VirtualSchema virtualSchemaPostgresUppercaseTable;
    private static final String TABLE_JOIN_1 = "TABLE_JOIN_1";
    private static final String TABLE_JOIN_2 = "TABLE_JOIN_2";
    private static VirtualSchema virtualSchemaPostgresPreserveOriginalCase;
    private static String qualifiedTableJoinName1;
    private static String qualifiedTableJoinName2;
    private static Statement statementExasol;

    @BeforeAll
    static void beforeAll() throws SQLException {
        final Statement statementPostgres = SETUP.getPostgresqlStatement();
        statementPostgres.execute("CREATE SCHEMA " + SCHEMA_POSTGRES);
        statementPostgres.execute("CREATE SCHEMA " + SCHEMA_POSTGRES_UPPERCASE_TABLE);
        createPostgresTestTableSimple(statementPostgres);
        createPostgresTestTableAllDataTypes(statementPostgres);
        createPostgresTestTableMixedCase(statementPostgres);
        createPostgresTestTableLowerCase(statementPostgres);
        createTestTablesForJoinTests(SCHEMA_POSTGRES);
        statementExasol = SETUP.getExasolStatement();
        virtualSchemaPostgres = SETUP.createVirtualSchema(SCHEMA_POSTGRES, Map.of());
        virtualSchemaPostgresUppercaseTable = SETUP.createVirtualSchema(SCHEMA_POSTGRES_UPPERCASE_TABLE,
                Map.of("IGNORE_ERRORS", "POSTGRESQL_UPPERCASE_TABLES"));
        virtualSchemaPostgresPreserveOriginalCase = SETUP.createVirtualSchema(SCHEMA_POSTGRES_UPPERCASE_TABLE,
                Map.of("POSTGRESQL_IDENTIFIER_MAPPING", "PRESERVE_ORIGINAL_CASE"));
        qualifiedTableJoinName1 = virtualSchemaPostgres.getName() + "." + TABLE_JOIN_1;
        qualifiedTableJoinName2 = virtualSchemaPostgres.getName() + "." + TABLE_JOIN_2;
    }

    private static void createPostgresTestTableSimple(final Statement statementPostgres) throws SQLException {
        final String qualifiedTableName = SCHEMA_POSTGRES + "." + TABLE_POSTGRES_SIMPLE;
        statementPostgres.execute("CREATE TABLE " + qualifiedTableName + " (x INT)");
        statementPostgres.execute("INSERT INTO " + qualifiedTableName + " VALUES (1)");
    }

    private static void createPostgresTestTableAllDataTypes(final Statement statementPostgres) throws SQLException {
        final String qualifiedTableName = SCHEMA_POSTGRES + "." + TABLE_POSTGRES_ALL_DATA_TYPES;
        statementPostgres.execute("CREATE TABLE " + qualifiedTableName + " (" //
                + "myBigint BIGINT,	" //
                + "myBigserial BIGSERIAL, " //
                + "myBit BIT, " //
                + "myBitVar BIT VARYING(5), " //
                + "myBoolean BOOLEAN, " //
                + "myBox BOX, " //
                + "myBytea BYTEA, " //
                + "myCharacter CHARACTER(1000), " //
                + "myCharacterVar CHARACTER VARYING(1000), " //
                + "myCidr CIDR, " //
                + "myCircle CIRCLE, " //
                + "myDate DATE, " //
                + "myDouble DOUBLE PRECISION, " //
                + "myInet INET, " //
                + "myInteger INTEGER, " //
                + "myInterval INTERVAL, " //
                + "myJson JSON, " //
                + "myJsonB JSONB, " //
                + "myLine LINE, " //
                + "myLseg LSEG,	" //
                + "myMacAddr MACADDR, " //
                + "myMoney MONEY, " //
                + "myNumeric NUMERIC(36, 10), " //
                + "myPath PATH, " //
                + "myPoint POINT, " //
                + "myPolygon POLYGON, " //
                + "myReal REAL, " //
                + "mySmallint SMALLINT, " //
                + "myText TEXT, " //
                + "myTime TIME, " //
                + "myTimeWithTimeZone TIME WITH TIME ZONE, " //
                + "myTimestamp TIMESTAMP, " //
                + "myTimestamp0 TIMESTAMP(0), " //
                + "myTimestamp3 TIMESTAMP(3), " //
                + "myTimestamp6 TIMESTAMP(6), " //
                + "myTimestampWithTimeZone TIMESTAMP WITH TIME ZONE, " //
                + "myTsquery TSQUERY, " //
                + "myTsvector TSVECTOR, " //
                + "myUuid UUID, " //
                + "myXml XML " //
                + ")");
        statementPostgres.execute("INSERT INTO " + qualifiedTableName + " VALUES (" //
                + "10000000000, " // myBigint
                + "nextval('" + qualifiedTableName + "_myBigserial_seq'::regclass), " // myBigserial
                + "B'1', " // myBit
                + "B'0', " // myBitVar
                + "false, " // myBoolean
                + "'( ( 1 , 8 ) , ( 4 , 16 ) )', " // myBox
                + "E'\\\\000'::bytea, " // myBytea
                + "'hajksdf', " // myCharacter
                + "'hjkdhjgfh', " // myCharacterVar
                + "'192.168.100.128/25'::cidr, " // myCidr
                + "'( ( 1 , 5 ) , 3 )'::circle, " // myCircle
                + "'2010-01-01', " // myDate
                + "192189234.1723854, " // myDouble
                + "'192.168.100.128'::inet, " // myInet
                + "7189234, " // myInteger
                + "INTERVAL '1' YEAR, " // myInterval
                + "'{\"bar\": \"baz\", \"balance\": 7.77, \"active\": false}'::json, " // myJson
                + "'{\"bar\": \"baz\", \"balance\": 7.77, \"active\": false}'::jsonb, " // myJsonB
                + "'{ 1, 2, 3 }'::line, " // myLine
                + "'[ ( 1 , 2 ) , ( 3 , 4 ) ]'::lseg, " // myLseg
                + "'08:00:2b:01:02:03'::macaddr, " // myMacAddr
                + "100.01, " // myMoney
                + "24.23, " // myNumeric
                + "'[ ( 1 , 2 ) , ( 3 , 4 ) ]'::path, " // myPath
                + "'( 1 , 3 )'::point, " // myPoint
                + "'( ( 1 , 2 ) , (2,4),(3,7) )'::polygon, " // myPolygon
                + "10.12, " // myReal
                + "100, " // mySmallint
                + "'This cat is super cute', " // myText
                + "'11:11:11', " // myTime
                + "'11:11:11 +01:00', " // myTimeWithTimeZone
                + "'2010-01-01 11:11:11', " // myTimestamp
                + "'2010-01-01 11:11:11', " // myTimestamp0
                + "'2010-01-01 11:11:11.123', " // myTimestamp3
                + "'2010-01-01 11:11:11.123456', " // myTimestamp6
                + "'2010-01-01 11:11:11 +01:00', " // myTimestampwithtimezone
                + "'fat & rat'::tsquery, " // myTsquery
                + "to_tsvector('english', 'The Fat Rats'), " // myTsvector
                + "'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11'::uuid, " // myUuid
                + "XMLPARSE (DOCUMENT '<?xml version=\"1.0\"?><book><title>Manual</title><chapter>...</chapter></book>') " // myXml
                + ")");
    }

    private static void createPostgresTestTableMixedCase(final Statement statementPostgres) throws SQLException {
        final String qualifiedTableName = SCHEMA_POSTGRES_UPPERCASE_TABLE + ".\"" + TABLE_POSTGRES_MIXED_CASE + "\"";
        statementPostgres.execute("CREATE TABLE " + qualifiedTableName + " (x INT, \"Y\" INT)");
    }

    private static void createPostgresTestTableLowerCase(final Statement statementPostgres) throws SQLException {
        final String qualifiedTableName = SCHEMA_POSTGRES_UPPERCASE_TABLE + "." + TABLE_POSTGRES_LOWER_CASE;
        statementPostgres.execute("CREATE TABLE " + qualifiedTableName + " (x INT, y INT)");
    }

    private static void createTestTablesForJoinTests(final String schemaName) throws SQLException {
        final Statement statement = SETUP.getPostgresqlStatement();
        statement.execute("CREATE TABLE " + schemaName + "." + TABLE_JOIN_1 + "(x INT, y VARCHAR(100))");
        statement.execute("INSERT INTO " + schemaName + "." + TABLE_JOIN_1 + " VALUES (1,'aaa')");
        statement.execute("INSERT INTO " + schemaName + "." + TABLE_JOIN_1 + " VALUES (2,'bbb')");
        statement.execute("CREATE TABLE " + schemaName + "." + TABLE_JOIN_2 + "(x INT, y VARCHAR(100))");
        statement.execute("INSERT INTO " + schemaName + "." + TABLE_JOIN_2 + " VALUES (2,'bbb')");
        statement.execute("INSERT INTO " + schemaName + "." + TABLE_JOIN_2 + " VALUES (3,'ccc')");
    }

    @Test
    void testSelectSingleColumn() {
        assertResult("SELECT * FROM " + virtualSchemaPostgres.getName() + "." + TABLE_POSTGRES_SIMPLE,
                table().row(1).matches(TypeMatchMode.NO_JAVA_TYPE_CHECK));
    }

    @Test
    void testInnerJoin() {
        final String query = "SELECT * FROM " + qualifiedTableJoinName1 + " a INNER JOIN  "
                + qualifiedTableJoinName2 + " b ON a.x=b.x";
        assertResult(query,
                table("BIGINT", "VARCHAR", "BIGINT", "VARCHAR").row(2L, "bbb", 2L, "bbb").matches());
    }

    private void assertResult(final String query, final Matcher<ResultSet> matcher) {
        try (ResultSet resultSet = getActualResultSet(query)) {
            assertThat(resultSet, matcher);
        } catch (final SQLException exception) {
            throw new IllegalStateException(String.format("Failed to execute query '%s'", query));
        }
    }

    private void assertEmptyResult(final String query) {
        try (ResultSet resultSet = getActualResultSet(query)) {
            assertThat(resultSet.next(), is(false));
        } catch (final SQLException exception) {
            throw new IllegalStateException(String.format("Failed to execute query '%s'", query));
        }
    }

    private ResultSet getActualResultSet(final String query) throws SQLException {
        return statementExasol.executeQuery(query);
    }

    @Test
    void testInnerJoinWithProjection() {
        final String query = "SELECT b.y || " + qualifiedTableJoinName1 + ".y FROM " + qualifiedTableJoinName1
                + " INNER JOIN  " + qualifiedTableJoinName2 + " b ON " + qualifiedTableJoinName1 + ".x=b.x";
        assertResult(query, table("VARCHAR").row("bbbbbb").matches());
    }

    @Test
    void testLeftJoin() {
        final String query = "SELECT * FROM " + qualifiedTableJoinName1 + " a LEFT OUTER JOIN  "
                + qualifiedTableJoinName2 + " b ON a.x=b.x ORDER BY a.x";
        assertResult(query, table("BIGINT", "VARCHAR", "BIGINT", "VARCHAR").row(1L, "aaa", null, null)
                .row(2L, "bbb", 2L, "bbb").matches());
    }

    @Test
    void testRightJoin() {
        final String query = "SELECT * FROM " + qualifiedTableJoinName1 + " a RIGHT OUTER JOIN  "
                + qualifiedTableJoinName2 + " b ON a.x=b.x ORDER BY a.x";
        assertResult(query, table("BIGINT", "VARCHAR", "BIGINT", "VARCHAR").row(2L, "bbb", 2L, "bbb")
                .row(null, null, 3L, "ccc").matches());
    }

    @Test
    void testFullOuterJoin() {
        final String query = "SELECT * FROM " + qualifiedTableJoinName1 + " a FULL OUTER JOIN  "
                + qualifiedTableJoinName2 + " b ON a.x=b.x ORDER BY a.x";
        assertResult(query, table("BIGINT", "VARCHAR", "BIGINT", "VARCHAR")
                .row(1L, "aaa", null, null)
                .row(2L, "bbb", 2L, "bbb")
                .row(null, null, 3L, "ccc").matches());
    }

    @Test
    void testRightJoinWithComplexCondition() {
        final String query = "SELECT * FROM " + qualifiedTableJoinName1 + " a RIGHT OUTER JOIN  "
                + qualifiedTableJoinName2 + " b ON a.x||a.y=b.x||b.y ORDER BY a.x";
        assertResult(query, table("BIGINT", "VARCHAR", "BIGINT", "VARCHAR")
                .row(2L, "bbb", 2L, "bbb")
                .row(null, null, 3L, "ccc").matches());
    }

    @Test
    void testFullOuterJoinWithComplexCondition() {
        final String query = "SELECT * FROM " + qualifiedTableJoinName1 + " a FULL OUTER JOIN  "
                + qualifiedTableJoinName2 + " b ON a.x-b.x=0 ORDER BY a.x";
        assertResult(query, table("BIGINT", "VARCHAR", "BIGINT", "VARCHAR")
                .row(1L, "aaa", null, null)
                .row(2L, "bbb", 2L, "bbb")
                .row(null, null, 3L, "ccc").matches());
    }

    @Test
    void testYearScalarFunctionFromTimeStamp() {
        final String query = "SELECT year(\"MYTIMESTAMP\") FROM " + virtualSchemaPostgres.getName() + "."
                + TABLE_POSTGRES_ALL_DATA_TYPES;
        assertResult(query, table().row((short) 2010).matches());
    }

    @Test
    void testYearScalarFunctionFromDate() {
        final String query = "SELECT year(\"MYDATE\") FROM " + virtualSchemaPostgres.getName() + "."
                + TABLE_POSTGRES_ALL_DATA_TYPES;
        assertResult(query, table().row((short) 2010).matches());
    }

    @Test
    void testCurrentSchemaScalarFunction() {
        final String query = " SELECT current_schema FROM " + virtualSchemaPostgres.getName() + "."
                + TABLE_POSTGRES_ALL_DATA_TYPES;
        assertResult(query, table().row("public").matches());
    }

    @Test
    void testFloatDivFunction() {
        final String query = "SELECT MYINTEGER / MYINTEGER FROM " + virtualSchemaPostgres.getName() + "."
                + TABLE_POSTGRES_ALL_DATA_TYPES;
        assertResult(query, table("DOUBLE PRECISION").row(1.0).matches());
    }

    @Test
    void testCountAll() {
        final String qualifiedExpectedTableName = virtualSchemaPostgres.getName() + "." + TABLE_POSTGRES_SIMPLE;
        final String query = "SELECT COUNT(*) FROM " + qualifiedExpectedTableName;
        assertResult(query, table("BIGINT").row(1L).matches());
    }

    @Test
    void testCreateSchemaWithUpperCaseTablesThrowsException() {
        final Exception exception = assertThrows(DatabaseObjectException.class,
                () -> SETUP.createVirtualSchema(SCHEMA_POSTGRES_UPPERCASE_TABLE, Map.of()));
        assertThat(exception.getMessage(), containsString("Failed to write to object"));
    }

    @Test
    void testQueryUpperCaseTableQuotedThrowsException() {
        final String selectStatement = "SELECT x FROM  " + virtualSchemaPostgresUppercaseTable.getName() + ".\""
                + TABLE_POSTGRES_MIXED_CASE + "\"";
        final Exception exception = assertThrows(SQLException.class, () -> statementExasol.execute(selectStatement));
        assertThat(exception.getMessage(), containsString(".\"" + TABLE_POSTGRES_MIXED_CASE + "\" not found"));
    }

    @Test
    void testQueryUpperCaseTableThrowsException() {
        final Exception exception = assertThrows(SQLException.class, () -> statementExasol.execute(
                "SELECT x FROM  " + virtualSchemaPostgresUppercaseTable.getName() + "." + TABLE_POSTGRES_MIXED_CASE));
        assertThat(exception.getMessage(), containsString("object " + virtualSchemaPostgresUppercaseTable.getName()
                + "." + TABLE_POSTGRES_MIXED_CASE.toUpperCase() + " not found"));
    }

    @Test
    void testQueryLowerCaseTable() {
        assertEmptyResult(
                "SELECT x FROM " + virtualSchemaPostgresUppercaseTable.getName() + "." + TABLE_POSTGRES_LOWER_CASE);
    }

    @Test
    void testUnsetIgnoreUpperCaseTablesAndRefreshThrowsException() throws SQLException {
        statementExasol.execute(
                "ALTER VIRTUAL SCHEMA " + virtualSchemaPostgresUppercaseTable.getName() + " set ignore_errors=''");
        statementExasol.execute("ALTER VIRTUAL SCHEMA " + virtualSchemaPostgresUppercaseTable.getName()
                + " set POSTGRESQL_IDENTIFIER_MAPPING = 'CONVERT_TO_UPPER'");
        final Exception exception = assertThrows(SQLException.class, () -> statementExasol
                .execute("ALTER VIRTUAL SCHEMA " + virtualSchemaPostgresUppercaseTable.getName() + " REFRESH"));
        assertThat(exception.getMessage(), containsString("E-VSPG-6: Table '" + TABLE_POSTGRES_MIXED_CASE
                + "' cannot be used in virtual schema. Set property 'IGNORE_ERRORS' to 'POSTGRESQL_UPPERCASE_TABLES' to enforce schema creation."));
    }

    @Test
    void testSetIgnoreUpperCaseTablesAndRefresh() throws SQLException {
        statementExasol.execute("ALTER VIRTUAL SCHEMA " + virtualSchemaPostgresUppercaseTable.getName()
                + " set ignore_errors='POSTGRESQL_UPPERCASE_TABLES'");
        final String refresh_schema_query = "ALTER VIRTUAL SCHEMA " + virtualSchemaPostgresUppercaseTable.getName()
                + " REFRESH";
        assertDoesNotThrow(() -> statementExasol.execute(refresh_schema_query));
    }

    @Test
    void testPreserveCaseQueryLowerCaseTableThrowsException() {
        final SQLException exception = assertThrows(SQLException.class,
                () -> statementExasol.executeQuery("SELECT x FROM  "
                        + virtualSchemaPostgresPreserveOriginalCase.getName() + "." + TABLE_POSTGRES_LOWER_CASE));
        assertThat(exception.getMessage(),
                containsString("object " + virtualSchemaPostgresPreserveOriginalCase.getName() + "."
                        + TABLE_POSTGRES_LOWER_CASE.toUpperCase() + " not found"));
    }

    @Test
    void testPreserveCaseQueryLowerCaseTableWithQuotes() {
        assertEmptyResult("SELECT \"x\" FROM  "
                + virtualSchemaPostgresPreserveOriginalCase.getName() + ".\"" + TABLE_POSTGRES_LOWER_CASE + "\"");
    }

    @Test
    void testPreserveCaseQueryUpperCaseTableWithQuotes() {
        assertEmptyResult("SELECT \"Y\" FROM  "
                + virtualSchemaPostgresPreserveOriginalCase.getName() + ".\"" + TABLE_POSTGRES_MIXED_CASE + "\"");
    }

    @Test
    void testDatatypeBigint() {
        assertSingleValue("myBigint", "DECIMAL(19,0)", "10000000000");
    }

    @Test
    void testPreserveCaseQueryUpperCaseTableWithQuotesLowerCaseColumn() {
        assertEmptyResult("SELECT \"x\" FROM  "
                + virtualSchemaPostgresPreserveOriginalCase.getName() + ".\"" + TABLE_POSTGRES_MIXED_CASE + "\"");
    }

    @Test
    void testDatatypeBigSerial() {
        assertSingleValue("myBigserial", "DECIMAL(19,0)", "1");
    }

    @Test
    void testDatatypeBit() {
        assertSingleValue("myBit", "BOOLEAN", true);
    }

    @Test
    void testDatatypeBitVar() {
        assertSingleValue("myBitvar", "VARCHAR(5) UTF8", "0");
    }

    @Test
    void testDatatypeBoolean() {
        assertSingleValue("myBoolean", "BOOLEAN", false);
    }

    @Test
    void testDatatypeBox() {
        assertSingleValue("myBox", "VARCHAR(2000000) UTF8", "(4,16),(1,8)");
    }

    @Test
    void testDatatypeBytea() {
        assertSingleValue("myBytea", "VARCHAR(2000000) UTF8", "bytea NOT SUPPORTED");
    }

    @Test
    void testDatatypeCharacter() {
        final String empty = " ";
        final String expected = "hajksdf" + String.join("", Collections.nCopies(993, empty));
        assertSingleValue("myCharacter", "CHAR(1000) UTF8", expected);
    }

    @Test
    void testDatatypeCharacterVar() {
        assertSingleValue("myCharactervar", "VARCHAR(1000) UTF8", "hjkdhjgfh");
    }

    @Test
    void testDatatypeCidr() {
        assertSingleValue("myCidr", "VARCHAR(2000000) UTF8", "192.168.100.128/25");
    }

    @Test
    void testDatatypeCircle() {
        assertSingleValue("myCircle", "VARCHAR(2000000) UTF8", "<(1,5),3>");
    }

    @Test
    void testDatatypeDate() throws ParseException {
        final java.util.Date expectedDate = new SimpleDateFormat("yyyy-MM-dd").parse("2010-01-01");
        assertSingleValue("myDate", "DATE", expectedDate);
    }

    @Test
    void testDatatypeDouble() {
        assertSingleValue("myDouble", "DOUBLE", "192189234.1723854");
    }

    @Test
    void testDatatypeInet() {
        assertSingleValue("myInet", "VARCHAR(2000000) UTF8", "192.168.100.128/32");
    }

    @Test
    void testDatatypeInteger() {
        assertSingleValue("myInteger", "DECIMAL(10,0)", "7189234");
    }

    @Test
    void testDatatypeIntervalYM() {
        assertSingleValue("myInterval", "VARCHAR(2000000) UTF8", "1 year");
    }

    @Test
    void testDatatypeJSON() {
        assertSingleValue("myJson", "VARCHAR(2000000) UTF8",
                "{\"bar\": \"baz\", \"balance\": 7.77, \"active\": false}");
    }

    @Test
    void testDatatypeJSONB() {
        assertSingleValue("myJsonb", "VARCHAR(2000000) UTF8",
                "{\"bar\": \"baz\", \"active\": false, \"balance\": 7.77}");
    }

    @Test
    void testDatatypeLine() {
        assertSingleValue("myLine", "VARCHAR(2000000) UTF8", "{1,2,3}");
    }

    @Test
    void testDatatypeLSeg() {
        assertSingleValue("myLseg", "VARCHAR(2000000) UTF8", "[(1,2),(3,4)]");
    }

    @Test
    void testDatatypeMACAddr() {
        assertSingleValue("myMacaddr", "VARCHAR(2000000) UTF8", "08:00:2b:01:02:03");
    }

    @Test
    void testDatatypeMoney() {
        assertSingleValue("myMoney", "DOUBLE", 100.01);
    }

    @Test
    void testDatatypeNumeric() {
        assertSingleValue("myNumeric", "VARCHAR(2000000) UTF8", 24.2300000000);
    }

    @Test
    void testDatatypePath() {
        assertSingleValue("myPath", "VARCHAR(2000000) UTF8", "[(1,2),(3,4)]");
    }

    @Test
    void testDatatypePoint() {
        assertSingleValue("myPoint", "VARCHAR(2000000) UTF8", "(1,3)");
    }

    @Test
    void testDatatypePolygon() {
        assertSingleValue("myPolygon", "VARCHAR(2000000) UTF8", "((1,2),(2,4),(3,7))");
    }

    @Test
    void testDatatypeReal() {
        assertSingleValue("myReal", "DOUBLE", 10.12);
    }

    @Test
    void testDatatypeSmallInt() {
        assertSingleValue("mySmallint", "DECIMAL(5,0)", 100);
    }

    @Test
    void testDatatypeText() {
        assertSingleValue("myText", "VARCHAR(2000000) UTF8", "This cat is super cute");
    }

    @Test
    void testDatatypeTime() {
        assertSingleValue("myTime", "VARCHAR(2000000) UTF8", "1970-01-01 11:11:11.0");
    }

    @Test
    void testDatatypeTimeWithTimezone() {
        assertSingleValue("myTimeWithTimeZone", "VARCHAR(2000000) UTF8", "1970-01-01 11:11:11.0");
    }

    @ParameterizedTest
    @CsvSource({
            "myTimestamp, TIMESTAMP, 2010-01-01 11:11:11",
            "myTimestamp0, TIMESTAMP, 2010-01-01 11:11:11",
            "myTimestamp3, TIMESTAMP, 2010-01-01 11:11:11.123",
            "myTimestampwithtimezone, TIMESTAMP, 2010-01-01 11:11:11",
    })
    void testDatatypeTimestamp(final String column, final String expectedType, final String expectedTimestamp) {
        assertSingleValue(column, expectedType, Timestamp.valueOf(expectedTimestamp));
    }

    @Test
    void testDatatypeTimestampWithPrecision6() {
        assumeTrue(supportTimestampPrecision());
        assertSingleValue("myTimestamp6", "TIMESTAMP", Timestamp.valueOf("2010-01-01 11:11:11.123456"));
    }

    @Test
    void testDatatypeTimestampWithoutPrecision6() {
        assumeFalse(supportTimestampPrecision());
        assertSingleValue("myTimestamp6", "TIMESTAMP", Timestamp.valueOf("2010-01-01 11:11:11.123"));
    }

    @Test
    void testDatatypeTsQuery() {
        assertSingleValue("myTsquery", "VARCHAR(2000000) UTF8", "'fat' & 'rat'");
    }

    @Test
    void testDatatypeTsvector() {
        assertSingleValue("myTsvector", "VARCHAR(2000000) UTF8", "'fat':2 'rat':3");
    }

    @Test
    void testDatatypeUUID() {
        assertSingleValue("myUuid", "VARCHAR(2000000) UTF8", "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11");
    }

    @Test
    void testDatatypeXML() {
        assertSingleValue("myXml", "VARCHAR(2000000) UTF8",
                "<?xml version=\"1.0\"?><book><title>Manual</title><chapter>...</chapter></book>");
    }

    private void assertSingleValue(final String columnName, final String expectedColumnType,
            final Object expectedValue) {
        final String query = "SELECT " + columnName + " FROM "
                + virtualSchemaPostgres.getName() + "." + TABLE_POSTGRES_ALL_DATA_TYPES;
        assertResult(query, table().row(expectedValue).matches(TypeMatchMode.NO_JAVA_TYPE_CHECK));
    }

    private boolean supportTimestampPrecision() {
        final ExasolDockerImageReference reference = SETUP.getExasolContainer().getDockerImageReference();
        return reference.getMajor() > 8 || (reference.getMajor() == 8 && reference.getMinor() >= 32);
    }
}
