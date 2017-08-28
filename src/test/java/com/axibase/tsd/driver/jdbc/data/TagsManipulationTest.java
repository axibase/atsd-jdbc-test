package com.axibase.tsd.driver.jdbc.data;

import com.axibase.tsd.driver.jdbc.ext.AtsdPreparedStatement;
import com.axibase.tsd.driver.jdbc.ext.AtsdResultSet;
import com.axibase.tsd.driver.jdbc.ext.AtsdStatement;
import io.qameta.allure.Description;
import io.qameta.allure.Issue;
import io.qameta.allure.Step;
import lombok.SneakyThrows;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import util.TestProperties;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Map;
import java.util.TreeMap;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;

@Issue("4351")
public class TagsManipulationTest extends AbstractDataTest {
    private static final String TAGS_COLUMN = "tags";

    @Rule
    public final ExpectedException expectedException = ExpectedException.none();

    @BeforeClass
    @SneakyThrows(InterruptedException.class)
    public static void prepareData() throws SQLException {
        String query = "INSERT INTO \"m-with-tags\" (value, entity, time%s) VALUES (0,'test-quotes',0%s)";
        try (final Statement statement = connection.createStatement()) {
            statement.addBatch(String.format(query, "", ""));
            statement.addBatch(String.format(query, ", tags.tag1", ",'value1'"));
            statement.addBatch(String.format(query, ", tags.tag1, tags.tag2", ",'value1','value2'"));
            statement.addBatch(String.format(query, ", tags.tag1, tags.tag2, tags.key\"\"quote", ",'value1','value2','true'"));
            statement.addBatch(String.format(query, ", tags.tag1, tags.tag2, tags.key\"\"quote", ",'value1','value2','value\"\"quote'"));
            assertThat(statement.executeBatch(), is(new int[]{1,1,1,1,1}));
            Thread.sleep(TestProperties.INSERT_WAIT);
        }
    }

    @Test
    @Description("Test that AtsdResultSet#getTags method works with AtsdStatement")
    public void testStatement() throws SQLException {
        String sql = "SELECT * FROM \"m-with-tags\" WHERE \"tags.key\"\"quote\"='true'";
        String expectedTagsString = "key\"quote=true;tag1=value1;tag2=value2";
        try (final AtsdStatement statement = (AtsdStatement) connection.createStatement()) {
            statement.setTagsEncoding(true);
            final AtsdResultSet resultSet = (AtsdResultSet) statement.executeQuery(sql);
            testRecordFound(resultSet);
            Map<String, String> expectedTagsMap = new TreeMap<>();
            expectedTagsMap.put("tag1", "value1");
            expectedTagsMap.put("key\"quote", "true");
            expectedTagsMap.put("tag2", "value2");
            testGetTags(resultSet, TAGS_COLUMN, expectedTagsMap);
            testGetString(resultSet, TAGS_COLUMN, expectedTagsString);
        }
    }

    @Test
    public void testPreparedStatement() throws SQLException {
        String sql = "SELECT * FROM \"m-with-tags\" WHERE tags = ?";
        String expectedTagsString = "key\"quote=true;tag1=value1;tag2=value2";
        try (final AtsdPreparedStatement preparedStatement = (AtsdPreparedStatement) connection.prepareStatement(sql)) {
            preparedStatement.setTagsEncoding(true);
            Map<String, String> searchedTags = new TreeMap<>();
            searchedTags.put("tag1", "value1");
            searchedTags.put("key\"quote", "true");
            searchedTags.put("tag2", "value2");
            preparedStatement.setTags(1, searchedTags);
            final AtsdResultSet resultSet = (AtsdResultSet) preparedStatement.executeQuery();
            testRecordFound(resultSet);
            testGetTags(resultSet, TAGS_COLUMN, searchedTags);
            testGetString(resultSet, TAGS_COLUMN, expectedTagsString);
        }
    }

    @Test
    public void testStatementWithoutTagsEncoding() throws SQLException {
        String sql = "SELECT * FROM \"m-with-tags\" WHERE \"tags.key\"\"quote\"='true'";
        String expectedTags = "key\"quote=true;tag1=value1;tag2=value2";
        try (final AtsdStatement statement = (AtsdStatement) connection.createStatement()) {
            final AtsdResultSet resultSet = (AtsdResultSet) statement.executeQuery(sql);
            testRecordFound(resultSet);
            testGetString(resultSet, TAGS_COLUMN, expectedTags);
            expectedException.expect(SQLException.class);
            resultSet.getTags(TAGS_COLUMN);
        }
    }

    @Test
    public void testPreparedStatementWithoutTagsEncoding() throws SQLException {
        String sql = "SELECT * FROM \"m-with-tags\" WHERE tags = ?";
        try (final AtsdPreparedStatement preparedStatement = (AtsdPreparedStatement) connection.prepareStatement(sql)) {
            expectedException.expect(SQLException.class);
            Map<String, String> searchedTags = new TreeMap<>();
            searchedTags.put("tag1", "value1");
            searchedTags.put("key\"quote", "true");
            searchedTags.put("tag2", "value2");
            preparedStatement.setTags(1, searchedTags);
        }
    }

    @Step("Test that record is found")
    private void testRecordFound(ResultSet resultSet) throws SQLException {
        assertThat(resultSet.next(), is(true));
    }

    @Step("Test that AtsdResultSet#getTags deserialized tags properly")
    private void testGetTags(AtsdResultSet atsdResultSet, String column, Map<String, String> expected) throws SQLException {
        assertThat(atsdResultSet.getTags(column), is(expected));
    }

    @Step("Test that ResultSet#getString works properly")
    private void testGetString(ResultSet resultSet, String column, String expected) throws SQLException {
        assertThat(resultSet.getString(column), is(expected));
    }
}
