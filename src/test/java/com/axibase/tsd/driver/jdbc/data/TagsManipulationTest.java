package com.axibase.tsd.driver.jdbc.data;

import com.axibase.tsd.driver.jdbc.ext.AtsdPreparedStatement;
import com.axibase.tsd.driver.jdbc.ext.AtsdResultSet;
import com.axibase.tsd.driver.jdbc.ext.AtsdStatement;
import io.qameta.allure.Issue;
import io.qameta.allure.Step;
import io.qameta.allure.junit4.DisplayName;
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
    private static final String[] TAGS_COLUMNS = {"tags", "metric.tags", "entity.tags"};
    private static final String[] TAG_VALUES_PREFIXES = {"", "mt-", "et-"};

    @Rule
    public final ExpectedException expectedException = ExpectedException.none();

    @BeforeClass
    @SneakyThrows(InterruptedException.class)
    public static void prepareData() throws SQLException {
        String metricEntityQuery = "INSERT INTO \"m-with-tags\" (value, entity, time, " +
                "entity.tags.tag1, entity.tags.tag2, entity.tags.key\"\"quo;te," +
                "metric.tags.tag1, metric.tags.tag2, metric.tags.key\"\"quo;te,) VALUES (0,'test-quotes',0," +
                "'et-value1','et-value2','et-true'," +
                "'mt-value1','mt-value2','mt-true')";
        String query = "INSERT INTO \"m-with-tags\" (value, entity, time, %s) VALUES (0,'test-quotes',0,%s)";
        try (final Statement statement = connection.createStatement()) {
            statement.addBatch(metricEntityQuery);
            statement.addBatch(String.format(query, "tags.tag1", "'value1'"));
            statement.addBatch(String.format(query, "tags.tag1, tags.tag2", "'value1','value2'"));
            statement.addBatch(String.format(query, "tags.tag1, tags.tag2, tags.key\"\"quo;te", "'value1','value2','true'"));
            statement.addBatch(String.format(query, "tags.tag1, tags.tag2, tags.key\"\"quo;te", "'value1','value2','value\"\"quo;te'"));
            assertThat(statement.executeBatch(), is(new int[]{3,1,1,1,1}));
            Thread.sleep(TestProperties.INSERT_WAIT);
        }
    }

    private Map<String, String> prepareExpectedTagsMap(int columnOrder) {
        Map<String, String> expectedTagsMap = new TreeMap<>();
        expectedTagsMap.put("tag1", TAG_VALUES_PREFIXES[columnOrder] + "value1");
        expectedTagsMap.put("key\"quo;te", TAG_VALUES_PREFIXES[columnOrder] + "true");
        expectedTagsMap.put("tag2", TAG_VALUES_PREFIXES[columnOrder] + "value2");
        return expectedTagsMap;
    }

    private String prepareExpectedTagsSerialized(int columnOrder) {
        return "key\"quo;te=" + TAG_VALUES_PREFIXES[columnOrder] + "true;" +
                "tag1=" + TAG_VALUES_PREFIXES[columnOrder] + "value1;" +
                "tag2=" + TAG_VALUES_PREFIXES[columnOrder] + "value2";
    }

    @Test
    @DisplayName("Test that AtsdResultSet#getTags method works with AtsdStatement")
    public void testStatement() throws SQLException {
        String sql = "SELECT *, metric.tags, entity.tags FROM \"m-with-tags\" WHERE tags.\"key\"\"quo;te\"='true'";
        try (final AtsdStatement statement = (AtsdStatement) connection.createStatement()) {
            statement.setTagsEncoding(true);
            final AtsdResultSet resultSet = (AtsdResultSet) statement.executeQuery(sql);
            testRecordFound(resultSet);

            for (int i = 0; i < TAGS_COLUMNS.length; i++) {
                testGetTags(resultSet, TAGS_COLUMNS[i], prepareExpectedTagsMap(i));
                testGetString(resultSet, TAGS_COLUMNS[i], prepareExpectedTagsSerialized(i));
            }
        }
    }

    @Test
    @DisplayName("Test that AtsdResultSet#getTags method works with AtsdPreparedStatement")
    public void testPreparedStatement() throws SQLException {
        String sql = "SELECT *, metric.tags, entity.tags FROM \"m-with-tags\" WHERE tags = ?";
        try (final AtsdPreparedStatement preparedStatement = (AtsdPreparedStatement) connection.prepareStatement(sql)) {
            preparedStatement.setTagsEncoding(true);
            Map<String, String> searchedTags = prepareExpectedTagsMap(0);
            preparedStatement.setTags(1, searchedTags);
            final AtsdResultSet resultSet = (AtsdResultSet) preparedStatement.executeQuery();
            testRecordFound(resultSet);
            for (int i = 0; i < TAGS_COLUMNS.length; i++) {
                testGetTags(resultSet, TAGS_COLUMNS[i], prepareExpectedTagsMap(i));
                testGetString(resultSet, TAGS_COLUMNS[i], prepareExpectedTagsSerialized(i));
            }
        }
    }

    @Test
    @DisplayName("Test that exception is thrown when AtsdResultSet#getTags is called unless tagsEncoding is set on AtsdStatement")
    public void testStatementWithoutTagsEncoding() throws SQLException {
        String sql = "SELECT *, metric.tags, entity.tags FROM \"m-with-tags\" WHERE tags.\"key\"\"quo;te\"='true'";
        try (final AtsdStatement statement = (AtsdStatement) connection.createStatement()) {
            final AtsdResultSet resultSet = (AtsdResultSet) statement.executeQuery(sql);
            testRecordFound(resultSet);
            for (int i = 0; i < TAGS_COLUMNS.length; i++) {
                testGetString(resultSet, TAGS_COLUMNS[i], prepareExpectedTagsSerialized(i));
            }
            expectedException.expect(SQLException.class);
            resultSet.getTags(TAGS_COLUMNS[0]);
        }
    }

    @Test
    @DisplayName("Test that exception is thrown when AtsdResultSet#getTags is called unless tagsEncoding is set on AtsdPreparedStatement")
    public void testPreparedStatementWithoutTagsEncoding() throws SQLException {
        String sql = "SELECT *, metric.tags, entity.tags FROM \"m-with-tags\" WHERE tags = ?";
        try (final AtsdPreparedStatement preparedStatement = (AtsdPreparedStatement) connection.prepareStatement(sql)) {
            preparedStatement.setTags(1, prepareExpectedTagsMap(0));
            final AtsdResultSet resultSet = (AtsdResultSet) preparedStatement.executeQuery();
            testRecordFound(resultSet);
            for (int i = 0; i < TAGS_COLUMNS.length; i++) {
                testGetString(resultSet, TAGS_COLUMNS[i], prepareExpectedTagsSerialized(i));
            }
            expectedException.expect(SQLException.class);
            resultSet.getTags(TAGS_COLUMNS[0]);
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
