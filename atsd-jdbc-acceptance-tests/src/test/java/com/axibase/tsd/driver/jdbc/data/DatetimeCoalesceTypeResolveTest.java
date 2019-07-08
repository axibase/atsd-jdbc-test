package com.axibase.tsd.driver.jdbc.data;

import com.axibase.tsd.driver.jdbc.util.IsoDateParseUtil;
import io.qameta.allure.Step;
import io.qameta.allure.junit4.DisplayName;
import lombok.AllArgsConstructor;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;
import static org.hamcrest.core.IsInstanceOf.instanceOf;

@RunWith(Parameterized.class)
@AllArgsConstructor
public class DatetimeCoalesceTypeResolveTest extends AbstractDataTest {
    private final String expression;
    private final String columnDatatype;
    private final Class<?> dataClass;

    @Parameterized.Parameters
    public static Object[][] queries() {
        return new Object[][] {
                { "COALESCE(datetime, time)", "java_object", String.class },
                { "COALESCE(time, datetime)", "java_object", Double.class },
                { "COALESCE(datetime, 'test')", "java_object", String.class },
                { "COALESCE(datetime, value)", "java_object", String.class },
                { "COALESCE(time, 'test')", "java_object", Double.class },
                { "COALESCE(time, value)", "double", Double.class },
        };
    }

    @Test
    @DisplayName("Test that COALESCE function sets appropriate types in metadata")
    public void testResolveDatatype() throws SQLException {
        final String sql = String.format("SELECT time, %s FROM jvm_memory_used LIMIT 1", expression);
        try (final Statement statement = connection.createStatement();
             final ResultSet resultSet = statement.executeQuery(sql)) {
            testMetadataTypes(columnDatatype, expression, resultSet);
            testResultClass(expression, dataClass, resultSet);
            testTimestampValuesMatch(expression, resultSet);
        }
    }

    @Step("Test that {0} datatype is set in metadata for column '{1}'")
    private void testMetadataTypes(String columnDatatype, String expression, ResultSet resultSet) throws SQLException {
        final ResultSetMetaData metaData = resultSet.getMetaData();
        assertThat(metaData.getColumnTypeName(2), is(columnDatatype));
        assertThat(resultSet.next(), is(true));
    }

    @Step("Test that {0} value is instance of {1} class")
    private void testResultClass(String expression, Class<?> dataClass, ResultSet resultSet) throws SQLException {
        final Object timestampObject = resultSet.getObject(2);
        assertThat(timestampObject, instanceOf(dataClass));
    }

    @Step("Test that timestamp can be parsed from {0} value")
    private void testTimestampValuesMatch(String expression, ResultSet resultSet) throws SQLException {
        final long rawTime = resultSet.getLong(1);
        final Object timestampObject = resultSet.getObject(2);
        if (String.class.equals(dataClass)) {
            assertThat(IsoDateParseUtil.parseIso8601(timestampObject.toString()), is(rawTime));
        } else {
            assertThat(((Number)timestampObject).longValue(), is(rawTime));
        }
    }
}
