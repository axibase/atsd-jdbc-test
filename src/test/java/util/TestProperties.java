package util;


import com.axibase.tsd.driver.jdbc.ext.AtsdConnectionInfo;
import com.axibase.tsd.driver.jdbc.util.ConnectStringComposer;
import lombok.experimental.UtilityClass;
import lombok.extern.slf4j.Slf4j;

@UtilityClass
@Slf4j
public final class TestProperties {
    public static final String HTTP_ATSD_URL = System.getProperty("axibase.tsd.driver.jdbc.url");
    public static final String LOGIN_NAME = System.getProperty("axibase.tsd.driver.jdbc.username");
    public static final String LOGIN_PASSWORD = System.getProperty("axibase.tsd.driver.jdbc.password");
    public static final String READ_STRATEGY = System.getProperty("axibase.tsd.driver.jdbc.strategy");
    public static final boolean REDIRECT_OUTPUT_TO_ALLURE = "true".equalsIgnoreCase(System.getProperty("output.redirect.allure"));
    public static final long INSERT_WAIT = getInsertWait(System.getProperty("insert.wait"));

    public static final String DEFAULT_JDBC_ATSD_URL;
    public static final AtsdConnectionInfo DEFAULT_CONNECTION_INFO;
    static {
        final ConnectStringComposer composer = new ConnectStringComposer(HTTP_ATSD_URL, LOGIN_NAME, LOGIN_PASSWORD);
        DEFAULT_JDBC_ATSD_URL = composer.composeConnectString();
        DEFAULT_CONNECTION_INFO = composer.composeConnectionInfo();
    }

    private static long getInsertWait(String property) {
        if (property != null) {
            try {
                return Long.parseLong(property);
            } catch (NumberFormatException e) {
                log.error("Error while setting insert.wait parameter: {}", property, e);
            }
        }
        return 1000L;
    }

}
