package util;


import com.axibase.tsd.driver.jdbc.ext.AtsdConnectionInfo;
import com.axibase.tsd.driver.jdbc.util.ConnectStringComposer;
import lombok.experimental.UtilityClass;

@UtilityClass
public final class TestProperties {
    public static final String HTTP_ATSD_URL = System.getProperty("axibase.tsd.driver.jdbc.url");
    public static final String LOGIN_NAME = System.getProperty("axibase.tsd.driver.jdbc.username");
    public static final String LOGIN_PASSWORD = System.getProperty("axibase.tsd.driver.jdbc.password");
    public static final String READ_STRATEGY = System.getProperty("axibase.tsd.driver.jdbc.strategy");
    public static final boolean REDIRECT_OUTPUT_TO_ALLURE = "true".equalsIgnoreCase(System.getProperty("output.redirect.allure"));

    public static final String DEFAULT_JDBC_ATSD_URL;
    public static final AtsdConnectionInfo DEFAULT_CONNECTION_INFO;
    static {
        final ConnectStringComposer composer = new ConnectStringComposer(HTTP_ATSD_URL, LOGIN_NAME, LOGIN_PASSWORD);
        DEFAULT_JDBC_ATSD_URL = composer.composeConnectString();
        DEFAULT_CONNECTION_INFO = composer.composeConnectionInfo();
    }

}
