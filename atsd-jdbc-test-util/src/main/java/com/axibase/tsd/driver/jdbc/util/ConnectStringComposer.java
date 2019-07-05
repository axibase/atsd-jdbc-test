package com.axibase.tsd.driver.jdbc.util;

import com.axibase.tsd.driver.jdbc.DriverConstants;
import com.axibase.tsd.driver.jdbc.ext.AtsdConnectionInfo;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class ConnectStringComposer {
    private final Map<String, String> connectStringParameters = new HashMap<>();
    private final String atsdHttpUrl;
    private final String login;
    private final String password;
    private String catalog;

    public ConnectStringComposer(String atsdHttpUrl, String login, String password) {
        this.atsdHttpUrl = atsdHttpUrl;
        this.login = login;
        this.password = password;
    }

    public ConnectStringComposer withCatalog(String catalog) {
        this.catalog = catalog;
        return this;
    }

    public ConnectStringComposer withTrust(String trust) {
        connectStringParameters.put(DriverConstants.TRUST_PARAM_NAME, trust);
        return this;
    }

    public ConnectStringComposer withSecure(String secure) {
        connectStringParameters.put(DriverConstants.SECURE_PARAM_NAME, secure);
        return this;
    }

    public ConnectStringComposer withStrategy(String strategy) {
        connectStringParameters.put(DriverConstants.STRATEGY_PARAM_NAME, strategy);
        return this;
    }

    public ConnectStringComposer withTables(String tables) {
        connectStringParameters.put(DriverConstants.TABLES_PARAM_NAME, tables);
        return this;
    }

    public ConnectStringComposer withExpandTags(String expandTags) {
        connectStringParameters.put(DriverConstants.EXPAND_TAGS_PARAM_NAME, expandTags);
        return this;
    }

    public ConnectStringComposer withMetaColumns(String metaColumns) {
        connectStringParameters.put(DriverConstants.META_COLUMNS_PARAM_NAME, metaColumns);
        return this;
    }

    public ConnectStringComposer withAssignColumnNames(String assignColumnNames) {
        connectStringParameters.put(DriverConstants.ASSIGN_INNER_COLUMN_NAMES_PARAM, assignColumnNames);
        return this;
    }

    public ConnectStringComposer withTimestamptz(String timestamptz) {
        connectStringParameters.put(DriverConstants.USE_TIMESTAMP_WITH_TIME_ZONE_PARAM, timestamptz);
        return this;
    }

    public ConnectStringComposer withMissingMetric(String missingMetric) {
        connectStringParameters.put(DriverConstants.ON_MISSING_METRIC_PARAM, missingMetric);
        return this;
    }

    public ConnectStringComposer withCompatibility(String compatibility) {
        connectStringParameters.put(DriverConstants.COMPATIBILITY_PARAM, compatibility);
        return this;
    }
    

    public String composeConnectString() {
        StringBuilder buffer = new StringBuilder(DriverConstants.CONNECT_URL_PREFIX)
                .append(atsdHttpUrl);
        if (catalog != null) {
            buffer.append('/').append(catalog);
        }

        for (Map.Entry<String, String> entry : connectStringParameters.entrySet()) {
            appendParameter(entry.getKey(), entry.getValue(), buffer);
        }

        return buffer.toString();
    }

    public AtsdConnectionInfo composeConnectionInfo() {
        final Properties info = new Properties();
        info.setProperty("user", login);
        info.setProperty("password", password);
        String url = atsdHttpUrl + (catalog == null ? "" : catalog);
        info.setProperty("url", url);
        return new AtsdConnectionInfo(info);
    }

    private void appendParameter(String parameterName, String value, StringBuilder buffer) {
        if (value != null) {
            buffer.append(DriverConstants.CONNECTION_STRING_PARAM_SEPARATOR)
                    .append(parameterName).append('=').append(value);
        }
    }
}
