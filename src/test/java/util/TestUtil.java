package util;

import com.axibase.tsd.driver.jdbc.content.ContentMetadata;
import com.axibase.tsd.driver.jdbc.content.StatementContext;
import com.axibase.tsd.driver.jdbc.enums.AtsdType;
import com.axibase.tsd.driver.jdbc.enums.DefaultColumn;
import lombok.experimental.UtilityClass;
import org.apache.calcite.avatica.ColumnMetaData;
import org.apache.calcite.avatica.Meta;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.helpers.MessageFormatter;

import java.sql.*;
import java.util.*;

@UtilityClass
public class TestUtil {
    private static final Map<String, AtsdType> columnPrefixAtsdTypeMapping = createColumnPrefixAtsdTypeMapping();

    public static List<ColumnMetaData> prepareMetadata(String header) {
        final boolean odbcCompatible = false;
        if (header.startsWith("#")) {
            return Collections.singletonList(ColumnMetaData.dummy(AtsdType.STRING_DATA_TYPE.getAvaticaType(odbcCompatible), false));
        } else {
            String[] columnNames = header.split(",");
            ColumnMetaData[] meta = new ColumnMetaData[columnNames.length];
            for (int i = 0; i < columnNames.length; i++) {
                final String columnName = columnNames[i];
                meta[i] = new ContentMetadata.ColumnMetaDataBuilder(false, odbcCompatible)
                        .withName(columnName)
                        .withLabel(columnName)
                        .withColumnIndex(i)
                        .withNullable(columnName.startsWith("tag") ? 1 : 0)
                        .withAtsdType(getAtsdTypeByColumnName(columnName))
                        .build();
            }
            return Arrays.asList(meta);
        }
    }

    private static AtsdType getAtsdTypeByColumnName(String columnName) {
        int dotIndex = columnName.indexOf('.');
        final String prefix = dotIndex == -1 ? columnName : columnName.substring(0, dotIndex);
        AtsdType type = columnPrefixAtsdTypeMapping.get(prefix);
        if (type == null) {
            type = AtsdType.DEFAULT_TYPE;
        }
        return type;
    }

    private static Map<String, AtsdType> createColumnPrefixAtsdTypeMapping() {
        Map<String, AtsdType> mapping = new HashMap<>();
        for (DefaultColumn type : DefaultColumn.values()) {
            mapping.put(type.getColumnNamePrefix(), type.getType(AtsdType.DEFAULT_VALUE_TYPE));
        }
        return Collections.unmodifiableMap(mapping);
    }

    public static String buildVariableName(String suffix) {
        String methodName = Thread.currentThread().getStackTrace()[2].getMethodName();
        StringBuilder result = new StringBuilder();
        for (int i = 0; i < methodName.length(); i++) {
            Character ch = methodName.charAt(i);
            if (Character.isUpperCase(ch)) {
                result.append("-");
            } else if ('_' == ch) {
                continue;
            }
            result.append(Character.toLowerCase(ch));
        }
        result.append(":tst-");
        if (StringUtils.isNotBlank(suffix)) {
            result.append(suffix);
        }
        return result.toString();
    }

    public static String format(String pattern, Object... args) {
        return args == null || args.length == 0 ? pattern : MessageFormatter.arrayFormat(pattern, args).getMessage();
    }

    public static Map<String, Object> getLastInserted(Connection connection, String sql) throws SQLException, InterruptedException {
        Thread.sleep(TestProperties.INSERT_WAIT);
        try(PreparedStatement stmt = connection.prepareStatement(sql)) {
            try(ResultSet rs = stmt.executeQuery()) {
                ResultSetMetaData rsmd = rs.getMetaData();
                Map<String, Object> map = new HashMap<>();
                if(rs.next()) {
                    for (int i=1;i<=rsmd.getColumnCount();i++) {
                        map.put(rsmd.getColumnName(i), getValue(rs, i, rsmd.getColumnType(i)));
                    }
                }
                return map;
            }
        }
    }

    private static Object getValue(ResultSet rs, int columnIndex, int columnType) throws SQLException {
        switch (columnType) {
            case Types.BIGINT : return rs.getLong(columnIndex);
            case Types.REAL : return rs.getDouble(columnIndex);
            case Types.TIMESTAMP : return rs.getTimestamp(columnIndex);
            case Types.BOOLEAN : return rs.getBoolean(columnIndex);
            case Types.JAVA_OBJECT : {
                String str = rs.getString(columnIndex);
                return StringUtils.isEmpty(str) ? null : str;
            }
            default : return rs.getString(columnIndex);
        }
    }

    private static Meta.StatementHandle createStatementHandle() {
        return new Meta.StatementHandle("12345678", 1,null);
    }

    public static StatementContext createStatementContext() {
        return new StatementContext(createStatementHandle(), false);
    }

}
