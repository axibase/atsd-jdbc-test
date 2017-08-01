package util;

import com.axibase.tsd.driver.jdbc.content.ContentMetadata;
import com.axibase.tsd.driver.jdbc.enums.AtsdType;
import com.axibase.tsd.driver.jdbc.enums.DefaultColumn;
import lombok.experimental.UtilityClass;
import org.apache.calcite.avatica.ColumnMetaData;

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
}
