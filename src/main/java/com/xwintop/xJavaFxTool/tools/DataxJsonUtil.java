package com.xwintop.xJavaFxTool.tools;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

import java.sql.Types;

@Slf4j
public class DataxJsonUtil {
    public static String getDbDefaultPort(String DB_TYPE) {
        if ("sqlserverold".equalsIgnoreCase(DB_TYPE)) {
            DB_TYPE = "sqlserver";
        } else if ("oracleSid".equalsIgnoreCase(DB_TYPE)) {
            DB_TYPE = "oracle";
        }
        if ("sqlserver".equalsIgnoreCase(DB_TYPE)) {
            return "1433";
        } else if ("oracle".equalsIgnoreCase(DB_TYPE)) {
            return "1521";
        } else if ("mysql".equalsIgnoreCase(DB_TYPE)) {
            return "3306";
        } else if ("postgresql".equalsIgnoreCase(DB_TYPE)) {
            return "5432";
        } else if ("dm".equalsIgnoreCase(DB_TYPE)) {
            return "5236";
        }
        return null;
    }

    /**
     * 获取jdbc连接地址
     */
    public static String getJdbcUrl(String jdbcUrl, String DB_TYPE, String dbIp, String dbPort, String dbName) {
        if (StringUtils.isNotEmpty(jdbcUrl)) {
            return jdbcUrl;
        }
        if ("sqlserver".equalsIgnoreCase(DB_TYPE)) {
            //jdbc:sqlserver://localhost:3433;DatabaseName=dbname
            jdbcUrl = String.format("jdbc:sqlserver://%s:%s;DatabaseName=%s", dbIp, dbPort, dbName);
        } else if ("oracle".equalsIgnoreCase(DB_TYPE)) {
            //jdbc:oracle:thin:@[HOST_NAME]:PORT:[DATABASE_NAME]
            jdbcUrl = String.format("jdbc:oracle:thin:@%s:%s/%s", dbIp, dbPort, dbName);
        } else if ("oracleSid".equalsIgnoreCase(DB_TYPE)) {
            //jdbc:oracle:thin:@[HOST_NAME]:PORT:[DATABASE_NAME]
            jdbcUrl = String.format("jdbc:oracle:thin:@%s:%s:%s", dbIp, dbPort, dbName);
        } else if ("mysql".equalsIgnoreCase(DB_TYPE)) {
            //jdbc:mysql://bad_ip:3306/database
            jdbcUrl = String.format("jdbc:mysql://%s:%s/%s?characterEncoding=utf8", dbIp, dbPort, dbName);
        } else if ("postgresql".equalsIgnoreCase(DB_TYPE)) {
            jdbcUrl = String.format("jdbc:postgresql://%s:%s/%s", dbIp, dbPort, dbName);
        } else if ("sqlserverold".equalsIgnoreCase(DB_TYPE)) {
            jdbcUrl = String.format("jdbc:jtds:sqlserver://%s:%s/%s", dbIp, dbPort, dbName);
        } else if ("dm".equalsIgnoreCase(DB_TYPE)) {
            jdbcUrl = String.format("jdbc:dm://%s:%s/%s", dbIp, dbPort, dbName);
        }
        log.info("解析出jdbcUrl: " + jdbcUrl);
        return jdbcUrl;
    }

    public static String convertDatabaseCharsetType(String userName, String schema, String type) {
        if ("sqlserverold".equalsIgnoreCase(type)) {
            type = "sqlserver";
        } else if ("oracleSid".equalsIgnoreCase(type)) {
            type = "oracle";
        }
        String dbUser;
        if (type.equals("oracle")) {
            dbUser = StringUtils.defaultIfBlank(schema, userName).toUpperCase();
        } else if (type.equals("postgresql")) {
            dbUser = StringUtils.defaultIfBlank(schema, "public");
        } else if (type.equals("mysql")) {
            dbUser = null;
        } else if (type.equals("sqlserver")) {
            dbUser = StringUtils.defaultIfBlank(schema, "dbo");
        } else if (type.equals("db2")) {
            dbUser = StringUtils.defaultIfBlank(schema, userName).toUpperCase();
        } else {
            dbUser = StringUtils.defaultIfBlank(schema, userName);
        }
        return dbUser;
    }

    public static String getTableColumnType(int jdbcType, int size) {
        if (size == 0) {
            size = 32;
        }
        String type = "varchar(" + size + ")";
        switch (jdbcType) {
            case Types.CHAR:
            case Types.NCHAR:
            case Types.VARCHAR:
            case Types.LONGVARCHAR:
            case Types.NVARCHAR:
            case Types.LONGNVARCHAR:
                break;
            case Types.CLOB:
            case Types.NCLOB:
                break;
            case Types.SMALLINT:
            case Types.TINYINT:
            case Types.INTEGER:
            case Types.BIGINT:
                break;
            case Types.NUMERIC:
            case Types.DECIMAL:
                break;
            case Types.FLOAT:
            case Types.REAL:
            case Types.DOUBLE:
                break;
            case Types.TIME:
                type = "datetime";
                break;
            case Types.DATE:
                type = "datetime";
                break;
            case Types.TIMESTAMP:
                type = "datetime";
                break;
            case Types.BINARY:
            case Types.VARBINARY:
            case Types.BLOB:
            case Types.LONGVARBINARY:
                break;
            case Types.BOOLEAN:
            case Types.BIT:
                break;
            case Types.NULL:
                break;
            default:
                break;
        }
        return type;
    }

    public static String getTableColumnTypeByOracle(int jdbcType, int size) {
        if (size == 0) {
            size = 32;
        }
        String type = "VARCHAR2(" + size + " CHAR)";
        switch (jdbcType) {
            case Types.TIME:
                type = "TIMESTAMP";
                break;
            case Types.DATE:
                type = "TIMESTAMP";
                break;
            case Types.TIMESTAMP:
                type = "TIMESTAMP";
                break;
            default:
                break;
        }
        return type;
    }
}
