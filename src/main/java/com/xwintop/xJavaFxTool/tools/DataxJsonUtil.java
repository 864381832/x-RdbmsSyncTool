package com.xwintop.xJavaFxTool.tools;

import com.alibaba.fastjson.JSONObject;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.time.DateFormatUtils;

import java.sql.Timestamp;
import java.sql.Types;

public class DataxJsonUtil {
    public static String getDbDefaultPort(String DB_TYPE) {
        if ("sqlserverold".equalsIgnoreCase(DB_TYPE)) {
            DB_TYPE = "sqlserver";
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
            jdbcUrl = "jdbc:sqlserver://" + dbIp + ":" + dbPort + ";DatabaseName=" + dbName;
        } else if ("oracle".equalsIgnoreCase(DB_TYPE)) {
            //jdbc:oracle:thin:@[HOST_NAME]:PORT:[DATABASE_NAME]
            jdbcUrl = "jdbc:oracle:thin:@" + dbIp + ":" + dbPort + "/" + dbName;
        } else if ("mysql".equalsIgnoreCase(DB_TYPE)) {
            //jdbc:mysql://bad_ip:3306/database
            jdbcUrl = "jdbc:mysql://" + dbIp + ":" + dbPort + "/" + dbName + "?characterEncoding=utf8";
        } else if ("postgresql".equalsIgnoreCase(DB_TYPE)) {
            //jdbc:mysql://bad_ip:3306/database
            jdbcUrl = "jdbc:postgresql://" + dbIp + ":" + dbPort + "/" + dbName;
        } else if ("sqlserverold".equalsIgnoreCase(DB_TYPE)) {
            //jdbc:jtds:sqlserver://127.0.0.1:1433;datebaseName=test
            jdbcUrl = "jdbc:jtds:sqlserver://" + dbIp + ":" + dbPort + "/" + dbName;
        } else if ("dm".equalsIgnoreCase(DB_TYPE)) {
            //jdbc:jtds:sqlserver://127.0.0.1:1433;datebaseName=test
            jdbcUrl = "jdbc:dm://" + dbIp + ":" + dbPort + "/" + dbName;
        }
        return jdbcUrl;
    }

    public static String convertDatabaseCharsetType(String userName, String schema, String type) {
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
