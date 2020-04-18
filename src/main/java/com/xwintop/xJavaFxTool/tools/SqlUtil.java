package com.xwintop.xJavaFxTool.tools;

import com.alibaba.druid.pool.DruidDataSource;
import lombok.extern.slf4j.Slf4j;

import java.sql.Types;

@Slf4j
public class SqlUtil {
    public static DruidDataSource getDruidDataSource(String dbType, String dbIp, String dbPort, String dbName, String dbUserName, String dbUserPassword) {
        DruidDataSource dataSource = new DruidDataSource();
        dataSource.setUrl(SqlUtil.getJdbcUrl(dbType, dbIp, dbPort, dbName));
        dataSource.setUsername(dbUserName);
        dataSource.setPassword(dbUserPassword);
        dataSource.setFailFast(true);
        return dataSource;
    }

    /**
     * 获取jdbc连接地址
     */
    public static String getJdbcUrl(String DB_TYPE, String dbIp, String dbPort, String dbName) {
        String jdbcUrl = null;
        if ("sqlserver".equalsIgnoreCase(DB_TYPE)) {
            //jdbc:sqlserver://localhost:3433;DatabaseName=dbname
            jdbcUrl = "jdbc:sqlserver://" + dbIp + ":" + dbPort + ";DatabaseName=" + dbName;
        } else if ("oracle".equalsIgnoreCase(DB_TYPE)) {
            //jdbc:oracle:thin:@[HOST_NAME]:PORT:[DATABASE_NAME]
            jdbcUrl = "jdbc:oracle:thin:@" + dbIp + ":" + dbPort + "/" + dbName;
        } else if ("mysql".equalsIgnoreCase(DB_TYPE)) {
            //jdbc:mysql://bad_ip:3306/database
            jdbcUrl = "jdbc:mysql://" + dbIp + ":" + dbPort + "/" + dbName + "?characterEncoding=utf8";
        }
        return jdbcUrl;
    }

    public static String convertDatabaseCharsetType(String in, String type) {
        String dbUser;
        if (in != null) {
            if (type.equals("oracle")) {
                dbUser = in.toUpperCase();
            } else if (type.equals("postgresql")) {
                dbUser = "public";
            } else if (type.equals("mysql")) {
                dbUser = null;
            } else if (type.equals("sqlserver")) {
                dbUser = null;
            } else if (type.equals("db2")) {
                dbUser = in.toUpperCase();
            } else {
                dbUser = in;
            }
        } else {
            dbUser = "public";
        }
        return dbUser;
    }

    public static String getTableColumnType(int jdbcType, int size) {
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
}
