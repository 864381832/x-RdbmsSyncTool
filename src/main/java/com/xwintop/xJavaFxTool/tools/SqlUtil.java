package com.xwintop.xJavaFxTool.tools;

import cn.hutool.db.ds.simple.SimpleDataSource;
import com.alibaba.druid.pool.DruidDataSource;
import com.alibaba.druid.util.JdbcUtils;
import com.xwintop.xJavaFxTool.controller.debugTools.RdbmsSyncToolController;
import com.xwintop.xJavaFxTool.services.debugTools.RdbmsSyncToolService;
import com.xwintop.xcore.util.javafx.TooltipUtil;
import javafx.scene.control.CheckBoxTreeItem;
import javafx.scene.control.TreeView;
import lombok.extern.slf4j.Slf4j;
import org.springframework.jdbc.datasource.DriverManagerDataSource;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@Slf4j
public class SqlUtil {
    public static DruidDataSource getDruidDataSource(String dbType, String dbIp, String dbPort, String dbName, String dbUserName, String dbUserPassword, String jdbcUrl) {
        DruidDataSource dataSource = new DruidDataSource();
        dataSource.setUrl(DataxJsonUtil.getJdbcUrl(jdbcUrl, dbType, dbIp, dbPort, dbName));
        dataSource.setUsername(dbUserName);
        dataSource.setPassword(dbUserPassword);
        dataSource.setTestWhileIdle(false);
        dataSource.setFailFast(true);
        return dataSource;
    }

    public static DataSource getDataSource(String dbType, String dbIp, String dbPort, String dbName, String dbUserName, String dbUserPassword, String jdbcUrl, String dataSourceType) {
        DataSource dataSource = null;
        if ("Druid".equals(dataSourceType)) {
            dataSource = getDruidDataSource(dbType, dbIp, dbPort, dbName, dbUserName, dbUserPassword, jdbcUrl);
        } else if ("Driver".equals(dataSourceType)) {
            dataSource = new DriverManagerDataSource(DataxJsonUtil.getJdbcUrl(jdbcUrl, dbType, dbIp, dbPort, dbName), dbUserName, dbUserPassword);
        } else if ("Simple".equals(dataSourceType)) {
            dataSource = getSimpleDataSource(dbType, dbIp, dbPort, dbName, dbUserName, dbUserPassword, jdbcUrl);
        }
        return dataSource;
    }

    public static SimpleDataSource getSimpleDataSource(String dbType, String dbIp, String dbPort, String dbName, String dbUserName, String dbUserPassword, String jdbcUrl) {
        jdbcUrl = DataxJsonUtil.getJdbcUrl(jdbcUrl, dbType, dbIp, dbPort, dbName);
        String driver = null;
        try {
            driver = JdbcUtils.getDriverClassName(jdbcUrl);
        } catch (SQLException e) {
            e.printStackTrace();
        }
        SimpleDataSource dataSource = new SimpleDataSource(jdbcUrl, dbUserName, dbUserPassword, driver);
        return dataSource;
    }

    public static SimpleDataSource getDataSourceByViewType(RdbmsSyncToolController rdbmsSyncToolController, TreeView<String> tableTreeView) {
        SimpleDataSource dataSource = null;
        if (tableTreeView == rdbmsSyncToolController.getTableTreeView1()) {
            String dbType = rdbmsSyncToolController.getDbTypeText1().getValue();
            String dbIp = rdbmsSyncToolController.getHostText1().getText();
            String dbPort = rdbmsSyncToolController.getPortText1().getText();
            String dbName = rdbmsSyncToolController.getDbNameText1().getText();
            String dbUserName = rdbmsSyncToolController.getUserNameText1().getText();
            String dbUserPassword = rdbmsSyncToolController.getPwdText1().getText();
            dataSource = SqlUtil.getSimpleDataSource(dbType, dbIp, dbPort, dbName, dbUserName, dbUserPassword, rdbmsSyncToolController.getJdbcUrlField1().getText());
        } else if (tableTreeView == rdbmsSyncToolController.getTableTreeView2()) {
            String dbType2 = rdbmsSyncToolController.getDbTypeText2().getValue();
            String dbIp2 = rdbmsSyncToolController.getHostText2().getText();
            String dbPort2 = rdbmsSyncToolController.getPortText2().getText();
            String dbName2 = rdbmsSyncToolController.getDbNameText2().getText();
            String dbUserName2 = rdbmsSyncToolController.getUserNameText2().getText();
            String dbUserPassword2 = rdbmsSyncToolController.getPwdText2().getText();
            dataSource = SqlUtil.getSimpleDataSource(dbType2, dbIp2, dbPort2, dbName2, dbUserName2, dbUserPassword2, rdbmsSyncToolController.getJdbcUrlField2().getText());
        }
        return dataSource;
    }

    public static void executeSql(RdbmsSyncToolController rdbmsSyncToolController, TreeView<String> tableTreeView, String sql) {
        SimpleDataSource dataSource = SqlUtil.getDataSourceByViewType(rdbmsSyncToolController, tableTreeView);
        try {
            JdbcUtils.execute(dataSource, sql);
            TooltipUtil.showToast("执行sql成功：" + sql);
        } catch (Exception e) {
            log.error("executeSql:" + sql + " 错误：", e);
            TooltipUtil.showToast("executeSql:" + sql + " 错误：" + e.getMessage());
        } finally {
            JdbcUtils.close(dataSource);
        }
    }

    public static List<String> showSqlServerTables(Connection conn, String dbType) throws SQLException {
        List<String> tables = new ArrayList<String>();
        Statement stmt = null;
        ResultSet rs = null;
        try {
            stmt = conn.createStatement();
            if ("sqlserver".equals(dbType)) {
                rs = stmt.executeQuery("select c.name from sys.objects c where c.type='u'");
            } else if ("sqlserverold".equals(dbType)) {
                rs = stmt.executeQuery("select c.name from sysobjects c where c.type='u'");
            }
            while (rs.next()) {
                String tableName = rs.getString(1);
                tables.add(tableName);
            }
        } finally {
            JdbcUtils.close(rs);
            JdbcUtils.close(stmt);
        }
        return tables;
    }

    public static List<Map<String, Object>> executeQuerySql(RdbmsSyncToolController rdbmsSyncToolController, TreeView<String> tableTreeView, String sql) {
        SimpleDataSource dataSource = SqlUtil.getDataSourceByViewType(rdbmsSyncToolController, tableTreeView);
        try {
            List<Map<String, Object>> queryData = JdbcUtils.executeQuery(dataSource, sql);
            TooltipUtil.showToast("执行QuerySql成功：" + sql);
            return queryData;
        } catch (Exception e) {
            log.error("executeQuerySql:" + sql + " 错误：", e);
            TooltipUtil.showToast("executeQuerySql:" + sql + " 错误：" + e.getMessage());
        } finally {
            JdbcUtils.close(dataSource);
        }
        return null;
    }

    public static String createrSelectSql(RdbmsSyncToolService rdbmsSyncToolService, CheckBoxTreeItem<String> selectedItem, String tableName, boolean isMysql) {
        StringBuffer stringBuffer = new StringBuffer("select ");
        Map tableInfoMap = rdbmsSyncToolService.getTableInfoMap(selectedItem.getChildren());
        String[] columnList = (String[]) tableInfoMap.get("columnList");
        for (String column : columnList) {
            if (isMysql) {
                stringBuffer.append("`");
            }
            stringBuffer.append(column);
            if (isMysql) {
                stringBuffer.append("`");
            }
            stringBuffer.append(",");
        }
        stringBuffer.deleteCharAt(stringBuffer.length() - 1);
        stringBuffer.append(" FROM ").append(tableName);
        return stringBuffer.toString();
    }

    public static String createUserSql(RdbmsSyncToolController rdbmsSyncToolController, String tableName) {
        StringBuffer tableString = new StringBuffer();
        tableString.append("--2-1. 新增数据类别目标表（只插入一次，如果有了就不用插入了）\n" +
                "Insert into DATAEX_CATAGORY\n" +
                "AND NOT EXISTS (SELECT 1 FROM DATAEX_JOB_ITEM_MAP D WHERE A.JOB_KEY=D.FK_JOB_KEY AND B.CATAGORY_ITEM_KEY=D.FK_CATAGORY_ITEM_KEY_SRC AND C.CATAGORY_ITEM_KEY=D.FK_CATAGORY_ITEM_KEY_TGT);\n\n");
        return tableString.toString();
    }
}
