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
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.springframework.transaction.TransactionStatus;
import org.springframework.transaction.support.DefaultTransactionDefinition;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

@Slf4j
public class SqlUtil {
    public static DruidDataSource getDruidDataSource(String dbType, String dbIp, String dbPort, String dbName, String dbUserName, String dbUserPassword, String jdbcUrl) {
        DruidDataSource dataSource = new DruidDataSource();
        dataSource.setUrl(DataxJsonUtil.getJdbcUrl(jdbcUrl, dbType, dbIp, dbPort, dbName));
        if ("access".equals(dbType)) {
            dataSource.setDriverClassName("net.ucanaccess.jdbc.UcanaccessDriver");
        }
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
            if ("access".equals(dbType)) {
                driver = "net.ucanaccess.jdbc.UcanaccessDriver";
            } else {
                driver = JdbcUtils.getDriverClassName(jdbcUrl);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        SimpleDataSource dataSource = new SimpleDataSource(jdbcUrl, dbUserName, dbUserPassword, driver);
        return dataSource;
    }

    public static DataSource getDataSourceByViewType(RdbmsSyncToolController rdbmsSyncToolController, TreeView<String> tableTreeView) {
        DataSource dataSource = null;
        if (tableTreeView == rdbmsSyncToolController.getTableTreeView1()) {
            String dbType = rdbmsSyncToolController.getDbTypeText1().getValue();
            String dbIp = rdbmsSyncToolController.getHostText1().getText();
            String dbPort = rdbmsSyncToolController.getPortText1().getText();
            String dbName = rdbmsSyncToolController.getDbNameText1().getText();
            String dbUserName = rdbmsSyncToolController.getUserNameText1().getText();
            String dbUserPassword = rdbmsSyncToolController.getPwdText1().getText();
            dataSource = SqlUtil.getDataSource(dbType, dbIp, dbPort, dbName, dbUserName, dbUserPassword, rdbmsSyncToolController.getJdbcUrlField1().getText(), rdbmsSyncToolController.getDataSourceTypeChoiceBox().getValue());
        } else if (tableTreeView == rdbmsSyncToolController.getTableTreeView2()) {
            String dbType2 = rdbmsSyncToolController.getDbTypeText2().getValue();
            String dbIp2 = rdbmsSyncToolController.getHostText2().getText();
            String dbPort2 = rdbmsSyncToolController.getPortText2().getText();
            String dbName2 = rdbmsSyncToolController.getDbNameText2().getText();
            String dbUserName2 = rdbmsSyncToolController.getUserNameText2().getText();
            String dbUserPassword2 = rdbmsSyncToolController.getPwdText2().getText();
            dataSource = SqlUtil.getDataSource(dbType2, dbIp2, dbPort2, dbName2, dbUserName2, dbUserPassword2, rdbmsSyncToolController.getJdbcUrlField2().getText(), rdbmsSyncToolController.getDataSourceTypeChoiceBox().getValue());
        }
        return dataSource;
    }

    public static void executeSql(RdbmsSyncToolController rdbmsSyncToolController, TreeView<String> tableTreeView, String sql) {
        DataSource dataSource = SqlUtil.getDataSourceByViewType(rdbmsSyncToolController, tableTreeView);
        try {
            JdbcUtils.execute(dataSource, sql);
            TooltipUtil.showToast("执行sql成功：" + sql);
        } catch (Exception e) {
            log.error("executeSql:" + sql + " 错误：", e);
            TooltipUtil.showToast("executeSql:" + sql + " 错误：" + e.getMessage());
        } finally {
//            JdbcUtils.close(dataSource);
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
            } else if ("access".equals(dbType)) {
                rs = stmt.executeQuery("select table_name from information_schema.tables");
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
        DataSource dataSource = SqlUtil.getDataSourceByViewType(rdbmsSyncToolController, tableTreeView);
        try {
            List<Map<String, Object>> queryData = JdbcUtils.executeQuery(dataSource, sql);
            TooltipUtil.showToast("执行QuerySql成功：" + sql);
            return queryData;
        } catch (Exception e) {
            log.error("executeQuerySql:" + sql + " 错误：", e);
            TooltipUtil.showToast("executeQuerySql:" + sql + " 错误：" + e.getMessage());
        } finally {
//            JdbcUtils.close(dataSource);
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

    public static void doBatchInsert(String sql, List<Object[]> batchUpdateData, JdbcTemplate jdbcTemplate, AtomicInteger dirtyDataNumber) {
        DefaultTransactionDefinition transactionDefinition = new DefaultTransactionDefinition();
        DataSourceTransactionManager dm = new DataSourceTransactionManager(jdbcTemplate.getDataSource());
        TransactionStatus tmp = dm.getTransaction(transactionDefinition);
        try {
            jdbcTemplate.batchUpdate(sql, batchUpdateData);
            dm.commit(tmp);
        } catch (Exception e) {
            dm.rollback(tmp);
            log.warn("回滚此次写入, 采用每次写入一行方式提交. 因为:" + e.getMessage());
            for (Object[] batchUpdateDatum : batchUpdateData) {
                try {
                    jdbcTemplate.update(sql, batchUpdateDatum);
                } catch (Exception e1) {
                    log.error(" 脏数据: " + Arrays.toString(batchUpdateDatum), e1.getMessage());
                    dirtyDataNumber.getAndIncrement();
                }
            }
        }
    }
}
