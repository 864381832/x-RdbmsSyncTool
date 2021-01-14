package com.xwintop.xJavaFxTool.tools;

import cn.hutool.db.ds.simple.SimpleDataSource;
import com.alibaba.druid.pool.DruidDataSource;
import com.alibaba.druid.util.JdbcUtils;
import com.xwintop.xJavaFxTool.controller.debugTools.RdbmsSyncToolController;
import com.xwintop.xJavaFxTool.services.debugTools.RdbmsSyncToolService;
import com.xwintop.xcore.util.javafx.TooltipUtil;
import com.zaxxer.hikari.HikariDataSource;
import javafx.scene.control.CheckBoxTreeItem;
import javafx.scene.control.TreeView;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.springframework.transaction.TransactionStatus;
import org.springframework.transaction.support.DefaultTransactionDefinition;

import javax.sql.DataSource;
import java.io.Closeable;
import java.sql.SQLException;
import java.sql.Timestamp;
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
        } else if ("Hikari".equals(dataSourceType)) {
            dataSource = getHikariDataSource(dbType, dbIp, dbPort, dbName, dbUserName, dbUserPassword, jdbcUrl);
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

    public static HikariDataSource getHikariDataSource(String dbType, String dbIp, String dbPort, String dbName, String dbUserName, String dbUserPassword, String jdbcUrl) {
        jdbcUrl = DataxJsonUtil.getJdbcUrl(jdbcUrl, dbType, dbIp, dbPort, dbName);
        String driver = null;
        try {
            driver = JdbcUtils.getDriverClassName(jdbcUrl);
        } catch (SQLException e) {
            e.printStackTrace();
        }
        HikariDataSource dataSource = new HikariDataSource();
        dataSource.setJdbcUrl(jdbcUrl);
        dataSource.setUsername(dbUserName);
        dataSource.setPassword(dbUserPassword);
        dataSource.setDriverClassName(driver);
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
            if (dataSource instanceof DruidDataSource) {
                JdbcUtils.close((Closeable) dataSource);
            }
        }
    }

    public static List<String> showSqlServerTables(DataSource dataSource, String dbType) throws SQLException {
        List<String> tables = new ArrayList<>();
        String sql = "";
        if ("sqlserver".equals(dbType)) {
            sql = "select c.name from sys.objects c where c.type='u'";
        } else if ("sqlserverold".equals(dbType)) {
            sql = "select c.name from sysobjects c where c.type='u'";
        } else if ("access".equals(dbType)) {
            sql = "select table_name from information_schema.tables";
        } else {
            return tables;
        }
        tables = new JdbcTemplate(dataSource).queryForList(sql, String.class);
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
            if (dataSource instanceof DruidDataSource) {
                JdbcUtils.close((Closeable) dataSource);
            }
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

    public static String getDataxWhereSql(String filterLongKeyColumn, Long maxKeyValue, Long lastMaxValue) {
        return String.format(" (%s < %s AND %s <= %s) ", maxKeyValue, filterLongKeyColumn, filterLongKeyColumn, lastMaxValue);
    }

    public static String getDataxWhereSql(String DB_TYPE, String filterTimeColumn, Timestamp lastSyncTime, Timestamp maxLastupdate) {
        StringBuffer stringBuffer = new StringBuffer(filterTimeColumn);
        if ("sqlserver".equalsIgnoreCase(DB_TYPE)) {
            stringBuffer.append(" > CONVERT(datetime,'" + DateFormatUtils.format(lastSyncTime, "yyyy-MM-dd HH:mm:ss.SSS") + "',21) and ");
            stringBuffer.append(filterTimeColumn).append(" <= CONVERT(datetime,'" + DateFormatUtils.format(maxLastupdate, "yyyy-MM-dd HH:mm:ss.SSS") + "',21)");
        } else if ("oracle".equalsIgnoreCase(DB_TYPE)) {
            stringBuffer.append(" > TO_TIMESTAMP('" + DateFormatUtils.format(lastSyncTime, "yyyy-MM-dd-HH:mm:ss") + String.format(".%09d", lastSyncTime.getNanos()) + "','yyyy-MM-dd-hh24:mi:ss.ff9') and ");
            stringBuffer.append(filterTimeColumn).append(" <= TO_TIMESTAMP('" + DateFormatUtils.format(maxLastupdate, "yyyy-MM-dd-HH:mm:ss") + String.format(".%09d", maxLastupdate.getNanos()) + "','yyyy-MM-dd-hh24:mi:ss.ff9')");
        } else if ("mysql".equalsIgnoreCase(DB_TYPE)) {
            stringBuffer.append(" > str_to_date('" + DateFormatUtils.format(lastSyncTime, "yyyy-MM-dd HH:mm:ss.SSS") + "','%Y-%m-%d %H:%i:%s.%f') and ");
            stringBuffer.append(filterTimeColumn).append(" <= str_to_date('" + DateFormatUtils.format(maxLastupdate, "yyyy-MM-dd HH:mm:ss.SSS") + "','%Y-%m-%d %H:%i:%s.%f')");
        } else if ("postgresq".equalsIgnoreCase(DB_TYPE)) {
            stringBuffer.append(" > to_timestamp('" + DateFormatUtils.format(lastSyncTime, "yyyy-MM-dd-HH:mm:ss.SSS") + "','yyyy-MM-dd-hh24:MI:ss.MS') and ");
            stringBuffer.append(filterTimeColumn).append(" <= to_timestamp('" + DateFormatUtils.format(maxLastupdate, "yyyy-MM-dd-HH:mm:ss.SSS") + "','yyyy-MM-dd-hh24:MI:ss.MS')");
        } else {
            stringBuffer.append(" > to_timestamp('" + DateFormatUtils.format(lastSyncTime, "yyyy-MM-dd-HH:mm:ss") + "','yyyy-MM-dd-hh24:MI:ss') and ");
            stringBuffer.append(filterTimeColumn).append(" <= to_timestamp('" + DateFormatUtils.format(maxLastupdate, "yyyy-MM-dd-HH:mm:ss") + "','yyyy-MM-dd-hh24:MI:ss')");
        }
        return stringBuffer.toString();
    }
}
