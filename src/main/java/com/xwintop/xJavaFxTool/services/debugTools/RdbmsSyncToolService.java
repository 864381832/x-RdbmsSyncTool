package com.xwintop.xJavaFxTool.services.debugTools;

import cn.hutool.db.Db;
import cn.hutool.db.Entity;
import cn.hutool.db.ds.simple.SimpleDataSource;
import cn.hutool.db.meta.Column;
import cn.hutool.db.meta.MetaUtil;
import cn.hutool.db.meta.Table;
import cn.hutool.db.meta.TableType;
import com.alibaba.druid.util.JdbcUtils;
import com.alibaba.fastjson.JSONObject;
import com.xwintop.xJavaFxTool.controller.debugTools.RdbmsSyncToolController;
import com.xwintop.xJavaFxTool.tools.DataxJsonUtil;
import com.xwintop.xJavaFxTool.tools.SqlUtil;
import com.xwintop.xcore.javafx.dialog.FxDialog;
import com.xwintop.xcore.util.javafx.AlertUtil;
import com.xwintop.xcore.util.javafx.JavaFxViewUtil;
import com.xwintop.xcore.util.javafx.TooltipUtil;
import javafx.collections.FXCollections;
import javafx.collections.ObservableList;
import javafx.scene.control.TextArea;
import javafx.scene.control.*;
import javafx.scene.layout.HBox;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;

import javax.sql.DataSource;
import java.awt.*;
import java.awt.datatransfer.StringSelection;
import java.io.File;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.util.List;
import java.util.*;
import java.util.concurrent.atomic.AtomicReference;

@Getter
@Setter
@Slf4j
public class RdbmsSyncToolService {
    private RdbmsSyncToolController rdbmsSyncToolController;
    private Map<String, Table> tableMap = new HashMap<>();
    private Map<String, Table> tableMap2 = new HashMap<>();

    public RdbmsSyncToolService(RdbmsSyncToolController rdbmsSyncToolController) {
        this.rdbmsSyncToolController = rdbmsSyncToolController;
    }

    //连接数据库
    public void connectAction(String dbType, String dbIp, String dbPort, String dbName, String dbUserName, String dbUserPassword, TreeView<String> tableTreeView) throws Exception {
        DataSource dataSource = null;
        try {
            List<String> tableNames = null;
            String jdbcUrl = rdbmsSyncToolController.getJdbcUrlField1().getText();
            if (tableTreeView == rdbmsSyncToolController.getTableTreeView2()) {
                jdbcUrl = rdbmsSyncToolController.getJdbcUrlField2().getText();
            }
            dataSource = SqlUtil.getDataSource(dbType, dbIp, dbPort, dbName, dbUserName, dbUserPassword, jdbcUrl, rdbmsSyncToolController.getDataSourceTypeChoiceBox().getValue());
            try {
                if ("TABLE+VIEW".equals(rdbmsSyncToolController.getTableTypeChoiceBox().getValue())) {
                    tableNames = MetaUtil.getTables(dataSource, DataxJsonUtil.convertDatabaseCharsetType(dbUserName, rdbmsSyncToolController.getSchemaTextField().getText(), dbType), TableType.TABLE, TableType.VIEW);
                } else {
                    tableNames = MetaUtil.getTables(dataSource, DataxJsonUtil.convertDatabaseCharsetType(dbUserName, rdbmsSyncToolController.getSchemaTextField().getText(), dbType), TableType.valueOf(rdbmsSyncToolController.getTableTypeChoiceBox().getValue()));
                }
            } catch (Throwable e) {
                log.error("getTables is error!尝试使用sql语句获取", e);
                Connection connection = dataSource.getConnection();
                try {
                    if ("sqlserver".equals(dbType) || "sqlserverold".equals(dbType)) {
                        tableNames = SqlUtil.showSqlServerTables(connection, dbType);
                    } else {
                        tableNames = JdbcUtils.showTables(connection, dbType);
                    }
                } finally {
                    JdbcUtils.close(connection);
                }
            }
            log.info("获取到表名:" + tableNames);
            CheckBoxTreeItem<String> treeItem = (CheckBoxTreeItem<String>) tableTreeView.getRoot();
            treeItem.getChildren().clear();
            if (tableTreeView == rdbmsSyncToolController.getTableTreeView1()) {
                tableMap.clear();
            } else {
                tableMap2.clear();
            }
            List<String> ignoreTableNameList = null;
            if (StringUtils.isNotEmpty(rdbmsSyncToolController.getIgnoreTableNameTextField().getText())) {
                String[] ignoreTableNames = rdbmsSyncToolController.getIgnoreTableNameTextField().getText().toLowerCase().trim().split(",");
                ignoreTableNameList = Arrays.asList(ignoreTableNames);
            }
            Connection connection = dataSource.getConnection();
            try {
                for (String tableName : tableNames) {
                    if (ignoreTableNameList != null && !ignoreTableNameList.contains(tableName.toLowerCase())) {
                        continue;
                    }
                    final CheckBoxTreeItem<String> tableNameTreeItem = new CheckBoxTreeItem<>(tableName);
                    Table table = new Table(tableName);
                    try {
                        table = MetaUtil.getTableMeta(dataSource, tableName);
                    } catch (Throwable e) {
                        log.error("获取表结构失败！使用jdbc原生方法获取" + tableName, e);
                        String querySql = String.format("select * from %s where 1=2", tableName);
                        Statement statement = connection.createStatement();
                        ResultSet rs = statement.executeQuery(querySql);
                        ResultSetMetaData rsMetaData = rs.getMetaData();
                        for (int i = 1, len = rsMetaData.getColumnCount(); i <= len; i++) {
                            Column column = new Column();
                            column.setName(rsMetaData.getColumnName(i));
                            column.setType(rsMetaData.getColumnType(i));
                            column.setSize(rsMetaData.getColumnDisplaySize(i));
                            column.setComment(rsMetaData.getColumnLabel(i));
                            table.setColumn(column);
                        }
                    }
                    if (tableTreeView == rdbmsSyncToolController.getTableTreeView1()) {
                        tableMap.put(tableName, table);
                    } else {
                        tableMap2.put(tableName, table);
                    }
                    for (Column column : table.getColumns()) {
                        CheckBox isPkCheckBox = new CheckBox("Pk");
                        if (table.getPkNames().contains(column.getName())) {
                            isPkCheckBox.setSelected(true);
                        }
                        final CheckBoxTreeItem<String> columnNameTreeItem = new CheckBoxTreeItem<>(column.getName(), isPkCheckBox);
                        tableNameTreeItem.getChildren().add(columnNameTreeItem);
                    }
                    treeItem.getChildren().add(tableNameTreeItem);
                }
            } finally {
                JdbcUtils.close(connection);
            }
        } finally {
//            JdbcUtils.close(dataSource);
            if (dataSource != null) {
                dataSource.getConnection().close();
            }
        }
    }

    public void buildCloneAction() throws Exception {
    }

    public void buildStandardAction() throws Exception {
    }

    public void buildDbToCsvAction() throws Exception {
    }

    public void buildCsvToDbAction() throws Exception {
    }

    public List<CheckBoxTreeItem<String>> getCheckBoxTreeItemList() {
        List<TreeItem<String>> rootList = new ArrayList<>();
        rootList.addAll(rdbmsSyncToolController.getTableTreeView1().getRoot().getChildren());
        rootList.addAll(rdbmsSyncToolController.getTableTreeView2().getRoot().getChildren());
        List<CheckBoxTreeItem<String>> checkBoxTreeItemList = new ArrayList<>();
        for (TreeItem<String> treeItem : rootList) {
            CheckBoxTreeItem<String> tableNameTreeItem = (CheckBoxTreeItem<String>) treeItem;
            if (tableNameTreeItem.isSelected()) {
                checkBoxTreeItemList.add(tableNameTreeItem);
            }
        }
        return checkBoxTreeItemList;
    }

    public List<CheckBoxTreeItem<String>> getCheckBoxTreeItemListByTreeView(TreeView<String> tableTreeView) {
        List<CheckBoxTreeItem<String>> checkBoxTreeItemList = new ArrayList<>();
        for (TreeItem<String> treeItem : tableTreeView.getRoot().getChildren()) {
            CheckBoxTreeItem<String> tableNameTreeItem = (CheckBoxTreeItem<String>) treeItem;
            if (tableNameTreeItem.isSelected()) {
                checkBoxTreeItemList.add(tableNameTreeItem);
            }
        }
        return checkBoxTreeItemList;
    }

    public Map getTableInfoMap(ObservableList<TreeItem<String>> tableNameTreeItemList) {
        List columnList = new ArrayList();
        String splitPk = "";
        String where = "";
        for (TreeItem<String> childTreeItem : tableNameTreeItemList) {
            CheckBoxTreeItem<String> columnNameTreeItem = (CheckBoxTreeItem<String>) childTreeItem;
            HBox hBox = (HBox) columnNameTreeItem.getGraphic();
            CheckBox isPkCheckBox = (CheckBox) hBox.getChildren().get(0);
            CheckBox isTimeCheckBox = (CheckBox) hBox.getChildren().get(1);
            if (isPkCheckBox.isSelected()) {
                splitPk = columnNameTreeItem.getValue();
            }
            if (isTimeCheckBox.isSelected()) {
                where = columnNameTreeItem.getValue();
            }
            if (columnNameTreeItem.isSelected()) {
                columnList.add(columnNameTreeItem.getValue());
            }
        }
        Map tableInfoMap = new HashMap();
        tableInfoMap.put("columnList", columnList.toArray(new String[0]));
        tableInfoMap.put("splitPk", splitPk);
        tableInfoMap.put("where", where);
        return tableInfoMap;
    }

    public void buildQuerySqlAction() throws Exception {
    }

    public void buildTaskConfigClient(JSONObject jsonObject, String where, String taskName, String fileName) throws Exception {
    }

    public void buildCsvToGatewayAction(int type) throws Exception {
    }

    public void showSqlAction(String dbType) throws Exception {
        List<CheckBoxTreeItem<String>> checkBoxTreeItemList = getCheckBoxTreeItemList();
        if (checkBoxTreeItemList.isEmpty()) {
            TooltipUtil.showToast("未勾选表！");
            return;
        }
        for (CheckBoxTreeItem<String> tableNameTreeItem : checkBoxTreeItemList) {
            StringBuffer tableString = new StringBuffer();
            String tableName = tableNameTreeItem.getValue();
            Table table = null;
            if ("源端库表".equals(tableNameTreeItem.getParent().getValue())) {
                table = tableMap.get(tableName);
            } else {
                table = tableMap2.get(tableName);
            }
            if ("mysql".equals(dbType)) {
                tableString.append("DROP TABLE IF EXISTS `" + tableName + "`;\nCREATE TABLE `" + tableName + "` (\n");
                for (Column column : table.getColumns()) {
                    tableString.append("   `").append(column.getName()).append("` ")//cln name
                            .append(DataxJsonUtil.getTableColumnType(column.getType(), column.getSize()))//类型
//                        .append(" " + (column.isNullable() ? "NULL" : "NOT NULL"))//是否非空
                            .append(" NULL")//是否非空
                            //`import_time` datetime DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '数据插入时间'
//                        .append(clnName.equals("import_time") ? " DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP" : "")
//                        .append(clnName.equals("id") ? " AUTO_INCREMENT" : "")
//                        .append(" COLLATE utf8_bin COMMENT ")
                            .append(" COMMENT '")//注释
                            .append(column.getComment())//字段名（中文名称）
                            .append("',\n");
                }
                tableString.deleteCharAt(tableString.length() - 2);
//            tableString.append("PRIMARY KEY (`" + pk + "`)\n) ");
                tableString.append(")ENGINE = InnoDB\n" +
                        "DEFAULT CHARACTER SET = utf8\n" +
                        "COLLATE = utf8_bin\n");
                tableString.append("COMMENT = '" + table.getComment() + "'\n" +
                        "ROW_FORMAT = COMPACT;");
            } else {
                if (StringUtils.isNotEmpty(rdbmsSyncToolController.getSchemaTextField().getText())) {
                    tableName = rdbmsSyncToolController.getSchemaTextField().getText() + "." + tableName;
                }
                tableString.append("CREATE TABLE " + tableName + "(\n");
                StringBuffer commentStr = new StringBuffer();
                for (Column column : table.getColumns()) {
                    tableString.append("   ").append(column.getName()).append(" ")//cln name
                            .append(DataxJsonUtil.getTableColumnTypeByOracle(column.getType(), column.getSize()))//类型
                            .append(",\n");
                    if (StringUtils.isNotEmpty(column.getComment()) && !"null".equalsIgnoreCase(column.getComment())) {
                        commentStr.append("COMMENT ON COLUMN " + tableName + "." + column.getName() + " IS '" + column.getComment() + "';\n");
                    }
                }
                tableString.deleteCharAt(tableString.length() - 2);
                tableString.append(");\n");
                tableString.append(commentStr.toString());
            }
            String fileName = tableName + "_" + dbType + ".sql";
            if (rdbmsSyncToolController.getIsShowCheckBox().isSelected()) {
                TextArea textArea = new TextArea(tableString.toString());
                textArea.setWrapText(true);
                JavaFxViewUtil.openNewWindow(fileName, textArea);
            } else {
                String outputPath = StringUtils.defaultIfBlank(rdbmsSyncToolController.getOutputPathComboBox().getValue(), "./executor");
                File jsonFile = new File(outputPath, fileName);
                FileUtils.writeStringToFile(jsonFile, tableString.toString(), "utf-8");
                log.info("生成成功:" + jsonFile.getCanonicalPath());
                TooltipUtil.showToast("生成成功:" + jsonFile.getCanonicalPath());
            }
        }
    }

    public void testInstertSqlAction() throws Exception {
        List<CheckBoxTreeItem<String>> checkBoxTreeItemList = new ArrayList<>();
        List<CheckBoxTreeItem<String>> checkBoxTreeItemList2 = new ArrayList<>();
        for (TreeItem<String> treeItem : rdbmsSyncToolController.getTableTreeView1().getRoot().getChildren()) {
            CheckBoxTreeItem<String> tableNameTreeItem = (CheckBoxTreeItem<String>) treeItem;
            if (tableNameTreeItem.isSelected()) {
                checkBoxTreeItemList.add(tableNameTreeItem);
            }
        }
        for (TreeItem<String> treeItem : rdbmsSyncToolController.getTableTreeView2().getRoot().getChildren()) {
            CheckBoxTreeItem<String> tableNameTreeItem = (CheckBoxTreeItem<String>) treeItem;
            if (tableNameTreeItem.isSelected()) {
                checkBoxTreeItemList2.add(tableNameTreeItem);
            }
        }
        if (checkBoxTreeItemList.size() != 1 || checkBoxTreeItemList2.size() != 1) {
            TooltipUtil.showToast("两端各勾选一张表！");
            return;
        }
        CheckBoxTreeItem<String> tableNameTreeItem = checkBoxTreeItemList.get(0);
        Map tableInfoMap = getTableInfoMap(tableNameTreeItem.getChildren());
        CheckBoxTreeItem<String> tableNameTreeItem2 = checkBoxTreeItemList2.get(0);
        Map tableInfoMap2 = getTableInfoMap(tableNameTreeItem2.getChildren());
        String tableName = tableNameTreeItem.getValue();
        String tableName2 = tableNameTreeItem2.getValue();
        SimpleDataSource dataSource1 = SqlUtil.getDataSourceByViewType(rdbmsSyncToolController, rdbmsSyncToolController.getTableTreeView1());
        SimpleDataSource dataSource2 = SqlUtil.getDataSourceByViewType(rdbmsSyncToolController, rdbmsSyncToolController.getTableTreeView2());
        String[] columnList = (String[]) tableInfoMap.get("columnList");
        Entity entity1 = Entity.create(tableName);
        entity1.addFieldNames(columnList);
        try {
            List<Entity> entityList = null;
            if (rdbmsSyncToolController.getSyncDataNumberSpinner().getValue() == -1) {
                entityList = Db.use(dataSource1).findAll(tableName);
            } else {
                entityList = Db.use(dataSource1).pageForEntityList(Entity.create(tableName), 0, rdbmsSyncToolController.getSyncDataNumberSpinner().getValue());
            }
            Db db = Db.use(dataSource2);
            for (Entity entity : entityList) {
//                Entity entity2 = Entity.create(tableName2);
                String[] columnList2 = (String[]) tableInfoMap2.get("columnList");
                Object[] parameters = new Object[columnList2.length];
                List<String> valueHolders = new ArrayList<>(columnList2.length);
                for (int i = 0; i < columnList2.length; i++) {
//                    entity2.set(columnList2[i], entity.get(columnList[i]));
                    parameters[i] = entity.get(columnList[i]);
                    valueHolders.add("?");
                }
//                db.insert(entity2);
                String sql = "INSERT INTO " + tableName2 + " (" + StringUtils.join(columnList2, ",") + ") VALUES(" + StringUtils.join(valueHolders, ",") + ")";
                JdbcUtils.executeUpdate(dataSource2, sql, parameters);
            }
        } finally {
            JdbcUtils.close(dataSource1);
            JdbcUtils.close(dataSource2);
        }
        TooltipUtil.showToast("生成测试同步数据完成");
    }

    public void showTableData(String tableName, TreeView<String> tableTreeView) {
        SimpleDataSource dataSource = SqlUtil.getDataSourceByViewType(rdbmsSyncToolController, tableTreeView);
        try {
            List<Entity> entityList = null;
            if (StringUtils.isNotEmpty(rdbmsSyncToolController.getSchemaTextField().getText())) {
                tableName = rdbmsSyncToolController.getSchemaTextField().getText() + "." + tableName;
            }
            if (rdbmsSyncToolController.getSyncDataNumberSpinner().getValue() == -1) {
                entityList = Db.use(dataSource).findAll(tableName);
            } else {
                entityList = Db.use(dataSource).pageForEntityList(Entity.create(tableName), 0, rdbmsSyncToolController.getSyncDataNumberSpinner().getValue());
            }
            TableView tableView = null;
            ObservableList<Map<String, String>> tableData = FXCollections.observableArrayList();
            for (Entity entity : entityList) {
                if (tableView == null) {
                    tableView = new TableView();
                    tableView.setEditable(true);
                    for (String fieldName : entity.getFieldNames()) {
                        TableColumn tableColumn = new TableColumn(fieldName);
                        JavaFxViewUtil.setTableColumnMapValueFactory(tableColumn, fieldName);
                        tableView.getColumns().add(tableColumn);
                    }
                    tableView.setItems(tableData);
                }
                Map<String, String> map = new HashMap<>();
                for (String fieldName : entity.getFieldNames()) {
                    map.put(fieldName, entity.getStr(fieldName));
                }
                tableData.add(map);
            }
            if (tableView == null) {
                TooltipUtil.showToast("表中无数据！");
            } else {
                JavaFxViewUtil.openNewWindow(tableName, tableView);
            }
        } catch (Exception e) {
            log.error("显示表数据错误：", e);
            TooltipUtil.showToast("显示表数据错误：" + e.getMessage());
        } finally {
            JdbcUtils.close(dataSource);
        }
    }

    public void copySelectSql(CheckBoxTreeItem<String> selectedItem, TreeView<String> tableTreeView, boolean isMysql) {
        if (selectedItem == null) {
            List<CheckBoxTreeItem<String>> checkBoxTreeItemList = getCheckBoxTreeItemListByTreeView(tableTreeView);
            if (checkBoxTreeItemList.isEmpty()) {
                TooltipUtil.showToast("未勾选表！");
                return;
            }
            StringBuffer stringBuffer = new StringBuffer();
            for (CheckBoxTreeItem<String> tableNameTreeItem : checkBoxTreeItemList) {
                String selectSql = SqlUtil.createrSelectSql(this, tableNameTreeItem, tableNameTreeItem.getValue(), isMysql);
                stringBuffer.append(selectSql).append(";\n");
            }
            StringSelection selection = new StringSelection(stringBuffer.toString());
            Toolkit.getDefaultToolkit().getSystemClipboard().setContents(selection, selection);
            TooltipUtil.showToast("复制查询语句成功！" + stringBuffer.toString());
        } else {
            if (!selectedItem.isSelected()) {
                TooltipUtil.showToast("未勾选表！");
                return;
            }
            String selectSql = SqlUtil.createrSelectSql(this, selectedItem, selectedItem.getValue(), isMysql);
            StringSelection selection = new StringSelection(selectSql);
            // 获取系统剪切板，复制表名
            Toolkit.getDefaultToolkit().getSystemClipboard().setContents(selection, selection);
            TooltipUtil.showToast("复制查询语句成功！" + selectSql);
        }
    }

    public void selectTableCount(String tableName, TreeView<String> tableTreeView) {
        if ("*".equals(tableName)) {
            List<String> selectTableNameList = getSelectTableNameList(tableTreeView);
            if (selectTableNameList.isEmpty()) {
                TooltipUtil.showToast("未勾选表！");
                return;
            }
            TableView tableView = new TableView();
            ObservableList<Map<String, String>> tableData = FXCollections.observableArrayList();
            tableView.setEditable(true);
            TableColumn tableColumn = new TableColumn("表名");
            JavaFxViewUtil.setTableColumnMapValueFactory(tableColumn, "table");
            TableColumn sqlColumn = new TableColumn("sql");
            JavaFxViewUtil.setTableColumnMapValueFactory(sqlColumn, "sql");
            TableColumn countColumn = new TableColumn("数量");
            JavaFxViewUtil.setTableColumnMapValueFactory(countColumn, "count");
            tableView.getColumns().add(tableColumn);
            tableView.getColumns().add(sqlColumn);
            tableView.getColumns().add(countColumn);
            tableView.setItems(tableData);
            for (String selectTableName : selectTableNameList) {
                if (StringUtils.isNotEmpty(rdbmsSyncToolController.getSchemaTextField().getText())) {
                    selectTableName = rdbmsSyncToolController.getSchemaTextField().getText() + "." + selectTableName;
                }
                Map<String, String> tableMap = new HashMap<>();
                tableMap.put("table", selectTableName);
                tableMap.put("sql", "select count(*) from " + selectTableName + ";");
                List<Map<String, Object>> queryData = SqlUtil.executeQuerySql(rdbmsSyncToolController, tableTreeView, "select count(*) from " + selectTableName);
                if (queryData != null && !queryData.isEmpty()) {
                    Map<String, Object> map = queryData.get(0);
                    tableMap.put("count", "" + map.values().toArray()[0]);
                }
                tableData.add(tableMap);
            }
            JavaFxViewUtil.openNewWindow("查询表数量", tableView);
        } else {
            if (StringUtils.isNotEmpty(rdbmsSyncToolController.getSchemaTextField().getText())) {
                tableName = rdbmsSyncToolController.getSchemaTextField().getText() + "." + tableName;
            }
            List<Map<String, Object>> queryData = SqlUtil.executeQuerySql(rdbmsSyncToolController, tableTreeView, "select count(*) from " + tableName);
            if (queryData != null && !queryData.isEmpty()) {
                Map<String, Object> map = queryData.get(0);
                AlertUtil.showInfoAlert(tableName, "select count(*) from " + tableName + "\n\n数量：" + map.values().toArray()[0]);
            }
        }
    }

    public void dropTable(String tableName, TreeView<String> tableTreeView) {
        boolean isOk = AlertUtil.confirmOkCancel("提示", "确定要drop table吗？");
        if (!isOk) {
            return;
        }
        if ("*".equals(tableName)) {
            List<String> selectTableNameList = getSelectTableNameList(tableTreeView);
            if (selectTableNameList.isEmpty()) {
                TooltipUtil.showToast("未勾选表！");
                return;
            }
            for (String selectTableName : selectTableNameList) {
                SqlUtil.executeSql(rdbmsSyncToolController, tableTreeView, "drop table " + selectTableName);
            }
        } else {
            SqlUtil.executeSql(rdbmsSyncToolController, tableTreeView, "drop table " + tableName);
        }
    }

    public void deleteTableData(String tableName, TreeView<String> tableTreeView) {
        boolean isOk = AlertUtil.confirmOkCancel("提示", "确定要deleteTableData吗？");
        if (!isOk) {
            return;
        }
        if ("*".equals(tableName)) {
            List<String> selectTableNameList = getSelectTableNameList(tableTreeView);
            if (selectTableNameList.isEmpty()) {
                TooltipUtil.showToast("未勾选表！");
                return;
            }
            for (String selectTableName : selectTableNameList) {
                SqlUtil.executeSql(rdbmsSyncToolController, tableTreeView, "delete from " + selectTableName);
            }
        } else {
            SqlUtil.executeSql(rdbmsSyncToolController, tableTreeView, "delete from " + tableName);
        }
    }

    public void truncateTableData(String tableName, TreeView<String> tableTreeView) {
        boolean isOk = AlertUtil.confirmOkCancel("提示", "确定要truncateTableData吗？");
        if (!isOk) {
            return;
        }
        if ("*".equals(tableName)) {
            List<String> selectTableNameList = getSelectTableNameList(tableTreeView);
            if (selectTableNameList.isEmpty()) {
                TooltipUtil.showToast("未勾选表！");
                return;
            }
            for (String selectTableName : selectTableNameList) {
                SqlUtil.executeSql(rdbmsSyncToolController, tableTreeView, "truncate table " + selectTableName);
            }
        } else {
            SqlUtil.executeSql(rdbmsSyncToolController, tableTreeView, "truncate table " + tableName);
        }
    }

    public void executeSql(TreeView<String> tableTreeView) {
        TextArea textArea = new TextArea();
        FxDialog<TextArea> dialog = new FxDialog<TextArea>();
        dialog.setTitle("请输入sql").setBody(textArea).setButtonTypes(ButtonType.OK, ButtonType.CANCEL);
        AtomicReference<Boolean> isExecuteSql = new AtomicReference<>(false);
        dialog.setButtonHandler(ButtonType.OK, (actionEvent, stage) -> {
            isExecuteSql.set(true);
            stage.close();
        }).setButtonHandler(ButtonType.CANCEL, (actionEvent, stage) -> stage.close());
        dialog.showAndWait();
        if (!isExecuteSql.get()) {
            return;
        }
        String sqlSb = textArea.getText();
        if (StringUtils.isEmpty(sqlSb)) {
            TooltipUtil.showToast("执行sql不能为空");
            return;
        }
//        List<String> sqlList = new ArrayList<String>();
        // Windows 下换行是 /r/n, Linux 下是 /n
//        String[] sqlArr = sqlSb.split("(;//s*//r//n)|(;//s*//n)");
//        for (int i = 0; i < sqlArr.length; i++) {
//            String sql = sqlArr[i].replaceAll("--.*", "").trim();
//            if (!sql.equals("")) {
//                sqlList.add(sql);
//            }
//        }
        SqlUtil.executeSql(rdbmsSyncToolController, tableTreeView, sqlSb);
    }

    public void showCreateUserSqlAction() throws Exception {
        List<CheckBoxTreeItem<String>> checkBoxTreeItemList = getCheckBoxTreeItemList();
        if (checkBoxTreeItemList.isEmpty()) {
            TooltipUtil.showToast("未勾选表！");
            return;
        }
        for (CheckBoxTreeItem<String> tableNameTreeItem : checkBoxTreeItemList) {
            String tableName = tableNameTreeItem.getValue();
            String tableString = SqlUtil.createUserSql(rdbmsSyncToolController, tableName);
            String fileName = tableName + "_createUser.sql";
            if (rdbmsSyncToolController.getIsShowCheckBox().isSelected()) {
                TextArea textArea = new TextArea(tableString.toString());
                textArea.setWrapText(true);
                JavaFxViewUtil.openNewWindow(fileName, textArea);
            } else {
                String outputPath = StringUtils.defaultIfBlank(rdbmsSyncToolController.getOutputPathComboBox().getValue(), "./executor");
                File jsonFile = new File(outputPath, fileName);
                FileUtils.writeStringToFile(jsonFile, tableString.toString(), "utf-8");
                log.info("生成成功:" + jsonFile.getCanonicalPath());
                TooltipUtil.showToast("生成成功:" + jsonFile.getCanonicalPath());
            }
        }
    }

    public List<String> getSelectTableNameList(TreeView<String> tableTreeView) {
        List<String> selectTableNameList = new ArrayList<>();
        for (TreeItem<String> treeItem : tableTreeView.getRoot().getChildren()) {
            CheckBoxTreeItem<String> tableNameTreeItem = (CheckBoxTreeItem<String>) treeItem;
            if (tableNameTreeItem.isSelected()) {
                selectTableNameList.add(tableNameTreeItem.getValue());
            }
        }
        return selectTableNameList;
    }

}
