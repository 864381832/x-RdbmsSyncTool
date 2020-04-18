package com.xwintop.xJavaFxTool.services.debugTools;

import cn.hutool.db.Db;
import cn.hutool.db.Entity;
import cn.hutool.db.meta.Column;
import cn.hutool.db.meta.MetaUtil;
import cn.hutool.db.meta.Table;
import cn.hutool.db.meta.TableType;
import com.alibaba.druid.pool.DruidDataSource;
import com.xwintop.xJavaFxTool.controller.debugTools.RdbmsSyncToolController;
import com.xwintop.xJavaFxTool.tools.SqlUtil;
import com.xwintop.xcore.util.javafx.JavaFxViewUtil;
import com.xwintop.xcore.util.javafx.TooltipUtil;
import javafx.collections.ObservableList;
import javafx.scene.control.*;
import javafx.scene.layout.HBox;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.FileUtils;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Getter
@Setter
@Slf4j
public class RdbmsSyncToolService {
    private RdbmsSyncToolController entDataToolController;
    private Map<String, Table> tableMap = new HashMap<>();

    public RdbmsSyncToolService(RdbmsSyncToolController entDataToolController) {
        this.entDataToolController = entDataToolController;
    }

    //连接数据库
    public void connectAction(String dbType, String dbIp, String dbPort, String dbName, String dbUserName, String dbUserPassword, TreeView<String> tableTreeView) throws Exception {
        DruidDataSource dataSource = SqlUtil.getDruidDataSource(dbType, dbIp, dbPort, dbName, dbUserName, dbUserPassword);
        try {
            List<String> tableNames = MetaUtil.getTables(dataSource, SqlUtil.convertDatabaseCharsetType(dbUserName, dbType), TableType.TABLE);
            log.info("获取到表名:" + tableNames);
            CheckBoxTreeItem<String> treeItem = (CheckBoxTreeItem<String>) tableTreeView.getRoot();
            treeItem.getChildren().clear();
            tableMap.clear();
            for (String tableName : tableNames) {
                final CheckBoxTreeItem<String> tableNameTreeItem = new CheckBoxTreeItem<>(tableName);
                treeItem.getChildren().add(tableNameTreeItem);
                Table table = MetaUtil.getTableMeta(dataSource, tableName);
                tableMap.put(tableName, table);
                for (Column column : table.getColumns()) {
                    CheckBox isPkCheckBox = new CheckBox("Pk");
                    if (table.getPkNames().size() == 1) {
                        if (column.getName().equals(table.getPkNames().toArray()[0])) {
                            isPkCheckBox.setSelected(true);
                        }
                    }
                    CheckBox isTimeCheckBox = new CheckBox("Time");
                    HBox hBox = new HBox();
                    hBox.getChildren().addAll(isPkCheckBox, isTimeCheckBox);
                    final CheckBoxTreeItem<String> columnNameTreeItem = new CheckBoxTreeItem<>(column.getName(), hBox);
                    tableNameTreeItem.getChildren().add(columnNameTreeItem);
                }
            }
        } finally {
            if (dataSource != null) {
                dataSource.close();
            }
        }
    }

    public List<CheckBoxTreeItem<String>> getCheckBoxTreeItemList() {
        List<TreeItem<String>> rootList = new ArrayList<>();
        rootList.addAll(entDataToolController.getTableTreeView1().getRoot().getChildren());
        rootList.addAll(entDataToolController.getTableTreeView2().getRoot().getChildren());
        List<CheckBoxTreeItem<String>> checkBoxTreeItemList = new ArrayList<>();
        for (TreeItem<String> treeItem : rootList) {
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

    public void showSqlAction() throws Exception {
        List<CheckBoxTreeItem<String>> checkBoxTreeItemList = getCheckBoxTreeItemList();
        if (checkBoxTreeItemList.isEmpty()) {
            TooltipUtil.showToast("未勾选表！");
            return;
        }
        for (CheckBoxTreeItem<String> tableNameTreeItem : checkBoxTreeItemList) {
            StringBuffer tableString = new StringBuffer();
            String tableName = tableNameTreeItem.getValue();
            Table table = tableMap.get(tableName);
            tableString.append("DROP TABLE IF EXISTS `" + tableName + "`;\nCREATE TABLE `" + tableName + "` (\n");
            for (Column column : table.getColumns()) {
                tableString.append("   `").append(column.getName()).append("` ")//cln name
                        .append(SqlUtil.getTableColumnType(column.getType(), column.getSize()))//类型
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
            tableString.append("INDBTIME timestamp DEFAULT CURRENT_TIMESTAMP COMMENT '入库时间'\n");
//            tableString.deleteCharAt(tableString.length() - 2);
//            tableString.append("PRIMARY KEY (`" + pk + "`)\n) ");
            tableString.append(")ENGINE = InnoDB\n" +
                    "DEFAULT CHARACTER SET = utf8\n" +
                    "COLLATE = utf8_bin\n");
//            if (StringUtils.isNotEmpty(table.getComment())) {
            tableString.append("COMMENT = '" + table.getComment() + "'\n" +
                    "ROW_FORMAT = COMPACT;");
//            }
            if (entDataToolController.getIsShowCheckBox().isSelected()) {
                TextArea textArea = new TextArea(tableString.toString());
                textArea.setWrapText(true);
                JavaFxViewUtil.openNewWindow(tableName, textArea);
            } else {
                File jsonFile = new File("./executor", tableName + ".sql");
                FileUtils.writeStringToFile(jsonFile, tableString.toString(), "utf-8");
                log.info("生成成功:" + jsonFile.getCanonicalPath());
                TooltipUtil.showToast("生成成功:" + jsonFile.getCanonicalPath());
            }
        }
    }

    public void testInstertSqlAction() throws Exception {
        List<CheckBoxTreeItem<String>> checkBoxTreeItemList = new ArrayList<>();
        List<CheckBoxTreeItem<String>> checkBoxTreeItemList2 = new ArrayList<>();
        for (TreeItem<String> treeItem : entDataToolController.getTableTreeView1().getRoot().getChildren()) {
            CheckBoxTreeItem<String> tableNameTreeItem = (CheckBoxTreeItem<String>) treeItem;
            if (tableNameTreeItem.isSelected()) {
                checkBoxTreeItemList.add(tableNameTreeItem);
            }
        }
        for (TreeItem<String> treeItem : entDataToolController.getTableTreeView2().getRoot().getChildren()) {
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
        String dbType = entDataToolController.getDbTypeText1().getValue();
        String tableName = tableNameTreeItem.getValue();
        String tableName2 = tableNameTreeItem2.getValue();
        String dbIp = entDataToolController.getHostText1().getText();
        String dbPort = entDataToolController.getPortText1().getText();
        String dbName = entDataToolController.getDbNameText1().getText();
        String dbUserName = entDataToolController.getUserNameText1().getText();
        String dbUserPassword = entDataToolController.getPwdText1().getText();
        String dbType2 = entDataToolController.getDbTypeText1().getValue();
        String dbIp2 = entDataToolController.getHostText2().getText();
        String dbPort2 = entDataToolController.getPortText2().getText();
        String dbName2 = entDataToolController.getDbNameText2().getText();
        String dbUserName2 = entDataToolController.getUserNameText2().getText();
        String dbUserPassword2 = entDataToolController.getPwdText2().getText();
        DruidDataSource dataSource1 = SqlUtil.getDruidDataSource(dbType, dbIp, dbPort, dbName, dbUserName, dbUserPassword);
        DruidDataSource dataSource2 = SqlUtil.getDruidDataSource(dbType2, dbIp2, dbPort2, dbName2, dbUserName2, dbUserPassword2);
        String[] columnList = (String[]) tableInfoMap.get("columnList");
        Entity entity1 = Entity.create(tableName);
        entity1.addFieldNames(columnList);
        try {
            List<Entity> entityList = null;
            if (entDataToolController.getSyncDataNumberSpinner().getValue() == -1) {
                entityList = Db.use(dataSource1).findAll(tableName);
            } else {
                entityList = Db.use(dataSource1).pageForEntityList(Entity.create(tableName), 0, entDataToolController.getSyncDataNumberSpinner().getValue());
            }
            Db db = Db.use(dataSource2);
            for (Entity entity : entityList) {
                Entity entity2 = Entity.create(tableName2);
                String[] columnList2 = (String[]) tableInfoMap2.get("columnList");
                for (int i = 0; i < columnList2.length; i++) {
                    entity2.set(columnList2[i], entity.get(columnList[i]));
                }
                db.insert(entity2);
            }
        } finally {
            if (dataSource1 != null) {
                dataSource1.close();
            }
            if (dataSource2 != null) {
                dataSource2.close();
            }
        }
        TooltipUtil.showToast("生成测试同步数据完成");
    }
}
