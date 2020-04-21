package com.xwintop.xJavaFxTool.controller.debugTools;

import cn.hutool.core.swing.clipboard.ClipboardUtil;
import cn.hutool.core.thread.ThreadUtil;
import com.xwintop.xJavaFxTool.services.debugTools.RdbmsSyncToolService;
import com.xwintop.xJavaFxTool.services.debugTools.UrlDocumentDialogService;
import com.xwintop.xJavaFxTool.tools.DataxJsonUtil;
import com.xwintop.xJavaFxTool.view.debugTools.RdbmsSyncToolView;
import com.xwintop.xcore.util.javafx.JavaFxViewUtil;
import com.xwintop.xcore.util.javafx.TooltipUtil;
import javafx.event.ActionEvent;
import javafx.fxml.FXML;
import javafx.fxml.FXMLLoader;
import javafx.scene.control.*;
import javafx.scene.control.cell.CheckBoxTreeCell;
import javafx.scene.input.MouseButton;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.controlsfx.control.MaskerPane;

import java.net.URL;
import java.util.List;
import java.util.Map;
import java.util.ResourceBundle;

@Getter
@Setter
@Slf4j
public class RdbmsSyncToolController extends RdbmsSyncToolView {
    private RdbmsSyncToolService entDataToolService = new RdbmsSyncToolService(this);
    private ContextMenu contextMenu = new ContextMenu();
    private String[] dbTypeStrings = new String[]{"mysql", "oracle", "oracleSid", "postgresql", "sqlserver", "sqlserverold", "dm", "sqlite", "h2Embedded", "h2Server","access"};
    private String[] jsonNameSuffixStrings = new String[]{".json"};
    private String[] outputPathStrings = new String[]{"./executor"};
    private String[] quartzChoiceBoxStrings = new String[]{"SIMPLE", "CRON"};
    private String[] tableTypeStrings = new String[]{"TABLE+VIEW", "TABLE", "VIEW", "SYSTEM_TABLE", "GLOBAL_TEMPORARY", "LOCAL_TEMPORARY", "ALIAS", "SYNONYM"};
    private String[] dataSourceTypeStrings = new String[]{"Druid", "Driver", "Simple"};
    private MaskerPane masker = new MaskerPane();

    @Override
    public void initialize(URL location, ResourceBundle resources) {
        initView();
        initEvent();
        initService();
    }

    private void initView() {
        masker.setVisible(false);
        loadingStackPane.getChildren().add(masker);
        hostText1.setText("192.168.129.121");
        dbNameText1.setText("test");
        pwdText1.setText("easipass");

        JavaFxViewUtil.setPasswordTextFieldFactory(pwdText1);
        JavaFxViewUtil.setPasswordTextFieldFactory(pwdText2);
        dbTypeText1.getItems().addAll(dbTypeStrings);
        dbTypeText1.setValue(dbTypeStrings[0]);
        dbTypeText2.getItems().addAll(dbTypeStrings);
        dbTypeText2.setValue(dbTypeStrings[0]);
        jsonNameSuffixComboBox.getItems().addAll(jsonNameSuffixStrings);
        outputPathComboBox.getItems().addAll(outputPathStrings);

        tableTreeView1.setCellFactory(CheckBoxTreeCell.<String>forTreeView());
        tableTreeView1.setRoot(new CheckBoxTreeItem<>("源端库表"));
        tableTreeView1.getRoot().setExpanded(true);
        tableTreeView1.setShowRoot(true);
        tableTreeView2.setCellFactory(CheckBoxTreeCell.<String>forTreeView());
        tableTreeView2.setRoot(new CheckBoxTreeItem<>("目标端库表"));
        tableTreeView2.getRoot().setExpanded(true);
        tableTreeView2.setShowRoot(true);
        JavaFxViewUtil.setSpinnerValueFactory(syncDataNumberSpinner, -1, Integer.MAX_VALUE, 10);

        quartzChoiceBox.getItems().addAll(quartzChoiceBoxStrings);
        quartzChoiceBox.getSelectionModel().select(0);
        JavaFxViewUtil.setSpinnerValueFactory(intervalSpinner, 1, Integer.MAX_VALUE, 5);
        JavaFxViewUtil.setSpinnerValueFactory(repeatCountSpinner, -1, Integer.MAX_VALUE, 0);

        tableTypeChoiceBox.getItems().addAll(tableTypeStrings);
        tableTypeChoiceBox.setValue(tableTypeChoiceBox.getItems().get(1));
        dataSourceTypeChoiceBox.getItems().addAll(dataSourceTypeStrings);
        dataSourceTypeChoiceBox.setValue(dataSourceTypeChoiceBox.getItems().get(0));
    }

    private void initEvent() {
        addUrlDocumentDialogController(hostText1);
        addUrlDocumentDialogController(hostText2);
        addTableTreeViewMouseClicked(tableTreeView1);
        addTableTreeViewMouseClicked(tableTreeView2);
        quartzChoiceBox.valueProperty().addListener((observable, oldValue, newValue) -> {
            if (quartzChoiceBoxStrings[0].equals(newValue)) {
                cronTextField.setVisible(false);
                simpleScheduleAnchorPane.setVisible(true);
            } else if (quartzChoiceBoxStrings[1].equals(newValue)) {
                cronTextField.setVisible(true);
                simpleScheduleAnchorPane.setVisible(false);
            }
        });
        dbTypeText1.valueProperty().addListener((observable, oldValue, newValue) -> {
            portText1.setText(DataxJsonUtil.getDbDefaultPort(newValue));
        });
        dbTypeText2.valueProperty().addListener((observable, oldValue, newValue) -> {
            portText2.setText(DataxJsonUtil.getDbDefaultPort(newValue));
        });
    }

    private void addTableTreeViewMouseClicked(TreeView<String> tableTreeView) {
        tableTreeView.setOnMouseClicked(event -> {
            TreeItem<String> selectedItem = tableTreeView.getSelectionModel().getSelectedItem();
            if (selectedItem == null) {
                return;
            }
            if (event.getButton() == MouseButton.PRIMARY) {
                selectedItem.setExpanded(!selectedItem.isExpanded());
            } else if (event.getButton() == MouseButton.SECONDARY) {
                MenuItem menu_UnfoldAll = new MenuItem("展开所有");
                menu_UnfoldAll.setOnAction(event1 -> {
                    tableTreeView.getRoot().setExpanded(true);
                    tableTreeView.getRoot().getChildren().forEach(stringTreeItem -> {
                        stringTreeItem.setExpanded(true);
                    });
                });
                MenuItem menu_FoldAll = new MenuItem("折叠所有");
                menu_FoldAll.setOnAction(event1 -> {
                    tableTreeView.getRoot().getChildren().forEach(stringTreeItem -> {
                        stringTreeItem.setExpanded(false);
                    });
                });
                MenuItem menu_executeSql = new MenuItem("执行Sql");
                menu_executeSql.setOnAction(event1 -> {
                    entDataToolService.executeSql(tableTreeView);
                });
                ContextMenu contextMenu = new ContextMenu(menu_UnfoldAll, menu_FoldAll, menu_executeSql);
                if ("源端库表".equals(selectedItem.getValue()) || "目标端库表".equals(selectedItem.getValue())) {
                    MenuItem menu_copySelectSql = new MenuItem("一键复制查询语句");
                    menu_copySelectSql.setOnAction(event1 -> {
                        entDataToolService.copySelectSql(null, tableTreeView, false);
                    });
                    contextMenu.getItems().add(menu_copySelectSql);
                    MenuItem menu_copySelectSqlMysql = new MenuItem("一键复制查询语句Mysql");
                    menu_copySelectSqlMysql.setOnAction(event1 -> {
                        entDataToolService.copySelectSql(null, tableTreeView, true);
                    });
                    contextMenu.getItems().add(menu_copySelectSqlMysql);
                    MenuItem menu_copyCreateTableSqlMysql = new MenuItem("一键生成建表语句Mysql");
                    menu_copyCreateTableSqlMysql.setOnAction(event1 -> {
                        entDataToolService.showSqlAction("mysql");
                    });
                    contextMenu.getItems().add(menu_copyCreateTableSqlMysql);
                    MenuItem menu_copyCreateTableSqlOracle = new MenuItem("一键生成建表语句Oracle");
                    menu_copyCreateTableSqlOracle.setOnAction(event1 -> {
                        entDataToolService.showSqlAction("oracle");
                    });
                    contextMenu.getItems().add(menu_copyCreateTableSqlOracle);
                    MenuItem menu_selectTableCount = new MenuItem("一键查看表中数据量");
                    menu_selectTableCount.setOnAction(event1 -> {
                        entDataToolService.selectTableCount("*", tableTreeView);
                    });
                    contextMenu.getItems().add(menu_selectTableCount);
                    MenuItem menu_DropTable = new MenuItem("一键Drop删除表结构");
                    menu_DropTable.setOnAction(event1 -> {
                        entDataToolService.dropTable("*", tableTreeView);
                    });
                    contextMenu.getItems().add(menu_DropTable);
                    MenuItem menu_deleteTable = new MenuItem("一键delete删除表数据");
                    menu_deleteTable.setOnAction(event1 -> {
                        entDataToolService.deleteTableData("*", tableTreeView);
                    });
                    contextMenu.getItems().add(menu_deleteTable);
                    MenuItem menu_TruncateTable = new MenuItem("一键truncate删除表数据");
                    menu_TruncateTable.setOnAction(event1 -> {
                        entDataToolService.truncateTableData("*", tableTreeView);
                    });
                    contextMenu.getItems().add(menu_TruncateTable);
                } else {
                    if ("源端库表".equals(selectedItem.getParent().getValue()) || "目标端库表".equals(selectedItem.getParent().getValue())) {
                        MenuItem menu_copyTableName = new MenuItem("复制表名");
                        menu_copyTableName.setOnAction(event1 -> {
                            ClipboardUtil.setStr(selectedItem.getValue());
                        });
                        contextMenu.getItems().add(menu_copyTableName);
                        MenuItem menu_copySelectSql = new MenuItem("复制查询语句");
                        menu_copySelectSql.setOnAction(event1 -> {
                            entDataToolService.copySelectSql((CheckBoxTreeItem<String>) selectedItem, tableTreeView, false);
                        });
                        contextMenu.getItems().add(menu_copySelectSql);
                        MenuItem menu_copySelectSqlMysql = new MenuItem("复制查询语句mysql");
                        menu_copySelectSqlMysql.setOnAction(event1 -> {
                            entDataToolService.copySelectSql((CheckBoxTreeItem<String>) selectedItem, tableTreeView, true);
                        });
                        contextMenu.getItems().add(menu_copySelectSqlMysql);
                        MenuItem menu_selectTableCount = new MenuItem("查看表中数据量");
                        menu_selectTableCount.setOnAction(event1 -> {
                            entDataToolService.selectTableCount(selectedItem.getValue(), tableTreeView);
                        });
                        contextMenu.getItems().add(menu_selectTableCount);
                        MenuItem menu_ViewTable = new MenuItem("查看表内容");
                        menu_ViewTable.setOnAction(event1 -> {
                            entDataToolService.showTableData(selectedItem.getValue(), tableTreeView);
                        });
                        contextMenu.getItems().add(menu_ViewTable);
                        MenuItem menu_DropTable = new MenuItem("Drop删除表结构");
                        menu_DropTable.setOnAction(event1 -> {
                            entDataToolService.dropTable(selectedItem.getValue(), tableTreeView);
                        });
                        contextMenu.getItems().add(menu_DropTable);
                        MenuItem menu_deleteTable = new MenuItem("delete删除表数据");
                        menu_deleteTable.setOnAction(event1 -> {
                            entDataToolService.deleteTableData(selectedItem.getValue(), tableTreeView);
                        });
                        contextMenu.getItems().add(menu_deleteTable);
                        MenuItem menu_TruncateTable = new MenuItem("truncate删除表数据");
                        menu_TruncateTable.setOnAction(event1 -> {
                            entDataToolService.truncateTableData(selectedItem.getValue(), tableTreeView);
                        });
                        contextMenu.getItems().add(menu_TruncateTable);
                    } else {
                        MenuItem menu_copyTableName = new MenuItem("复制字段名");
                        menu_copyTableName.setOnAction(event1 -> {
                            ClipboardUtil.setStr(selectedItem.getValue());
                        });
                        contextMenu.getItems().add(menu_copyTableName);
                    }
                }

                tableTreeView.setContextMenu(contextMenu);
            }
        });
    }

    private void initService() {
    }

    private void addUrlDocumentDialogController(TextField hostText) {
        hostText.setOnMouseClicked(event -> {
            if (contextMenu.isShowing()) {
                contextMenu.hide();
            }
            contextMenu.getItems().clear();
            List<Map<String, String>> list = UrlDocumentDialogService.getConfig();
            if (list != null) {
                for (Map<String, String> map : list) {
                    MenuItem menu_tab = new MenuItem(map.get("name"));
                    menu_tab.setOnAction(event1 -> {
                        hostText.setText(map.get("host"));
                        if (hostText == hostText1) {
                            portText1.setText(map.get("port"));
                            dbNameText1.setText(map.get("dbName"));
                            dbTypeText1.setValue(map.get("dbType"));
                            userNameText1.setText(map.get("userName"));
                            pwdText1.setText(map.get("password"));
                        } else if (hostText == hostText2) {
                            portText2.setText(map.get("port"));
                            dbNameText2.setText(map.get("dbName"));
                            dbTypeText2.setValue(map.get("dbType"));
                            userNameText2.setText(map.get("userName"));
                            pwdText2.setText(map.get("password"));
                        }
                    });
                    contextMenu.getItems().add(menu_tab);
                }
            }
            MenuItem menu_tab = new MenuItem("编辑历史连接");
            menu_tab.setOnAction(event1 -> {
                try {
                    FXMLLoader fXMLLoader = UrlDocumentDialogController.getFXMLLoader();
                    JavaFxViewUtil.openNewWindow("历史连接编辑", fXMLLoader.load());
                } catch (Exception e) {
                    log.error("加载历史连接编辑界面失败", e);
                }
            });
            contextMenu.getItems().add(menu_tab);
            contextMenu.show(hostText, null, 0, hostText.getHeight());
        });
    }

    @FXML
    private void connectAction1(ActionEvent event) {
        masker.setVisible(true);
        ThreadUtil.execute(() -> {
            try {
                UrlDocumentDialogService.addConfig(hostText1.getText(), portText1.getText(), userNameText1.getText(), pwdText1.getText(), dbNameText1.getText(), dbTypeText1.getValue());
                entDataToolService.connectAction(dbTypeText1.getValue(), hostText1.getText(), portText1.getText(), dbNameText1.getText(), userNameText1.getText(), pwdText1.getText(), tableTreeView1);
            } catch (Exception e) {
                log.error("连接失败：", e);
            } finally {
                masker.setVisible(false);
            }
        });
    }

    @FXML
    private void connectAction2(ActionEvent event) {
        masker.setVisible(true);
        ThreadUtil.execute(() -> {
            try {
                UrlDocumentDialogService.addConfig(hostText2.getText(), portText2.getText(), userNameText2.getText(), pwdText2.getText(), dbNameText2.getText(), dbTypeText2.getValue());
                entDataToolService.connectAction(dbTypeText2.getValue(), hostText2.getText(), portText2.getText(), dbNameText2.getText(), userNameText2.getText(), pwdText2.getText(), tableTreeView2);
            } catch (Exception e) {
                log.error("连接失败：", e);
            } finally {
                masker.setVisible(false);
            }
        });
    }

    @FXML
    private void testInstertSqlAction(ActionEvent event) {
        try {
            entDataToolService.testInstertSqlAction();
        } catch (Exception e) {
            log.error("测试失败：", e);
            TooltipUtil.showToast("测试失败：" + e.getMessage());
        }
    }
}
