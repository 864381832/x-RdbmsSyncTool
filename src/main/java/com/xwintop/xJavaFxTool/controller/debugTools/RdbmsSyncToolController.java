package com.xwintop.xJavaFxTool.controller.debugTools;

import cn.hutool.core.swing.clipboard.ClipboardUtil;
import cn.hutool.core.thread.ThreadUtil;
import com.xwintop.xJavaFxTool.services.debugTools.RdbmsSyncToolService;
import com.xwintop.xJavaFxTool.tools.DataxJsonUtil;
import com.xwintop.xJavaFxTool.utils.ActionScheduleUtil;
import com.xwintop.xJavaFxTool.view.debugTools.RdbmsSyncToolView;
import com.xwintop.xcore.util.javafx.JavaFxViewUtil;
import com.xwintop.xcore.util.javafx.TextFieldInputHistoryDialog;
import com.xwintop.xcore.util.javafx.TooltipUtil;
import javafx.event.ActionEvent;
import javafx.fxml.FXML;
import javafx.scene.control.*;
import javafx.scene.control.cell.CheckBoxTreeCell;
import javafx.scene.input.MouseButton;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.controlsfx.control.MaskerPane;

import java.net.URL;
import java.util.ResourceBundle;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

@Getter
@Setter
@Slf4j
public class RdbmsSyncToolController extends RdbmsSyncToolView {
    private RdbmsSyncToolService entDataToolService = new RdbmsSyncToolService(this);
    private TextFieldInputHistoryDialog textFieldInputHistoryDialog = new TextFieldInputHistoryDialog("./javaFxConfigure/dbUrlDocumentConfigure.yml", "host", "port", "dbName", "dbType", "userName", "password", "jdbcUrl", "schema");
    private ContextMenu contextMenu = new ContextMenu();
    private String[] dbTypeStrings = new String[]{"mysql", "oracle", "oracleSid", "postgresql", "sqlserver", "sqlserverold", "dm", "sqlite", "h2Embedded", "h2Server", "access", "db2", "kingbase"};
    private String[] outputPathStrings = new String[]{"./executor"};
    private String[] tableTypeStrings = new String[]{"TABLE+VIEW", "TABLE", "VIEW", "SYSTEM_TABLE", "GLOBAL_TEMPORARY", "LOCAL_TEMPORARY", "ALIAS", "SYNONYM"};
    private String[] dataSourceTypeStrings = new String[]{"Druid", "Driver", "Simple", "Hikari"};
    private MaskerPane masker = new MaskerPane();
    private ActionScheduleUtil actionScheduleUtil;

    @Override
    public void initialize(URL location, ResourceBundle resources) {
        initView();
        initEvent();
        initService();
    }

    private void initView() {
        masker.setVisible(false);
        loadingStackPane.getChildren().add(masker);
//        hostText1.setText("127.0.0.1");
//        dbNameText1.setText("admin");
//        pwdText1.setText("admin");

        JavaFxViewUtil.setPasswordTextFieldFactory(pwdText1);
        JavaFxViewUtil.setPasswordTextFieldFactory(pwdText2);
        dbTypeText1.getItems().addAll(dbTypeStrings);
        dbTypeText1.setValue(dbTypeStrings[0]);
        dbTypeText2.getItems().addAll(dbTypeStrings);
        dbTypeText2.setValue(dbTypeStrings[0]);
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

        tableTypeChoiceBox.getItems().addAll(tableTypeStrings);
        tableTypeChoiceBox.setValue(tableTypeChoiceBox.getItems().get(1));
        dataSourceTypeChoiceBox.getItems().addAll(dataSourceTypeStrings);
        dataSourceTypeChoiceBox.setValue(dataSourceTypeChoiceBox.getItems().get(0));

        actionScheduleUtil = new ActionScheduleUtil();
        actionScheduleUtil.setScheduleNode(actionScheduleHBox);
        actionScheduleUtil.setJobAction(() -> {
            try {
                entDataToolService.testInstertSqlAction();
            } catch (Exception e) {
                log.error("同步数据失败：", e);
                TooltipUtil.showToast("同步数据失败：" + e.getMessage());
            }
        });
    }

    private void initEvent() {
        addUrlDocumentDialogController(hostText1);
        addUrlDocumentDialogController(hostText2);
        addTableTreeViewMouseClicked(tableTreeView1);
        addTableTreeViewMouseClicked(tableTreeView2);
        dbTypeText1.valueProperty().addListener((observable, oldValue, newValue) -> {
            portText1.setText(DataxJsonUtil.getDbDefaultPort(newValue));
        });
        dbTypeText2.valueProperty().addListener((observable, oldValue, newValue) -> {
            portText2.setText(DataxJsonUtil.getDbDefaultPort(newValue));
        });
    }

    private void addTableTreeViewMouseClicked(TreeView<String> tableTreeView) {
        tableTreeView.setOnMouseClicked(event -> {
            contextMenu.hide();
            contextMenu.getItems().clear();
            TreeItem<String> selectedItem = tableTreeView.getSelectionModel().getSelectedItem();
            if (selectedItem == null) {
                return;
            }
            if (event.getButton() == MouseButton.PRIMARY) {
                selectedItem.setExpanded(!selectedItem.isExpanded());
            } else if (event.getButton() == MouseButton.SECONDARY) {
                JavaFxViewUtil.addMenuItem(contextMenu, "展开所有", event1 -> {
                    tableTreeView.getRoot().setExpanded(true);
                    tableTreeView.getRoot().getChildren().forEach(stringTreeItem -> {
                        stringTreeItem.setExpanded(true);
                    });
                });
                JavaFxViewUtil.addMenuItem(contextMenu, "折叠所有", event1 -> {
                    tableTreeView.getRoot().getChildren().forEach(stringTreeItem -> {
                        stringTreeItem.setExpanded(false);
                    });
                });
                JavaFxViewUtil.addMenuItem(contextMenu, "执行Sql", event1 -> {
                    entDataToolService.executeSql(tableTreeView);
                });
                if ("源端库表".equals(selectedItem.getValue()) || "目标端库表".equals(selectedItem.getValue())) {
                    JavaFxViewUtil.addMenuItem(contextMenu, "一键复制表名", event1 -> {
                        String tableNames = String.join(",", RdbmsSyncToolService.getSelectNameList(selectedItem));
                        ClipboardUtil.setStr(tableNames);
                        TooltipUtil.showToast("复制表名成功：" + tableNames);
                    });
                    JavaFxViewUtil.addMenuItem(contextMenu, "一键复制查询语句", event1 -> {
                        entDataToolService.copySelectSql(null, tableTreeView, false);
                    });
                    JavaFxViewUtil.addMenuItem(contextMenu, "一键复制查询语句Mysql", event1 -> {
                        entDataToolService.copySelectSql(null, tableTreeView, true);
                    });
                    JavaFxViewUtil.addMenuItem(contextMenu, "一键生成建表语句Mysql", event1 -> {
                        entDataToolService.showSqlAction("mysql");
                    });
                    JavaFxViewUtil.addMenuItem(contextMenu, "一键生成建表语句Oracle", event1 -> {
                        entDataToolService.showSqlAction("oracle");
                    });
                    JavaFxViewUtil.addMenuItem(contextMenu, "一键生成查询表中数据量语句", event1 -> {
                        entDataToolService.copySelectTableCount("*", tableTreeView);
                    });
                    JavaFxViewUtil.addMenuItem(contextMenu, "一键查看表中数据量", event1 -> {
                        entDataToolService.selectTableCount("*", tableTreeView);
                    });
                    JavaFxViewUtil.addMenuItem(contextMenu, "一键生成查询表中最大值语句", event1 -> {
                        entDataToolService.copySelectTableMax("*", tableTreeView, null);
                    });
                    JavaFxViewUtil.addMenuItem(contextMenu, "一键查看表中数据最大值", event1 -> {
                        entDataToolService.selectTableMax("*", tableTreeView, null);
                    });
                    JavaFxViewUtil.addMenuItem(contextMenu, "一键Drop删除表结构", event1 -> {
                        entDataToolService.dropTable("*", tableTreeView);
                    });
                    JavaFxViewUtil.addMenuItem(contextMenu, "一键delete删除表数据", event1 -> {
                        entDataToolService.deleteTableData("*", tableTreeView);
                    });
                    JavaFxViewUtil.addMenuItem(contextMenu, "一键truncate删除表数据", event1 -> {
                        entDataToolService.truncateTableData("*", tableTreeView);
                    });
                } else {
                    if ("源端库表".equals(selectedItem.getParent().getValue()) || "目标端库表".equals(selectedItem.getParent().getValue())) {
                        JavaFxViewUtil.addMenuItem(contextMenu, "复制表名", event1 -> {
                            ClipboardUtil.setStr(selectedItem.getValue());
                            TooltipUtil.showToast("复制表名成功：" + selectedItem.getValue());
                        });
                        JavaFxViewUtil.addMenuItem(contextMenu, "复制字段名", event1 -> {
                            ClipboardUtil.setStr(String.join(",", RdbmsSyncToolService.getSelectNameList(selectedItem)));
                            TooltipUtil.showToast("复制字段名成功：" + String.join(",", RdbmsSyncToolService.getSelectNameList(selectedItem)));
                        });
                        JavaFxViewUtil.addMenuItem(contextMenu, "复制查询语句", event1 -> {
                            entDataToolService.copySelectSql((CheckBoxTreeItem<String>) selectedItem, tableTreeView, false);
                        });
                        JavaFxViewUtil.addMenuItem(contextMenu, "复制查询语句mysql", event1 -> {
                            entDataToolService.copySelectSql((CheckBoxTreeItem<String>) selectedItem, tableTreeView, true);
                        });
                        JavaFxViewUtil.addMenuItem(contextMenu, "查看表中数据量", event1 -> {
                            entDataToolService.selectTableCount(selectedItem.getValue(), tableTreeView);
                        });
                        JavaFxViewUtil.addMenuItem(contextMenu, "复制查看表中数据量语句", event1 -> {
                            entDataToolService.copySelectTableCount(selectedItem.getValue(), tableTreeView);
                        });
                        JavaFxViewUtil.addMenuItem(contextMenu, "查看表中数据最大值", event1 -> {
                            entDataToolService.selectTableMax(selectedItem.getValue(), tableTreeView, selectedItem);
                        });
                        JavaFxViewUtil.addMenuItem(contextMenu, "复制查询表中最大值语句", event1 -> {
                            entDataToolService.copySelectTableMax(selectedItem.getValue(), tableTreeView, selectedItem);
                        });
                        JavaFxViewUtil.addMenuItem(contextMenu, "查看表内容", event1 -> {
                            entDataToolService.showTableData(selectedItem.getValue(), tableTreeView);
                        });
                        JavaFxViewUtil.addMenuItem(contextMenu, "Drop删除表结构", event1 -> {
                            entDataToolService.dropTable(selectedItem.getValue(), tableTreeView);
                        });
                        JavaFxViewUtil.addMenuItem(contextMenu, "delete删除表数据", event1 -> {
                            entDataToolService.deleteTableData(selectedItem.getValue(), tableTreeView);
                        });
                        JavaFxViewUtil.addMenuItem(contextMenu, "truncate删除表数据", event1 -> {
                            entDataToolService.truncateTableData(selectedItem.getValue(), tableTreeView);
                        });
                    } else {
                        JavaFxViewUtil.addMenuItem(contextMenu, "复制字段名", event1 -> {
                            ClipboardUtil.setStr(selectedItem.getValue());
                        });
                    }
                }
                contextMenu.show(tableTreeView, null, event.getX(), event.getY());
            }
        });
    }

    private void initService() {
    }

    private void addUrlDocumentDialogController(TextField hostText) {
        textFieldInputHistoryDialog.setOnMouseClicked(hostText, map -> {
            hostText.setText(map.get("host"));
            if (hostText == hostText1) {
                portText1.setText(map.get("port"));
                dbNameText1.setText(map.get("dbName"));
                dbTypeText1.setValue(map.get("dbType"));
                userNameText1.setText(map.get("userName"));
                pwdText1.setText(map.get("password"));
                jdbcUrlField1.setText(StringUtils.defaultString(map.get("jdbcUrl")));
                schemaTextField1.setText(StringUtils.defaultString(map.get("schema")));
            } else if (hostText == hostText2) {
                portText2.setText(map.get("port"));
                dbNameText2.setText(map.get("dbName"));
                dbTypeText2.setValue(map.get("dbType"));
                userNameText2.setText(map.get("userName"));
                pwdText2.setText(map.get("password"));
                jdbcUrlField2.setText(StringUtils.defaultString(map.get("jdbcUrl")));
                schemaTextField2.setText(StringUtils.defaultString(map.get("schema")));
            }
        }, map -> map.get("name") + "/" + map.get("dbName") + "/" + map.get("userName"));
    }

    @FXML
    private void connectAction1(ActionEvent event) {
        masker.setVisible(true);
        ThreadUtil.execute(() -> {
            try {
                Future<?> future = ThreadUtil.execAsync(() -> {
                    try {
                        textFieldInputHistoryDialog.addConfig(hostText1.getText(), portText1.getText(), dbNameText1.getText(), dbTypeText1.getValue(), userNameText1.getText(), pwdText1.getText(), jdbcUrlField1.getText(), schemaTextField1.getText());
                        entDataToolService.connectAction(dbTypeText1.getValue(), hostText1.getText(), portText1.getText(), dbNameText1.getText(), userNameText1.getText(), pwdText1.getText(), jdbcUrlField1.getText(), schemaTextField1.getText(), tableTreeView1);
                    } catch (Exception e) {
                        log.error("连接失败：", e);
                    }
                });
                future.get(30, TimeUnit.SECONDS);
            } catch (Exception e) {
                log.error("连接超时：", e);
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
                Future<?> future = ThreadUtil.execAsync(() -> {
                    try {
                        textFieldInputHistoryDialog.addConfig(hostText2.getText(), portText2.getText(), dbNameText2.getText(), dbTypeText2.getValue(), userNameText2.getText(), pwdText2.getText(), jdbcUrlField2.getText(), schemaTextField2.getText());
                        entDataToolService.connectAction(dbTypeText2.getValue(), hostText2.getText(), portText2.getText(), dbNameText2.getText(), userNameText2.getText(), pwdText2.getText(), jdbcUrlField2.getText(), schemaTextField2.getText(), tableTreeView2);
                    } catch (Exception e) {
                        log.error("连接失败：", e);
                    }
                });
                future.get(30, TimeUnit.SECONDS);
            } catch (Exception e) {
                log.error("连接超时：", e);
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
            log.error("同步数据失败：", e);
            TooltipUtil.showToast("同步数据失败：" + e.getMessage());
        }
    }
}
