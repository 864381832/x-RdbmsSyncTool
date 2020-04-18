package com.xwintop.xJavaFxTool.controller.debugTools;

import com.xwintop.xJavaFxTool.services.debugTools.RdbmsSyncToolService;
import com.xwintop.xJavaFxTool.services.debugTools.UrlDocumentDialogService;
import com.xwintop.xJavaFxTool.view.debugTools.RdbmsSyncToolView;
import com.xwintop.xcore.util.javafx.JavaFxViewUtil;
import com.xwintop.xcore.util.javafx.TooltipUtil;
import javafx.event.ActionEvent;
import javafx.fxml.FXML;
import javafx.fxml.FXMLLoader;
import javafx.scene.control.CheckBoxTreeItem;
import javafx.scene.control.ContextMenu;
import javafx.scene.control.MenuItem;
import javafx.scene.control.TextField;
import javafx.scene.control.cell.CheckBoxTreeCell;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

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
    private String[] dbTypeStrings = new String[]{"mysql", "sqlserver", "oracle"};

    @Override
    public void initialize(URL location, ResourceBundle resources) {
        initView();
        initEvent();
        initService();
    }

    private void initView() {
        hostText1.setText("192.168.129.121");
        dbNameText1.setText("test");
        pwdText1.setText("easipass");

        JavaFxViewUtil.setPasswordTextFieldFactory(pwdText1);
        JavaFxViewUtil.setPasswordTextFieldFactory(pwdText2);
        dbTypeText1.getItems().addAll(dbTypeStrings);
        dbTypeText1.setValue(dbTypeStrings[0]);
        dbTypeText2.getItems().addAll(dbTypeStrings);
        dbTypeText2.setValue(dbTypeStrings[0]);

        CheckBoxTreeItem<String> rootItem = new CheckBoxTreeItem<>("源端库表");
        rootItem.setExpanded(true);
        tableTreeView1.setCellFactory(CheckBoxTreeCell.<String>forTreeView());
        tableTreeView1.setRoot(rootItem);
        tableTreeView1.setShowRoot(true);
        tableTreeView2.setCellFactory(CheckBoxTreeCell.<String>forTreeView());
        tableTreeView2.setRoot(new CheckBoxTreeItem<>("目标端库表"));
        tableTreeView2.getRoot().setExpanded(true);
        tableTreeView2.setShowRoot(true);
        JavaFxViewUtil.setSpinnerValueFactory(channelSpinner, 1, Integer.MAX_VALUE, 6);
        JavaFxViewUtil.setSpinnerValueFactory(syncDataNumberSpinner, -1, Integer.MAX_VALUE, 10);
    }

    private void initEvent() {
        addUrlDocumentDialogController(hostText1);
        addUrlDocumentDialogController(hostText2);
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
        try {
            UrlDocumentDialogService.addConfig(hostText1.getText(), portText1.getText(), userNameText1.getText(), pwdText1.getText(), dbNameText1.getText(), dbTypeText1.getValue());
            entDataToolService.connectAction(dbTypeText1.getValue(), hostText1.getText(), portText1.getText(), dbNameText1.getText(), userNameText1.getText(), pwdText1.getText(), tableTreeView1);
        } catch (Exception e) {
            log.error("连接失败：", e);
            TooltipUtil.showToast("连接失败：" + e.getMessage());
        }
    }

    @FXML
    private void connectAction2(ActionEvent event) {
        try {
            UrlDocumentDialogService.addConfig(hostText2.getText(), portText2.getText(), userNameText2.getText(), pwdText2.getText(), dbNameText2.getText(), dbTypeText2.getValue());
            entDataToolService.connectAction(dbTypeText2.getValue(), hostText2.getText(), portText2.getText(), dbNameText2.getText(), userNameText2.getText(), pwdText2.getText(), tableTreeView2);
        } catch (Exception e) {
            log.error("连接失败：", e);
            TooltipUtil.showToast("连接失败：" + e.getMessage());
        }
    }

    @FXML
    private void showSqlAction(ActionEvent event) {
        try {
            entDataToolService.showSqlAction();
        } catch (Exception e) {
            log.error("生成失败：", e);
            TooltipUtil.showToast("生成失败：" + e.getMessage());
        }
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
