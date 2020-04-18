package com.xwintop.xJavaFxTool.view.debugTools;

import javafx.fxml.FXML;
import javafx.fxml.Initializable;
import javafx.scene.control.TableColumn;
import javafx.scene.control.TableView;
import lombok.Getter;
import lombok.Setter;

import java.util.Map;

/**
 * @ClassName: UrlDocumentDialogView
 * @Description: 历史连接编辑
 * @author: xufeng
 * @date: 2019/5/29 16:09
 */

@Getter
@Setter
public abstract class UrlDocumentDialogView implements Initializable {
    @FXML
    protected TableView<Map<String, String>> urlDocumentTableView;
    @FXML
    protected TableColumn<Map<String, String>, String> nameTableColumn;
    @FXML
    protected TableColumn<Map<String, String>, String> hostTableColumn;
    @FXML
    protected TableColumn<Map<String, String>, String> dbNameTableColumn;
    @FXML
    protected TableColumn<Map<String, String>, String> dbTypeTableColumn;
    @FXML
    protected TableColumn<Map<String, String>, String> portTableColumn;
    @FXML
    protected TableColumn<Map<String, String>, String> userNameTableColumn;
    @FXML
    protected TableColumn<Map<String, String>, String> passwordTableColumn;
    @FXML
    protected TableColumn<Map<String, String>, String> explainTableColumn;
}
