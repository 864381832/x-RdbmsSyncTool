package com.xwintop.xJavaFxTool.view.debugTools;

import javafx.fxml.FXML;
import javafx.fxml.Initializable;
import javafx.scene.control.*;
import javafx.scene.layout.AnchorPane;
import javafx.scene.layout.HBox;
import javafx.scene.layout.StackPane;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public abstract class RdbmsSyncToolView implements Initializable {
    @FXML
    protected TextField hostText1;
    @FXML
    protected TextField portText1;
    @FXML
    protected TextField dbNameText1;
    @FXML
    protected ChoiceBox<String> dbTypeText1;
    @FXML
    protected TextField userNameText1;
    @FXML
    protected PasswordField pwdText1;
    @FXML
    protected TextField jdbcUrlField1;
    @FXML
    protected Button connectButton1;
    @FXML
    protected TextField hostText2;
    @FXML
    protected TextField portText2;
    @FXML
    protected TextField dbNameText2;
    @FXML
    protected ChoiceBox<String> dbTypeText2;
    @FXML
    protected TextField userNameText2;
    @FXML
    protected PasswordField pwdText2;
    @FXML
    protected TextField jdbcUrlField2;
    @FXML
    protected Button connectButton2;
    @FXML
    protected TreeView<String> tableTreeView1;
    @FXML
    protected TreeView<String> tableTreeView2;
    @FXML
    protected Spinner<Integer> syncDataNumberSpinner;
    @FXML
    protected Button showSqlButton;
    @FXML
    protected CheckBox isShowCheckBox;
    @FXML
    protected TextField querySqlTextField;
    @FXML
    protected ComboBox<String> outputPathComboBox;
    @FXML
    protected StackPane loadingStackPane;
    @FXML
    protected TextField schemaTextField1;
    @FXML
    protected TextField schemaTextField2;
    @FXML
    protected TextField ignoreTableNameTextField;
    @FXML
    protected ChoiceBox<String> tableTypeChoiceBox;
    @FXML
    protected ChoiceBox<String> dataSourceTypeChoiceBox;
    @FXML
    protected CheckBox filterTimeCheckBox;
    @FXML
    protected TextField filterStartTimeTextField;
    @FXML
    protected TextField filterEntTimeTextField;
    @FXML
    protected CheckBox filterLongKeyCheckBox;
    @FXML
    protected TextField filterLongKeyStartTextField;
    @FXML
    protected TextField whereSqlTextField;
    @FXML
    protected TextField filterLongKeyEntTextField;
    @FXML
    protected HBox actionScheduleHBox;
}
