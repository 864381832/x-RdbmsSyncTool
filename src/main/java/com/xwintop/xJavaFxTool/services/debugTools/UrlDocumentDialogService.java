package com.xwintop.xJavaFxTool.services.debugTools;

import com.xwintop.xJavaFxTool.controller.debugTools.UrlDocumentDialogController;
import com.xwintop.xcore.util.javafx.TooltipUtil;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.yaml.snakeyaml.Yaml;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @ClassName: UrlDocumentDialogService
 * @Description: 历史连接编辑
 * @author: xufeng
 * @date: 2019/5/29 16:08
 */

@Getter
@Setter
@Slf4j
public class UrlDocumentDialogService {
    private UrlDocumentDialogController urlDocumentDialogController;

    private static final File CONFIG_FILE = new File("./javaFxConfigure/dbUrlDocumentConfigure.yml");

    public static List<Map<String, String>> getConfig() {
        try {
            if (!CONFIG_FILE.exists()) {
                FileUtils.touch(CONFIG_FILE);
            }
            Yaml yaml = new Yaml();
            List<Map<String, String>> list = yaml.load(FileUtils.readFileToString(CONFIG_FILE, "UTF-8"));
            return list;
        } catch (Exception e) {
            log.error("加载配置失败", e);
        }
        return null;
    }

    public static void addConfig(String host, String port, String userName, String password, String dbName, String dbType) {
        try {
            if (!CONFIG_FILE.exists()) {
                FileUtils.touch(CONFIG_FILE);
            }
            Map<String, String> map = new HashMap<>();
            map.put("name", host);
            map.put("host", host);
            map.put("port", port);
            map.put("dbName", dbName);
            map.put("dbType", dbType);
            map.put("userName", userName);
            map.put("password", password);
            Yaml yaml = new Yaml();
            List<Map<String, String>> list = yaml.load(FileUtils.readFileToString(CONFIG_FILE, "UTF-8"));
            if (list == null) {
                list = new ArrayList<>();
            }
            for (Map<String, String> smap : list) {
                if (StringUtils.equals(smap.get("host"), host) && StringUtils.equals(smap.get("port"), port)
                        && StringUtils.equals(smap.get("userName"), userName) && StringUtils.equals(smap.get("password"), password)
                        && StringUtils.equals(smap.get("dbName"), dbName) && StringUtils.equals(smap.get("dbType"), dbType)) {
                    return;
                }
            }
            list.add(map);
            FileUtils.writeStringToFile(CONFIG_FILE, yaml.dump(list), "UTF-8");
        } catch (Exception e) {
            log.error("保存配置失败", e);
        }
    }

    public UrlDocumentDialogService(UrlDocumentDialogController urlDocumentDialogController) {
        this.urlDocumentDialogController = urlDocumentDialogController;
    }

    public void saveConfigure() throws Exception {
        Yaml yaml = new Yaml();
        FileUtils.writeStringToFile(CONFIG_FILE, yaml.dump(urlDocumentDialogController.getTableData()), "UTF-8");
        TooltipUtil.showToast("保存配置成功,保存在：" + CONFIG_FILE.getPath());
    }

    public void loadingConfigure() throws Exception {
        try {
            urlDocumentDialogController.getTableData().clear();
            List<Map<String, String>> list = UrlDocumentDialogService.getConfig();
            if (list != null) {
                urlDocumentDialogController.getTableData().addAll(list);
            }
        } catch (Exception e) {
            log.error("加载配置失败：", e);
        }
    }
}
