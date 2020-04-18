package com.xwintop.xJavaFxTool.model;

import lombok.Data;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

/**
 * @ClassName: TaskConfigClient
 * @Description: 客户端任务配置
 * @author: xufeng
 * @date: 2020/2/28 0028 12:46
 */

@Data
public class TaskConfigClient implements Serializable {
    private String name;//任务名称(唯一标识，不可重复)
    private Boolean isEnable = true;//是否开启
    private String taskType;//任务类型(script/receiver/execute flow)
    private String triggerType = "CRON";//触发器类型(simple/cron)
    private Integer intervalTime = 5;//两次任务调度的间隔时间(simple类型触发器显示该信息)，单位为秒
    private Integer executeTimes = -1;//任务执行次数(simple类型触发器显示该信息，-1表示无限次)
    private String triggerCron;//任务调度的时间(cron类型触发器显示该信息)
    private Boolean isStatefulJob = true;//是否为有状态的job

    private String jobJson;//datax任务配置
    private Long lastSyncTime;//最后更新时间（任务执行时数据库的开始时间）
    private String password;//密码
    private boolean decrypt = false;//是否使用加密
    private String publicKey;//加密公钥

    private String tpassword;//入库方密码
    private boolean tdecrypt = false;//入库方是否使用加密
    private String tpublicKey;//入库方加密公钥

    private Map<String, Object> properties = new HashMap();//附加配置属性

    public Object getProperty(String key) {
        return properties.get(key);
    }

    public void setProperty(String key, Object value) {
        properties.put(key, value);
    }
}
