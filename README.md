RdbmsSyncTool 关系型数据库同步工具

#### 项目简介：
RdbmsSyncTool是使用javaFx开发的关系型数据库同步工具，完成关系型数据库表结构获取，快捷执行一些常用数据库脚本，支持多种类型数据库直接数据转移，同步。

目前支持的数据库类型有mysql、Oracle、sqlserver、PostgreSql、达梦、sqlite、h2、access、db2等。

**xJavaFxTool交流QQ群：== [387473650](https://jq.qq.com/?_wv=1027&k=59UDEAD) ==**

#### 环境搭建说明：
- 开发环境为jdk1.8，基于maven构建
- 使用eclipase或Intellij Idea开发(推荐使用[Intellij Idea](https://www.jetbrains.com/?from=xJavaFxTool))
- 该项目为javaFx开发的实用小工具集[xJavaFxTool](https://gitee.com/xwintop/xJavaFxTool)的插件。
- 本项目使用了[lombok](https://projectlombok.org/),在查看本项目时如果您没有下载lombok 插件，请先安装,不然找不到get/set等方法
- 依赖的[xcore包](https://gitee.com/xwintop/xcore)已上传至git托管的maven平台，git托管maven可参考教程(若无法下载请拉取项目自行编译)。[教程地址：点击进入](http://blog.csdn.net/u011747754/article/details/78574026)

![数据库同步工具.png](images/数据库同步工具.png)

#### 版本记录
- 0.0.1  20200419
  1. 完成基本功能配置（对表进行查询、删除、建表语句、同步数据等操作）
  2. 支持mysql、Oracle、sqlserver、PostgreSql、达梦等数据库连接
- 0.0.2  20200421
  1. 添加sqlite、h2、access数据库支持
  2. 优化数据同步功能
- 0.0.3
  1. 添加多Schema输入框
  2. 添加表名过滤正则支持
  3. 添加时间和主键过滤条件支持
  4. 添加db2数据库支持
