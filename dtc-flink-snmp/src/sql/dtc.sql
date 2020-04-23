/*
Navicat MySQL Data Transfer

Source Server         : MySQL3306
Source Server Version : 80015
Source Host           : localhost:3306
Source Database       : wj

Target Server Type    : MYSQL
Target Server Version : 80015
File Encoding         : 65001

Date: 2020-04-12 09:45:45
*/

SET FOREIGN_KEY_CHECKS=0;

-- ----------------------------
-- Table structure for 大屏
-- ----------------------------
DROP TABLE IF EXISTS `SC_DP_SECT_EVENT`;
CREATE TABLE `SC_DP_SECT_EVENT` (
  `id` int(10) NOT NULL AUTO_INCREMENT,
  `riqi` varchar(25) NOT NULL,
  `num` int(20) DEFAULT '0' COMMENT '每一分钟的安全事件数',
  PRIMARY KEY (`id`)
)ENGINE=InnoDB DEFAULT CHARSET=utf8;

CREATE TABLE `SC_DP_SJDP` (
  `id` int(5) NOT NULL AUTO_INCREMENT,
  `riqi` varchar(15) NOT NULL,
  `wo_num` int(10) DEFAULT '0' COMMENT '今日工单',
  `bg_num` int(10) DEFAULT '0' COMMENT '今日变更',
  `wb_num` int(10) DEFAULT '0' COMMENT '即将维保数',
  `qy_num` int(10) DEFAULT '0' COMMENT '即将弃用数',
  `jk_num` int(10) DEFAULT '0' COMMENT '健康机器数',
  `w_leve_1` int(10) DEFAULT '0' COMMENT '未处理一般告警数',
  `w_leve_2` int(10) DEFAULT '0' COMMENT '未处理较严重告警',
  `w_leve_3` int(10) DEFAULT '0' COMMENT '未处理严重告警数',
  `w_leve_4` int(10) DEFAULT '0' COMMENT '未处理灾难告警',
  `w_level_all` int(10) DEFAULT '0' COMMENT '未处理告警总数',
  `js_time` varchar(25) DEFAULT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `riqi` (`riqi`)
)ENGINE=InnoDB DEFAULT CHARSET=utf8;

CREATE TABLE `SC_DP_SJDP-30D` (
  `id` int(5) NOT NULL AUTO_INCREMENT,
  `riqi` varchar(15) NOT NULL,
  `name` varchar(20) NOT NULL COMMENT '设备打类名称',
  `alarm_level` varchar(10) DEFAULT '0' COMMENT '告警等级',
  `num` int(10) DEFAULT '0' COMMENT '告警数',
  `js_time` varchar(25) DEFAULT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `riqi` (`riqi`,`name`,`alarm_level`)
)ENGINE=InnoDB DEFAULT CHARSET=utf8;

CREATE TABLE `SC_DP_ZCDP` (
  `id` int(5) NOT NULL AUTO_INCREMENT,
  `riqi` varchar(15) NOT NULL,
  `parent_name` varchar(20) NOT NULL COMMENT '设备大类名称',
  `child_name` varchar(10) DEFAULT '0' COMMENT '子设备名称',
  `num` int(10) DEFAULT '0' COMMENT '设备数',
  `js_time` varchar(25) DEFAULT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `riqi` (`riqi`,`parent_name`,`child_name`)
)ENGINE=InnoDB DEFAULT CHARSET=utf8;
-- ----------------------------
-- Records of admin_menu
-- ----------------------------
CREATE TABLE `SC_JSC_GLDP_BP` (
  `id` int(40) NOT NULL AUTO_INCREMENT,
  `wclgd` varchar(40) DEFAULT NULL COMMENT '未处理工单',
  `jrpd` varchar(10) DEFAULT NULL COMMENT '今日派单',
  `jrdk` varchar(20) DEFAULT NULL COMMENT '今日打卡人数',
  `zjbcs` varchar(20) DEFAULT NULL COMMENT '周加班次数',
  `zjbrs` varchar(20) DEFAULT NULL COMMENT '周加班人数',
  `zrs` varchar(20) DEFAULT NULL COMMENT '总人数',
  `jrjbcs` varchar(20) DEFAULT NULL COMMENT '今日加班次数',
  `jrwjb` varchar(10) DEFAULT NULL COMMENT '今日未加班',
  `yjbcs` varchar(20) DEFAULT NULL COMMENT '月加班次数',
  `gjgd` varchar(10) DEFAULT NULL COMMENT '告警工单',
  `tbgjgd` varchar(20) DEFAULT NULL COMMENT '同比告警工单',
  `swgd` varchar(20) DEFAULT NULL COMMENT '事务工单',
  `tbswgd` varchar(20) DEFAULT NULL COMMENT '同比事务工单',
  `riqi` varchar(20) DEFAULT NULL COMMENT '计算数据日期',
  `js_time` varbinary(20) DEFAULT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `js_time` (`riqi`)
)ENGINE=InnoDB DEFAULT CHARSET=utf8;

CREATE TABLE `SC_JSC_GLDP_GDXX` (
  `id` int(10) NOT NULL AUTO_INCREMENT,
  `gname` varchar(40) DEFAULT NULL COMMENT '告警工单类型',
  `wcgd` varchar(10) DEFAULT NULL COMMENT '15分钟完成工单数',
  `gjsl` varchar(20) DEFAULT NULL COMMENT '告警数量',
  `riqi` varchar(20) DEFAULT NULL COMMENT '处理的日期',
  `js_time` varchar(20) DEFAULT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `js_time` (`riqi`)
)ENGINE=InnoDB DEFAULT CHARSET=utf8;

CREATE TABLE `SC_JSC_GLDP_ZZFW` (
  `id` int(40) NOT NULL AUTO_INCREMENT,
  `sjgd` varchar(20) DEFAULT NULL COMMENT '事件工单比例',
  `bgsx` varchar(20) DEFAULT NULL COMMENT '变更事项比例',
  `fwgd` varchar(20) DEFAULT NULL COMMENT '服务工单比例',
  `gjgd` varchar(20) DEFAULT NULL COMMENT '告警工单比例',
  `zysq` varchar(20) DEFAULT NULL COMMENT '资源申请比例',
  `wpgd` varchar(20) DEFAULT NULL COMMENT '外派申请比例',
  `qt` varchar(20) DEFAULT NULL COMMENT '其他比例',
  `riqi` varchar(20) NOT NULL COMMENT '计算日期',
  `js_time` varchar(20) DEFAULT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `js_time` (`riqi`)
)ENGINE=InnoDB DEFAULT CHARSET=utf8;

CREATE TABLE `SC_JSC_JKDP` (
  `id` int(5) NOT NULL AUTO_INCREMENT,
  `riqi` varchar(10) NOT NULL,
  `level_1` int(10) DEFAULT '0' COMMENT '告警等级1数',
  `level_2` int(10) DEFAULT '0' COMMENT '告警等级2数',
  `level_3` int(10) DEFAULT '0' COMMENT '告警等级3数',
  `level_4` int(10) DEFAULT '0' COMMENT '告警等级4数',
  `alarm_count` int(10) DEFAULT '0' COMMENT '总告警数',
  `not_close_alarm` int(10) DEFAULT '0' COMMENT '未关闭告警数',
  `SB_ALL_count` int(10) DEFAULT '0' COMMENT '总设备数',
  `SB_ZC_count` int(10) DEFAULT '0' COMMENT '正常设备数',
  `SB_YC_count` int(10) DEFAULT '0' COMMENT '异常设备数',
  `js_time` varchar(25) DEFAULT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `riqi` (`riqi`)
)ENGINE=InnoDB DEFAULT CHARSET=utf8;

CREATE TABLE `SC_JSC_JKDP_CSTOP` (
  `id` int(5) NOT NULL AUTO_INCREMENT,
  `riqi` varchar(10) NOT NULL,
  `code` int(5) NOT NULL,
  `cs_name` varchar(50) NOT NULL,
  `alarm_num` int(10) DEFAULT '0' COMMENT '厂商设备告警总数',
  `cs_sb_num` int(10) DEFAULT '0' COMMENT '厂商设备总数',
  `cs_alarm_rato` float DEFAULT '0' COMMENT '厂商设备告警比值',
  `js_time` varchar(25) DEFAULT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `riqi` (`riqi`,`code`)
)ENGINE=InnoDB DEFAULT CHARSET=utf8;

CREATE TABLE `SC_JSC_JKDP_SBFL` (
  `id` int(5) NOT NULL AUTO_INCREMENT,
  `riqi` varchar(10) NOT NULL,
  `sb_name` varchar(50) NOT NULL,
  `yc_num` int(10) DEFAULT '0' COMMENT '异常设备数',
  `sb_all_num` int(10) DEFAULT '0' COMMENT '设备总数',
  `sb_alarm_rato` float DEFAULT '0' COMMENT '设备告警比值',
  `sb_num_all` int(10) DEFAULT '0' COMMENT '设备总数',
  `sb_num_used` int(10) DEFAULT '0' COMMENT '已使用设备数',
  `sb_yc_num` int(10) DEFAULT '0' COMMENT '异常设备数',
  `sb_yc_rato` float DEFAULT '0' COMMENT '设备异常比',
  `js_time` varchar(25) DEFAULT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `riqi` (`riqi`,`sb_name`)
)ENGINE=InnoDB DEFAULT CHARSET=utf8;
-- ----------------------------
-- Table structure for admin_permission
-- ----------------------------
CREATE TABLE `SC_WDZL_GZDT` (
  `id` int(40) NOT NULL AUTO_INCREMENT,
  `zgd` varchar(40) DEFAULT NULL COMMENT '我创建的',
  `fpgw` varchar(10) DEFAULT NULL COMMENT '分派给我',
  `hd` varchar(20) DEFAULT NULL COMMENT '我回答',
  `tw` varchar(20) DEFAULT NULL COMMENT '提问',
  `zgj` varchar(20) DEFAULT NULL COMMENT '总告警',
  `gjdcl` varchar(20) DEFAULT NULL COMMENT '待处理',
  `riqi` varchar(20) NOT NULL COMMENT '计算日期',
  `js_time` varchar(20) DEFAULT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `id` (`id`),
  UNIQUE KEY `js_time` (`riqi`)
)ENGINE=InnoDB DEFAULT CHARSET=utf8;

CREATE TABLE `SC_WDZL_GZWT` (
  `id` int(10) NOT NULL AUTO_INCREMENT COMMENT '序号',
  `title` varchar(40) DEFAULT NULL COMMENT '工作标题',
  `ans` varchar(10) DEFAULT NULL COMMENT '回答数',
  `name` varchar(20) DEFAULT NULL COMMENT '发起人',
  `time` varchar(40) DEFAULT NULL COMMENT '创建日期',
  `riqi` varchar(20) DEFAULT NULL,
  `js_time` varchar(20) DEFAULT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `id` (`id`),
  UNIQUE KEY `js_time` (`riqi`)
)ENGINE=InnoDB DEFAULT CHARSET=utf8;

CREATE TABLE `SC_WDZL_WDGD` (
  `id` int(10) NOT NULL AUTO_INCREMENT COMMENT '序号',
  `title` varchar(40) DEFAULT NULL COMMENT '工单标题',
  `type` varchar(10) DEFAULT NULL COMMENT '工单类型',
  `name` varchar(20) DEFAULT NULL COMMENT '发起人',
  `state` varchar(20) DEFAULT NULL COMMENT '状态',
  `time` varchar(40) DEFAULT NULL COMMENT '创建日期',
  `riqi` varchar(20) DEFAULT NULL,
  `js_time` varchar(20) DEFAULT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `id` (`id`),
  UNIQUE KEY `js_time` (`riqi`)
)ENGINE=InnoDB DEFAULT CHARSET=utf8;

CREATE TABLE `SC_WDZL_WGLDGDFB` (
  `id` int(40) NOT NULL AUTO_INCREMENT,
  `sjgd` varchar(40) DEFAULT NULL COMMENT '事件工单',
  `bgsx` varchar(10) DEFAULT NULL COMMENT '变更事项',
  `fwgd` varchar(20) DEFAULT NULL COMMENT '服务工单',
  `gjgd` varchar(20) DEFAULT NULL COMMENT '告警工单',
  `zysq` varchar(20) DEFAULT NULL COMMENT '资源申请',
  `wpgd` varchar(20) DEFAULT NULL COMMENT '外派申请',
  `qt` varchar(20) DEFAULT NULL COMMENT '其他',
  `riqi` varchar(20) DEFAULT NULL COMMENT '计算日期',
  `js_time` varchar(20) DEFAULT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `js_time` (`riqi`)
)ENGINE=InnoDB DEFAULT CHARSET=utf8;

CREATE TABLE `SC_WDZL_XMDT` (
  `xmgd_zdq` varchar(40) DEFAULT NULL,
  `xmgd_dcl` varchar(10) DEFAULT NULL,
  `kq_yc` varchar(20) DEFAULT NULL,
  `kq_jb` varchar(20) DEFAULT NULL,
  `kq_wp` varchar(20) DEFAULT NULL,
  `kq_wdk` varchar(20) DEFAULT NULL,
  `kq_cd` varchar(20) DEFAULT NULL,
  `kq_zt` varchar(20) DEFAULT NULL,
  `jisuan_riqi` varchar(20) NOT NULL,
  PRIMARY KEY (`jisuan_riqi`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

CREATE TABLE `SC_WDZL_XMGDQS` (
  `id` int(40) NOT NULL AUTO_INCREMENT,
  `a` varchar(40) DEFAULT NULL COMMENT '当前月',
  `b` varchar(10) DEFAULT NULL COMMENT '前1个月',
  `c` varchar(20) DEFAULT NULL COMMENT '前2个月',
  `d` varchar(20) DEFAULT NULL COMMENT '前3个月',
  `e` varchar(20) DEFAULT NULL COMMENT '前4个月',
  `f` varchar(20) DEFAULT NULL COMMENT '前5个月',
  `riqi` varchar(20) DEFAULT NULL COMMENT '计算日期',
  `js_time` varchar(20) DEFAULT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `js_time` (`riqi`)
)ENGINE=InnoDB DEFAULT CHARSET=utf8;

CREATE TABLE `SC_WDZL_ZCZYFB` (
  `id` int(40) NOT NULL AUTO_INCREMENT,
  `zhuji` varchar(40) DEFAULT NULL COMMENT '主机占比',
  `wlsb` varchar(10) DEFAULT NULL COMMENT '物理设备',
  `aqsb` varchar(20) DEFAULT NULL COMMENT '安全设备',
  `ccsb` varchar(20) DEFAULT NULL COMMENT '存储设备',
  `jcss` varchar(20) DEFAULT NULL COMMENT '基础设施',
  `riqi` varchar(20) DEFAULT NULL COMMENT '计算日期',
  `js_time` varchar(20) DEFAULT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `js_time` (`riqi`)
)ENGINE=InnoDB DEFAULT CHARSET=utf8;

CREATE TABLE `SC_WDZL_ZSKFB` (
  `id` int(40) NOT NULL AUTO_INCREMENT,
  `szjy` varchar(40) DEFAULT NULL COMMENT '实战经验占比',
  `llzs` varchar(10) DEFAULT NULL COMMENT '理论知识',
  `pxkc` varchar(20) DEFAULT NULL COMMENT '培训课程',
  `riqi` varchar(40) DEFAULT NULL COMMENT '计算日期',
  `js_time` varchar(40) DEFAULT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `id` (`id`),
  UNIQUE KEY `js_time` (`riqi`)
)ENGINE=InnoDB DEFAULT CHARSET=utf8;




-- ---------------------------------------

CREATE TABLE `SC_ZHBB_ZYZC` (
  `id` int(40) NOT NULL AUTO_INCREMENT,
  `riqi` varchar(15) NOT NULL COMMENT '计算数据日期',
  `sblx` varchar(20) NOT NULL COMMENT '设备类型',
  `zlx` varchar(20) NOT NULL COMMENT '子类型',
  `cs` varchar(20) NOT NULL COMMENT '厂商',
  `gjsl` varchar(20) DEFAULT NULL COMMENT '告警数量',
  `js_time` varchar(20) NOT NULL COMMENT '程序计算日期',
  PRIMARY KEY (`id`),
  UNIQUE KEY `js_time` (`riqi`,`sblx`,`zlx`,`cs`)
)ENGINE=InnoDB DEFAULT CHARSET=utf8;


CREATE TABLE `SC_ZHBB_SCRENE_TWO` (
  `id` int(20) NOT NULL AUTO_INCREMENT,
  `riqi` varchar(10) NOT NULL COMMENT '计算数据日期',
  `level_id` varchar(2) NOT NULL COMMENT '告警等级',
  `type_id` varchar(2) NOT NULL COMMENT '告警资产类型',
  `zs_Num` int(40) DEFAULT '0' COMMENT '新增告警数',
  `old_Num` int(40) DEFAULT '0' COMMENT '旧存告警数',
  `wcl_Num` int(40) DEFAULT '0' COMMENT '待处理告警数',
  `ycl_Num` int(40) DEFAULT '0' COMMENT '已处理告警数',
  `js_time` varchar(20) DEFAULT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `riqi` (`riqi`,`level_id`,`type_id`)
)ENGINE=InnoDB DEFAULT CHARSET=utf8;



CREATE TABLE `dtc_win_disk` (
  `host_ip` varchar(10) NOT NULL,
  `last_code` varchar(10) NOT NULL,
  `result` varchar(500) DEFAULT NULL,
  PRIMARY KEY (`host_ip`,`last_code`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;



create table jifang(
id int(8) not null primary key auto_increment,
riqi varchar(8) not null,
room varchar(20) not null,
allNum int(20) not null,
zcNum int(20) not null,
wgbgiNum int(20) not null,
record float(5) not null,
js_time varchar(20) not null,
remark varchar(50) default null,
UNIQUE KEY `service_id` (`riqi`,room)
)ENGINE=InnoDB DEFAULT CHARSET=utf8;


create table jigui(
id int(8) not null primary key auto_increment,
riqi varchar(8) not null,
room varchar(20) not null,
partitions varchar(20) not null,
box varchar(20) not null,
allNum int(20) not null,
zcNum int(20) not null,
wgbgiNum int(20) not null,
record float(5) not null,
js_time varchar(20) not null,
remark varchar(50) default null,
UNIQUE KEY `service_id` (riqi,room,partitions,box)
)ENGINE=InnoDB DEFAULT CHARSET=utf8;


create table jfyc(
id int(20) not null primary key auto_increment,
asset_id varchar(20) not null,
`name` varchar(40) not null,
level_id varchar(2) not null,
ip varchar(10) not null,
num int(20) not null,
UNIQUE key service_id (asset_id,`name`,level_id,ip)
)ENGINE=InnoDB DEFAULT CHARSET=utf8;

create table quyu(
id int(8) not null primary key auto_increment,
riqi varchar(8) not null,
room varchar(20) not null,
partitions varchar(20) not null,
allNum int(20) not null,
zcNum int(20) not null,
wgbgiNum int(20) not null,
record float(5) not null,
js_time varchar(20) not null,
remark varchar(50) default null,
UNIQUE KEY `service_id` (riqi,room,partitions)
)ENGINE=InnoDB DEFAULT CHARSET=utf8;
