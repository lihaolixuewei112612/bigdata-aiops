/*
 Navicat MySQL Data Transfer

 Source Server         : Dtc_java_Test
 Source Server Type    : MySQL
 Source Server Version : 50726
 Source Host           : 10.3.7.231:3306
 Source Schema         : dtc_platform

 Target Server Type    : MySQL
 Target Server Version : 50726
 File Encoding         : 65001

 Date: 11/05/2020 11:46:43
*/

SET NAMES utf8mb4;
SET FOREIGN_KEY_CHECKS = 0;

-- ----------------------------
-- Table structure for SC_DP_SECT_EVENT
-- ----------------------------
DROP TABLE IF EXISTS `SC_DP_SECT_EVENT`;
CREATE TABLE `SC_DP_SECT_EVENT`  (
  `id` int(10) NOT NULL AUTO_INCREMENT,
  `riqi` varchar(25) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL,
  `num` int(20) NULL DEFAULT 0 COMMENT '每一分钟的安全事件数',
  PRIMARY KEY (`id`) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 8 CHARACTER SET = utf8 COLLATE = utf8_general_ci ROW_FORMAT = Dynamic;

-- ----------------------------
-- Table structure for SC_DP_SJDP
-- ----------------------------
DROP TABLE IF EXISTS `SC_DP_SJDP`;
CREATE TABLE `SC_DP_SJDP`  (
  `id` int(5) NOT NULL AUTO_INCREMENT,
  `riqi` varchar(15) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL,
  `wo_num` int(10) NULL DEFAULT 0 COMMENT '今日工单',
  `bg_num` int(10) NULL DEFAULT 0 COMMENT '今日变更',
  `wb_num` int(10) NULL DEFAULT 0 COMMENT '即将维保数',
  `qy_num` int(10) NULL DEFAULT 0 COMMENT '即将弃用数',
  `jk_num` int(10) NULL DEFAULT 0 COMMENT '健康机器数',
  `w_leve_1` int(10) NULL DEFAULT 0 COMMENT '未处理一般告警数',
  `w_leve_2` int(10) NULL DEFAULT 0 COMMENT '未处理较严重告警',
  `w_leve_3` int(10) NULL DEFAULT 0 COMMENT '未处理严重告警数',
  `w_leve_4` int(10) NULL DEFAULT 0 COMMENT '未处理灾难告警',
  `w_level_all` int(10) NULL DEFAULT 0 COMMENT '未处理告警总数',
  `js_time` varchar(25) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL,
  PRIMARY KEY (`id`) USING BTREE,
  UNIQUE INDEX `riqi`(`riqi`) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 200059 CHARACTER SET = utf8 COLLATE = utf8_general_ci ROW_FORMAT = Dynamic;

-- ----------------------------
-- Table structure for SC_DP_SJDP_30D
-- ----------------------------
DROP TABLE IF EXISTS `SC_DP_SJDP_30D`;
CREATE TABLE `SC_DP_SJDP_30D`  (
  `id` int(5) NOT NULL AUTO_INCREMENT,
  `riqi` varchar(15) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL,
  `name` varchar(20) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT '设备打类名称',
  `alarm_level` varchar(10) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT '0' COMMENT '告警等级',
  `num` int(10) NULL DEFAULT 0 COMMENT '告警数',
  `js_time` varchar(25) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL,
  PRIMARY KEY (`id`) USING BTREE,
  UNIQUE INDEX `riqi`(`riqi`, `name`, `alarm_level`) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 350994 CHARACTER SET = utf8 COLLATE = utf8_general_ci ROW_FORMAT = Dynamic;

-- ----------------------------
-- Table structure for SC_DP_ZCDP
-- ----------------------------
DROP TABLE IF EXISTS `SC_DP_ZCDP`;
CREATE TABLE `SC_DP_ZCDP`  (
  `id` int(5) NOT NULL AUTO_INCREMENT,
  `riqi` varchar(15) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL,
  `parent_name` varchar(20) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT '设备大类名称',
  `child_name` varchar(10) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT '0' COMMENT '子设备名称',
  `num` int(10) NULL DEFAULT 0 COMMENT '设备数',
  `js_time` varchar(25) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL,
  PRIMARY KEY (`id`) USING BTREE,
  UNIQUE INDEX `riqi`(`riqi`, `parent_name`, `child_name`) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 2842651 CHARACTER SET = utf8 COLLATE = utf8_general_ci ROW_FORMAT = Dynamic;

-- ----------------------------
-- Table structure for SC_JSC_GLDP_BP
-- ----------------------------
DROP TABLE IF EXISTS `SC_JSC_GLDP_BP`;
CREATE TABLE `SC_JSC_GLDP_BP`  (
  `id` int(40) NOT NULL AUTO_INCREMENT,
  `wclgd` varchar(40) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL COMMENT '未处理工单',
  `jrpd` varchar(10) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL COMMENT '今日派单',
  `jrdk` varchar(20) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL COMMENT '今日打卡人数',
  `zjbcs` varchar(20) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL COMMENT '周加班次数',
  `zjbrs` varchar(20) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL COMMENT '周加班人数',
  `zrs` varchar(20) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL COMMENT '总人数',
  `jrjbcs` varchar(20) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL COMMENT '今日加班次数',
  `jrwjb` varchar(10) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL COMMENT '今日未加班',
  `yjbcs` varchar(20) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL COMMENT '月加班次数',
  `gjgd` varchar(10) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL COMMENT '告警工单',
  `tbgjgd` varchar(20) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL COMMENT '同比告警工单',
  `swgd` varchar(20) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL COMMENT '事务工单',
  `tbswgd` varchar(20) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL COMMENT '同比事务工单',
  `riqi` varchar(20) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL COMMENT '计算数据日期',
  `js_time` varbinary(20) NULL DEFAULT NULL,
  PRIMARY KEY (`id`) USING BTREE,
  UNIQUE INDEX `js_time`(`riqi`) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 186605 CHARACTER SET = utf8 COLLATE = utf8_general_ci ROW_FORMAT = Dynamic;

-- ----------------------------
-- Table structure for SC_JSC_GLDP_GDXX
-- ----------------------------
DROP TABLE IF EXISTS `SC_JSC_GLDP_GDXX`;
CREATE TABLE `SC_JSC_GLDP_GDXX`  (
  `id` int(10) NOT NULL AUTO_INCREMENT,
  `gname` varchar(40) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL COMMENT '告警工单类型',
  `wcgd` varchar(10) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL COMMENT '15分钟完成工单数',
  `gjsl` varchar(20) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL COMMENT '告警数量',
  `riqi` varchar(20) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL COMMENT '处理的日期',
  `js_time` varchar(20) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL,
  PRIMARY KEY (`id`) USING BTREE,
  UNIQUE INDEX `js_time`(`riqi`, `gname`) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 1303071 CHARACTER SET = utf8 COLLATE = utf8_general_ci ROW_FORMAT = Dynamic;

-- ----------------------------
-- Table structure for SC_JSC_GLDP_ZZFW
-- ----------------------------
DROP TABLE IF EXISTS `SC_JSC_GLDP_ZZFW`;
CREATE TABLE `SC_JSC_GLDP_ZZFW`  (
  `id` int(40) NOT NULL AUTO_INCREMENT,
  `sjgd` varchar(20) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL COMMENT '事件工单比例',
  `bgsx` varchar(20) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL COMMENT '变更事项比例',
  `fwgd` varchar(20) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL COMMENT '服务工单比例',
  `gjgd` varchar(20) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL COMMENT '告警工单比例',
  `zysq` varchar(20) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL COMMENT '资源申请比例',
  `wpgd` varchar(20) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL COMMENT '外派申请比例',
  `qt` varchar(20) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL COMMENT '其他比例',
  `riqi` varchar(20) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT '计算日期',
  `js_time` varchar(20) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL,
  PRIMARY KEY (`id`) USING BTREE,
  UNIQUE INDEX `js_time`(`riqi`) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 192943 CHARACTER SET = utf8 COLLATE = utf8_general_ci ROW_FORMAT = Dynamic;

-- ----------------------------
-- Table structure for SC_JSC_JKDP
-- ----------------------------
DROP TABLE IF EXISTS `SC_JSC_JKDP`;
CREATE TABLE `SC_JSC_JKDP`  (
  `id` int(5) NOT NULL AUTO_INCREMENT,
  `riqi` varchar(10) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL,
  `level_1` int(10) NULL DEFAULT 0 COMMENT '告警等级1数',
  `level_2` int(10) NULL DEFAULT 0 COMMENT '告警等级2数',
  `level_3` int(10) NULL DEFAULT 0 COMMENT '告警等级3数',
  `level_4` int(10) NULL DEFAULT 0 COMMENT '告警等级4数',
  `alarm_count` int(10) NULL DEFAULT 0 COMMENT '总告警数',
  `not_close_alarm` int(10) NULL DEFAULT 0 COMMENT '未关闭告警数',
  `SB_ALL_count` int(10) NULL DEFAULT 0 COMMENT '总设备数',
  `SB_ZC_count` int(10) NULL DEFAULT 0 COMMENT '正常设备数',
  `SB_YC_count` int(10) NULL DEFAULT 0 COMMENT '异常设备数',
  `js_time` varchar(25) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL,
  PRIMARY KEY (`id`) USING BTREE,
  UNIQUE INDEX `riqi`(`riqi`) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 183755 CHARACTER SET = utf8 COLLATE = utf8_general_ci ROW_FORMAT = Dynamic;

-- ----------------------------
-- Table structure for SC_JSC_JKDP_CSTOP
-- ----------------------------
DROP TABLE IF EXISTS `SC_JSC_JKDP_CSTOP`;
CREATE TABLE `SC_JSC_JKDP_CSTOP`  (
  `id` int(5) NOT NULL AUTO_INCREMENT,
  `riqi` varchar(10) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL,
  `code` int(5) NOT NULL,
  `cs_name` varchar(50) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL,
  `alarm_num` int(10) NULL DEFAULT 0 COMMENT '厂商设备告警总数',
  `cs_sb_num` int(10) NULL DEFAULT 0 COMMENT '厂商设备总数',
  `cs_alarm_rato` float NULL DEFAULT 0 COMMENT '厂商设备告警比值',
  `js_time` varchar(25) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL,
  PRIMARY KEY (`id`) USING BTREE,
  UNIQUE INDEX `riqi`(`riqi`, `code`) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 864212 CHARACTER SET = utf8 COLLATE = utf8_general_ci ROW_FORMAT = Dynamic;

-- ----------------------------
-- Table structure for SC_JSC_JKDP_SBFL
-- ----------------------------
DROP TABLE IF EXISTS `SC_JSC_JKDP_SBFL`;
CREATE TABLE `SC_JSC_JKDP_SBFL`  (
  `id` int(5) NOT NULL AUTO_INCREMENT,
  `riqi` varchar(10) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL,
  `sb_name` varchar(50) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL,
  `yc_num` int(10) NULL DEFAULT 0 COMMENT '异常设备数',
  `sb_all_num` int(10) NULL DEFAULT 0 COMMENT '设备总数',
  `sb_alarm_rato` float NULL DEFAULT 0 COMMENT '设备告警比值',
  `sb_num_all` int(10) NULL DEFAULT 0 COMMENT '设备总数',
  `sb_num_used` int(10) NULL DEFAULT 0 COMMENT '已使用设备数',
  `sb_yc_num` int(10) NULL DEFAULT 0 COMMENT '异常设备数',
  `sb_yc_rato` float NULL DEFAULT 0 COMMENT '设备异常比',
  `js_time` varchar(25) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL,
  PRIMARY KEY (`id`) USING BTREE,
  UNIQUE INDEX `riqi`(`riqi`, `sb_name`) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 1528924 CHARACTER SET = utf8 COLLATE = utf8_general_ci ROW_FORMAT = Dynamic;

-- ----------------------------
-- Table structure for SC_KQ
-- ----------------------------
DROP TABLE IF EXISTS `SC_KQ`;
CREATE TABLE `SC_KQ`  (
  `id` int(20) NOT NULL AUTO_INCREMENT,
  `k_userid` varchar(40) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL,
  `mobile` varchar(20) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL,
  `department` varchar(20) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL,
  `k_OnDuty` varchar(20) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL,
  `k_OffDuty` varchar(20) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL,
  `riqi` varchar(20) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL,
  `k_name` varchar(10) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL,
  `k_jisuan_riqi` varchar(20) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL,
  `j_duration` varchar(10) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL,
  `j_start_time` varchar(20) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL,
  `j_end_time` varchar(20) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL,
  `q_duration` varchar(20) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL,
  `q_start_time` varchar(20) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL,
  `q_end_time` varchar(20) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL,
  `OnResult` varchar(30) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL,
  `OffResult` varchar(30) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL,
  PRIMARY KEY (`id`) USING BTREE,
  UNIQUE INDEX `id`(`k_userid`) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 4647 CHARACTER SET = utf8 COLLATE = utf8_general_ci ROW_FORMAT = Dynamic;

-- ----------------------------
-- Table structure for SC_KQ_JB
-- ----------------------------
DROP TABLE IF EXISTS `SC_KQ_JB`;
CREATE TABLE `SC_KQ_JB`  (
  `id` int(20) NOT NULL AUTO_INCREMENT,
  `user_id` varchar(40) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL,
  `duration` varchar(10) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL,
  `start_time` varchar(20) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL,
  `end_time` varchar(20) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL,
  `riqi` varchar(20) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL,
  `jisuan_riqi` varchar(20) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL,
  PRIMARY KEY (`user_id`, `jisuan_riqi`) USING BTREE,
  INDEX `id`(`id`) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 196 CHARACTER SET = utf8 COLLATE = utf8_general_ci ROW_FORMAT = Dynamic;

-- ----------------------------
-- Table structure for SC_KQ_QJ
-- ----------------------------
DROP TABLE IF EXISTS `SC_KQ_QJ`;
CREATE TABLE `SC_KQ_QJ`  (
  `id` int(20) NOT NULL AUTO_INCREMENT,
  `user_id` varchar(40) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL,
  `duration` varchar(10) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL,
  `start_time` varchar(20) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL,
  `end_time` varchar(20) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL,
  `riqi` varchar(20) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL,
  `jisuan_riqi` varchar(20) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL,
  PRIMARY KEY (`user_id`, `jisuan_riqi`) USING BTREE,
  INDEX `id`(`id`) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 63 CHARACTER SET = utf8 COLLATE = utf8_general_ci ROW_FORMAT = Dynamic;

-- ----------------------------
-- Table structure for SC_KQ_USER
-- ----------------------------
DROP TABLE IF EXISTS `SC_KQ_USER`;
CREATE TABLE `SC_KQ_USER`  (
  `id` int(10) NOT NULL AUTO_INCREMENT,
  `user_id` varchar(40) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL,
  `unionid` varchar(50) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL,
  `openId` varchar(50) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL,
  `mobile` varchar(20) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL,
  `department` varchar(20) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL,
  `OnDuty` varchar(20) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL,
  `OffDuty` varchar(20) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL,
  `riqi` varchar(20) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL,
  `name` varchar(10) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL,
  `jisuan_riqi` varchar(20) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL,
  `OnResult` varchar(10) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL,
  `OffResult` varchar(10) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL,
  `Z_Time` varchar(5) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL,
  PRIMARY KEY (`id`) USING BTREE,
  UNIQUE INDEX `id`(`user_id`) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 128484 CHARACTER SET = utf8 COLLATE = utf8_general_ci ROW_FORMAT = Dynamic;

-- ----------------------------
-- Table structure for SC_META_ECHARTS_MAPPING
-- ----------------------------
DROP TABLE IF EXISTS `SC_META_ECHARTS_MAPPING`;
CREATE TABLE `SC_META_ECHARTS_MAPPING`  (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `domain` varchar(30) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT '业务域(场景) 编号',
  `wd_code` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT '维度代码',
  `wd_name` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL COMMENT '维度名称',
  `zb_code` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT '指标代码',
  `zb_name` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL COMMENT '指标名称',
  `echarts_type` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL COMMENT '图表类型  折线图、柱状图、饼图 多个以‘|’分隔',
  PRIMARY KEY (`id`) USING BTREE,
  UNIQUE INDEX `UK_INDEX_ChART_MAPPING`(`domain`, `wd_code`, `zb_code`) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 17 CHARACTER SET = utf8 COLLATE = utf8_general_ci COMMENT = '指标维度--图表类型 配置元数据表' ROW_FORMAT = Dynamic;

-- ----------------------------
-- Table structure for SC_META_ZC
-- ----------------------------
DROP TABLE IF EXISTS `SC_META_ZC`;
CREATE TABLE `SC_META_ZC`  (
  `id` int(4) NOT NULL AUTO_INCREMENT,
  `domain` varchar(30) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT '业务域',
  `b_scene` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT '场景名称',
  `database_nam` varchar(30) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT '库名',
  `table_name` varchar(30) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT '表名',
  `fileds` varchar(20) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT '字段名',
  `fileds_type` varchar(20) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT '字段类型',
  `unit` varchar(10) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL COMMENT '单位',
  `update_time` varchar(30) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL COMMENT '更新时间',
  `files_name` varchar(20) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL COMMENT '字段中文名称',
  `wd_zb` varchar(20) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL COMMENT '0:维度 1:指标',
  `zd_flag` varchar(3) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL COMMENT '字典类型编号',
  `fileds_ref` varchar(20) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL COMMENT '字典项关联的字段',
  `page_no` int(11) NULL DEFAULT NULL COMMENT '页面排序',
  PRIMARY KEY (`id`) USING BTREE,
  UNIQUE INDEX `domain`(`domain`, `database_nam`, `table_name`, `fileds`) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 14 CHARACTER SET = utf8 COLLATE = utf8_general_ci ROW_FORMAT = Dynamic;

-- ----------------------------
-- Table structure for SC_WDZL_GZDT
-- ----------------------------
DROP TABLE IF EXISTS `SC_WDZL_GZDT`;
CREATE TABLE `SC_WDZL_GZDT`  (
  `id` int(40) NOT NULL AUTO_INCREMENT,
  `zgd` varchar(40) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL COMMENT '我创建的',
  `fpgw` varchar(10) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL COMMENT '分派给我',
  `hd` varchar(20) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL COMMENT '我回答',
  `tw` varchar(20) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL COMMENT '提问',
  `zgj` varchar(20) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL COMMENT '总告警',
  `gjdcl` varchar(20) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL COMMENT '待处理',
  `riqi` varchar(20) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT '计算日期',
  `js_time` varchar(20) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL,
  PRIMARY KEY (`id`) USING BTREE,
  UNIQUE INDEX `id`(`id`) USING BTREE,
  UNIQUE INDEX `js_time`(`riqi`) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 4905 CHARACTER SET = utf8 COLLATE = utf8_general_ci ROW_FORMAT = Dynamic;

-- ----------------------------
-- Table structure for SC_WDZL_GZWT
-- ----------------------------
DROP TABLE IF EXISTS `SC_WDZL_GZWT`;
CREATE TABLE `SC_WDZL_GZWT`  (
  `id` int(10) NOT NULL AUTO_INCREMENT COMMENT '序号',
  `title` varchar(40) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL COMMENT '工作标题',
  `ans` varchar(10) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL COMMENT '回答数',
  `name` varchar(20) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL COMMENT '发起人',
  `time` varchar(40) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL COMMENT '创建日期',
  `riqi` varchar(20) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL,
  `js_time` varchar(20) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL,
  PRIMARY KEY (`id`) USING BTREE,
  UNIQUE INDEX `id`(`id`) USING BTREE,
  UNIQUE INDEX `js_time`(`riqi`) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 145661 CHARACTER SET = utf8 COLLATE = utf8_general_ci ROW_FORMAT = Dynamic;

-- ----------------------------
-- Table structure for SC_WDZL_WDGD
-- ----------------------------
DROP TABLE IF EXISTS `SC_WDZL_WDGD`;
CREATE TABLE `SC_WDZL_WDGD`  (
  `id` int(10) NOT NULL AUTO_INCREMENT COMMENT '序号',
  `title` varchar(40) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL COMMENT '工单标题',
  `type` varchar(10) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL COMMENT '工单类型',
  `name` varchar(20) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL COMMENT '发起人',
  `state` varchar(20) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL COMMENT '状态',
  `time` varchar(40) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL COMMENT '创建日期',
  `riqi` varchar(20) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL,
  `js_time` varchar(20) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL,
  PRIMARY KEY (`id`) USING BTREE,
  UNIQUE INDEX `id`(`id`) USING BTREE,
  UNIQUE INDEX `js_time`(`riqi`) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 10895 CHARACTER SET = utf8 COLLATE = utf8_general_ci ROW_FORMAT = Dynamic;

-- ----------------------------
-- Table structure for SC_WDZL_WGLDGDFB
-- ----------------------------
DROP TABLE IF EXISTS `SC_WDZL_WGLDGDFB`;
CREATE TABLE `SC_WDZL_WGLDGDFB`  (
  `id` int(40) NOT NULL AUTO_INCREMENT,
  `sjgd` varchar(40) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL COMMENT '事件工单',
  `bgsx` varchar(10) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL COMMENT '变更事项',
  `fwgd` varchar(20) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL COMMENT '服务工单',
  `gjgd` varchar(20) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL COMMENT '告警工单',
  `zysq` varchar(20) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL COMMENT '资源申请',
  `wpgd` varchar(20) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL COMMENT '外派申请',
  `qt` varchar(20) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL COMMENT '其他',
  `riqi` varchar(20) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL COMMENT '计算日期',
  `js_time` varchar(20) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL,
  PRIMARY KEY (`id`) USING BTREE,
  UNIQUE INDEX `js_time`(`riqi`) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 4585 CHARACTER SET = utf8 COLLATE = utf8_general_ci ROW_FORMAT = Dynamic;

-- ----------------------------
-- Table structure for SC_WDZL_XMDT
-- ----------------------------
DROP TABLE IF EXISTS `SC_WDZL_XMDT`;
CREATE TABLE `SC_WDZL_XMDT`  (
  `xmgd_zdq` varchar(40) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL,
  `xmgd_dcl` varchar(10) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL,
  `kq_yc` varchar(20) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL,
  `kq_jb` varchar(20) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL,
  `kq_wp` varchar(20) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL,
  `kq_wdk` varchar(20) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL,
  `kq_cd` varchar(20) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL,
  `kq_zt` varchar(20) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL,
  `jisuan_riqi` varchar(20) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL,
  PRIMARY KEY (`jisuan_riqi`) USING BTREE
) ENGINE = InnoDB CHARACTER SET = utf8 COLLATE = utf8_general_ci ROW_FORMAT = Dynamic;

-- ----------------------------
-- Table structure for SC_WDZL_XMGDQS
-- ----------------------------
DROP TABLE IF EXISTS `SC_WDZL_XMGDQS`;
CREATE TABLE `SC_WDZL_XMGDQS`  (
  `id` int(40) NOT NULL AUTO_INCREMENT,
  `a` varchar(40) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL COMMENT '当前月',
  `b` varchar(10) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL COMMENT '前1个月',
  `c` varchar(20) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL COMMENT '前2个月',
  `d` varchar(20) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL COMMENT '前3个月',
  `e` varchar(20) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL COMMENT '前4个月',
  `f` varchar(20) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL COMMENT '前5个月',
  `riqi` varchar(20) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL COMMENT '计算日期',
  `js_time` varchar(20) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL,
  PRIMARY KEY (`id`) USING BTREE,
  UNIQUE INDEX `js_time`(`riqi`) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 4585 CHARACTER SET = utf8 COLLATE = utf8_general_ci ROW_FORMAT = Dynamic;

-- ----------------------------
-- Table structure for SC_WDZL_ZCZYFB
-- ----------------------------
DROP TABLE IF EXISTS `SC_WDZL_ZCZYFB`;
CREATE TABLE `SC_WDZL_ZCZYFB`  (
  `id` int(40) NOT NULL AUTO_INCREMENT,
  `zhuji` varchar(40) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL COMMENT '主机占比',
  `wlsb` varchar(10) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL COMMENT '物理设备',
  `aqsb` varchar(20) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL COMMENT '安全设备',
  `ccsb` varchar(20) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL COMMENT '存储设备',
  `jcss` varchar(20) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL COMMENT '基础设施',
  `riqi` varchar(20) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL COMMENT '计算日期',
  `js_time` varchar(20) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL,
  PRIMARY KEY (`id`) USING BTREE,
  UNIQUE INDEX `js_time`(`riqi`) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 4585 CHARACTER SET = utf8 COLLATE = utf8_general_ci ROW_FORMAT = Dynamic;

-- ----------------------------
-- Table structure for SC_WDZL_ZSKFB
-- ----------------------------
DROP TABLE IF EXISTS `SC_WDZL_ZSKFB`;
CREATE TABLE `SC_WDZL_ZSKFB`  (
  `id` int(40) NOT NULL AUTO_INCREMENT,
  `szjy` varchar(40) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL COMMENT '实战经验占比',
  `llzs` varchar(10) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL COMMENT '理论知识',
  `pxkc` varchar(20) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL COMMENT '培训课程',
  `riqi` varchar(40) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL COMMENT '计算日期',
  `js_time` varchar(40) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL,
  PRIMARY KEY (`id`) USING BTREE,
  UNIQUE INDEX `id`(`id`) USING BTREE,
  UNIQUE INDEX `js_time`(`riqi`) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 4852 CHARACTER SET = utf8 COLLATE = utf8_general_ci ROW_FORMAT = Dynamic;

-- ----------------------------
-- Table structure for SC_ZD_ZC
-- ----------------------------
DROP TABLE IF EXISTS `SC_ZD_ZC`;
CREATE TABLE `SC_ZD_ZC`  (
  `id` int(4) NOT NULL AUTO_INCREMENT,
  `flag` varchar(3) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT '类型标签，0:主设备；1:子设备；2:厂商;3 日期',
  `fk_id` int(11) NULL DEFAULT NULL COMMENT '其他字典表id',
  `code` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL COMMENT '项目编码',
  `name` varchar(30) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT '项目名称',
  `flag_name` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL COMMENT '字典类型名称',
  `parent_id` int(11) NULL DEFAULT NULL COMMENT '父级id',
  `order_no` int(11) NULL DEFAULT NULL COMMENT '页面排列顺序   小的靠前',
  PRIMARY KEY (`id`) USING BTREE,
  UNIQUE INDEX `flag`(`flag`, `name`) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 114 CHARACTER SET = utf8 COLLATE = utf8_general_ci ROW_FORMAT = Dynamic;

-- ----------------------------
-- Table structure for SC_ZHBB_SCRENE_TWO
-- ----------------------------
DROP TABLE IF EXISTS `SC_ZHBB_SCRENE_TWO`;
CREATE TABLE `SC_ZHBB_SCRENE_TWO`  (
  `id` int(20) NOT NULL AUTO_INCREMENT,
  `riqi` varchar(10) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT '计算数据日期',
  `level_id` varchar(2) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT '告警等级',
  `type_id` varchar(2) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT '告警资产类型',
  `zs_Num` int(40) NULL DEFAULT 0 COMMENT '新增告警数',
  `old_Num` int(40) NULL DEFAULT 0 COMMENT '旧存告警数',
  `wcl_Num` int(40) NULL DEFAULT 0 COMMENT '待处理告警数',
  `ycl_Num` int(40) NULL DEFAULT 0 COMMENT '已处理告警数',
  `js_time` varchar(20) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL,
  PRIMARY KEY (`id`) USING BTREE,
  UNIQUE INDEX `riqi`(`riqi`, `level_id`, `type_id`) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 1109 CHARACTER SET = utf8 COLLATE = utf8_general_ci ROW_FORMAT = Dynamic;

-- ----------------------------
-- Table structure for SC_ZHBB_ZYZC
-- ----------------------------
DROP TABLE IF EXISTS `SC_ZHBB_ZYZC`;
CREATE TABLE `SC_ZHBB_ZYZC`  (
  `id` int(40) NOT NULL AUTO_INCREMENT,
  `riqi` varchar(15) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT '计算数据日期',
  `sblx` varchar(20) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT '设备类型',
  `zlx` varchar(20) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT '子类型',
  `cs` varchar(20) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT '厂商',
  `gjsl` varchar(20) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL COMMENT '告警数量',
  `js_time` varchar(20) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT '程序计算日期',
  PRIMARY KEY (`id`) USING BTREE,
  UNIQUE INDEX `js_time`(`riqi`, `sblx`, `zlx`, `cs`) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 9843 CHARACTER SET = utf8 COLLATE = utf8_general_ci ROW_FORMAT = Dynamic;

-- ----------------------------
-- Table structure for alarm
-- ----------------------------
DROP TABLE IF EXISTS `alarm`;
CREATE TABLE `alarm`  (
  `id` mediumint(9) NOT NULL AUTO_INCREMENT,
  `code` varchar(20) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT 'The serial number of this alarm.',
  `name` varchar(45) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL,
  `asset_id` mediumint(9) NOT NULL COMMENT 'The serial number of the machine to which this alarm is belong.',
  `indice_id` mediumint(9) NOT NULL COMMENT 'The serial number of the indice which this alarm is associated with.',
  `indice_val` varchar(45) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL COMMENT 'The value of the associated indice.',
  `level_id` mediumint(9) NOT NULL COMMENT '1一般2较严重3严重4灾难The serial number of the alarm level.',
  `type_id` mediumint(9) NULL DEFAULT 1 COMMENT '////1一般2较严重3严重4灾难',
  `status` varchar(45) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT '0' COMMENT '0待处理、1已派单、2已关闭   //0:untreated,1:treated,2:confirmed,3:distributed,4:other',
  `description` varchar(45) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL COMMENT 'The description of this alarm.',
  `time_occur` datetime(0) NULL DEFAULT NULL COMMENT 'The time of alarm which happens',
  `solution` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL COMMENT '解决方案',
  `remark` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL COMMENT '备注',
  `confirm_people` varchar(45) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL COMMENT '确认人',
  `confirm_time` datetime(0) NULL DEFAULT NULL COMMENT '确认时间',
  `field1` varchar(45) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL,
  `field2` varchar(45) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL,
  `field3` varchar(45) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL,
  `rule` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL COMMENT '触发规则',
  PRIMARY KEY (`id`) USING BTREE,
  UNIQUE INDEX `code_UNIQUE`(`code`) USING BTREE,
  INDEX `fk_asset_alarm_asset_indice1_idx`(`indice_id`) USING BTREE,
  INDEX `fk_asset_alarm_asset_alarm_level1_idx`(`level_id`) USING BTREE,
  INDEX `fk_asset_alarm_asset1_idx`(`asset_id`) USING BTREE,
  INDEX `fk_alarm_alarm_type1_idx`(`type_id`) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 4 CHARACTER SET = utf8 COLLATE = utf8_general_ci COMMENT = '资产告警表' ROW_FORMAT = Dynamic;

-- ----------------------------
-- Table structure for alarm_level
-- ----------------------------
DROP TABLE IF EXISTS `alarm_level`;
CREATE TABLE `alarm_level`  (
  `id` mediumint(9) NOT NULL AUTO_INCREMENT,
  `code` varchar(20) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT 'The seril number of this alarm level.',
  `name` varchar(20) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT 'The name of this alarm level.',
  `field1` varchar(45) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL,
  `field2` varchar(45) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL,
  `field3` varchar(45) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL,
  PRIMARY KEY (`id`) USING BTREE,
  UNIQUE INDEX `code_UNIQUE`(`code`) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 5 CHARACTER SET = utf8 COLLATE = utf8_general_ci COMMENT = '资产告警等级表' ROW_FORMAT = Dynamic;

-- ----------------------------
-- Table structure for alarm_strategy
-- ----------------------------
DROP TABLE IF EXISTS `alarm_strategy`;
CREATE TABLE `alarm_strategy`  (
  `id` int(10) NOT NULL AUTO_INCREMENT,
  `strategy_name` varchar(20) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL,
  `strategy_desc` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL,
  `asset_kind` varchar(10) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL,
  `strategy_kind` int(10) NULL DEFAULT NULL,
  `is_enable` varchar(5) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL,
  `alarm_level` varchar(2) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL,
  `up_time` datetime(0) NULL DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP(0),
  `project_id` int(10) NULL DEFAULT NULL,
  `create_person` int(10) NULL DEFAULT NULL,
  `object_flag` bit(1) NULL DEFAULT b'0' COMMENT '接收对象，0接收组，1接收人',
  `user_ids` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL,
  `group_ids` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL COMMENT '通知分组ids',
  `notice_time_start` time(0) NULL DEFAULT NULL,
  `notice_time_end` time(0) NULL DEFAULT NULL,
  `notice_way` varchar(20) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL,
  PRIMARY KEY (`id`) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 99 CHARACTER SET = utf8 COLLATE = utf8_general_ci ROW_FORMAT = Dynamic;

-- ----------------------------
-- Table structure for alarm_type
-- ----------------------------
DROP TABLE IF EXISTS `alarm_type`;
CREATE TABLE `alarm_type`  (
  `id` mediumint(9) NOT NULL AUTO_INCREMENT,
  `code` varchar(45) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL,
  `name` varchar(45) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL,
  `description` varchar(45) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL,
  `field1` varchar(45) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL,
  `field2` varchar(45) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL,
  `field3` varchar(45) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL,
  PRIMARY KEY (`id`) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 2 CHARACTER SET = utf8 COLLATE = utf8_general_ci ROW_FORMAT = Dynamic;

-- ----------------------------
-- Table structure for area
-- ----------------------------
DROP TABLE IF EXISTS `area`;
CREATE TABLE `area`  (
  `id` int(20) NOT NULL AUTO_INCREMENT COMMENT 'id',
  `parent_id` int(20) NOT NULL COMMENT '父id',
  `name` varchar(20) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT '名称',
  `type` int(20) NULL DEFAULT NULL,
  PRIMARY KEY (`id`) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 3409 CHARACTER SET = utf8 COLLATE = utf8_general_ci COMMENT = '省和市表' ROW_FORMAT = Dynamic;

-- ----------------------------
-- Table structure for asset
-- ----------------------------
DROP TABLE IF EXISTS `asset`;
CREATE TABLE `asset`  (
  `id` mediumint(9) NOT NULL AUTO_INCREMENT,
  `code` varchar(100) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT 'The serial number of this machine.',
  `name` varchar(100) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT 'The name of this ma.chine',
  `ipv4` varchar(15) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL COMMENT 'The ipv4 address of this machine.',
  `ipv6` varchar(40) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL COMMENT 'The ipv6 address of this machine.',
  `mac` varchar(20) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL,
  `system` varchar(15) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL COMMENT 'The operating system of this machine.',
  `manufacturer_id` mediumint(9) NULL DEFAULT NULL COMMENT 'The vendor of this machine.',
  `time_running` bigint(20) NULL DEFAULT NULL COMMENT 'The total time that this machine has kept running.',
  `status_running` int(11) NULL DEFAULT NULL COMMENT 'The type of this machine\'s running status.',
  `status_monitoring` int(11) NULL DEFAULT NULL COMMENT 'The status number indicates that whether this machine is in monitoring or not.',
  `region_id` mediumint(9) NULL DEFAULT NULL COMMENT 'The serial numebr of the region in which this machine is settled.',
  `position` varchar(45) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL,
  `level` int(11) NULL DEFAULT NULL,
  `manager` varchar(45) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL COMMENT 'the person who focus on this asset',
  `brand` varchar(20) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL,
  `purchasing_time` datetime(0) NULL DEFAULT NULL COMMENT 'the time to buy this asset',
  `maintenance_time` datetime(0) NULL DEFAULT NULL COMMENT 'the time of this asset to maintenance for free',
  `waste_time` datetime(0) NULL DEFAULT NULL COMMENT 'the time to drop this asset',
  `switch_mark` varchar(5) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '1',
  `height` varchar(10) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL COMMENT 'the height of asset',
  `description` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL COMMENT 'the description of asset',
  `field1` varchar(45) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL,
  `field2` varchar(45) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL,
  `field3` varchar(45) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL,
  `model_name` varchar(50) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL COMMENT 'the number of model',
  `partitions` varchar(50) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL,
  `room` varchar(50) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL,
  `box` varchar(50) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL,
  `unit` varchar(50) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL,
  `hardware_name` varchar(50) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL,
  `sysVersion` varchar(25) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL,
  `mainboard` varchar(25) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL,
  `mainboard_manu` varchar(25) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL,
  `mainboard_serial` varchar(25) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL,
  `cpu` varchar(25) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL,
  `cpu_cores_physic` int(3) NULL DEFAULT NULL,
  `cpu_cores_logic` int(3) NULL DEFAULT NULL,
  `serial_number` varchar(50) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL,
  `vendor` varchar(25) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL,
  `vendor_man` varchar(50) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL,
  `vendor_phone` varchar(25) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL,
  `status` int(3) NULL DEFAULT NULL COMMENT '0 means online, 1 means offline, 2 means fixing, 3 means stored',
  `update_time` datetime(0) NULL DEFAULT CURRENT_TIMESTAMP(0) ON UPDATE CURRENT_TIMESTAMP(0),
  `model` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL,
  `size` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL,
  `attach_time` datetime(0) NULL DEFAULT NULL,
  `dept_id` int(11) NULL DEFAULT NULL,
  `use_team_id` int(11) NULL DEFAULT NULL,
  `product_id` int(11) NULL DEFAULT NULL,
  `bocket_name` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL,
  `bocket_capcity` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL,
  `device_id` int(11) NULL DEFAULT NULL,
  PRIMARY KEY (`id`) USING BTREE,
  UNIQUE INDEX `code_UNIQUE`(`code`) USING BTREE,
  INDEX `fk_asset_asset_region1_idx`(`region_id`) USING BTREE,
  INDEX `fk_asset_asset_manufacturer1`(`manufacturer_id`) USING BTREE,
  CONSTRAINT `asset_ibfk_1` FOREIGN KEY (`manufacturer_id`) REFERENCES `manufacturer` (`id`) ON DELETE NO ACTION ON UPDATE NO ACTION
) ENGINE = InnoDB AUTO_INCREMENT = 787 CHARACTER SET = utf8 COLLATE = utf8_general_ci COMMENT = '资产表' ROW_FORMAT = Dynamic;

-- ----------------------------
-- Table structure for asset_category
-- ----------------------------
DROP TABLE IF EXISTS `asset_category`;
CREATE TABLE `asset_category`  (
  `id` mediumint(9) NOT NULL AUTO_INCREMENT,
  `parent_id` mediumint(9) NULL DEFAULT NULL,
  `code` varchar(20) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT 'The serial number of this machine category.',
  `name` varchar(200) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL,
  `type` int(5) NULL DEFAULT NULL COMMENT '0 means first level, 1 means second level',
  `field1` varchar(45) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL,
  `field2` varchar(45) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL,
  `field3` varchar(45) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL,
  PRIMARY KEY (`id`) USING BTREE,
  UNIQUE INDEX `code_UNIQUE`(`code`) USING BTREE,
  INDEX `fk_asset_category_asset_category1_idx`(`parent_id`) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 34 CHARACTER SET = utf8 COLLATE = utf8_general_ci COMMENT = '资产类型表' ROW_FORMAT = Dynamic;

-- ----------------------------
-- Table structure for asset_category_indice_mapping
-- ----------------------------
DROP TABLE IF EXISTS `asset_category_indice_mapping`;
CREATE TABLE `asset_category_indice_mapping`  (
  `asset_category_id` mediumint(9) NOT NULL,
  `indice_id` mediumint(9) NOT NULL,
  `field1` varchar(45) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL,
  `field2` varchar(45) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL,
  `field3` varchar(45) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL,
  PRIMARY KEY (`asset_category_id`, `indice_id`) USING BTREE,
  INDEX `fk_asset_category_indice_mapping_indice_id`(`indice_id`) USING BTREE,
  CONSTRAINT `asset_category_indice_mapping_ibfk_1` FOREIGN KEY (`asset_category_id`) REFERENCES `asset_category` (`id`) ON DELETE NO ACTION ON UPDATE NO ACTION,
  CONSTRAINT `asset_category_indice_mapping_ibfk_2` FOREIGN KEY (`indice_id`) REFERENCES `asset_indice` (`id`) ON DELETE NO ACTION ON UPDATE NO ACTION
) ENGINE = InnoDB CHARACTER SET = utf8 COLLATE = utf8_general_ci ROW_FORMAT = Dynamic;

-- ----------------------------
-- Table structure for asset_category_mapping
-- ----------------------------
DROP TABLE IF EXISTS `asset_category_mapping`;
CREATE TABLE `asset_category_mapping`  (
  `asset_category_id` mediumint(9) NOT NULL,
  `asset_id` mediumint(9) NOT NULL,
  `field1` varchar(45) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL,
  `field2` varchar(45) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL,
  `field3` varchar(45) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL,
  PRIMARY KEY (`asset_category_id`, `asset_id`) USING BTREE,
  INDEX `fk_asset_category_has_asset_asset1_idx`(`asset_id`) USING BTREE,
  INDEX `fk_asset_category_has_asset_category_id_idx`(`asset_category_id`) USING BTREE
) ENGINE = InnoDB CHARACTER SET = utf8 COLLATE = utf8_general_ci COMMENT = '资产与资产分类的关联表' ROW_FORMAT = Dynamic;

-- ----------------------------
-- Table structure for asset_dept
-- ----------------------------
DROP TABLE IF EXISTS `asset_dept`;
CREATE TABLE `asset_dept`  (
  `id` smallint(6) UNSIGNED NOT NULL AUTO_INCREMENT COMMENT '使用部门id',
  `name` varchar(20) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '使用部门名称',
  `pid` mediumint(8) UNSIGNED NOT NULL DEFAULT 0 COMMENT '项目Id',
  `close` tinyint(2) UNSIGNED NOT NULL DEFAULT 0 COMMENT '是否关闭；0：正常；1：关闭；',
  PRIMARY KEY (`id`) USING BTREE
) ENGINE = MyISAM AUTO_INCREMENT = 89 CHARACTER SET = utf8 COLLATE = utf8_general_ci COMMENT = '资产所属部门表' ROW_FORMAT = Dynamic;

-- ----------------------------
-- Table structure for asset_device
-- ----------------------------
DROP TABLE IF EXISTS `asset_device`;
CREATE TABLE `asset_device`  (
  `id` smallint(6) UNSIGNED NOT NULL AUTO_INCREMENT COMMENT '使用部门id',
  `name` varchar(20) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '使用部门名称',
  `pid` mediumint(8) UNSIGNED NOT NULL DEFAULT 0 COMMENT '项目Id',
  `close` tinyint(2) UNSIGNED NOT NULL DEFAULT 0 COMMENT '是否关闭；0：正常；1：关闭；',
  PRIMARY KEY (`id`) USING BTREE
) ENGINE = MyISAM AUTO_INCREMENT = 89 CHARACTER SET = utf8 COLLATE = utf8_general_ci COMMENT = '资产设备分组表' ROW_FORMAT = Dynamic;

-- ----------------------------
-- Table structure for asset_indice
-- ----------------------------
DROP TABLE IF EXISTS `asset_indice`;
CREATE TABLE `asset_indice`  (
  `id` mediumint(9) NOT NULL AUTO_INCREMENT,
  `code` varchar(20) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT 'The serial number of this indice.',
  `name` varchar(20) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT 'The name of this indice.',
  `description` varchar(100) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL COMMENT 'The description of this indice.',
  `unit` varchar(45) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL,
  `field2` varchar(45) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL,
  `field3` varchar(45) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL,
  PRIMARY KEY (`id`) USING BTREE,
  UNIQUE INDEX `code_UNIQUE`(`code`) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 140 CHARACTER SET = utf8 COLLATE = utf8_general_ci COMMENT = '资产监控指标表' ROW_FORMAT = Dynamic;

-- ----------------------------
-- Table structure for asset_place
-- ----------------------------
DROP TABLE IF EXISTS `asset_place`;
CREATE TABLE `asset_place`  (
  `id` int(20) NOT NULL AUTO_INCREMENT COMMENT 'the primary key',
  `parent_id` int(20) NULL DEFAULT NULL,
  `code` varchar(20) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT 'the code of this place',
  `name` varchar(20) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL COMMENT 'the name of this  place',
  `field1` varchar(50) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL,
  `field2` varchar(50) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL,
  `field3` varchar(50) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL,
  PRIMARY KEY (`id`) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 422 CHARACTER SET = utf8 COLLATE = utf8_general_ci ROW_FORMAT = Dynamic;

-- ----------------------------
-- Table structure for asset_place_mapping
-- ----------------------------
DROP TABLE IF EXISTS `asset_place_mapping`;
CREATE TABLE `asset_place_mapping`  (
  `asset_id` mediumint(20) NOT NULL COMMENT 'the id of asset',
  `asset_place_id` int(20) NOT NULL,
  `field1` varchar(50) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL,
  `field2` varchar(50) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL,
  `field3` varchar(50) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL,
  PRIMARY KEY (`asset_id`, `asset_place_id`) USING BTREE,
  INDEX `asset_place_foreign`(`asset_place_id`) USING BTREE,
  CONSTRAINT `asset_place_mapping_ibfk_1` FOREIGN KEY (`asset_id`) REFERENCES `asset` (`id`) ON DELETE NO ACTION ON UPDATE NO ACTION,
  CONSTRAINT `asset_place_mapping_ibfk_2` FOREIGN KEY (`asset_place_id`) REFERENCES `asset_place` (`id`) ON DELETE NO ACTION ON UPDATE NO ACTION
) ENGINE = InnoDB CHARACTER SET = utf8 COLLATE = utf8_general_ci ROW_FORMAT = Dynamic;

-- ----------------------------
-- Table structure for asset_record
-- ----------------------------
DROP TABLE IF EXISTS `asset_record`;
CREATE TABLE `asset_record`  (
  `id` mediumint(9) NOT NULL AUTO_INCREMENT,
  `code` varchar(20) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL,
  `name` varchar(50) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL,
  `asset_id` mediumint(9) NOT NULL COMMENT 'The serial number of the asset which this changing record associated with.',
  `description` varchar(1000) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL COMMENT 'The details of this changing record.',
  `assigner_code` mediumint(20) NULL DEFAULT NULL COMMENT 'The serial number of the assigner who submit this changing record.',
  `executor_code` varchar(20) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL COMMENT 'The serial number of the executor who is going to implement this changing.',
  `exe_date` datetime(0) NULL DEFAULT NULL COMMENT 'The deadline to execute this changing.',
  `exe_res` varchar(100) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL COMMENT 'The result of this changing.',
  `field1` varchar(45) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL,
  `field2` varchar(45) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL,
  `field3` varchar(45) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL,
  `operate_kind` varchar(10) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL,
  PRIMARY KEY (`id`) USING BTREE,
  UNIQUE INDEX `code_UNIQUE`(`code`) USING BTREE,
  INDEX `fk_asset_record_asset1_idx`(`asset_id`) USING BTREE,
  CONSTRAINT `asset_record_ibfk_1` FOREIGN KEY (`asset_id`) REFERENCES `asset` (`id`) ON DELETE NO ACTION ON UPDATE NO ACTION
) ENGINE = InnoDB AUTO_INCREMENT = 525 CHARACTER SET = utf8 COLLATE = utf8_general_ci COMMENT = '资产变更记录表' ROW_FORMAT = Dynamic;

-- ----------------------------
-- Table structure for asset_use_team
-- ----------------------------
DROP TABLE IF EXISTS `asset_use_team`;
CREATE TABLE `asset_use_team`  (
  `id` smallint(6) UNSIGNED NOT NULL AUTO_INCREMENT COMMENT '使用部门id',
  `name` varchar(20) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '使用部门名称',
  `pid` mediumint(8) UNSIGNED NOT NULL DEFAULT 0 COMMENT '项目Id',
  `close` tinyint(2) UNSIGNED NOT NULL DEFAULT 0 COMMENT '是否关闭；0：正常；1：关闭；',
  PRIMARY KEY (`id`) USING BTREE
) ENGINE = MyISAM AUTO_INCREMENT = 89 CHARACTER SET = utf8 COLLATE = utf8_general_ci COMMENT = '资产使用部门表' ROW_FORMAT = Dynamic;

-- ----------------------------
-- Table structure for assets_product
-- ----------------------------
DROP TABLE IF EXISTS `assets_product`;
CREATE TABLE `assets_product`  (
  `id` mediumint(8) UNSIGNED NOT NULL AUTO_INCREMENT,
  `utid` smallint(6) UNSIGNED NULL DEFAULT 0 COMMENT '使用部门id',
  `name` varchar(20) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '产品名称',
  `pid` mediumint(8) UNSIGNED NOT NULL DEFAULT 0 COMMENT '父级id',
  `close` tinyint(2) UNSIGNED NOT NULL DEFAULT 0 COMMENT '是否关闭；0：正常；1：关闭；',
  PRIMARY KEY (`id`) USING BTREE
) ENGINE = MyISAM AUTO_INCREMENT = 237 CHARACTER SET = utf8 COLLATE = utf8_general_ci COMMENT = '产品表' ROW_FORMAT = Dynamic;

-- ----------------------------
-- Table structure for attendance
-- ----------------------------
DROP TABLE IF EXISTS `attendance`;
CREATE TABLE `attendance`  (
  `id` int(10) NOT NULL AUTO_INCREMENT,
  `attendance_person` int(10) NULL DEFAULT NULL COMMENT 'the id of person who attendance',
  `onWork_time` datetime(0) NULL DEFAULT NULL COMMENT 'the time to go to work',
  `offWork_time` datetime(0) NULL DEFAULT NULL COMMENT 'the time to go off work',
  `work_duration` varchar(5) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL COMMENT 'the duration of work',
  `status` varchar(5) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL COMMENT '0:normal,1:late,2:early,3:off',
  `project_id` int(20) NOT NULL COMMENT '项目id',
  PRIMARY KEY (`id`) USING BTREE,
  INDEX `fk_attendance_user`(`attendance_person`) USING BTREE,
  INDEX `fk_attendance_fk_project`(`project_id`) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 5 CHARACTER SET = utf8 COLLATE = utf8_general_ci ROW_FORMAT = Dynamic;

-- ----------------------------
-- Table structure for control_cabin
-- ----------------------------
DROP TABLE IF EXISTS `control_cabin`;
CREATE TABLE `control_cabin`  (
  `jrgj` varchar(40) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL COMMENT '今日告警数',
  `yb` varchar(40) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL COMMENT '今日告警（一般）',
  `jyz` varchar(40) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL COMMENT '今日告警（较严重）',
  `yz` varchar(40) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL COMMENT '今日告警（严重）',
  `tbzt` varchar(40) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL COMMENT '同比昨天',
  `yzgj` varchar(40) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL COMMENT '一周告警',
  `tbsz` varchar(40) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL COMMENT '同比上周',
  `wcl` varchar(40) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL COMMENT '未处理告警占比',
  `sbyc` varchar(40) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL COMMENT '设备异常占比',
  `wclgd` varchar(40) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL COMMENT '未处理工单',
  `jrdk` varchar(40) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL COMMENT '今日到岗',
  `zjbcs` varchar(40) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL COMMENT '周加班次数',
  `zjbrs` varchar(40) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL COMMENT '周加班人数',
  `jisuan_riqi` varchar(40) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT '计算的时间',
  `zgd` varchar(40) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL,
  `fpgw` varchar(40) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL,
  `hd` varchar(40) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL,
  `tw` varchar(40) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL,
  `zgj` varchar(40) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL,
  `gjdcl` varchar(40) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL,
  PRIMARY KEY (`jisuan_riqi`) USING BTREE
) ENGINE = InnoDB CHARACTER SET = utf8 COLLATE = utf8_general_ci COMMENT = '驾驶舱表' ROW_FORMAT = Dynamic;

-- ----------------------------
-- Table structure for customer
-- ----------------------------
DROP TABLE IF EXISTS `customer`;
CREATE TABLE `customer`  (
  `id` int(20) NOT NULL AUTO_INCREMENT COMMENT 'id',
  `code` varchar(20) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL COMMENT '编号',
  `province_id` int(20) NULL DEFAULT NULL COMMENT '省份id',
  `city_id` int(20) NULL DEFAULT NULL COMMENT '地市id',
  `organization` varchar(100) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL COMMENT '机构',
  `department` varchar(100) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL COMMENT '部门',
  `name` varchar(30) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL COMMENT '姓名',
  `position` varchar(30) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL COMMENT '职位',
  `board` varchar(100) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL COMMENT '负责版块',
  `tel` varchar(20) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL COMMENT '联系电话',
  `mail` varchar(30) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL COMMENT '邮箱',
  `create_time` datetime(0) NOT NULL DEFAULT CURRENT_TIMESTAMP(0) COMMENT '创建日期',
  `project_id` int(20) NULL DEFAULT NULL,
  PRIMARY KEY (`id`) USING BTREE,
  INDEX `fk_customer_fk_project`(`project_id`) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 36 CHARACTER SET = utf8 COLLATE = utf8_general_ci COMMENT = '客户表' ROW_FORMAT = Dynamic;

-- ----------------------------
-- Table structure for disk
-- ----------------------------
DROP TABLE IF EXISTS `disk`;
CREATE TABLE `disk`  (
  `id` mediumint(9) NOT NULL AUTO_INCREMENT,
  `asset_id` mediumint(9) NOT NULL,
  `name` varchar(25) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL,
  `capacity` varchar(25) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL,
  PRIMARY KEY (`id`) USING BTREE,
  INDEX `fk_disk_asset1_idx`(`asset_id`) USING BTREE,
  CONSTRAINT `disk_ibfk_1` FOREIGN KEY (`asset_id`) REFERENCES `asset` (`id`) ON DELETE NO ACTION ON UPDATE NO ACTION
) ENGINE = InnoDB AUTO_INCREMENT = 65 CHARACTER SET = utf8 COLLATE = utf8_general_ci COMMENT = '磁盘表' ROW_FORMAT = Dynamic;

-- ----------------------------
-- Table structure for dome
-- ----------------------------
DROP TABLE IF EXISTS `dome`;
CREATE TABLE `dome`  (
  `a` varchar(40) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL,
  `b` varchar(10) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL,
  `c` varchar(20) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL,
  `d` varchar(20) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL,
  `e` varchar(20) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL,
  `f` varchar(20) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL,
  `g` varchar(10) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL,
  `h` varchar(20) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL,
  `i` varchar(10) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL,
  `j` varchar(20) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL,
  `k` varchar(20) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL,
  `l` varchar(20) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL,
  `m` varchar(20) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL,
  `n` varchar(20) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL,
  `o` varchar(20) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL,
  `p` varchar(20) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL,
  `jisuan_riqi` varchar(20) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL,
  PRIMARY KEY (`jisuan_riqi`) USING BTREE
) ENGINE = InnoDB CHARACTER SET = utf8 COLLATE = utf8_general_ci ROW_FORMAT = Dynamic;

-- ----------------------------
-- Table structure for dp_icon_manage
-- ----------------------------
DROP TABLE IF EXISTS `dp_icon_manage`;
CREATE TABLE `dp_icon_manage`  (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `asset_type_code` varchar(200) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL COMMENT '设备类型编号',
  `asset_type_name` varchar(200) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL COMMENT '设备类型名称',
  `asset_id` int(11) NULL DEFAULT NULL COMMENT '设备id 用于关联设备',
  `assert_num` varchar(200) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL COMMENT '设备编号 （机柜编号）',
  `icon_path` varchar(200) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL COMMENT '图片路径',
  `icon_layer` int(11) NULL DEFAULT 1 COMMENT '图层位置  1为最底层',
  `x_coord` float NULL DEFAULT NULL COMMENT '图片左上角 X坐标位置 单位px',
  `y_coord` float NULL DEFAULT NULL COMMENT '图片左上角 Y坐标位置 单位px',
  `assert_status` int(11) NULL DEFAULT 0 COMMENT '设备状态',
  `org_code` varchar(200) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL COMMENT '客户（用户）机构编码',
  `org_name` varchar(200) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL COMMENT '客户（用户）机构名称',
  `room_code` varchar(200) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL COMMENT '机房编号',
  `room_name` varchar(200) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL COMMENT '机房名称',
  PRIMARY KEY (`id`) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 92 CHARACTER SET = utf8 COLLATE = utf8_general_ci COMMENT = '大屏 机房设备部署图 信息管理表' ROW_FORMAT = Dynamic;

-- ----------------------------
-- Table structure for dp_rittalassert_icon_mapping
-- ----------------------------
DROP TABLE IF EXISTS `dp_rittalassert_icon_mapping`;
CREATE TABLE `dp_rittalassert_icon_mapping`  (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `asset_type_code` varchar(200) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL COMMENT '设备类型编号',
  `icon_path` varchar(200) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL COMMENT '图片路径',
  PRIMARY KEY (`id`) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 7 CHARACTER SET = utf8 COLLATE = utf8_general_ci COMMENT = '大屏 机柜设备与图片映射关系表' ROW_FORMAT = Dynamic;

-- ----------------------------
-- Table structure for dtc_win_disk
-- ----------------------------
DROP TABLE IF EXISTS `dtc_win_disk`;
CREATE TABLE `dtc_win_disk`  (
  `host_ip` varchar(10) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL,
  `last_code` varchar(10) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL,
  `result` varchar(500) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL,
  PRIMARY KEY (`host_ip`, `last_code`) USING BTREE
) ENGINE = InnoDB CHARACTER SET = utf8 COLLATE = utf8_general_ci ROW_FORMAT = Dynamic;

-- ----------------------------
-- Table structure for expatriate
-- ----------------------------
DROP TABLE IF EXISTS `expatriate`;
CREATE TABLE `expatriate`  (
  `id` int(10) NOT NULL AUTO_INCREMENT,
  `expatriate_person` int(10) NULL DEFAULT NULL COMMENT 'the person who is expatriated',
  `expatriate_time` datetime(0) NULL DEFAULT NULL COMMENT 'the time to expatriate',
  `expatriate_project` int(20) NULL DEFAULT NULL COMMENT 'the project to expatriate',
  `plan_duration` varchar(10) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL COMMENT 'the duration plan to work',
  `status` varchar(5) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL COMMENT '0:todo,1:done',
  `assess_person` int(10) NULL DEFAULT NULL COMMENT 'the person assess',
  `approval_person` int(10) NULL DEFAULT NULL COMMENT 'the person who approval this expatriate',
  `project_id` int(20) NOT NULL COMMENT '项目id',
  PRIMARY KEY (`id`) USING BTREE,
  INDEX `fk_expatriate_person`(`expatriate_person`) USING BTREE,
  INDEX `fk_assess_person`(`assess_person`) USING BTREE,
  INDEX `fk_approval_person`(`approval_person`) USING BTREE,
  INDEX `fk_expatriate_fk_project`(`project_id`) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 5 CHARACTER SET = utf8 COLLATE = utf8_general_ci COMMENT = '外派信息表' ROW_FORMAT = Dynamic;

-- ----------------------------
-- Table structure for file
-- ----------------------------
DROP TABLE IF EXISTS `file`;
CREATE TABLE `file`  (
  `id` int(20) NOT NULL AUTO_INCREMENT COMMENT 'id',
  `fk_id` int(20) NULL DEFAULT NULL COMMENT '工单id、荣誉id、重大节日保障id',
  `name` varchar(100) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL COMMENT '名称（包含后缀）',
  `url` varchar(100) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL COMMENT '路径',
  `type` varchar(20) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL COMMENT '类型，1：工单、2：荣誉、3：重大节日保障、4：知识、5：模板、6：拓扑图 7：告警解决方案 8:工作周报',
  `position` tinyint(4) NOT NULL DEFAULT 1 COMMENT '展示的位置',
  `create_time` datetime(0) NOT NULL DEFAULT CURRENT_TIMESTAMP(0) COMMENT '创建日期',
  PRIMARY KEY (`id`) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 504 CHARACTER SET = utf8 COLLATE = utf8_general_ci COMMENT = '附件表' ROW_FORMAT = Dynamic;

-- ----------------------------
-- Table structure for guarantee
-- ----------------------------
DROP TABLE IF EXISTS `guarantee`;
CREATE TABLE `guarantee`  (
  `id` int(20) NOT NULL AUTO_INCREMENT COMMENT 'id',
  `holiday_id` int(20) NOT NULL COMMENT '节日id',
  `project_id` int(20) NOT NULL COMMENT '项目id',
  `support_plan` varchar(200) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL COMMENT '保障方案',
  `inspection_report` varchar(200) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL COMMENT '巡检报告',
  `optimize_report` varchar(200) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL COMMENT '优化报告',
  `contingency_plan` varchar(200) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL COMMENT '应急预案',
  `guarantee_unit` varchar(400) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL COMMENT '保障单位',
  `summary_report` varchar(400) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL COMMENT '总结报告',
  `create_person_id` int(20) NOT NULL COMMENT '创建人',
  `create_time` datetime(0) NOT NULL DEFAULT CURRENT_TIMESTAMP(0) COMMENT '创建日期',
  PRIMARY KEY (`id`) USING BTREE,
  INDEX `fk_guarantee_holiday`(`holiday_id`) USING BTREE,
  INDEX `fk_guarantee_project`(`project_id`) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 15 CHARACTER SET = utf8 COLLATE = utf8_general_ci COMMENT = '重大节日保障表' ROW_FORMAT = Dynamic;

-- ----------------------------
-- Table structure for gzkh_week_dic
-- ----------------------------
DROP TABLE IF EXISTS `gzkh_week_dic`;
CREATE TABLE `gzkh_week_dic`  (
  `id` int(11) NOT NULL AUTO_INCREMENT COMMENT '主键id',
  `wname` varchar(255) CHARACTER SET utf8 COLLATE utf8_bin NULL DEFAULT NULL COMMENT '工作周',
  `date_begin` varchar(255) CHARACTER SET utf8 COLLATE utf8_bin NULL DEFAULT NULL COMMENT '开始日期',
  `date_end` varchar(255) CHARACTER SET utf8 COLLATE utf8_bin NULL DEFAULT NULL COMMENT '结束日期',
  PRIMARY KEY (`id`) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 4 CHARACTER SET = utf8 COLLATE = utf8_bin COMMENT = '绩效考核--工作周字典表' ROW_FORMAT = Dynamic;

-- ----------------------------
-- Table structure for gzkh_weekly
-- ----------------------------
DROP TABLE IF EXISTS `gzkh_weekly`;
CREATE TABLE `gzkh_weekly`  (
  `id` int(11) NOT NULL AUTO_INCREMENT COMMENT '主键id',
  `wid` int(11) NULL DEFAULT NULL COMMENT '工作周id',
  `wname` varchar(255) CHARACTER SET utf8 COLLATE utf8_bin NULL DEFAULT NULL COMMENT '工作周',
  `summary` varchar(1000) CHARACTER SET utf8 COLLATE utf8_bin NULL DEFAULT NULL COMMENT '工作总结',
  `next_plan` varchar(1000) CHARACTER SET utf8 COLLATE utf8_bin NULL DEFAULT NULL COMMENT '下周计划',
  `remark` varchar(1000) CHARACTER SET utf8 COLLATE utf8_bin NULL DEFAULT NULL COMMENT '备注',
  `cuid` int(11) NULL DEFAULT NULL COMMENT '创建人id',
  `cr_time` datetime(0) NULL DEFAULT NULL COMMENT '创建时间',
  `up_time` datetime(0) NULL DEFAULT NULL COMMENT '最近一次更新时间',
  `state` char(1) CHARACTER SET utf8 COLLATE utf8_bin NULL DEFAULT NULL COMMENT '周报状态 1有效  0删除',
  `work_finish` varchar(1000) CHARACTER SET utf8 COLLATE utf8_bin NULL DEFAULT NULL COMMENT '完成的工作',
  PRIMARY KEY (`id`) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 17 CHARACTER SET = utf8 COLLATE = utf8_bin COMMENT = '绩效考核--周报信息表' ROW_FORMAT = Dynamic;

-- ----------------------------
-- Table structure for gzkh_weekly_lookup
-- ----------------------------
DROP TABLE IF EXISTS `gzkh_weekly_lookup`;
CREATE TABLE `gzkh_weekly_lookup`  (
  `id` int(11) NOT NULL AUTO_INCREMENT COMMENT '主键id',
  `weekly_id` int(11) NULL DEFAULT NULL COMMENT '周报id',
  `uid` int(11) NULL DEFAULT NULL COMMENT '查阅人id',
  `state` char(1) CHARACTER SET utf8 COLLATE utf8_bin NULL DEFAULT NULL COMMENT '查阅状态  1已阅  0未阅',
  `look_time` datetime(0) NULL DEFAULT NULL COMMENT '查阅时间',
  PRIMARY KEY (`id`) USING BTREE,
  INDEX `index_weekly_id`(`weekly_id`) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 62 CHARACTER SET = utf8 COLLATE = utf8_bin COMMENT = '周报查阅情况表' ROW_FORMAT = Dynamic;

-- ----------------------------
-- Table structure for history_record
-- ----------------------------
DROP TABLE IF EXISTS `history_record`;
CREATE TABLE `history_record`  (
  `id` int(20) NOT NULL AUTO_INCREMENT COMMENT 'id',
  `fk_id` int(20) NOT NULL COMMENT '工单id、荣誉id 、重大节日保障id等',
  `type` varchar(20) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL COMMENT '类型，1：工单、2：荣誉、3：重大节日保障、4：知识、5：模板 6告警',
  `description` varchar(200) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL COMMENT '描述',
  `create_person_id` int(20) NOT NULL COMMENT '创建人',
  `create_time` datetime(0) NOT NULL DEFAULT CURRENT_TIMESTAMP(0) COMMENT '创建日期',
  PRIMARY KEY (`id`) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 676 CHARACTER SET = utf8 COLLATE = utf8_general_ci COMMENT = '历史记录表' ROW_FORMAT = Dynamic;

-- ----------------------------
-- Table structure for holiday
-- ----------------------------
DROP TABLE IF EXISTS `holiday`;
CREATE TABLE `holiday`  (
  `id` int(20) NOT NULL AUTO_INCREMENT COMMENT 'id',
  `name` varchar(200) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT '节日名称',
  `start_time` datetime(0) NOT NULL COMMENT '开始时间',
  `end_time` datetime(0) NOT NULL COMMENT '结束时间',
  `create_time` datetime(0) NOT NULL DEFAULT CURRENT_TIMESTAMP(0) COMMENT '创建日期',
  PRIMARY KEY (`id`) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 3 CHARACTER SET = utf8 COLLATE = utf8_general_ci COMMENT = '节日表' ROW_FORMAT = Dynamic;

-- ----------------------------
-- Table structure for honour
-- ----------------------------
DROP TABLE IF EXISTS `honour`;
CREATE TABLE `honour`  (
  `id` int(20) NOT NULL AUTO_INCREMENT COMMENT 'id',
  `title` varchar(100) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL COMMENT '标题',
  `organization` varchar(100) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL COMMENT '机构',
  `department` varchar(100) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL COMMENT '部门',
  `publish_time` datetime(0) NULL DEFAULT NULL COMMENT '发布日期',
  `description` varchar(300) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL COMMENT '内容简述',
  `remark` varchar(400) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL COMMENT '备注',
  `create_person_id` int(20) NOT NULL COMMENT '创建人/录入人',
  `create_time` datetime(0) NOT NULL DEFAULT CURRENT_TIMESTAMP(0) COMMENT '创建日期',
  `project_id` int(20) NULL DEFAULT NULL COMMENT '项目id',
  PRIMARY KEY (`id`) USING BTREE,
  INDEX `fk_honour_fk_project`(`project_id`) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 110 CHARACTER SET = utf8 COLLATE = utf8_general_ci COMMENT = '荣誉表' ROW_FORMAT = Dynamic;

-- ----------------------------
-- Table structure for hour
-- ----------------------------
DROP TABLE IF EXISTS `hour`;
CREATE TABLE `hour`  (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `hour` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL,
  PRIMARY KEY (`id`) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 25 CHARACTER SET = utf8 COLLATE = utf8_general_ci ROW_FORMAT = Dynamic;

-- ----------------------------
-- Table structure for ip_manager
-- ----------------------------
DROP TABLE IF EXISTS `ip_manager`;
CREATE TABLE `ip_manager`  (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `ip_address` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL,
  `ip_kind` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL,
  `segment_id` int(255) NULL DEFAULT NULL,
  `asset_id` int(11) NULL DEFAULT NULL,
  `up_time` datetime(0) NULL DEFAULT NULL,
  `create_person` int(10) NULL DEFAULT NULL,
  PRIMARY KEY (`id`) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 60 CHARACTER SET = utf8 COLLATE = utf8_general_ci ROW_FORMAT = Dynamic;

-- ----------------------------
-- Table structure for ip_segment
-- ----------------------------
DROP TABLE IF EXISTS `ip_segment`;
CREATE TABLE `ip_segment`  (
  `id` int(255) NOT NULL AUTO_INCREMENT,
  `subnet` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL,
  `segment_number` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL,
  `segment_count` int(255) NULL DEFAULT NULL,
  `company` varchar(20) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL,
  `up_time` datetime(0) NULL DEFAULT NULL,
  `create_person` int(10) NULL DEFAULT NULL,
  PRIMARY KEY (`id`) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 14 CHARACTER SET = utf8 COLLATE = utf8_general_ci ROW_FORMAT = Dynamic;

-- ----------------------------
-- Table structure for issue
-- ----------------------------
DROP TABLE IF EXISTS `issue`;
CREATE TABLE `issue`  (
  `id` int(20) NOT NULL AUTO_INCREMENT COMMENT 'id',
  `code` varchar(20) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL COMMENT '编号',
  `project_id` int(20) NOT NULL COMMENT '项目id',
  `type` int(4) NULL DEFAULT NULL COMMENT '问题类型，事件工单、变更事项、服务工单、告警工单、资源申请、外派申请、其他',
  `work_order_id` int(20) NOT NULL COMMENT '工单id',
  `title` varchar(100) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL COMMENT '标题',
  `description` varchar(200) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL COMMENT '描述',
  `solution` varchar(200) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL COMMENT '解决方案',
  `state` int(4) NOT NULL DEFAULT 1 COMMENT '状态，1：未解决、2：已解决、3：已取消',
  `create_person_id` int(20) NOT NULL COMMENT '创建人/录入人',
  `create_time` datetime(0) NOT NULL DEFAULT CURRENT_TIMESTAMP(0) COMMENT '创建日期',
  PRIMARY KEY (`id`) USING BTREE,
  INDEX `fk_issue_holiday`(`work_order_id`) USING BTREE,
  INDEX `fk_issue_project`(`project_id`) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 1 CHARACTER SET = utf8 COLLATE = utf8_general_ci COMMENT = '问题表' ROW_FORMAT = Dynamic;

-- ----------------------------
-- Table structure for issue_solution
-- ----------------------------
DROP TABLE IF EXISTS `issue_solution`;
CREATE TABLE `issue_solution`  (
  `id` int(20) NOT NULL AUTO_INCREMENT COMMENT 'id',
  `issue_id` int(20) NOT NULL COMMENT '问题id',
  `user_id` int(20) NOT NULL COMMENT '问题回答人id',
  `solution` varchar(200) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL COMMENT '解决方案',
  `accept` bit(1) NOT NULL DEFAULT b'0' COMMENT '状态，0：未采纳、1：已采纳',
  `create_time` datetime(0) NOT NULL DEFAULT CURRENT_TIMESTAMP(0) COMMENT '创建日期',
  PRIMARY KEY (`id`) USING BTREE,
  INDEX `fk_issue_solution_issue`(`issue_id`) USING BTREE,
  CONSTRAINT `fk_issue_solution_issue` FOREIGN KEY (`issue_id`) REFERENCES `issue` (`id`) ON DELETE NO ACTION ON UPDATE NO ACTION
) ENGINE = InnoDB AUTO_INCREMENT = 1 CHARACTER SET = utf8 COLLATE = utf8_general_ci COMMENT = '问题解决方案表' ROW_FORMAT = Dynamic;

-- ----------------------------
-- Table structure for jfyc
-- ----------------------------
DROP TABLE IF EXISTS `jfyc`;
CREATE TABLE `jfyc`  (
  `id` int(20) NOT NULL AUTO_INCREMENT,
  `asset_id` varchar(20) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL,
  `name` varchar(40) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL,
  `level_id` varchar(2) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL,
  `ip` varchar(20) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL,
  `num` int(20) NOT NULL,
  `riqi` varchar(8) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL,
  `js_time` varchar(20) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL,
  PRIMARY KEY (`id`) USING BTREE,
  UNIQUE INDEX `service_id`(`asset_id`, `name`, `level_id`, `ip`) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 593 CHARACTER SET = utf8 COLLATE = utf8_general_ci ROW_FORMAT = Dynamic;

-- ----------------------------
-- Table structure for jifang
-- ----------------------------
DROP TABLE IF EXISTS `jifang`;
CREATE TABLE `jifang`  (
  `id` int(8) NOT NULL AUTO_INCREMENT,
  `riqi` varchar(8) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL,
  `room` varchar(20) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL,
  `allNum` int(20) NOT NULL,
  `zcNum` int(20) NOT NULL,
  `wgbgiNum` int(20) NOT NULL,
  `record` float NOT NULL,
  `js_time` varchar(20) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL,
  `remark` varchar(50) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL,
  PRIMARY KEY (`id`) USING BTREE,
  UNIQUE INDEX `service_id`(`riqi`, `room`) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 463 CHARACTER SET = utf8 COLLATE = utf8_general_ci ROW_FORMAT = Dynamic;

-- ----------------------------
-- Table structure for jigui
-- ----------------------------
DROP TABLE IF EXISTS `jigui`;
CREATE TABLE `jigui`  (
  `id` int(8) NOT NULL AUTO_INCREMENT,
  `riqi` varchar(8) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL,
  `room` varchar(20) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL,
  `partitions` varchar(20) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL,
  `box` varchar(20) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL,
  `allNum` int(20) NOT NULL,
  `zcNum` int(20) NOT NULL,
  `wgbgiNum` int(20) NOT NULL,
  `record` float NOT NULL,
  `js_time` varchar(20) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL,
  `remark` varchar(50) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL,
  PRIMARY KEY (`id`) USING BTREE,
  UNIQUE INDEX `service_id`(`riqi`, `room`, `partitions`, `box`) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 3337 CHARACTER SET = utf8 COLLATE = utf8_general_ci ROW_FORMAT = Dynamic;

-- ----------------------------
-- Table structure for knowledge
-- ----------------------------
DROP TABLE IF EXISTS `knowledge`;
CREATE TABLE `knowledge`  (
  `id` int(20) NOT NULL AUTO_INCREMENT COMMENT 'id',
  `code` varchar(20) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL COMMENT '编号',
  `project_id` int(20) NULL DEFAULT NULL COMMENT '项目id',
  `type` tinyint(4) NULL DEFAULT NULL COMMENT '知识类型，1：实战经验、2：理论知识、3：培训课程',
  `classify` tinyint(4) NULL DEFAULT NULL COMMENT '知识分类',
  `title` varchar(100) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL COMMENT '标题',
  `description` varchar(200) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL COMMENT '描述',
  `solution` varchar(200) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL COMMENT '方案',
  `state` tinyint(4) NOT NULL DEFAULT 1 COMMENT '状态，1：待、2：审核通过、3：审核不通过',
  `review_id` int(20) NULL DEFAULT NULL COMMENT '审核人',
  `review_time` datetime(0) NULL DEFAULT NULL COMMENT '审核时间',
  `remark` varchar(200) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL COMMENT '备注',
  `create_person_id` int(20) NOT NULL COMMENT '创建人/录入人',
  `create_time` datetime(0) NOT NULL DEFAULT CURRENT_TIMESTAMP(0) COMMENT '创建日期',
  PRIMARY KEY (`id`) USING BTREE,
  INDEX `fk_knowledge_fk_project`(`project_id`) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 29 CHARACTER SET = utf8 COLLATE = utf8_general_ci COMMENT = '知识表' ROW_FORMAT = Dynamic;

-- ----------------------------
-- Table structure for manufacturer
-- ----------------------------
DROP TABLE IF EXISTS `manufacturer`;
CREATE TABLE `manufacturer`  (
  `id` mediumint(9) NOT NULL,
  `code` varchar(10) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL,
  `name` varchar(45) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL COMMENT 'The name of manufacturer',
  `field1` varchar(45) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL,
  `field2` varchar(45) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL,
  `field3` varchar(45) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL,
  PRIMARY KEY (`id`) USING BTREE
) ENGINE = InnoDB CHARACTER SET = utf8 COLLATE = utf8_general_ci COMMENT = '设备产商表' ROW_FORMAT = Dynamic;

-- ----------------------------
-- Table structure for memory
-- ----------------------------
DROP TABLE IF EXISTS `memory`;
CREATE TABLE `memory`  (
  `id` mediumint(9) NOT NULL AUTO_INCREMENT,
  `asset_id` mediumint(9) NOT NULL,
  `name` varchar(25) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL,
  `capacity` varchar(25) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL,
  `manufacture` varchar(50) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL,
  `model_num` varchar(50) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL,
  PRIMARY KEY (`id`) USING BTREE,
  INDEX `fk_memory_asset1_idx`(`asset_id`) USING BTREE,
  CONSTRAINT `memory_ibfk_1` FOREIGN KEY (`asset_id`) REFERENCES `asset` (`id`) ON DELETE NO ACTION ON UPDATE NO ACTION
) ENGINE = InnoDB AUTO_INCREMENT = 70 CHARACTER SET = utf8 COLLATE = utf8_general_ci COMMENT = '内存表' ROW_FORMAT = Dynamic;

-- ----------------------------
-- Table structure for menu
-- ----------------------------
DROP TABLE IF EXISTS `menu`;
CREATE TABLE `menu`  (
  `id` int(9) NOT NULL AUTO_INCREMENT,
  `p_id` int(9) NULL DEFAULT 0 COMMENT '上级模块',
  `level` int(2) NULL DEFAULT 1 COMMENT '模块级别 1级最高 2 3.. 级别越低url越精确',
  `code` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT '模块代码',
  `name` varchar(20) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL COMMENT '模块名称',
  `url` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL COMMENT '模块访问路径url 如/user/list',
  `mark` varchar(20) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL COMMENT '模块标记 用户局部页面内部自定义模块',
  `describe` varchar(20) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL COMMENT '模块描述',
  PRIMARY KEY (`id`) USING BTREE,
  INDEX `index_url`(`url`) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 1 CHARACTER SET = utf8 COLLATE = utf8_general_ci ROW_FORMAT = Dynamic;

-- ----------------------------
-- Table structure for netcard
-- ----------------------------
DROP TABLE IF EXISTS `netcard`;
CREATE TABLE `netcard`  (
  `id` mediumint(9) NOT NULL AUTO_INCREMENT,
  `asset_id` mediumint(9) NOT NULL,
  `name` varchar(25) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL,
  `mac` varchar(25) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL,
  `ip` varchar(25) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL,
  `netmask` varchar(25) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL,
  PRIMARY KEY (`id`) USING BTREE,
  INDEX `fk_netcard_asset1_idx`(`asset_id`) USING BTREE,
  CONSTRAINT `netcard_ibfk_1` FOREIGN KEY (`asset_id`) REFERENCES `asset` (`id`) ON DELETE NO ACTION ON UPDATE NO ACTION
) ENGINE = InnoDB AUTO_INCREMENT = 81 CHARACTER SET = utf8 COLLATE = utf8_general_ci COMMENT = '网卡表' ROW_FORMAT = Dynamic;

-- ----------------------------
-- Table structure for notification_group
-- ----------------------------
DROP TABLE IF EXISTS `notification_group`;
CREATE TABLE `notification_group`  (
  `id` mediumint(9) NOT NULL AUTO_INCREMENT,
  `name` varchar(25) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT 'Name of this group.',
  `description` varchar(100) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL COMMENT 'Some other information about this group.',
  `datetime` datetime(0) NULL DEFAULT NULL COMMENT 'Creating time',
  `strategy_id` int(10) NULL DEFAULT NULL,
  `notice_way` varchar(20) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL,
  `notice_time_start` time(0) NULL DEFAULT NULL,
  `notice_time_end` time(0) NULL DEFAULT NULL,
  PRIMARY KEY (`id`) USING BTREE,
  INDEX `notification_group1_idx`(`name`) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 42 CHARACTER SET = utf8 COLLATE = utf8_general_ci COMMENT = 'Divide notifications into different groups.' ROW_FORMAT = Dynamic;

-- ----------------------------
-- Table structure for num3
-- ----------------------------
DROP TABLE IF EXISTS `num3`;
CREATE TABLE `num3`  (
  `id` int(10) NOT NULL AUTO_INCREMENT,
  `riqi` varchar(8) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL,
  `room` varchar(20) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL,
  `zhNum` int(20) NULL DEFAULT NULL,
  `zcNum` int(20) NULL DEFAULT NULL,
  `wgbgjNum` int(20) NULL DEFAULT NULL,
  `record` float NULL DEFAULT NULL,
  `flag` varchar(5) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL,
  PRIMARY KEY (`id`) USING BTREE,
  UNIQUE INDEX `service_id`(`riqi`, `room`) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 1093 CHARACTER SET = utf8 COLLATE = utf8_general_ci ROW_FORMAT = Dynamic;

-- ----------------------------
-- Table structure for overtime
-- ----------------------------
DROP TABLE IF EXISTS `overtime`;
CREATE TABLE `overtime`  (
  `id` int(10) NOT NULL AUTO_INCREMENT,
  `apply_person` int(10) NULL DEFAULT NULL COMMENT 'the person who apply this overtime',
  `apply_time` datetime(0) NULL DEFAULT NULL COMMENT 'the time of which happens',
  `reason` varchar(50) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL COMMENT 'the reason of this overtime',
  `duration` varchar(10) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL COMMENT 'the duration of this overtime',
  `status` varchar(5) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL COMMENT '0:todo,1:done',
  `approval_person` int(10) NULL DEFAULT NULL COMMENT 'the person of who approval this overtime',
  `approval_time` datetime(0) NULL DEFAULT NULL COMMENT 'the time of person who approval this overtime',
  `project_id` int(20) NOT NULL COMMENT '项目id',
  PRIMARY KEY (`id`) USING BTREE,
  INDEX `fk_overtime_apply`(`apply_person`) USING BTREE,
  INDEX `fk_overtime_approval`(`approval_person`) USING BTREE,
  INDEX `fk_overtime_fk_project`(`project_id`) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 5 CHARACTER SET = utf8 COLLATE = utf8_general_ci COMMENT = '加班记录表' ROW_FORMAT = Dynamic;

-- ----------------------------
-- Table structure for privilege
-- ----------------------------
DROP TABLE IF EXISTS `privilege`;
CREATE TABLE `privilege`  (
  `id` int(9) NOT NULL AUTO_INCREMENT,
  `code` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT '权限代码',
  `name` varchar(20) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL COMMENT '权限名称',
  `describe` varchar(20) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL COMMENT '权限描述',
  `menu_id` int(9) NULL DEFAULT NULL COMMENT '访问模块 与 url 不会同时生效 url有值时失效',
  `url` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL COMMENT '自定义访问路径url 精确控制 与 menu_id 不会同时生效',
  `option` tinyint(4) NULL DEFAULT 15 COMMENT '访问操作  1增 2删 4改 8查',
  PRIMARY KEY (`id`) USING BTREE,
  INDEX `index_url`(`url`) USING BTREE,
  INDEX `index_menu_id`(`menu_id`) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 1 CHARACTER SET = utf8 COLLATE = utf8_general_ci ROW_FORMAT = Dynamic;

-- ----------------------------
-- Table structure for project
-- ----------------------------
DROP TABLE IF EXISTS `project`;
CREATE TABLE `project`  (
  `id` int(20) NOT NULL AUTO_INCREMENT COMMENT 'id',
  `name` varchar(100) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL COMMENT '项目名称',
  `description` varchar(300) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL COMMENT '项目描述',
  `status` tinyint(4) NOT NULL DEFAULT 1 COMMENT '项目状态,1：未开始、2、进行中、3：已结束',
  `create_time` datetime(0) NOT NULL DEFAULT CURRENT_TIMESTAMP(0) COMMENT '创建日期',
  PRIMARY KEY (`id`) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 2 CHARACTER SET = utf8 COLLATE = utf8_general_ci COMMENT = '项目表' ROW_FORMAT = Dynamic;

-- ----------------------------
-- Table structure for project_group_mapping
-- ----------------------------
DROP TABLE IF EXISTS `project_group_mapping`;
CREATE TABLE `project_group_mapping`  (
  `id` int(20) NOT NULL AUTO_INCREMENT COMMENT 'id',
  `project_id` int(20) NOT NULL COMMENT '项目id',
  `group_id` int(10) NOT NULL COMMENT '用户组id',
  PRIMARY KEY (`id`) USING BTREE,
  INDEX `fk_project_group_mapping_group`(`group_id`) USING BTREE,
  INDEX `fk_project_group_mapping_project`(`project_id`) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 1 CHARACTER SET = utf8 COLLATE = utf8_general_ci COMMENT = '项目和用户组关联表' ROW_FORMAT = Dynamic;

-- ----------------------------
-- Table structure for question
-- ----------------------------
DROP TABLE IF EXISTS `question`;
CREATE TABLE `question`  (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `code` varchar(20) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL COMMENT 'the number of question',
  `title` varchar(50) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL COMMENT 'the title of question',
  `name` varchar(20) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL COMMENT 'the name of question',
  `kind` varchar(10) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL COMMENT 'the kind of question',
  `launch_person` int(5) NULL DEFAULT NULL COMMENT 'the person who assign this question',
  `status` varchar(5) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL COMMENT '0:undo,1:done,2:cancel',
  `cTime` datetime(0) NULL DEFAULT NULL COMMENT 'the time to create this question',
  `project_id` int(5) NULL DEFAULT NULL COMMENT 'the id of project',
  `description` varchar(100) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL COMMENT 'the detail of this question',
  `notice_person` int(5) NULL DEFAULT NULL COMMENT 'the peson who was noticed',
  `order_id` int(11) NULL DEFAULT NULL COMMENT 'the id of work order ',
  PRIMARY KEY (`id`) USING BTREE,
  INDEX `fr_launch_person`(`launch_person`) USING BTREE,
  INDEX `fr_question_project`(`project_id`) USING BTREE,
  INDEX `fr_notice_person`(`notice_person`) USING BTREE,
  INDEX `fr_question_order`(`order_id`) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 67 CHARACTER SET = utf8 COLLATE = utf8_general_ci ROW_FORMAT = Dynamic;

-- ----------------------------
-- Table structure for question_detail
-- ----------------------------
DROP TABLE IF EXISTS `question_detail`;
CREATE TABLE `question_detail`  (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `question_id` int(11) NULL DEFAULT NULL,
  `operation_kind` varchar(5) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL COMMENT 'the opertion of this question,0:create,1:answer,2:cancel,3:redo,4:accept',
  `operation_desc` varchar(10) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL,
  `operation_time` datetime(0) NULL DEFAULT NULL COMMENT 'the time to operate this question',
  `operation_person` int(11) NULL DEFAULT NULL COMMENT 'the peson who operate this question',
  `solution` varchar(200) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL COMMENT 'the solution of this question',
  `is_accept` varchar(5) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL COMMENT '1:accept,2not accept,0:default',
  PRIMARY KEY (`id`) USING BTREE,
  INDEX `fr_operate_person`(`operation_person`) USING BTREE,
  INDEX `fr_question_detail`(`question_id`) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 102 CHARACTER SET = utf8 COLLATE = utf8_general_ci ROW_FORMAT = Dynamic;

-- ----------------------------
-- Table structure for quyu
-- ----------------------------
DROP TABLE IF EXISTS `quyu`;
CREATE TABLE `quyu`  (
  `id` int(8) NOT NULL AUTO_INCREMENT,
  `riqi` varchar(8) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL,
  `room` varchar(20) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL,
  `partitions` varchar(20) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL,
  `allNum` int(20) NOT NULL,
  `zcNum` int(20) NOT NULL,
  `wgbgiNum` int(20) NOT NULL,
  `record` float NOT NULL,
  `js_time` varchar(20) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL,
  `remark` varchar(50) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL,
  PRIMARY KEY (`id`) USING BTREE,
  UNIQUE INDEX `service_id`(`riqi`, `room`, `partitions`) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 5018 CHARACTER SET = utf8 COLLATE = utf8_general_ci ROW_FORMAT = Dynamic;

-- ----------------------------
-- Table structure for role
-- ----------------------------
DROP TABLE IF EXISTS `role`;
CREATE TABLE `role`  (
  `id` int(9) NOT NULL AUTO_INCREMENT,
  `code` varchar(20) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT 'Role serial number.',
  `name` varchar(20) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT 'Role name.',
  `description` varchar(100) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL COMMENT 'Description of this role.',
  `privileges` varchar(3000) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL,
  PRIMARY KEY (`id`) USING BTREE,
  UNIQUE INDEX `code_UNIQUE`(`code`) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 5 CHARACTER SET = utf8 COLLATE = utf8_general_ci COMMENT = '角色表' ROW_FORMAT = Dynamic;

-- ----------------------------
-- Table structure for role_bak
-- ----------------------------
DROP TABLE IF EXISTS `role_bak`;
CREATE TABLE `role_bak`  (
  `id` int(9) NOT NULL AUTO_INCREMENT,
  `code` varchar(20) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT 'Role serial number.',
  `name` varchar(20) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT 'Role name.',
  `description` varchar(100) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL COMMENT 'Description of this role.',
  `privileges` varchar(3000) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL,
  PRIMARY KEY (`id`) USING BTREE,
  UNIQUE INDEX `code_UNIQUE`(`code`) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 5 CHARACTER SET = utf8 COLLATE = utf8_general_ci COMMENT = '角色表' ROW_FORMAT = Dynamic;

-- ----------------------------
-- Table structure for role_privilege_mapping
-- ----------------------------
DROP TABLE IF EXISTS `role_privilege_mapping`;
CREATE TABLE `role_privilege_mapping`  (
  `role_id` int(9) NOT NULL,
  `privilege_id` int(9) NOT NULL,
  PRIMARY KEY (`role_id`, `privilege_id`) USING BTREE,
  INDEX `fk_role_privilege_privilege`(`privilege_id`) USING BTREE,
  CONSTRAINT `fk_role_privilege_privilege` FOREIGN KEY (`privilege_id`) REFERENCES `privilege` (`id`) ON DELETE RESTRICT ON UPDATE RESTRICT,
  CONSTRAINT `fk_role_privilege_role` FOREIGN KEY (`role_id`) REFERENCES `role` (`id`) ON DELETE RESTRICT ON UPDATE RESTRICT
) ENGINE = InnoDB CHARACTER SET = utf8 COLLATE = utf8_general_ci ROW_FORMAT = Dynamic;

-- ----------------------------
-- Table structure for strategy_asset_mapping
-- ----------------------------
DROP TABLE IF EXISTS `strategy_asset_mapping`;
CREATE TABLE `strategy_asset_mapping`  (
  `id` int(10) NOT NULL AUTO_INCREMENT,
  `strategy_id` int(10) NULL DEFAULT NULL,
  `asset_id` int(10) NULL DEFAULT NULL,
  PRIMARY KEY (`id`) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 159 CHARACTER SET = utf8 COLLATE = utf8_general_ci COMMENT = '策略-资产关系表' ROW_FORMAT = Dynamic;

-- ----------------------------
-- Table structure for strategy_trigger
-- ----------------------------
DROP TABLE IF EXISTS `strategy_trigger`;
CREATE TABLE `strategy_trigger`  (
  `id` int(10) NOT NULL AUTO_INCREMENT,
  `trigger_kind` varchar(20) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL,
  `trigger_name` varchar(40) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL,
  `count_cycle` varchar(20) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL,
  `comparator` varchar(10) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL COMMENT 'gt:>,lt:<,eq:=,gte:>=,lte:<=',
  `number` int(10) NULL DEFAULT NULL,
  `unit` varchar(10) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL,
  `continue_time` varchar(10) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL,
  `rate` varchar(10) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL,
  `strategy_id` int(10) NULL DEFAULT NULL,
  `code` varchar(20) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL COMMENT 'The serial number of this indice.',
  PRIMARY KEY (`id`) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 203 CHARACTER SET = utf8 COLLATE = utf8_general_ci COMMENT = '策略触发机制表' ROW_FORMAT = Dynamic;

-- ----------------------------
-- Table structure for strategy_user_mapping
-- ----------------------------
DROP TABLE IF EXISTS `strategy_user_mapping`;
CREATE TABLE `strategy_user_mapping`  (
  `id` int(10) NOT NULL AUTO_INCREMENT,
  `strategy_id` int(10) NULL DEFAULT NULL,
  `user_id` int(10) NULL DEFAULT NULL,
  `inform_time` varchar(20) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL,
  `group_name` varchar(10) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL,
  `inform_way` varchar(10) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL,
  PRIMARY KEY (`id`) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 1 CHARACTER SET = utf8 COLLATE = utf8_general_ci COMMENT = '告警策略--用户关系表' ROW_FORMAT = Dynamic;

-- ----------------------------
-- Table structure for sys_sno
-- ----------------------------
DROP TABLE IF EXISTS `sys_sno`;
CREATE TABLE `sys_sno`  (
  `id` int(11) NOT NULL AUTO_INCREMENT COMMENT '主键',
  `sCode` varchar(50) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL COMMENT '编码 1：工单 2：客户编号 3：知识编号 4：模板标号',
  `sName` varchar(100) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL COMMENT '名称',
  `sQz` varchar(50) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL COMMENT '前缀',
  `sValue` varchar(80) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL COMMENT '值',
  PRIMARY KEY (`id`) USING BTREE,
  UNIQUE INDEX `I_sCode`(`sCode`) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 5 CHARACTER SET = utf8 COLLATE = utf8_general_ci COMMENT = '流水号管理表' ROW_FORMAT = Dynamic;

-- ----------------------------
-- Table structure for t_dic_item
-- ----------------------------
DROP TABLE IF EXISTS `t_dic_item`;
CREATE TABLE `t_dic_item`  (
  `id` int(5) NOT NULL AUTO_INCREMENT,
  `type_code` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT '字典类型',
  `type_name` varchar(20) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT '字段描述',
  `code` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT '代码',
  `name` varchar(20) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT '名称',
  PRIMARY KEY (`id`) USING BTREE,
  INDEX `index_code`(`code`) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 38 CHARACTER SET = utf8 COLLATE = utf8_general_ci ROW_FORMAT = Dynamic;

-- ----------------------------
-- Table structure for template
-- ----------------------------
DROP TABLE IF EXISTS `template`;
CREATE TABLE `template`  (
  `id` int(20) NOT NULL AUTO_INCREMENT COMMENT 'id',
  `code` varchar(20) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL COMMENT '编号',
  `project_id` int(20) NULL DEFAULT NULL COMMENT '项目id',
  `type` tinyint(4) NULL DEFAULT NULL COMMENT '模板分类，1：标准模板、2：理论知识、3：培训课程',
  `title` varchar(100) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL COMMENT '标题',
  `description` varchar(200) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL COMMENT '描述',
  `create_person_id` int(20) NOT NULL COMMENT '创建人/上传人',
  `create_time` datetime(0) NOT NULL DEFAULT CURRENT_TIMESTAMP(0) COMMENT '创建日期',
  PRIMARY KEY (`id`) USING BTREE,
  INDEX `fk_template_fk_project`(`project_id`) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 10 CHARACTER SET = utf8 COLLATE = utf8_general_ci COMMENT = '模板表' ROW_FORMAT = Dynamic;

-- ----------------------------
-- Table structure for topology
-- ----------------------------
DROP TABLE IF EXISTS `topology`;
CREATE TABLE `topology`  (
  `id` mediumint(20) NOT NULL AUTO_INCREMENT,
  `name` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL COMMENT '名称',
  `type` mediumint(20) NULL DEFAULT NULL COMMENT '拓扑类型  ：1:物理拓扑 2：逻辑拓扑  3：其他',
  `description` varchar(400) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL COMMENT '备注',
  `create_time` datetime(0) NOT NULL COMMENT '创建时间',
  `create_person` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL COMMENT '创建人',
  PRIMARY KEY (`id`) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 71 CHARACTER SET = utf8 COLLATE = utf8_general_ci COMMENT = '拓补图表' ROW_FORMAT = Dynamic;

-- ----------------------------
-- Table structure for user
-- ----------------------------
DROP TABLE IF EXISTS `user`;
CREATE TABLE `user`  (
  `id` int(5) NOT NULL AUTO_INCREMENT,
  `name` char(20) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL,
  `nick_name` varchar(20) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL COMMENT 'the nikeName of user',
  `account` varchar(20) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL COMMENT 'the account of user',
  `job_number` varchar(20) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL COMMENT 'the number of user',
  `entry_time` datetime(0) NULL DEFAULT NULL COMMENT 'the time of user entry ',
  `telephone` varchar(13) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL COMMENT 'the telephone number of user',
  `password` varchar(40) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL COMMENT 'the password of user',
  `belong_group` int(10) NULL DEFAULT NULL COMMENT 'the user belong to which group',
  `job_code` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL COMMENT '职位代码',
  `sex` char(1) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL COMMENT '性别 1 男 0 女',
  `email` varchar(32) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL COMMENT '电子邮件',
  `skill` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL COMMENT '特长',
  `hobby` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL COMMENT '爱好',
  `image_path` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL COMMENT '头像存储路径',
  PRIMARY KEY (`id`) USING BTREE,
  INDEX `fk_user_group`(`belong_group`) USING BTREE,
  INDEX `index_account`(`account`) USING BTREE,
  INDEX `index_job_number`(`job_number`) USING BTREE,
  INDEX `index_job_code`(`job_code`) USING BTREE,
  CONSTRAINT `fk_user_group` FOREIGN KEY (`belong_group`) REFERENCES `work_group` (`id`) ON DELETE NO ACTION ON UPDATE NO ACTION
) ENGINE = InnoDB AUTO_INCREMENT = 10 CHARACTER SET = utf8 COLLATE = utf8_general_ci ROW_FORMAT = Dynamic;

-- ----------------------------
-- Table structure for user_count
-- ----------------------------
DROP TABLE IF EXISTS `user_count`;
CREATE TABLE `user_count`  (
  `id` int(10) NOT NULL AUTO_INCREMENT,
  `month_of_year` datetime(0) NULL DEFAULT NULL,
  `counts_user` varchar(10) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL,
  PRIMARY KEY (`id`) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 4 CHARACTER SET = utf8 COLLATE = utf8_general_ci ROW_FORMAT = Dynamic;

-- ----------------------------
-- Table structure for user_dding
-- ----------------------------
DROP TABLE IF EXISTS `user_dding`;
CREATE TABLE `user_dding`  (
  `id` int(10) NOT NULL AUTO_INCREMENT,
  `user_id` varchar(40) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL,
  `unionid` varchar(50) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL,
  `openId` varchar(20) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL,
  `mobile` varchar(20) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL,
  `department` varchar(20) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL,
  `OnDuty` varchar(20) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL,
  `OffDuty` varchar(20) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL,
  `riqi` varchar(20) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL,
  `name` varchar(10) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL,
  `jisuan_riqi` varchar(20) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL,
  `OnResult` varchar(10) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL,
  `OffResult` varchar(10) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL,
  PRIMARY KEY (`user_id`, `jisuan_riqi`) USING BTREE,
  UNIQUE INDEX `id`(`id`) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 1253 CHARACTER SET = utf8 COLLATE = utf8_general_ci ROW_FORMAT = Dynamic;

-- ----------------------------
-- Table structure for user_notification_group_mapping
-- ----------------------------
DROP TABLE IF EXISTS `user_notification_group_mapping`;
CREATE TABLE `user_notification_group_mapping`  (
  `id` mediumint(9) NOT NULL AUTO_INCREMENT,
  `user_id` int(5) NOT NULL COMMENT 'Id of user who belong to this group.',
  `notification_grp_id` mediumint(9) NOT NULL COMMENT 'Notification group id.',
  PRIMARY KEY (`id`) USING BTREE,
  INDEX `notification_grp_id`(`notification_grp_id`) USING BTREE,
  INDEX `user_id`(`user_id`) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 368 CHARACTER SET = utf8 COLLATE = utf8_general_ci COMMENT = '用户--通知分组关系表' ROW_FORMAT = Dynamic;

-- ----------------------------
-- Table structure for user_role_mapping
-- ----------------------------
DROP TABLE IF EXISTS `user_role_mapping`;
CREATE TABLE `user_role_mapping`  (
  `user_id` int(9) NOT NULL,
  `role_id` int(9) NOT NULL,
  PRIMARY KEY (`user_id`, `role_id`) USING BTREE,
  INDEX `fk_user_role_role1_idx`(`role_id`) USING BTREE
) ENGINE = InnoDB CHARACTER SET = utf8 COLLATE = utf8_general_ci COMMENT = '用户角色映射表' ROW_FORMAT = Dynamic;

-- ----------------------------
-- Table structure for work_group
-- ----------------------------
DROP TABLE IF EXISTS `work_group`;
CREATE TABLE `work_group`  (
  `id` int(10) NOT NULL AUTO_INCREMENT,
  `name` varchar(20) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL COMMENT 'the name of this group ',
  `description` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL COMMENT '描述',
  PRIMARY KEY (`id`) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 4 CHARACTER SET = utf8 COLLATE = utf8_general_ci ROW_FORMAT = Dynamic;

-- ----------------------------
-- Table structure for work_order
-- ----------------------------
DROP TABLE IF EXISTS `work_order`;
CREATE TABLE `work_order`  (
  `id` int(20) NOT NULL AUTO_INCREMENT COMMENT 'id',
  `code` varchar(20) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT '工单号',
  `name` varchar(50) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL COMMENT '名称',
  `title` varchar(50) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT '标题',
  `type` tinyint(4) NOT NULL DEFAULT 1 COMMENT '工单类型,1:事件工单、2:变更事项、3:服务工单、4:告警工单、5:资源申请、6:外派申请、7:其他',
  `project_id` int(20) NOT NULL COMMENT '项目id',
  `create_person_id` int(10) NOT NULL COMMENT '发起人',
  `designate_id` int(10) NULL DEFAULT NULL COMMENT '指派人',
  `state` varchar(4) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '1' COMMENT '状态,1:未开始/待处理、2:被驳回、3:处理中、4:待验收、5:已关闭/验收通过、6:已取消',
  `estimate_id` int(10) NULL DEFAULT NULL COMMENT '评估人',
  `review_id` int(10) NULL DEFAULT NULL COMMENT '审批人',
  `operate_state` tinyint(4) NULL DEFAULT NULL COMMENT '操作状态,1:新增需求、2:变更需求、3:故障修复、4:其他',
  `change_type` tinyint(4) NULL DEFAULT NULL COMMENT '变更类型(变更工单)/服务类型(服务工单)/资源类型',
  `open_count` tinyint(4) NULL DEFAULT NULL COMMENT '开通数量',
  `alarm_id` int(20) NULL DEFAULT NULL COMMENT ' 关联告警',
  `description` varchar(500) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL COMMENT '描述',
  `reject_reason` varchar(500) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL COMMENT '驳回原因',
  `cancel_remark` varchar(500) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL COMMENT '取消备注',
  `handle_start_time` datetime(0) NULL DEFAULT NULL COMMENT '处理开始时间',
  `handle_finish_time` datetime(0) NULL DEFAULT NULL COMMENT '处理完成时间',
  `urgent_level` tinyint(4) NULL DEFAULT NULL COMMENT '紧急程度，1：一般、2：紧急、3：重大',
  `handle_description` varchar(500) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL COMMENT '处理描述',
  `solution` varchar(500) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL COMMENT '解决方案',
  `to_question` tinyint(4) NULL DEFAULT NULL COMMENT '是否转为问题',
  `accept_remark` varchar(500) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL COMMENT '验收备注',
  `accept_reject_reason` varchar(500) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL COMMENT '验收驳回原因',
  `create_time` datetime(0) NOT NULL DEFAULT CURRENT_TIMESTAMP(0) COMMENT '创建日期/发起时间',
  `finish_time` datetime(0) NULL DEFAULT NULL COMMENT '完成日期',
  `predict_start_time` datetime(0) NULL DEFAULT NULL COMMENT '预计开始',
  `predict_hours` varchar(20) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL COMMENT '预计工时',
  `estimate_time` datetime(0) NULL DEFAULT NULL COMMENT '评估时间',
  `estimate_remark` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL COMMENT '评估备注',
  `review_time` datetime(0) NULL DEFAULT NULL COMMENT '审批时间',
  `review_remark` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL COMMENT '审批备注',
  `real_hours` varchar(20) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL COMMENT ' 实际工时',
  `question_type` tinyint(4) NULL DEFAULT NULL COMMENT '问题类型：1云平台、2网络、安全、基础设施、灾备中心、应用',
  `advice_type` tinyint(4) NULL DEFAULT NULL COMMENT '发送通知给：0小组成员、1项目成员',
  PRIMARY KEY (`id`) USING BTREE,
  INDEX `fk_work_order_fk_project`(`project_id`) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 204 CHARACTER SET = utf8 COLLATE = utf8_general_ci COMMENT = '工单表' ROW_FORMAT = Dynamic;

-- ----------------------------
-- Table structure for work_order_asset
-- ----------------------------
DROP TABLE IF EXISTS `work_order_asset`;
CREATE TABLE `work_order_asset`  (
  `id` int(20) NOT NULL AUTO_INCREMENT COMMENT 'id',
  `work_order_id` int(20) NOT NULL COMMENT '工单id',
  `asset_id` int(20) NOT NULL COMMENT '资产id',
  PRIMARY KEY (`id`) USING BTREE,
  INDEX `fk_work_order_asset_fk_work_order`(`work_order_id`) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 106 CHARACTER SET = utf8 COLLATE = utf8_general_ci COMMENT = '工单关联资产表' ROW_FORMAT = Dynamic;

-- ----------------------------
-- Procedure structure for GetSerialNo
-- ----------------------------
DROP PROCEDURE IF EXISTS `GetSerialNo`;
delimiter ;;
CREATE PROCEDURE `GetSerialNo`(IN tsCode VARCHAR(50),OUT result VARCHAR(200))
BEGIN   
   DECLARE  tsValue  VARCHAR(50);  
   DECLARE  tdToday  VARCHAR(20);       
   DECLARE  nowdate  VARCHAR(20);        
   DECLARE  tsQZ     VARCHAR(50);  
   DECLARE t_error INTEGER DEFAULT 0;    
   DECLARE CONTINUE HANDLER FOR SQLEXCEPTION SET t_error=1; 
    UPDATE sys_sno  SET sValue=sValue WHERE sCode=tsCode; 
      SELECT sValue INTO tsValue  FROM sys_sno  WHERE sCode=tsCode for UPDATE;  
      SELECT sQz INTO tsQZ FROM sys_sno WHERE sCode=tsCode ;  
      -- 因子表中没有记录，插入初始值     
      IF tsValue IS NULL  THEN   
          SELECT CONCAT(DATE_FORMAT(NOW(),'%Y'),'0001') INTO tsValue;  
          UPDATE sys_sno SET sValue=tsValue WHERE sCode=tsCode ;  
          SELECT CONCAT(tsQZ,tsValue) INTO result;  
      ELSE                  
          SELECT  SUBSTRING(tsValue,1,4) INTO tdToday;  
          SELECT  CONVERT(DATE_FORMAT(NOW(),'%Y'),SIGNED) INTO nowdate;
          -- 判断年是否需要更新
          IF tdToday = nowdate THEN  
             SET  tsValue=CONVERT(tsValue,SIGNED) + 1;  
          ELSE  
             SELECT CONCAT(DATE_FORMAT(NOW(),'%Y') ,'0001') INTO tsValue ;  
          END IF;  
          UPDATE sys_sno SET sValue =tsValue WHERE sCode=tsCode;  
          SELECT CONCAT(tsQZ,tsValue) INTO result;  
     END IF;  
     SELECT  result ;     
END
;;
delimiter ;

-- ----------------------------
-- Event structure for deleteCabin
-- ----------------------------
DROP EVENT IF EXISTS `deleteCabin`;
delimiter ;;
CREATE EVENT `deleteCabin`
ON SCHEDULE
EVERY '1' HOUR STARTS '2020-03-26 20:19:38'
ON COMPLETION PRESERVE
DO BEGIN
delete from control_cabin where HOUR(now())-HOUR(jisuan_ri)>1;
END
;;
delimiter ;

SET FOREIGN_KEY_CHECKS = 1;
