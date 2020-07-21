##基础表-主体信息表(entity_info)
CREATE TABLE dsc_bas.entity_info(
    entityCode varchar(100) COMMENT '客户编码',
    entityName varchar(200) COMMENT '客户名称',
    entityShortName varchar(200) DEFAULT NULL COMMENT '客户简称',
    industry varchar(100) COMMENT '所属行业（申万最明细行业）',
    majorBusiness varchar(500) DEFAULT NULL COMMENT '主营业务',
    regAddress varchar(500) DEFAULT NULL COMMENT '注册地',
    entityType varchar(100) DEFAULT NULL COMMENT '主体类型',
    entityType2 varchar(100) DEFAULT NULL COMMENT '客户类型',
    address varchar(500) DEFAULT NULL COMMENT '企业地址',
    cusc varchar(50) DEFAULT NULL COMMENT '统一社会信用代码',
    estDate varchar(8) DEFAULT NULL COMMENT '成立日期',
    representative varchar(50) DEFAULT NULL COMMENT '法人代表',
    regCapital double DEFAULT NULL COMMENT '注册资本金',
    phone varchar(30) DEFAULT NULL COMMENT '公司电话',
    website varchar(100) DEFAULT NULL COMMENT '公司网址',
    controller_e varchar(100) DEFAULT NULL COMMENT '实际控制人（企业）',
    controller_m varchar(100) DEFAULT NULL COMMENT '实际控制人（个人）',
    subCompany varchar(600) DEFAULT NULL COMMENT '子公司',
    modelMaster varchar(20) COMMENT '模型敞口类别',
    rateInitiator varchar(20) DEFAULT NULL COMMENT '评级发起人',
    rateInidate varchar(8) DEFAULT NULL COMMENT '评级发起日期',
    region varchar(10) DEFAULT NULL COMMENT '地区'
)
COMMENT '基础表-主体信息表';

##基础表-债券信息表(bond_info)
CREATE TABLE dsc_bas.bond_info(
    bondFullName varchar(500) COMMENT '债券全称',
    bondName varchar(200) COMMENT '债券简称',
    ibCode varchar(50) COMMENT '债券编码',
    bondCode varchar(100) COMMENT '债券代码',
    bondType varchar(60) COMMENT '债券类型',
    publisher varchar(200) COMMENT '发债主体',
    guarantor varchar(200) DEFAULT NULL COMMENT '担保人',
    leadUnderwriter varchar(200) DEFAULT NULL COMMENT '主承销商',
    pubTotal varchar(200) DEFAULT NULL COMMENT '发行总额（亿元）',
    pubPrice double DEFAULT NULL COMMENT '发行价格（元/百元面值）',
    faceValue double DEFAULT NULL COMMENT '面值（元）',
    pubDate varchar(8) DEFAULT NULL COMMENT '发行日',
    recordDate varchar(8) DEFAULT NULL COMMENT '登记日',
    circulateDate varchar(8) DEFAULT NULL COMMENT '流通日',
    deadline int(10,0) DEFAULT NULL COMMENT '期限',
    endDate varchar(8) DEFAULT NULL COMMENT '到期（兑付）日',
    bondRate varchar(20) DEFAULT NULL COMMENT '产品评级',
    bondRateOrg varchar(20) DEFAULT NULL COMMENT '产品评级机构',
    calInterestMet varchar(100) DEFAULT NULL COMMENT '计息方式',
    yearInterRate double(16,2) DEFAULT NULL COMMENT '票面年利率（%）',
    interestFre varchar(100) DEFAULT NULL COMMENT '付息频率',
    valueDate varchar(8) DEFAULT NULL COMMENT '起息日',
    bondbalance decimal(15,4) DEFAULT NULL COMMENT '债券余额',
    status varchar(1) DEFAULT NULL COMMENT '债券状态（1：正常，2：暂停，3：到期，4：违约，null-不预警）警）',
    nextpaydt date DEFAULT NULL COMMENT '下一付息日'
)
COMMENT '基础表-债券信息表';

###基础表-主体关联关系(entity_relation)
CREATE TABLE dsc_bas.entity_relation(
    ID int COMMENT 'ID',
    entityName varchar(150) DEFAULT NULL COMMENT '主体名称',
    relationEntityName varchar(150) DEFAULT NULL COMMENT '关系主体/个人名字',
    ratio decimal(11,4) DEFAULT NULL COMMENT '比例',
    relation varchar(1) DEFAULT NULL COMMENT '关系( 1:股东 ,5:担保,7:实际控制人)'
)
COMMENT '基础表-主体关联关系';


###基础表-配置表(data_dict)
CREATE TABLE dsc_bas.data_dict(
    ID int COMMENT 'ID',
    NAME varchar(100) COMMENT '名称',
    VALUE varchar(10) COMMENT '值',
    TYPE varchar(100) COMMENT '类型',
    DESCRIPTION varchar(1000) COMMENT '描述'
)
COMMENT '基础表-配置表';



###评级数据-外评表(data_grade_result_out)
CREATE TABLE dsc_bas.data_grade_result_out(
    id bigint(20) COMMENT 'ID',
    entityCode varchar(100) COMMENT '企业CODE',
    gradeDate varchar(10) COMMENT '评级日期',
    gradeOrg varchar(100) COMMENT '评级机构',
    gradeResultValue varchar(10) COMMENT '评级结果',
    type varchar(20) COMMENT '长期信用评级',
    gradeForword varchar(100) COMMENT '评级展望'
)
COMMENT '评级数据-外评表';


###评级数据-敞口映射表(model_master)
CREATE TABLE dsc_bas.model_master(
    ID bigint(20) COMMENT 'ID',
    MASTER_ID varchar(10) COMMENT '敞口ID',
    NAME varchar(200) COMMENT '敞口名称',
    DESCRIPTION varchar(1000) DEFAULT NULL COMMENT '敞口描述'
)
COMMENT '评级数据-敞口映射表';

###评级数据-定量指标长清单(model_quan_factor)
CREATE TABLE dsc_bas.model_quan_factor(
    ID bigint(20) COMMENT 'ID',
    QUAN_ID varchar(10) COMMENT '指标ID',
    NAME varchar(200)  COMMENT '指标名称',
    CODE varchar(100)  COMMENT '指标对应CODE',
    MODEL_TYPE varchar(1)  COMMENT '是否入模（1入模；0非入模）',
    DESCRIPTION varchar(1000) DEFAULT NULL COMMENT '指标描述',
    MODEL_CODE varchar(100)  COMMENT '敞口CODE'
)
COMMENT '评级数据-定量指标长清单';



###评级数据-定性指标长清单(model_qual_factor)
CREATE TABLE dsc_bas.model_qual_factor(
    ID bigint(20)  COMMENT 'ID',
    QUAL_ID varchar(10)  COMMENT '指标ID',
    NAME varchar(200)  COMMENT '指标名称',
    CODE varchar(100)  COMMENT '指标对应CODE',
    MODEL_TYPE varchar(1)  COMMENT '是否入模（1入模；0非入模）',
    DESCRIPTION varchar(1000) DEFAULT NULL COMMENT '指标描述',
    MODEL_CODE varchar(100)  COMMENT '敞口CODE'
)
COMMENT '评级数据-定性指标长清单';


###评级数据-调整事项清单(model_adjust)
CREATE TABLE dsc_bas.model_adjust(
    ID bigint(20)  COMMENT 'ID',
    ADJUST_ID varchar(100)  COMMENT '调整事项ID',
    NAME varchar(200)  COMMENT '调整事项',
    ADJUST_TYPE varchar(100)  COMMENT '调整事项分类',
    DESCRIPTION varchar(1000) DEFAULT NULL COMMENT '描述'
)
COMMENT '评级数据-调整事项清单';


###评级数据-财务科目清单(model_fin_dict)
CREATE TABLE dsc_bas.model_fin_dict(
    ID bigint(20)  COMMENT 'ID',
    CODE varchar(100)  COMMENT '科目CODE',
    NAME varchar(200)  COMMENT '科目名称',
    TYPE varchar(100)  COMMENT '所属财务报表（查看数据字典）',
    FIN_DICT_YEAR varchar(8)  COMMENT '当前财务科目的年份'
)
COMMENT '评级数据-财务科目清单';


###评级数据-定量指标信息(fin_quan_data)
CREATE TABLE dsc_bas.fin_quan_data(
    ID bigint(20)  COMMENT 'ID',
    ENTITY_CODE varchar(100)  COMMENT '企业编码',
    QUAN_ID varchar(10)  COMMENT '定量指标ID',
    QUAN_VALUE double DEFAULT NULL COMMENT '定量指标值',
    QUAN_SCORE double DEFAULT NULL COMMENT '定量指标得分',
    GRADE_ID varchar(100) DEFAULT NULL COMMENT '对应评级结果ID',
    DATA_YEAR varchar(8)  COMMENT '数据年份'
)
COMMENT '评级数据-定量指标信息';


###评级数据-定性指标信息(fin_qual_data)
CREATE TABLE dsc_bas.fin_qual_data(
    ID bigint(20)  COMMENT 'ID',
    ENTITY_CODE varchar(100)  COMMENT '企业编码',
    QUAL_ID varchar(10)  COMMENT '定性指标ID',
    QUAL_VALUE double DEFAULT NULL COMMENT '定性指标档位',
    QUAL_SCORE double DEFAULT NULL COMMENT '定性指标得分',
    EVIDENCE text DEFAULT NULL COMMENT '定性指标evidence',
    GRADE_ID varchar(100) DEFAULT NULL COMMENT '对应评级结果ID',
    DATA_YEAR varchar(8)  COMMENT '数据年份'
)
COMMENT '评级数据-定性指标信息';



###评级数据-企业财报信息(fin_data)
CREATE TABLE dsc_bas.fin_data(
    ID bigint(20)  COMMENT 'ID',
    ENTITY_CODE varchar(100)  COMMENT '企业编码',
    FIN_DATE varchar(8)  COMMENT '财报日期',
    FIN_CODE varchar(10)  COMMENT '科目代码',
    FIN_VALUE varchar DEFAULT NULL COMMENT '科目值',
    TIMESTAMP varchar(20)  COMMENT '时间戳',
    DATA_YEAR varchar(8)  COMMENT '数据年份'
)
COMMENT '评级数据-企业财报信息';

###评级数据-调整事项信息(fin_adjust_data)
CREATE TABLE dsc_bas.fin_adjust_data(
    ID bigint(20)  COMMENT 'ID',
    ENTITY_CODE varchar(100)  COMMENT '企业编码',
    ADJUST_ID varchar(100)  COMMENT '调整事项ID',
    GRADE_ID varchar(100)  COMMENT '对应评级结果ID'
)
COMMENT '评级数据-调整事项信息';


###评级数据-企业评级结果(data_cus_grade)
CREATE TABLE dsc_bas.data_cus_grade(
    ID bigint(20)  COMMENT 'ID',
    GRADE_ID varchar(100)  COMMENT '评级结果ID',
    ENTITY_CODE varchar(100)  COMMENT '企业编码',
    GRADE_RESULT varchar(10)  COMMENT '评级结果（参见数据字典）',
    MASTER_ID varchar(10)  COMMENT '企业对应敞口ID',
    DATE_YEAR varchar(8)  COMMENT '评级数据日期',
    GRADE_DATE date(8)  COMMENT '评级日期'
)
COMMENT '评级数据-企业评级结果';


##政府数据-政府评级信息(data_gov_grade)
CREATE TABLE dsc_bas.data_gov_grade(
    ID bigint(20)  COMMENT 'ID',
    govCode varchar(100)  COMMENT '政府编码',
    pareGovCode varchar(100)  COMMENT '上级政府编码',
    govName varchar(100)  COMMENT '政府名称',
    govLevel int  COMMENT '政府级别（1：省，2：市；3：县）',
    Grade_Result double  COMMENT '政府得分',
    Date_year varchar(8)  COMMENT '评级数据年份',
    Grade_date date  COMMENT '评级日期',
    CREATED date  COMMENT '创建时间',
    UPDATED date  COMMENT '更新时间'
)
COMMENT '政府数据-政府评级信息';

###政府数据-政府指标长清单(model_gov_qual_factor)
CREATE TABLE dsc_bas.model_gov_qual_factor(
    ID bigint(20)  COMMENT 'ID',
    qual_id varchar(10)  COMMENT '指标ID',
    name varchar(200)  COMMENT '指标名称',
    code varchar(200)  COMMENT '指标对应code',
    Model_type double  COMMENT '是否入模（1入模：0非入模）',
    description varchar(1000) DEFAULT NULL COMMENT '指标描述',
    timestamp varchar(20)  COMMENT '时间戳',
    Model_master varchar(10)  COMMENT '政府',
    CREATED date  COMMENT '创建时间',
    UPDATED date  COMMENT '更新时间'
)
COMMENT '政府数据-政府指标长清单';

###政府数据-政府指标数据(fin_gov_qual_factor)
CREATE TABLE dsc_bas.fin_gov_qual_factor(
    ID bigint(20)  COMMENT 'ID',
    gov_code varchar(10)  COMMENT '地方政府编码',
    Qual_id varchar(200)  COMMENT '定性指标ID',
    Qual_value double  COMMENT '定性指标值',
    Qual_score double  COMMENT '定性指标得分',
    evidence varchar DEFAULT NULL COMMENT '定性指标证据',
    Grade_id varchar(100)  COMMENT '对应评级结果ID',
    Data_year varchar(4)  COMMENT '数据年份',
    CREATED date  COMMENT '创建时间',
    UPDATED date  COMMENT '更新时间'
)
COMMENT '政府数据-政府指标数据';

###政府数据-政府与企业关联关系(entity_gov_rel)
CREATE TABLE dsc_bas.entity_gov_rel(
    ID bigint(20)  COMMENT 'ID',
    gov_code varchar(10)  COMMENT '地方政府编码',
    Entity_code varchar(200)  COMMENT '企业编码'
)
COMMENT '政府数据-政府与企业关联关系';

###预警数据-债券预警信息(warn_bond_info)
CREATE TABLE dsc_bas.warn_bond_info(
    ID bigint(20)  COMMENT 'ID',
    ibCode varchar(50)  COMMENT '债券编码',
    warnScore double  COMMENT '预警分数',
    warnLevel int  COMMENT '预警等级（参见数据字典）',
    sigScore double  COMMENT '信用压力指数',
    sigRanking int  COMMENT '信用压力指数排名',
    sigRankingWeek int  COMMENT '一周排名变动',
    sigRankingMonth int  COMMENT '一月排名变动',
    closeingPriceMin double DEFAULT NULL COMMENT '成交价（最低）',
    closeingPriceMax double DEFAULT NULL COMMENT '成交价（最高）',
    warnDate date  COMMENT '预警日期'
)
COMMENT '预警数据-债券预警信息';

##预警数据-主体预警信息(warn_entity_info)
CREATE TABLE dsc_bas.warn_entity_info(
    ID bigint(20)  COMMENT 'ID',
    entityCode varchar  COMMENT '主体编码',
    warnScore double  COMMENT '预警分数',
    warnLevel int  COMMENT '预警等级（参见数据字典）',
    sigScore double  COMMENT '信用压力指数',
    sigRanking int  COMMENT '信用压力指数排名',
    sigRankingWeek int  COMMENT '一周排名变动',
    sigRankingMonth int  COMMENT '一月排名变动',
    isHaveNews int DEFAULT NULL COMMENT '今日是否有负面舆情(0 无; 1 有)',
    warnDate date  COMMENT '预警日期',
    isMarketVolatility int DEFAULT NULL COMMENT '今日是否市场波动'
)
COMMENT '预警数据-主体预警信息';

###舆情数据-舆情信息(news)
CREATE TABLE dsc_bas.news(
    ID bigint(20)  COMMENT 'ID',
    newsTitle varchar(1000) DEFAULT NULL COMMENT '舆情标题',
    newsUrl varchar(200) DEFAULT NULL COMMENT '舆情链接',
    newsType varchar(20) DEFAULT NULL COMMENT '舆情类别',
    newsDate varchar(8) DEFAULT NULL COMMENT '舆情日期'
)
COMMENT '舆情数据-舆情信息';


##舆情数据-舆情正文(news_content)
CREATE TABLE dsc_bas.news_content(
    ID bigint(20)  COMMENT 'ID',
    newsId bigint(20)  COMMENT '舆情ID',
    newsContent text DEFAULT NULL COMMENT '舆情正文',
    newsFile varchar(200) DEFAULT NULL COMMENT '公告链接（可以内部）'
)
COMMENT '舆情数据-舆情正文';

##舆情数据-舆情标签(news_tags)
CREATE TABLE dsc_bas.news_tags(
    ID bigint(20)  COMMENT 'ID',
    newsId bigint  COMMENT '舆情ID',
    newsTag1 varchar(100) DEFAULT NULL COMMENT '负面舆情类型',
    newsTag2 varchar(100) DEFAULT NULL COMMENT '负面舆情表情'
)
COMMENT '舆情数据-舆情标签';

###舆情数据-舆情与主体对应关系(news_entity_relation)
CREATE TABLE dsc_bas.news_entity_relation(
    ID bigint(20)  COMMENT 'ID',
    newsId bigint  COMMENT '舆情ID',
    entityCode bigint)  COMMENT '主体编码'
)
COMMENT '舆情数据-舆情与主体对应关系';

