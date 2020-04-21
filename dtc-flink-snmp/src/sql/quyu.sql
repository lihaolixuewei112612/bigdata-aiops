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
