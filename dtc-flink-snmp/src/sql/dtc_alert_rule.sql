create table if not exists dtc_alert_rule(
	id int(24) not null primary key auto_increment,
	unique_id varchar(8) not null,
	host_ip varchar(12) not  null,
	items_code varchar(20) not null,
	alarm_threshold_garde varchar(20) not null,
    initial_time varchar(30),
    update_time varchar(30),
	used varchar(10) not null,
	commit varchar(40) DEFAULT ''
)ENGINE=InnoDB DEFAULT CHARSET=utf8;
