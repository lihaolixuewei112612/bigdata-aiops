create table if not exists real_time_alert(
	id int(24) not null primary key auto_increment,
	system_id varchar(20) not null,
	host_ip varchar(10) not null,
	itmes_code varchar(24) not null,
	last_code varchar(10),
	nameCN varchar(30),
	nameEN varchar(30),
	event_time varchar(40) not null,
	systme_time varchar(40)  not null,
	real_value varchar(30)  not null,
	alarm_threshold varchar(30)  not null,
	unque_id varchar(20) not null,
	alarm_garde varchar(20) not null,
	commit varcahr(20)
)ENGINE=InnoDB DEFAULT CHARSET=utf8;
