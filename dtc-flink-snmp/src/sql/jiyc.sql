create table jfyc(
id int(20) not null primary key auto_increment,
asset_id varchar(20) not null,
`name` varchar(40) not null,
level_id varchar(2) not null,
ip varchar(10) not null,
num int(20) not null,
UNIQUE key service_id (asset_id,`name`,level_id,ip)
)ENGINE=InnoDB DEFAULT CHARSET=utf8;
