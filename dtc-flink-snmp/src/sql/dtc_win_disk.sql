CREATE TABLE `dtc_win_disk` (
  `host_ip` varchar(10) NOT NULL,
  `last_code` varchar(10) NOT NULL,
  `result` varchar(500) DEFAULT NULL,
  PRIMARY KEY (`host_ip`,`last_code`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
