CREATE TABLE `kpi` (
`key` varchar(80) NOT NULL COMMENT 'KEY',
`datetime` varchar(20) NOT NULL DEFAULT '0' COMMENT 'KPI 对应的时间段',
`value` varchar(3000) NOT NULL COMMENT 'VALUE',
`update_time` timestamp NOT NULL default CURRENT_TIMESTAMP on update CURRENT_TIMESTAMP,
`ext` varchar(3000),
PRIMARY KEY (`key`, `datetime`),
KEY `idx_datetime_key` (`datetime`, `key`),
KEY `idx_update_time` (`update_time`)
)ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='KPI 通用表';
