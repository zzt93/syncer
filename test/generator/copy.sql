
CREATE TABLE IF NOT EXISTS  `toCopy` (
  `id` bigint(20) unsigned NOT NULL AUTO_INCREMENT ,
  `title` varchar(255) DEFAULT '',
  `content` longtext,
  `affair_id` bigint(20) unsigned DEFAULT '0',
  `task_id` bigint(20) unsigned DEFAULT '0',
  `thumb_content` longtext CHARACTER SET utf8mb4,
  `public_type` tinyint(3) unsigned DEFAULT '0',
  `state` tinyint(3) unsigned DEFAULT '0',
  `modify_time` timestamp(3) NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
  `alliance_id` bigint(20) unsigned NOT NULL DEFAULT '0',
  `plate_type` tinyint(3) unsigned DEFAULT '0' COMMENT '',
  `plate_sub_type` tinyint(3) unsigned DEFAULT '0' COMMENT '',
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 ROW_FORMAT=COMPACT;


CREATE TABLE IF NOT EXISTS  `toCopy_bak` (
  `id` bigint(20) unsigned NOT NULL AUTO_INCREMENT ,
  `title` varchar(255) DEFAULT '',
  `content` longtext,
  `affair_id` bigint(20) unsigned DEFAULT '0',
  `task_id` bigint(20) unsigned DEFAULT '0',
  `thumb_content` longtext CHARACTER SET utf8mb4,
  `public_type` tinyint(3) unsigned DEFAULT '0',
  `state` tinyint(3) unsigned DEFAULT '0',
  `modify_time` timestamp(3) NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
  `alliance_id` bigint(20) unsigned NOT NULL DEFAULT '0',
  `plate_type` tinyint(3) unsigned DEFAULT '0' COMMENT '',
  `plate_sub_type` tinyint(3) unsigned DEFAULT '0' COMMENT '',
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 ROW_FORMAT=COMPACT;
