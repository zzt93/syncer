
CREATE TABLE IF NOT EXISTS  `correctness` (
	`id` bigint UNSIGNED NOT NULL AUTO_INCREMENT,
	`time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
	`news_id` bigint UNSIGNED NOT NULL,
	`currency` varchar(5)  NOT NULL,
	`total` decimal(16,2)  UNSIGNED NOT NULL,
	`quantity` double UNSIGNED NOT NULL,
	`type` tinyint UNSIGNED NOT NULL,
	`name` varchar(32)  NOT NULL,
	`unit` varchar(5)  NOT NULL,
	PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARACTER SET=utf8 COLLATE=utf8_general_ci ROW_FORMAT=Dynamic;

CREATE TABLE IF NOT EXISTS  `news` (
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

CREATE TABLE IF NOT EXISTS  `types` (
	`id` bigint UNSIGNED NOT NULL AUTO_INCREMENT,
	`tinyint` tinyint UNSIGNED NOT NULL,
	`bigint` bigint UNSIGNED NOT NULL,
	`char` char(32)  NOT NULL,
	`varchar` varchar(5)  NOT NULL,
	`text` text  NOT NULL,
	`decimal` decimal(16,2)  UNSIGNED NOT NULL,
	`double` double UNSIGNED NOT NULL,
	`timestamp` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
	PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARACTER SET=utf8 COLLATE=utf8_general_ci ROW_FORMAT=Dynamic;


CREATE TABLE IF NOT EXISTS  `correctness_bak` (
	`id` bigint UNSIGNED NOT NULL AUTO_INCREMENT,
	`time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
	`news_id` bigint UNSIGNED NOT NULL,
	`currency` varchar(5)  NOT NULL,
	`total` decimal(16,2)  UNSIGNED NOT NULL,
	`quantity` double UNSIGNED NOT NULL,
	`type` tinyint UNSIGNED NOT NULL,
	`name` varchar(32)  NOT NULL,
	`unit` varchar(5)  NOT NULL,
	PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARACTER SET=utf8 COLLATE=utf8_general_ci ROW_FORMAT=Dynamic;

CREATE TABLE IF NOT EXISTS  `news_bak` (
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

CREATE TABLE IF NOT EXISTS  `types_bak` (
	`id` bigint UNSIGNED NOT NULL AUTO_INCREMENT,
	`tinyint` tinyint UNSIGNED NOT NULL,
	`bigint` bigint UNSIGNED NOT NULL,
	`char` char(32)  NOT NULL,
	`varchar` varchar(5)  NOT NULL,
	`text` text  NOT NULL,
	`decimal` decimal(16,2)  UNSIGNED NOT NULL,
	`double` double UNSIGNED NOT NULL,
	`timestamp` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
	PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARACTER SET=utf8 COLLATE=utf8_general_ci ROW_FORMAT=Dynamic;

