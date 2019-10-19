
CREATE TABLE IF NOT EXISTS  `toDiscard` (
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
