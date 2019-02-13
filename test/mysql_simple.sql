create database simple;
use simple;

CREATE TABLE `simple_type` (
	`id` bigint UNSIGNED NOT NULL AUTO_INCREMENT,
	`tinyint` tinyint UNSIGNED NOT NULL,
	`long` bigint UNSIGNED NOT NULL,
	`char` char(32)  NOT NULL,
	`varchar` varchar(5)  NOT NULL,
	`text` text  NOT NULL,
	`decimal` decimal(16,2)  UNSIGNED NOT NULL,
	`double` double UNSIGNED NOT NULL,
	`timestamp` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
	PRIMARY KEY (`id`),
) ENGINE=InnoDB DEFAULT CHARACTER SET=utf8 COLLATE=utf8_general_ci ROW_FORMAT=Dynamic;
