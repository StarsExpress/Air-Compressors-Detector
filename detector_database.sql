-- Database mw_task1_pm_db schema created by Jack Yao. Created time: 2023-08-03.
use mw_task1_pm_db;  -- Macao Water Predictive Maintenance.


DROP TABLE IF EXISTS `mw_task1_pm_db`.`pump_meta_data`;
CREATE TABLE IF NOT EXISTS `mw_task1_pm_db`.`pump_meta_data` (
	`id` VARCHAR(32) NOT NULL UNIQUE,
    `name` VARCHAR(6) NOT NULL,
    `asset_code` VARCHAR(10) NOT NULL,
    `power` FLOAT,
    `type` VARCHAR(20) NOT NULL,
	`location` VARCHAR(10) NOT NULL,  
    CONSTRAINT PK_PUMP_META_DATA PRIMARY KEY (`id`)
)
ENGINE = InnoDB;


DROP TABLE IF EXISTS `mw_task1_pm_db`.`water_network_data`;
CREATE TABLE IF NOT EXISTS `mw_task1_pm_db`.`water_network_data` (
	`id` VARCHAR(32) NOT NULL UNIQUE,
    `name` VARCHAR(10) NOT NULL,
    CONSTRAINT PK_PUMP_META_DATA PRIMARY KEY (`id`)
)
ENGINE = InnoDB;


DROP TABLE IF EXISTS `mw_task1_pm_db`.`ts_pump_energy`;
CREATE TABLE IF NOT EXISTS `mw_task1_pm_db`.`ts_pump_energy` (
    `id` VARCHAR(32) NOT NULL UNIQUE,
    `datetime` DATETIME NOT NULL,
    `asset_code` VARCHAR(10) NOT NULL,  
    `energy` FLOAT,
    `load_date` DATETIME,
    `status` INT,
    CONSTRAINT PK_TS_PUMP_ENERGY PRIMARY KEY (`id`)
)
ENGINE = InnoDB;


DROP TABLE IF EXISTS `mw_task1_pm_db`.`ts_network_flow`;
CREATE TABLE IF NOT EXISTS `mw_task1_pm_db`.`ts_network_flow` (
    `id` VARCHAR(32) NOT NULL UNIQUE,
    `datetime` DATETIME NOT NULL,
    `network_name` VARCHAR(10) NOT NULL,
    `flow` FLOAT,
    `load_date` DATETIME,
    `status` INT,
    CONSTRAINT PK_TS_NETWORK_FLOW PRIMARY KEY (`id`)
)
ENGINE = InnoDB;


DROP TABLE IF EXISTS `mw_task1_pm_db`.`ts_network_pressure`;
CREATE TABLE IF NOT EXISTS `mw_task1_pm_db`.`ts_network_pressure` (
    `id` VARCHAR(32) NOT NULL UNIQUE,
    `datetime` DATETIME NOT NULL,
    `network_name` VARCHAR(10) NOT NULL,
    `pressure` FLOAT,
    `load_date` DATETIME,
    `status` INT,
    CONSTRAINT PK_TS_NETWORK_PRESSURE PRIMARY KEY (`id`)
)
ENGINE = InnoDB;


DROP TABLE IF EXISTS `mw_task1_pm_db`.`ts_pump_result_status`;
CREATE TABLE IF NOT EXISTS `mw_task1_pm_db`.`ts_pump_result_status` (
    `id` VARCHAR(32) NOT NULL UNIQUE,
    `datetime` DATETIME NOT NULL,
    `asset_code` VARCHAR(10) NOT NULL,
    `result_status` VARCHAR(10),
    `load_date` DATETIME,
    CONSTRAINT PK_TS_PUMP_RESULT_STATUS PRIMARY KEY (`id`)
)
ENGINE = InnoDB;

-- Table ts_pump_result_status updated time: 2023-11-08.
ALTER TABLE ts_pump_result_status ADD COLUMN tag_name VARCHAR(20) AFTER asset_code;
ALTER TABLE ts_pump_result_status ADD COLUMN energy_detail VARCHAR(20) AFTER result_status;

-- Table ts_pump_result_status updated time: 2023-11-16.
ALTER TABLE ts_pump_result_status ADD COLUMN push_status INT AFTER energy_detail;


DROP TABLE IF EXISTS `mw_task1_pm_db`.`air_compressor_meta_data`;
CREATE TABLE IF NOT EXISTS `mw_task1_pm_db`.`air_compressor_meta_data` (
	`id` VARCHAR(32) NOT NULL UNIQUE,
    `name` VARCHAR(20) NOT NULL,
    `asset_code` VARCHAR(10) NOT NULL,
	`location` VARCHAR(10) NOT NULL,
    CONSTRAINT PK_AIR_COMPRESSOR_META_DATA PRIMARY KEY (`id`)
)
ENGINE = InnoDB;


DROP TABLE IF EXISTS `mw_task1_pm_db`.`ts_air_compressor_runtime`;
CREATE TABLE IF NOT EXISTS `mw_task1_pm_db`.`ts_air_compressor_runtime` (
    `id` VARCHAR(32) NOT NULL UNIQUE,
    `datetime` DATETIME NOT NULL,
    `asset_code` VARCHAR(10) NOT NULL,
    `runtime` INT,
    `last_7_day_avg` INT,
    `load_date` DATETIME,
    `status` INT,
    CONSTRAINT PK_TS_AIR_COMPRESSOR_RUNTIME PRIMARY KEY (`id`)
)
ENGINE = InnoDB;


DROP TABLE IF EXISTS `mw_task1_pm_db`.`ts_air_compressor_result_status`;
CREATE TABLE IF NOT EXISTS `mw_task1_pm_db`.`ts_air_compressor_result_status` (
    `id` VARCHAR(32) NOT NULL UNIQUE,
    `datetime` DATETIME NOT NULL,
    `asset_code` VARCHAR(10) NOT NULL,
    `result_status` VARCHAR(10),
    `load_date` DATETIME,
    CONSTRAINT PK_TS_AIR_COMPRESSOR_RESULT_STATUS PRIMARY KEY (`id`)
)
ENGINE = InnoDB;

-- Table ts_air_compressor_result_status updated time: 2023-10-12.
ALTER TABLE ts_air_compressor_result_status ADD COLUMN tag_name VARCHAR(20) AFTER asset_code;

-- Table ts_air_compressor_result_status updated time: 2023-11-16.
ALTER TABLE ts_air_compressor_result_status ADD COLUMN push_status INT AFTER result_status;


-- Table air_compressor_start_benchmark added time: 2023-10-12.
DROP TABLE IF EXISTS `mw_task1_pm_db`.`air_compressor_start_benchmark`;
CREATE TABLE IF NOT EXISTS `mw_task1_pm_db`.`air_compressor_start_benchmark` (
    `asset_code` VARCHAR(10) NOT NULL UNIQUE,
    `tag_name` VARCHAR(20),
    `avg_start_count` FLOAT,
    CONSTRAINT PK_AIR_COMPRESSOR_START_BENCHMARK PRIMARY KEY (`asset_code`)
)
ENGINE = InnoDB;


-- Table ts_air_compressor_start_count added time: 2023-10-12.
DROP TABLE IF EXISTS `mw_task1_pm_db`.`ts_air_compressor_start_count`;
CREATE TABLE IF NOT EXISTS `mw_task1_pm_db`.`ts_air_compressor_start_count` (
    `id` VARCHAR(32) NOT NULL UNIQUE,
    `datetime` DATETIME NOT NULL,
    `asset_code` VARCHAR(10) NOT NULL,
    `tag_name` VARCHAR(20),
    `start_count` INT,
    `alarm_message` VARCHAR(10),
    `load_date` DATETIME,
    CONSTRAINT PK_TS_AIR_COMPRESSOR_START_COUNT PRIMARY KEY (`id`)
)
ENGINE = InnoDB;
