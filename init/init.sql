
-- Staging Layer

SET input_format_csv_allow_whitespace_or_tab_as_delimiter = true;
SET format_csv_delimiter = '\t';
SET date_time_input_format = 'best_effort';

CREATE DATABASE IF NOT EXISTS staging;
CREATE TABLE IF NOT EXISTS staging.st_vacancy
(
    ids                     Int64,
	employer                String,
	name                    String,
	salary                  Bool,
	from_slr                Nullable(Float32),
	to_slr                  Nullable(Float32),
	experience              String,
	schedule                String,
	keys                    Nullable(String),
	description             Nullable(String),
	area                    String,
	professional_roles      Nullable(String),
	specializations         Nullable(String),
	profarea_names          Nullable(String),
	published_at            DateTime
)
ENGINE = MergeTree()
ORDER BY (ids);

INSERT INTO staging.st_vacancy
FROM INFILE '/usr/IT_vacancies_full.csv.gz'
COMPRESSION 'gzip' FORMAT CSV;

-- Core Layer

CREATE DATABASE IF NOT EXISTS core;
CREATE TABLE IF NOT EXISTS core.cr_vacancy
(
    ids                     Int64,
	employer                String,
	name                    String,
	salary                  Bool,
	from_slr                Nullable(Float32),
	to_slr                  Nullable(Float32),
	experience              String,
	schedule                String,
	keys                    Nullable(String),
	description             Nullable(String),
	area                    String,
	published_at            DateTime
)
ENGINE = MergeTree()
ORDER BY (ids);

INSERT INTO core.cr_vacancy
SELECT  ids, employer, name, salary, from_slr, to_slr,
        experience, schedule, keys, description, area, published_at
FROM staging.st_vacancy;

-- Datamart Layer

CREATE DATABASE IF NOT EXISTS datamart;
CREATE TABLE IF NOT EXISTS datamart.dm_vacancy_msc_slr
(
    ids                     Int64,
	employer                String,
	name                    String,
	salary                  Bool,
	from_slr                Nullable(Float32),
	to_slr                  Nullable(Float32),
	experience              String,
	schedule                String,
	keys                    Nullable(String),
	description             Nullable(String),
	area                    String,
	published_at            DateTime
)
ENGINE = MergeTree()
ORDER BY (ids);

CREATE TABLE IF NOT EXISTS datamart.dm_vacancy_spb_slr
(
    ids                     Int64,
	employer                String,
	name                    String,
	salary                  Bool,
	from_slr                Nullable(Float32),
	to_slr                  Nullable(Float32),
	experience              String,
	schedule                String,
	keys                    Nullable(String),
	description             Nullable(String),
	area                    String,
	published_at            DateTime
)
ENGINE = MergeTree()
ORDER BY (ids);

INSERT INTO datamart.dm_vacancy_msc_slr
SELECT  ids, employer, name, salary, from_slr, to_slr,
        experience, schedule, keys, description, area, published_at
FROM staging.st_vacancy
WHERE
    area = 'Москва'
    AND salary = true;

INSERT INTO datamart.dm_vacancy_spb_slr
SELECT  ids, employer, name, salary, from_slr, to_slr,
        experience, schedule, keys, description, area, published_at
FROM staging.st_vacancy
WHERE
    area = 'Санкт-Петербург'
    AND salary = true;

CREATE TABLE IF NOT EXISTS datamart.dm_salary_avg
(
	name                    String,                 -- Наименование вакансии
	msc_from_avg            Nullable(Float32),      -- ср мин зп в Москве
	msc_to_avg              Nullable(Float32),      -- ср макс зп в Москве
	spb_from_avg            Nullable(Float32),      -- ср мин зп в Санкт-Петербурге
	spb_to_avg              Nullable(Float32),      -- ср макс зп в Санкт-Петербурге
	most_common             String                  -- Город, в котором наиболее распространена вакансия
)
ENGINE = AggregatingMergeTree()
ORDER BY (name);

INSERT INTO datamart.dm_salary_avg
WITH
msc_avg_salary AS (
	SELECT
		name,
		avg(from_slr) AS from_avg,
		avg(to_slr) AS to_avg,
		count(name) AS vac_cnt,
		area
	FROM datamart.dm_vacancy_msc_slr
	GROUP BY name, area
),
spb_avg_salary AS (
	SELECT
		name,
		avg(from_slr) AS from_avg,
		avg(to_slr) AS to_avg,
		count(name) AS vac_cnt,
		area
	FROM datamart.dm_vacancy_spb_slr
	GROUP BY name, area
)
SELECT
	CASE
		WHEN mas.vac_cnt >= sas.vac_cnt
		THEN mas.name
		ELSE sas.name
	END AS name,
	mas.from_avg AS msc_from_avg,
	mas.to_avg AS msc_to_avg,
	sas.from_avg AS spb_from_avg,
	sas.to_avg AS spb_to_avg,
	CASE
		WHEN mas.vac_cnt >= sas.vac_cnt
		THEN mas.area
		ELSE sas.area
	END AS most_common
FROM msc_avg_salary AS mas
FULL OUTER JOIN spb_avg_salary AS sas
	ON mas.name = sas.name
ORDER BY name
