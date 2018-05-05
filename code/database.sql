drop table census;

create table census (
	id INTEGER PRIMARY KEY,
	age INTEGER NOT NULL,
	workclass VARCHAR(50) NOT NULL,
	fnlwgt INTEGER NOT NULL,
	education VARCHAR(50) NOT NULL,
	education_num INTEGER NOT NULL,
	marital_status VARCHAR(50) NOT NULL,
	occupation VARCHAR(50) NOT NULL,
	relationship VARCHAR(50) NOT NULL,
	race VARCHAR(50) NOT NULL,
	sex VARCHAR(50) NOT NULL,
	capital_gain INTEGER NOT NULL,
	capital_loss INTEGER NOT NULL,
	hours_per_week INTEGER NOT NULL,
	native_country VARCHAR(50) NOT NULL,
	salary VARCHAR(50) NOT NULL);

COPY census FROM '/home/mili/Comp/umass/seedb/data/census_data.csv'
DELIMITER ',' CSV HEADER;
