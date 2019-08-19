drop table if exists office_depot_COVERAGE_ANALYTICS_VIEW_MONTHLY;
create table office_depot_COVERAGE_ANALYTICS_VIEW_MONTHLY  
(month integer not null, // data stored in year and month format
country varchar(30) not null, // code of the country
sales_represntative_id varchar(60) not null, // id of sales representatives
account_id  varchar(60) not null, // accounts_id
total_prospect integer, // Total number of prospects
total_revenue decimal(38,20),  // Total revenue generated 
sales_represntative_name varchar(100), // Name of sales representatives
account_name varchar(100), //  accounts_name
constraint pk primary key(month,COUNTRY,sales_represntative_id,account_id) )
compression = 'snappy',SALT_BUCKETS = 3;