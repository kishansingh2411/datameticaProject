drop table if exists officedepot_clickstream_view_daily;
create table officedepot_clickstream_view_daily  
(day integer not null, // data stored in year,month and day format
country varchar(60) not null,  // name of the country
visitor_segment varchar(50) not null, // types of the visitor visiting the site
total_orders decimal(30,10), // Total number of order placed in a day
total_visits integer, // Total number of visits by customer in a day
total_page_views integer, // Total page visited in a day
total_rev_frm_ncust decimal(30,10),  // Total revenue generated from new customer
total_rev_frm_ecust decimal(30,10), // Total revenue generated from exixting customer
total_cart_started integer, // Total number of cart started
total_cart_aband integer, // Total number of cart abandant
total_view_products integer, // Total number of product viewed
total_rev_frm_anymcust decimal(30,10) // Total revenue generated from unknown customers
constraint pk primary key(day,country,visitor_segment) )
compression = 'snappy',SALT_BUCKETS = 3;
drop table if exists officedepot_clickstream_view_weekly;
create table officedepot_clickstream_view_weekly
(week integer not null, // data stored in year and week format
country varchar(60) not null, // name of the country
visitor_segment varchar(50) not null,  // types of the visitor visiting the site
total_orders decimal(30,10), // Total number of order placed in a week
total_visits integer, // Total number of visits by customer in a week
total_page_views integer, // Total page visited in a week
total_rev_frm_ncust decimal(30,10), // Total revenue generated from new customer
total_rev_frm_ecust decimal(30,10), // Total revenue generated from exixting customer
total_cart_started integer, // Total number of cart started
total_cart_aband integer, // Total number of cart abandant
total_view_products integer, // Total number of product viewed
total_rev_frm_anymcust decimal(30,10) // Total revenue generated from unknown customers
constraint pk primary key(week, country,visitor_segment) ) // primary key for the row id generation
compression = 'snappy',SALT_BUCKETS = 3;