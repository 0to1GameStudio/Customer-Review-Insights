/*
-------------------------------- CRI Customer Review Insights Project --------------------------------------------------
 DDL Data Description Language.
  Creating Tables - 3
  One - 1 - Fact Table
  Two - 2 - Dimensions Tables
*/
create table fact_reviews(
	review_id BigSerial primary key,
	product_id int references dim_product(product_id),
	review_summary_id int references dim_review_summary(review_summary_id),
	rate int,
	sentiment varchar(10)
);

create table dim_product (
	product_id BigSerial primary key,
	product_name varchar(255),
	product_price int
);

create table dim_review_summary(
	review_summary_id BigSerial primary key,
	review TEXT,
	summary varchar(255)
);