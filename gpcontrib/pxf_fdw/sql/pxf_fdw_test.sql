CREATE extension pxf_fdw;
CREATE SERVER
amazon_s3
FOREIGN DATA WRAPPER pxf_fdw
OPTIONS (protocol 's3');

CREATE USER MAPPING FOR public 
SERVER amazon_s3;

CREATE FOREIGN TABLE stats(
  dvalue date,
  email varchar,
  hour interval,
  login_time interval,
  logout_time interval,
  available_time interval
)
SERVER amazon_s3
OPTIONS (
  hostname 's3.amazonaws.com',
  bucketname 'reports',
  filename 'hourly.csv',
  format 'csv',
  delimiter E',',
  quote E'"',
  header 'true'
);

select * from stats;
