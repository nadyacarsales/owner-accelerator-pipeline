drop table if exists car_valuation_temp;

create temp table car_valuation_temp (like cspot_owner_acc.car_valuation);

copy car_valuation_temp from  '{}'
iam_role '{}'
ESCAPE
FILLRECORD
TRIMBLANKS
TRUNCATECOLUMNS
DELIMITER '|'
BLANKSASNULL
REMOVEQUOTES
ACCEPTINVCHARS
maxerror as 50
IGNOREHEADER 1 
timeformat 'auto' dateformat 'auto';

truncate cspot_owner_acc.car_valuation;
insert into cspot_owner_acc.car_valuation select distinct * from car_valuation_temp;

truncate cspot_owner_acc.car_valuation_bkp;
insert into cspot_owner_acc.car_valuation_bkp select * from cspot_owner_acc.car_valuation;