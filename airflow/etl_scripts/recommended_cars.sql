drop table if exists recommended_cars_temp;

create temp table recommended_cars_tmp (like cspot_owner_acc.recommended_cars);

copy  recommended_cars_tmp from '{}'
iam_role '{}'
ESCAPE
FILLRECORD
TRIMBLANKS
TRUNCATECOLUMNS
DELIMITER ','
BLANKSASNULL
REMOVEQUOTES
ACCEPTINVCHARS
IGNOREHEADER 1 
timeformat 'auto' dateformat 'auto';
delete from cspot_owner_acc.recommended_cars;
insert into cspot_owner_acc.recommended_cars select distinct * from recommended_cars_tmp;