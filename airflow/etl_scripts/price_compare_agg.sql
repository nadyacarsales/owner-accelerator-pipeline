drop table if exists PRICE_COMPARE_AGG_temp;
create temp TABLE PRICE_COMPARE_AGG_temp AS
select low_range.garage_id ,
cast (CAR_NUMB_LOW_RANGE as bigint) CAR_NUMB_LOW_RANGE ,cast (CAR_MEDIAN_PRICE_LOW_RANGE as bigint) CAR_MEDIAN_PRICE_LOW_RANGE ,
cast (CAR_NUMB_MID_RANGE as bigint) CAR_NUMB_MID_RANGE ,cast (CAR_MEDIAN_PRICE_MID_RANGE as bigint) CAR_MEDIAN_PRICE_MID_RANGE ,
cast (CAR_NUMB_HIGH_RANGE as bigint) CAR_NUMB_HIGH_RANGE ,cast (CAR_MEDIAN_PRICE_HIGH_RANGE as bigint) CAR_MEDIAN_PRICE_HIGH_RANGE
from
(
select garage_id ,
count( network_id ) as CAR_NUMB_LOW_RANGE,
median (  a.cs_price_dap   ) as CAR_MEDIAN_PRICE_LOW_RANGE
from
cspot_owner_acc.price_compare b,
cspot_inventory.xx_csn_inventory_car_d  a
where
a.current_ind=1
and lower(a.make)=lower (b.make)
and lower(a.model)=lower(b.model )
and b.year between a.release_year -3 and a.release_year +3
and a.release_year is not null
and b.year is not null
and a.odometer is not null
and b.CAR_VAL_HIGH_RANGE_max >0
and a.date_publish_online  > sysdate -60
and cast (a.odometer as integer ) between cast (CAR_VAL_LOW_RANGE_min as integer ) and cast (CAR_VAL_LOW_RANGE_max as integer )
group by garage_id
) low_range,
(
select garage_id ,
count( network_id ) as CAR_NUMB_MID_RANGE,
median (  a.cs_price_dap   ) as CAR_MEDIAN_PRICE_MID_RANGE
from
cspot_owner_acc.price_compare b,
cspot_inventory.xx_csn_inventory_car_d  a
where  a.current_ind=1
and lower(a.make)=lower (b.make)
and lower(a.model)=lower(b.model )
and b.year between a.release_year -3 and a.release_year +3
and a.release_year is not null
and b.year is not null
and a.odometer is not null
and b.CAR_VAL_HIGH_RANGE_max >0
and a.date_publish_online  > sysdate -60
and cast (a.odometer as integer ) between cast (CAR_VAL_MID_RANGE_min as integer ) and cast (CAR_VAL_MID_RANGE_max as integer )
group by garage_id
) mid_range,
(
select
garage_id ,
count( network_id ) as CAR_NUMB_HIGH_RANGE,
median (  a.cs_price_dap   ) as CAR_MEDIAN_PRICE_HIGH_RANGE
from
cspot_owner_acc.price_compare b,
cspot_inventory.xx_csn_inventory_car_d  a
where
a.current_ind=1
and lower(a.make)=lower (b.make)
and lower(a.model)=lower(b.model )
and b.year between a.release_year -3 and a.release_year +3
and a.release_year is not null
and b.year is not null
and b.CAR_VAL_HIGH_RANGE_max >0
and a.odometer is not null
and a.date_publish_online  > sysdate -60
and cast (a.odometer as integer ) between cast ( CAR_VAL_HIGH_RANGE_min as integer ) and cast (CAR_VAL_HIGH_RANGE_max as integer )
group by garage_id
) high_range
where low_range.garage_id=mid_range.garage_id
and low_range.garage_id=high_range.garage_id;
delete from cspot_owner_acc.PRICE_COMPARE_AGG;
insert into cspot_owner_acc.PRICE_COMPARE_AGG select * from PRICE_COMPARE_AGG_temp;