drop table if exists price_compare_temp;

create temp table price_compare_temp as
select
    a.garage_vehicle_id as garage_id,
    b.make,b.model,
    a.year,
    a.kms,
    cast (min ( b.odometer ) as bigint  ) as CAR_VAL_LOW_RANGE_min ,
    cast ((min ( b.odometer) + ((max ( b.odometer) - min ( b.odometer) ) /6)) as bigint  )  as CAR_VAL_LOW_RANGE_max,
    cast ((min ( b.odometer) + ((max ( b.odometer) - min ( b.odometer) ) /6) + 1) as bigint  )  as CAR_VAL_MID_RANGE_min,
    cast ((min ( b.odometer) + ((max ( b.odometer) - min ( b.odometer) ) /6) *2 ) as bigint  )  as CAR_VAL_MID_RANGE_max,
    cast ((min ( b.odometer) + ((max ( b.odometer) - min ( b.odometer) ) /6) *2 + 1 ) as bigint  )  as CAR_VAL_HIGH_RANGE_min,
    cast (max ( b.odometer)as bigint  ) as CAR_VAL_HIGH_RANGE_max,
    count(*) as TOTAL_CARS
from
    cspot_inventory.xx_csn_inventory_car_d b ,
    cspot_owner_acc.my_garage a
where lower(a.make)=lower (b.make)
    and lower(a.model)=lower(b.model )
    and b.current_ind=1
    and a.year between b.release_year -3
    and b.release_year +3
    and a.year is not null
    and b.release_year is not null
    and b.odometer is not null
    and b.date_publish_online  > sysdate -60
    and b.odometer between 100 and 200000
group by garage_vehicle_id , b.make,b.model , a.year ,a.kms having max ( b.odometer) >0;
delete from cspot_owner_acc.PRICE_COMPARE;

insert into cspot_owner_acc.PRICE_COMPARE select * from PRICE_COMPARE_temp;