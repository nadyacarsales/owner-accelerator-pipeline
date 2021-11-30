drop table if exists cspot_simialar_cars_temp;
create  table cspot_simialar_cars_temp
as
select
    garage_vehicle_id,
    garage_vehicle_spot_id,
    garage_vehicle_make,
    garage_vehicle_model,
    garage_vehicle_year,
    garage_vehicle_badge,
    garage_vehicle_series,

    similar_car_spot_id,
    similar_car_make,
    similar_car_model,
    similar_car_year,
    similar_car_badge,
    similar_car_series,

    customer_id,
    releasetype,
    network_id ,
    rnum,
    NULL as rec_cars,
    NULL as reco_cars_rank,
    r1_spot,

    r1_make,
    r1_model,
    r1_badge_desc,
    r1_series,
    r2_spot_id,

    r2_make,
    r2_model ,
    r2_badge_desc,
    r2_series
from
(
    select garage_vehicle_id,
    a.spot_id garage_vehicle_spot_id,
    b.network_id,
    a.make garage_vehicle_make ,
    b.make similar_car_make,
    a.model garage_vehicle_model,
    b.model similar_car_model,
    a.kms,
    b.odometer,
    a.year  garage_vehicle_year,
    b.release_year similar_car_year,
    a.customer_id_ as customer_id,
    a.releasetype,
    r1.spot_id r1_spot,
    b.spot_id similar_car_spot_id,
    r1.make r1_make,
    r1.model r1_model,
    r1.badge_desc r1_badge_desc,
    r1.badge_desc garage_vehicle_badge,
    r1.series r1_series,
    r1.series garage_vehicle_series,
    r2.spot_id r2_spot_id,

    r2.make r2_make,
    r2.model r2_model,
    r2.badge_desc r2_badge_desc,
    r2.series r2_series,
    r2.badge_desc similar_car_badge,
    r2.series similar_car_series,
    row_number () over (partition by a.garage_vehicle_id order by abs( kms - cast (b.odometer as integer ) ) asc, cs_price_dap asc ) as rnum
    from
    cspot_inventory.xx_csn_inventory_car_d b,
    cspot_owner_acc.my_garage a, cspot_landing.xx_csn_redbook_d R1,
    cspot_landing.xx_csn_redbook_d R2
    where  lower(a.make)=lower(b.make)
        and lower(a.model)=lower(b.model)
        and b.current_ind=1
        and lower(a.SPOT_id)=lower( R1.spot_id )
        and lower(b.spot_id)=lower(R2.spot_id)
        and lower(R1.badge_desc)=lower(r2.badge_desc)
        and lower(r1.series)=lower(r2.series)
        and lower(R1.make)=lower(r2.make)
        and lower(r1.model)=lower(r2.model)
        and lower(R1.release_year)=lower(r2.release_year)
        and cast (a.year as integer ) =cast (b.release_year as integer )
        and a.year is not null
        and b.release_year is not null
        and b.odometer is not null
        and lower(sale_status)='for sale'
        and b.date_offline is  null
        and cast (b.odometer as bigint ) between 5000 and 250000
        and b.date_publish_online  > sysdate -30 
) where rnum < 6;

delete from cspot_owner_acc.cspot_simialar_cars_new;
insert into cspot_owner_acc.cspot_simialar_cars_new select * from cspot_simialar_cars_temp;