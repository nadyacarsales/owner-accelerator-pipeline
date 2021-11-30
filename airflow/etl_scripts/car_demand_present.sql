delete from cspot_owner_acc.car_demand_present;
insert into cspot_owner_acc.car_demand_present
select z.garage_vehicle_id,
    (count ( distinct leadid) + ( sum ( count_large_view ) / max ( car_demand_60) ) )/count ( distinct  b.network_id) as car_demand_30,
    sum ( count_large_view )/ count ( distinct   b.network_id) current_avg_views,
    max (car_demand) as prev_car_demand,
    max(x.car_demand_rating_date) as car_demand_rating_date_curr
from  cspot_owner_acc.my_garage z
left join  cspot_owner_acc.car_demand_previous x
    on lower (z.garage_vehicle_id ) = lower (x.garage_vehicle_id )
left join cspot_owner_acc.car_demand_historical y
    on lower (z.garage_vehicle_id ) = lower (y.garage_vehicle_id )
inner join cspot_owner_acc.car_demand_inv b
    on lower(b.make)=lower(z.make)
        and lower(b.model)=lower(z.model)
        and cast (b.release_year as integer ) =cast (z.year as integer )
inner join cspot_owner_acc.car_demand_view a
    on upper(b.network_id)=upper(a.network_id)
inner join cspot_owner_acc.car_demand_leads c
    on c.network_id=b.network_id
where   current_ind=1
    and b.odometer is not null and b.date_offline is not null
    and c.created_date> sysdate -30 
    and a.day_wid >  trunc(current_date) -60 
    and b.release_year is not null
    and b.odometer is not null
    and b.date_offline is not null
    and cast (z.kms as bigint ) between 5000 and 250000
    and b.date_publish_online  > sysdate -30 
group by z.garage_vehicle_id;
