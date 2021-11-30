delete from cspot_owner_acc.car_demand_historical;
insert into cspot_owner_acc.car_demand_historical 
select z.garage_vehicle_id,
    (count ( distinct leadid) + ( sum ( count_large_view ) / count ( distinct leadid) ) )/count ( distinct  b.network_id) as car_demand_60,
    sum ( count_large_view )/ count ( distinct   b.network_id) previous_avg_views,
    max (car_demand) as prev_car_demand,
    max(x.car_demand_rating_date) as car_demand_rating_date_curr
from  cspot_owner_acc.my_garage z
left join  cspot_owner_acc.car_demand_previous x
    on lower (z.garage_vehicle_id ) = lower (x.garage_vehicle_id )
inner join cspot_owner_acc.car_demand_inv b
    on lower(z.make)=lower(b.make)
        and lower(z.model)=lower(b.model)
        and cast ( z.year as integer ) =cast (b.release_year as integer )
inner join cspot_owner_acc.car_demand_view a
    on upper(b.network_id)=upper(a.network_id)
inner join cspot_owner_acc.car_demand_leads c
    on upper(b.network_id)=upper(c.network_id)
where   current_ind=1
    and b.odometer is not null and b.date_offline is not null
    and c.created_date> sysdate -60 and  c.created_date< sysdate -30
    and a.day_wid >  trunc(current_date) -60 -- and a.day_wid < trunc(current_date) -30
    and b.release_year is not null
    and b.odometer is not null
    and b.date_offline is not null
    and cast (z.kms as bigint ) between 5000 and 250000
    and b.date_publish_online  > sysdate -60  and b.date_publish_online  < sysdate -30
group by z.garage_vehicle_id;
