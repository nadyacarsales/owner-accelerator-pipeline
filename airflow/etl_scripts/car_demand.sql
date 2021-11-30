create temp table car_demand_table_temp as
select y1.garage_vehicle_id,
    car_demand_30,
    car_demand_60,
    y.CAR_PREV_AVG_VIEWS as CAR_CURR_AVG_VIEWS,
    ((y.CAR_PREV_AVG_VIEWS - y1.CAR_PREV_AVG_VIEWS)/CAR_CURR_AVG_VIEWS)*100 as PERCENTAGE,
    y1.prev_car_demand,
    y1.car_demand_rating_date_curr
from cspot_owner_acc.car_demand_historical y1, cspot_owner_acc.car_demand_present y
where y1.garage_vehicle_id = y.garage_vehicle_id and CAR_CURR_AVG_VIEWS > 1 and CAR_CURR_AVG_VIEWS is not null;

delete from cspot_owner_acc.car_demand;
insert into cspot_owner_acc.car_demand select * from car_demand_table_temp;
