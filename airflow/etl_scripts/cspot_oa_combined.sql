drop table if exists cspot_OA_combined_temp;

create temp table cspot_OA_combined_temp
as 
select upper(my_gar.customer_id_) as customer_id_ ,
lower(my_gar.garage_vehicle_id) as GARAGE_GUID,
my_gar.spot_id as SPOT_ID,
my_gar.make as MAKE,
my_gar.model as MODEL_TYPE,

decode(floor((cast (car_val.car_private_price_min  as double precision ) + 99) / 100) * 100 , '0.0', '',floor((cast (car_val.car_private_price_min  as double precision ) + 99) / 100) * 100) as CAR_PRIVATE_PRICE_MIN,
decode(floor((cast (car_val.car_private_price_max  as double precision ) + 99) / 100) * 100 , '0.0', '',floor((cast (car_val.car_private_price_max  as double precision ) + 99) / 100) * 100) as CAR_PRIVATE_PRICE_MAX,
decode(floor((cast (car_val.car_trade_in_price_min  as double precision ) + 99) / 100) * 100 , '0.0', '',floor((cast (car_val.car_trade_in_price_min  as double precision ) + 99) / 100) * 100) as CAR_TRADE_IN_PRICE_MIN,
decode(floor((cast (car_val.car_trade_in_price_max  as double precision ) + 99) / 100) * 100  , '0.0', '',floor((cast (car_val.car_trade_in_price_max  as double precision ) + 99) / 100) * 100) as CAR_TRADE_IN_PRICE_MAX,
upper(car_val.car_price_kmap) as CAR_PRICE_KMAP,

decode(price_ahead.pt1_price  , '0.0', '',price_ahead.pt1_price) as POINT_IN_TIME_1YR,
decode(price_ahead.pt3_price, '0.0', '',price_ahead.pt3_price ) as POINT_IN_TIME_3YRS,
price_ahead.trend as CAR_PRICE_MOVEMENT,

decode(floor((cast (price_ahead.price_difference  as double precision ) + 99) / 100) * 100  , '0.0', '',floor((cast (price_ahead.price_difference  as double precision ) + 99) / 100) * 100) as FUTURE_PRICE_DIFF,
price_com.car_val_low_range as CAR_VAL_LOW_RANGE ,
price_com.car_numb_low_range as CAR_NUMB_LOW_RANGE,
price_com.car_median_price_low_range as CAR_MEDIAN_PRICE_LOW_RANGE,
price_com.car_numb_low_percentage as CAR_NUMB_LOW_PERCENTAGE,
price_com.car_val_mid_range as CAR_VAL_MID_RANGE,
price_com.car_numb_mid_range as CAR_NUMB_MID_RANGE,
price_com.car_median_price_mid_range as CAR_MEDIAN_PRICE_MID_RANGE,
price_com.car_numb_mid_percentage as CAR_NUMB_MID_PERCENTAGE,
price_com.car_val_high_range as CAR_VAL_HIGH_RANGE,
price_com.car_numb_high_range as CAR_NUMB_HIGH_RANGE,
price_com.car_median_price_high_range as CAR_MEDIAN_PRICE_HIGH_RANGE,
price_com.car_numb_high_percentage as CAR_NUMB_HIGH_PERCENTAGE,
price_com.CAR_TOTAL_COMPARED as CAR_TOTAL_COMPARED,
price_com.CAR_VAL_INDICATOR as CAR_VAL_INDICATOR,
time_to_sell.car_curr_time_to_sell as CAR_CURR_TIME_TO_SELL,
time_to_sell.car_prev_time_to_sell as CAR_PREV_TIME_TO_SELL,
sim_car.releasetype as RELEASE_TYPE,
sim_car.similar_car_1 as SIMILAR_CAR_1,
sim_car.similar_car_2 as SIMILAR_CAR_2,
sim_car.similar_car_3 as SIMILAR_CAR_3,
sim_car.similar_car_4 as SIMILAR_CAR_4,
sim_car.similar_car_5 as SIMILAR_CAR_5,
car_demand.car_demand,
car_demand.CAR_CURR_AVG_VIEWS,
car_demand.CAR_DEMAND_MOVEMENT,
car_demand.CAR_DEMAND_RATING_DATE,
case when car_demand.demand_percentage < -150 then -10
when  car_demand.demand_percentage > 200 then 200
else car_demand.demand_percentage
end as car_demand_percentag,

decode(floor((cast (car_val.instant_offer_pice_min  as double precision ) + 99) / 100) * 100 , '0.0', '',floor((cast (car_val.instant_offer_pice_min  as double precision ) + 99) / 100) * 100) as instant_offer_pice_min ,
decode(floor((cast (car_val.instant_offer_pice_max  as double precision ) + 99) / 100) * 100 , '0.0', '',floor((cast (car_val.instant_offer_pice_max  as double precision ) + 99) / 100) * 100) as instant_offer_pice_max ,

decode(car_val.redbook_price_min , '0.0', '',car_val.redbook_price_min) as redbook_price_pice_min,
decode(car_val.redbook_price_max, '0.0', '',car_val.redbook_price_max) as redbook_price_max,
decode(floor((cast (car_val.lm_retail_price  as double precision ) + 99) / 100) * 100 , '0.0', '',floor((cast (car_val.lm_retail_price  as double precision ) + 99) / 100) * 100) as lm_retail_price_rounded,
decode(car_val.lm_retail_price, '0.0', '',car_val.lm_retail_price) as lm_retail_price_actual,
rec_car.recommended_car_1 ,
rec_car.recommended_car_2 ,
rec_car.recommended_car_3 ,
sysdate

from  cspot_owner_acc.my_garage my_gar
left join  cspot_owner_acc.similar_cars_v sim_car
on lower (my_gar.garage_vehicle_id)=lower (sim_car.garage_vehicle_id )
left join cspot_owner_acc.time_to_sell_v time_to_sell
on lower (my_gar.garage_vehicle_id)= lower (time_to_sell.garage_vehicle_id )
left join  cspot_owner_acc.car_valuation car_val
on lower (my_gar.garage_vehicle_id)=lower (car_val.garage_vehicle_id )
left join cspot_owner_acc.price_compare_v price_com
on lower (my_gar.garage_vehicle_id)= lower (price_com.garage_vehicle_id )
left join cspot_owner_acc.price_ahead_v price_ahead
on lower (my_gar.garage_vehicle_id)=lower (price_ahead.garage_vehicle_id  )
left join cspot_owner_acc.car_demand_v car_demand
on lower (my_gar.garage_vehicle_id)=lower (car_demand.garage_vehicle_id  )
left join cspot_owner_acc.recommended_cars rec_car
on lower (my_gar.garage_vehicle_id)=lower (rec_car.garage_vehicle_id  );

delete from cspot_owner_acc.cspot_OA_combined;
insert into cspot_owner_acc.cspot_OA_combined select distinct a.* from cspot_OA_combined_temp a;

