delete from cspot_owner_acc.car_demand_view;
insert into cspot_owner_acc.car_demand_view 
select * from cspot_inventory.xx_csn_car_view_f where day_wid> trunc(current_date) -60;
