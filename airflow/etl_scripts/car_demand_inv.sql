delete from cspot_owner_acc.car_demand_inv;
insert into cspot_owner_acc.car_demand_inv 
select * from cspot_inventory.xx_csn_inventory_car_d where date_publish_online > sysdate -60 and current_ind=1;
