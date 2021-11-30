delete from cspot_owner_acc.car_demand_previous;
insert into cspot_owner_acc.car_demand_previous select * from cspot_owner_acc.car_demand_v;
