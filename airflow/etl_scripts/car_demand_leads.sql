delete from cspot_owner_acc.car_demand_leads; 
insert into cspot_owner_acc.car_demand_leads select * from cspot_leads.land_leads_api where created_date> sysdate -60;
