Drop table if exists my_garage_temp;
create temp table my_garage_temp as 
select 
    GARAGE_VEHICLE_ID, 
    upper(CUSTOMER_ID_) as CUSTOMER_ID_, 
    a.SPOT_ID, 
    a.MAKE, 
    a.MODEL, 
    cast (a.YEAR as integer) as YEAR, 
    NULL as TRADE_VALUE, 
    NULL as BADGE, 
    cast (PREvious_KMS as integer) as PREvious_KMS, 
    (
    round(kms) - round(PREvious_KMS)
    ) as ODO_INCREMENTAL_CHANGE, 
    cast (kms as integer) as kms, 
    image_url, 
    a.Vin, 
    createdatUTC as garage_ad_date, 
    Registration, 
    odometer_update_dt as odometer_update_dt, 
    b.releasetype 
from 
    (
    select 
        * 
    from 
        (
        select 
            * 
        from 
            (
            SELECT 
                message_id as GARAGE_VEHICLE_ID, 
                message_memberid as CUSTOMER_ID_, 
                message_specid as SPOT_ID, 
                case when message_imageuri is not null then concat(
                'https://carsales.pxcrush.net/carsales', 
                [message_imageuri]
                ) else message_imageuri end as image_url, 
                message_vin vin, 
                substring (message_lastupdatedutc, 1, 10), 
                message_rego as Registration, 
                message_make as MAKE, 
                message_model as MODEL, 
                message_year as YEAR, 
                message_odometer as kms, 
                substring (message_lastupdatedutc, 1, 10) as odometer_update_dt, 
                ROW_NUMBER() OVER (
                PARTITION BY message_id 
                ORDER BY 
                    nvl(
                    cast(message_odometer as integer), 
                    0
                    ) desc, 
                    message_lastupdatedutc asc
                ) RNUM 
            FROM 
                datalake_landing.membership_vehicleupdated 
            where 
                lower(message_type) = 'car' 
                and lower(message_service)= 'carsales'
            ) 
        where 
            rnum = 1
        ) as x 
        join (
        select 
            * 
        from 
            (
            SELECT 
                message_id as VEHICLE_ID, 
                substring (message_lastupdatedutc, 1, 10) as createdatUTC, 
                ROW_NUMBER() OVER (
                PARTITION BY message_id 
                ORDER BY 
                    message_lastupdatedutc asc
                ) RNUM 
            FROM 
                datalake_landing.membership_vehicleupdated 
            where 
                lower(message_type) = 'car' 
                and lower(message_service)= 'carsales'
            ) 
        where 
            rnum = 1
        ) as y on x.GARAGE_VEHICLE_ID = y.VEHICLE_ID 
        left join (
        select 
            distinct vehicle_id, 
            PREvious_KMS 
        from 
            (
            select 
                distinct message_id as vehicle_id, 
                message_odometer as previous_KMS, 
                rank() over (
                partition by message_id 
                order by 
                    message_odometer
                ) as rnum 
            from 
                datalake_landing.membership_vehicleupdated
            ) 
        where 
            rnum = 2
        ) as z2 on y.vehicle_id = z2.vehicle_id
    ) a, 
    cspot_landing.xx_csn_redbook_d b 
where 
    a.SPOT_ID = b.SPOT_ID 
    and not exists (
    select 
        message_id as VEHICLE_ID 
    FROM 
        datalake_landing.membership_vehicleupdated x 
    where 
        lower(message_type) = 'car' 
        and lower(message_service)= 'carsales' 
        and message_datedeleted is not null 
        and x.message_id = a.GARAGE_VEHICLE_ID
    ) 
    and not exists (
    select 
        1 
    from 
        cspot_membership_mygarage.garageitem y5 
    where 
        lower (y5.garageitemguid)= lower(a.garage_vehicle_id) 
        and isdeleted = 1
    ) 
    and not exists (
    select 
        1 
    from 
        my_garage_exc y6 
    where 
        lower(a.customer_id_)= lower(y6.memberid) 
        and not exists (
        select 
            1 
        from 
            (
            select 
                memberid, 
                specificationid, 
                count(*) 
            from 
                cspot_membership_mygarage.garageitem 
            where 
                isdeleted = 0 
            group by 
                memberid, 
                specificationid 
            having 
                count(*) > 1
            ) y7 
        where 
            lower(a.customer_id_)= lower(y7.memberid)
        )
    );
delete from  cspot_owner_acc.my_garage; 
insert into  cspot_owner_acc.my_garage select * from my_garage_temp;