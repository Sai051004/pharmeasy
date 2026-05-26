-- Priority_delivery_dashboard query

set hive.exec.orc.split.strategy=BI;
set hive.exec.reducers.max=97;
set hive.vectorized.execution.enabled=false;
set hive.vectorized.execution.reduce.enabled=false;
set hive.groupby.orderby.position.alias=true;
set hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DummyTxnManager;
set hive.support.concurrency=false;
set hive.merge.tezfiles=true;
set hive.merge.mapfiles=true;
set hive.merge.mapredfiles=true;
set hive.merge.size.per.task=128000000;
set hive.merge.smallfiles.avgsize=128000000;

DROP TABLE IF EXISTS adhoc_analysis.temp_ttl;

CREATE TABLE adhoc_analysis.temp_ttl AS
SELECT tt1.shipment_id,
       MIN(
            CASE
                WHEN ttl1.status IN ('UNFINISHED','POSTPONED','CANCELLED','COMPLETED')
                THEN from_utc_timestamp(ttl1.created_on ,'IST')
            END
       ) as last_mile_first_attempt_time,

       MIN(
            CASE
                WHEN ttl1.status IN ('COMPLETED')
                THEN from_utc_timestamp(ttl1.created_on ,'IST')
            END
       ) AS actual_delivered_time

FROM pe_logistics_bolt_production_trip_service.trip_tasks_snapshot_nrt tt1

LEFT JOIN (
            SELECT shipment_id,
                   task_id,
                   status,
                   cast(created_on as timestamp) as created_on
            FROM pe_logistics_bolt_production_trip_service.trip_task_logs_snapshot_nrt
            WHERE cast(created_on as timestamp) >= date_sub(current_date(),60)
              AND dt >= date_sub(current_date(),60)
          ) ttl1

ON tt1.task_id = ttl1.task_id

WHERE cast(tt1.created_on as timestamp) >= date_sub(current_date(),60)

GROUP BY tt1.shipment_id;



DROP TABLE IF EXISTS adhoc_analysis.temp_medicine;

CREATE TABLE adhoc_analysis.temp_medicine AS
SELECT mn.order_id,
       count(distinct mn.ucode) as total_ucodes,
       count(distinct case when mnql.availability = 1 then mn.ucode end) as green_ucodes
FROM (
        SELECT medicine_notes_id,
               availability
        FROM pe_pe2_pe2.medicine_notes_quantity_log_snapshot_nrt
        WHERE dt >= date_sub(current_date(),60)
     ) mnql

INNER JOIN (
        SELECT id,
               order_id,
               ucode,
               cast(create_time as timestamp ) create_time
        FROM pe_pe2_pe2.medicine_notes_snapshot_nrt
        WHERE dt >= date_sub(current_date(),60)
     ) mn

ON mnql.medicine_notes_id = mn.id

GROUP BY mn.order_id;


DROP TABLE IF EXISTS adhoc_analysis.temp_ifo;

CREATE TABLE adhoc_analysis.temp_ifo AS
SELECT *
FROM data_model.integrated_f_order
WHERE dt >= date_sub(current_date(),60)
  AND mercury_client IN ('MP-OTC','MP-MARG','PE','MP')
  AND flash_priority_delivery_flag = 1
  AND coalesce(order_category, 'NA') NOT IN ('B2B_CD','B2B2B_CD');


DROP TABLE IF EXISTS adhoc_analysis.temp_fsm;

CREATE TABLE adhoc_analysis.temp_fsm AS
SELECT *
FROM data_model.flash_shipment_model
WHERE dt >= date_sub(current_date(),60);




INSERT OVERWRITE table mstr_tables.priority_delivery
select
        a.*
        ,b.total_ucodes
        ,b.green_ucodes
        ,case when order_source='PRODUCT_OMS' then (case when c.all_green_flag=1 and a.jit_flag=1 then 1 else 0 end)
            else (case when total_ucodes = green_ucodes and a.jit_flag = 1 then 1 else 0 end) end as green_to_jit_flag
        ,date(order_placed_time) as dt
from (
    select
        ifo.order_id as order_id
        ,ifo.external_order_id as wh_order_id
        ,case when fsm.delivered_time is not null then ifo.order_id else null end as delivered_order_id
        ,ifo.mercury_client
        ,ifo.order_status
        ,nvl(ifo.thea_city_name,ifo.supplier_city_name) as supplier_city_name
        ,ifo.delivery_city_name
        ,ifo.flash_asset_name as asset_name
        ,ifo.delivery_pincode as destination_pincode
        ,ifo.speinter_name as sprinter_name
        ,ftm.trip_id
        ,ftm.task_id
        ,ifo.order_placed_date
        ,ifo.order_placed_at as order_placed_time
        ,hour(ifo.order_placed_at) as order_placed_hour
        ,ifo.min_cancelled_time as cancellation_time
        ,CASE 
            WHEN hour(ifo.committed_expected_delivery_date)  = 0 then nvl(from_unixtime(unix_timestamp(date(ifo.committed_expected_delivery_date))+86399),from_unixtime(unix_timestamp(date(ifo.original_order_edd))+86399))
            WHEN hour(ifo.committed_expected_delivery_date) <> 0 then nvl(ifo.committed_expected_delivery_date,from_unixtime(unix_timestamp(date(ifo.original_order_edd))+86399))
            END as end_edd_time
        ,fsm.delivered_time
        ,feo.min_task_created_time as picker_task_creation_time
        ,feo.min_task_assigned_time as picker_task_assigned_time
        ,CASE WHEN feo.min_store_invoice_generated_time >= ifo.max_rfd_time THEN feo.min_ready_for_billing_time ELSE feo.min_store_invoice_generated_time end as wh_billing_time
        ,feo.min_ready_for_billing_time
        ,feo.min_customer_invoice_generated_time as cx_billing_time
        ,fsm.mid_mile_out_time_forward as retailer_dispatch_time
        ,fsm.first_mile_out_time_forward as warehouse_dispatch_time
        ,fsm.first_ofd_time
        ,fsm.latest_ofd_time
        ,fsm.shipment_cancellation_time as flash_cancellation_time
        ,case when fsm.shipment_cancellation_time is not null and fsm.shipment_cancellation_time >= fsm.latest_ofd_time then 1 else 0 end as Cancelled_after_OFD_flag
        ,ttl.last_mile_first_attempt_time
        ,CASE 
            WHEN hour(ifo.committed_expected_delivery_date)  = 0 and nvl(from_unixtime(unix_timestamp(date(ifo.committed_expected_delivery_date))+86399),from_unixtime(unix_timestamp(date(ifo.original_order_edd))+86399)) >= COALESCE(ttl.actual_delivered_time,fsm.delivered_time) THEN 1
            WHEN hour(ifo.committed_expected_delivery_date) <> 0 and nvl(ifo.committed_expected_delivery_date,from_unixtime(unix_timestamp(date(ifo.original_order_edd))+86399)) >= COALESCE(ttl.actual_delivered_time,fsm.delivered_time) THEN 1 ELSE 0 
            END as edt_adherence_flag
        ,CASE 
            WHEN hour(ifo.committed_expected_delivery_date)  = 0 and nvl(from_unixtime(unix_timestamp(date(ifo.committed_expected_delivery_date))+86399),from_unixtime(unix_timestamp(date(ifo.original_order_edd))+86399)) < COALESCE(ttl.actual_delivered_time,fsm.delivered_time) THEN 1
            WHEN hour(ifo.committed_expected_delivery_date) <> 0 and nvl(ifo.committed_expected_delivery_date,from_unixtime(unix_timestamp(date(ifo.original_order_edd))+86399)) < COALESCE(ttl.actual_delivered_time,fsm.delivered_time) THEN 1 ELSE 0 
            END as breached_flag
        ,case when po.is_doctor_program=true then 1 else 0 end as is_doctor_program
        ,case when po.is_cc_skip=true then 1 else 0 end as is_cc_skip
        ,fsm.is_cust_logistics_delay as cx_delay_flag
        ,case when ifo.is_jit = 1 then 1 else 0 end as jit_flag
        ,ifo.sidelined_flag
        ,case when fsm.shipment_status in ('VERIFY') or fsm.rerouted_flag = 1 then 1 else 0 end as wrong_pincode_flag
        ,po.cc_tat
        ,((unix_timestamp(COALESCE(ttl.actual_delivered_time,fsm.delivered_time)) - unix_timestamp(ifo.order_placed_at))/60) tat_placed_delivered
        ,CASE WHEN datediff(to_date(COALESCE(ttl.actual_delivered_time,fsm.delivered_time)),to_date(ifo.order_placed_at))>=1
                    AND ((unix_timestamp(COALESCE(ttl.actual_delivered_time,fsm.delivered_time)) - unix_timestamp(ifo.order_placed_at))/60)>10*60
                    THEN ((unix_timestamp(COALESCE(ttl.actual_delivered_time,fsm.delivered_time)) - unix_timestamp(ifo.order_placed_at))/60)-10*60
              WHEN ((unix_timestamp(COALESCE(ttl.actual_delivered_time,fsm.delivered_time)) - unix_timestamp(ifo.order_placed_at))/60)>0
                  THEN ((unix_timestamp(COALESCE(ttl.actual_delivered_time,fsm.delivered_time)) - unix_timestamp(ifo.order_placed_at))/60) else NULL END AS tat_placed_delivered_working_hrs
        ,((unix_timestamp(feo.min_task_assigned_time) - unix_timestamp(feo.min_task_created_time))/60) as picker_wait_time
        ,((unix_timestamp(feo.min_task_assigned_time) - unix_timestamp(feo.min_task_created_time))/60) as tat_picker_prioritization
        ,((unix_timestamp(COALESCE(ttl.actual_delivered_time,fsm.delivered_time)) - unix_timestamp(feo.max_rfd_time))/60) as tat_rfd_delivered
        ,((unix_timestamp(COALESCE(ifo.max_rfd_time ,first_mile_out_time_forward)) - unix_timestamp(feo.min_task_created_time))/60) as tat_picker_task_rfd
        ,((unix_timestamp(COALESCE(feo.min_store_invoice_generated_time)) - unix_timestamp(feo.min_task_assigned_time))/60) as tat_picker_task_assigned_wh_billing
        ,feo.delivery_charge as delivery_charges_collected
        ,oc.priority_delivery_charge priority_fee_collected
        ,foc.loyalty_order as loyalty_program_order_flag
        ,feo.total_amount as gmv
        ,CASE WHEN foc.user_type_monthly IN ('New User','Placed_but_not_fulfilled') THEN 'New User' ELSE 'Old User' END as new_old_user_flag
        ,foc.chronic_flag
        ,CASE WHEN pdc.is_credited = 1 THEN CAST(priority_delivery_cashback AS int) ELSE 0 END AS priority_delivery_cashback
        ,CASE WHEN pdc.is_credited is null then 0 else pdc.is_credited end as cashback_credited_flag
        ,ifo.marketplace_flag
        ,ifo.max_rfd_time as rfd_time
        ,ifo.refrigeration_flag as refrigerated_flag
        ,ifo.rerouted_flag as rerouted_flag
        ,ifo.is_partially_delivered
        ,foc.user_type_monthly
        ,foc.tenant
        ,case when rt.is_retailer_franchise_fofo = 1 then 'Franchise Retailer'
              when rt.is_retailer_franchise_coco = 1 then 'Franchise Retailer'
              when rt.is_retailer_inclinincpharmacy = 1 then 'InClinic Pharmacy'  else 'Others' end as retailer_type
        ,ifo.delivery_type
        ,case when fm_base.fm_created_on is not null then 1 else 0 end as fm_pickup
        ,ifo.order_source
        ,fsm.lbn as lbn
        ,fsm.partner_name
        ,fsm.partner as logistic_partner
    FROM adhoc_analysis.temp_ifo ifo

    LEFT JOIN ( 
                select cast(order_id as varchar(100)) as order_id,
                        case when is_cc_skip = true then 1 else 0 end as is_cc_skip,
                        case when is_doctor_program = true then 1 else 0 end as is_doctor_program,
                        cc_tat
                from data_model.f_order
                where dt >= date_sub(current_date(),60) and is_priority_delivery = 1
                union all
                select co.child_order_id
                        ,is_draft_order as is_cc_skip
                        ,is_teleconsult_order as is_doctor_program
                        ,cc_tat
                from ( select icos.external_id as child_order_id
                            ,icos.link_id
                        from pe_oms_iron.child_order_snapshot_nrt icos
                        where dt >= date_sub(current_date(),60)
                    ) co
                left join (
                            select parent_order_id,
                                link_id,
                                delivery_state,
                                is_rx_required,
                                is_atc,
                                is_draft_order,
                                is_teleconsult_order
                            from data_model.f_child_aggregated
                            where dt >= date_sub(current_date(),60) and oms_flag = 'New_OMS'
                         ) fc on fc.link_id=co.link_id
                left join (
                            select child_order_id
                                ,cc_tat_overall as cc_tat
                            from data_model.draft
                            where dt >= date_sub(current_date(),60)
                        ) cc on cc.child_order_id=co.child_order_id
            ) po ON po.order_id = ifo.order_id

    LEFT JOIN (
                select cast(order_id as varchar(100)) as order_id,
                        priority_delivery_charge
                from pe_pe2_pe2.order_charges_snapshot_nrt
                where created_at >= date_sub(current_date(),60)
                union all
                select icos.external_id as order_id,pdc.priority_delivery_charge
                from pe_oms_iron.child_order_snapshot_nrt icos
                left join (
                            select child_order_id
                                  ,charges_applicablevalue as priority_delivery_charge
                            from pe_oms_iron.child_order_commitments_charges_snapshot_nrt_vw
                            where dt >= date_sub(current_date(),60) and charges_type = 'EXPRESS_CHARGE'
                          ) pdc on pdc.child_order_id=icos.id
                where dt >= date_sub(current_date(),60)
            ) oc ON oc.order_id = ifo.order_id

    LEFT JOIN (
                select cast(order_id as varchar(100)) as order_id, 
                        is_credited, 
                        priority_delivery_cashback
                from pe_pe2_pe2.order_priority_delivery_cashback_mapping_snapshot_nrt
                where dt >= date_sub(current_date(),60)
                union all
                select icos.external_id as child_order_id,
                        case when ws.child_order_id is not null then 1 else 0 end as is_credited
                        ,cast(pdc.gratificationdetail_amount as varchar(100)) as priority_delivery_cashback
                from pe_oms_iron.child_order_snapshot_nrt icos
                left join ( 
                            select child_order_id
                                    ,gratificationdetail_amount
                            from pe_oms_iron.child_order_commitments_gratificationDetail_snapshot_nrt_vw
                            where dt >= date_sub(current_date(),60) and gratificationdetail_type = 'PRIORITY_DELIVERY_BREACH'
                        ) pdc on pdc.child_order_id=icos.id
                left join (
                            select vendor_order_id as parent_order_id, split(reason_info,' ')[size(split(reason_info,' '))-1] as child_order_id
                            from pe_wallets_wallet_service.transaction_request_detail_snapshot_nrt
                            where dt >= date_sub(current_date(),60) 
                                    and reason = 3
                                    and is_credit =1 
                                    and reason_info like '%Priority Delivery Guarantee against order%'
                        ) ws on ws.child_order_id = icos.external_id
                where icos.dt >= date_sub(current_date(),60) 
            ) pdc ON pdc.order_id = ifo.order_id

    LEFT JOIN (
                select order_id,
                        loyalty_order,
                        user_type_monthly,
                        chronic_flag,
                        tenant
                from data_model.f_order_consumer where dt >= date_sub(current_date(),60)
            ) foc ON foc.order_id = ifo.parent_order_id

    left join (
                select shipment_id,
                        reference_id,
                        committed_expected_delivery_date,
                        delivered_time,
                        partner_name,
                        partner,
                        lbn,
                        is_cust_logistics_delay,
                        first_mile_out_time_forward,
                        mid_mile_out_time_forward ,
                        first_ofd_time,
                        latest_ofd_time,
                        shipment_cancellation_time,
                        shipment_status ,
                        rerouted_flag
                FROM adhoc_analysis.temp_fsm
                where dt >= date_sub(current_date(),60)
            ) fsm on fsm.reference_id = ifo.reference_id
            
    left join (
                select lbn,fm_aggregator_id, (created_on  + interval '330' minute) as fm_created_on
                from
                    (
                      select lbn,fm_aggregator_id,cast(created_on as timestamp) as created_on, ROW_NUMBER() over ( partition by  lbn order by created_on desc) as rn_number 
                      from pe_logistics_bolt_production_trip_service.fm_shipment_logs_journal_nrt_vw 
                      where dt >= current_date - interval '60' day
                    )base 
                where rn_number=1 
            ) fm_base on fsm.lbn=fm_base.lbn
        
    left join (
                select external_order_id ,
                        min_task_created_time,
                        min_task_assigned_time ,
                        min_ready_for_billing_time,
                        min_store_invoice_generated_time ,
                        min_customer_invoice_generated_time,
                        max_rfd_time,
                        delivery_charge,
                        total_amount
                from data_model.f_external_order feo
                where dt >= date_sub(current_date(),60)
                and coalesce(order_category, 'NA') not in ('B2B_CD','B2B2B_CD')
            ) feo on feo.external_order_id = ifo.external_order_id
        
    left JOIN (
                select shipment_id,
                        trip_id,
                        task_id
                from data_model.flash_task_model where latest_task_flag = 1 and dt >= date_sub(current_date(),60)
            ) ftm ON fsm.shipment_id = ftm.shipment_id
        
    LEFT JOIN adhoc_analysis.temp_ttl ttl
    ON ttl.shipment_id = fsm.shipment_id
        
    left join data_model.partner_firm_type rt on ifo.partner_id=rt.partner_id
        
    where ifo.dt >= date_sub(current_date(),60) 
            and mercury_client in ('MP-OTC','MP-MARG','PE','MP')
            and ifo.flash_priority_delivery_flag = 1
            and coalesce(ifo.order_category, 'NA') not in ('B2B_CD','B2B2B_CD')
    ) a
LEFT JOIN adhoc_analysis.temp_medicine b
ON b.order_id = a.order_id
left join (
            select icos.external_id as child_order_id
                    ,case when a.green_ucodes=a.total_ucodes then 1 else 0 end as all_green_flag
            from pe_oms_iron.child_order_snapshot_nrt icos
            left join (
                        select icois.child_order_id
                                ,count(distinct case when icois.attributes_fulfilability_productfulfilability = 'IN_STOCK' then icois.item_id end) as green_ucodes
                                ,count(distinct icois.item_id) as total_ucodes
                        from pe_oms_iron.child_order_item_snapshot_nrt icois
                        where icois.dt >= date_sub(current_date(),60)
                        group by icois.child_order_id
                    ) a on a.child_order_id = icos.id
        ) c on c.child_order_id = a.order_id;
