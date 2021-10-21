DELIMITER $$
CREATE DEFINER=`root`@`10.102.50.174` PROCEDURE `archive_incomplete_lapp_meta`(IN `lapp_stage` VARCHAR(100),IN `lapp_status` VARCHAR(100), IN `archive_date` DATE)
    NO SQL
BEGIN
	SET NAMES utf8mb4 COLLATE utf8mb4_unicode_ci;
	SET SESSION TRANSACTION ISOLATION LEVEL READ COMMITTED;
    start transaction;

	#Initial Setup
	select @lapp_stage:=lapp_stage;
	select @lapp_status:=lapp_status;

	select @archive_date:=DATE_SUB(curdate(), INTERVAL 15 Day);
	  
	 #Step 0 : Create temporary table which will have all the archive data
	DROP temporary TABLE IF EXISTS t_data;
	CREATE temporary TABLE t_data (
	object_id varchar(100) NOT NULL,lapp_date date,stamp datetime,lapp_stage varchar(50)
	) ENGINE=InnoDB;

	ALTER TABLE t_data
	ADD PRIMARY KEY object_id (object_id);

	#Step 1 : Push all the data into t_data which needs to be archived
	insert into t_data(object_id,lapp_date,stamp,lapp_stage)
	select all_lapps.lapp_id as object_id,lapp_date,dataset_timestamp,@lapp_stage
	FROM data_store.all_apps_dataset as all_lapps join loantap_in.lapp_meta on  all_lapps.lapp_id=lapp_meta.object_id and meta_key='lapp_stage' and meta_value=@lapp_stage 
	and lapp_date <=@archive_date  and current_stage=@lapp_stage  and current_status=@lapp_status and is_archived='no'  and date(last_activity_dt) <= @archive_date limit 25000;

	 DELETE t_data 
	 FROM t_data 
	 JOIN  data_store.lapp_archive
	 on t_data.object_id= lapp_archive.lapp_id;

	#IF @lapp_stage!='junk' THEN
	
	#Step 2: Dump all the archive application into lapp_archive table
	insert ignore into data_store.lapp_archive(lapp_id ,lapp_date,stamp,lapp_stage)
	select * from t_data;

	#select * from t_data;
	#select * from data_store.lapp_archive where lapp_meta_deleted=0;
    
	#Step 3: Creating temporary table for updating the archive table so that we can create json
    
	UPDATE data_store.lapp_archive AS update_table
	join
	(
	SELECT  CONCAT
	(
	'[',
	GROUP_CONCAT(
	JSON_OBJECT('coll_id',coll_id,'coll_type',coll_type,'meta_key',meta_key, 'meta_value', meta_value)
	),
	']'
	) as js,t_data.object_id from loantap_in.lapp_meta  
	join t_data on lapp_meta.object_id=t_data.object_id
	group by loantap_in.lapp_meta.object_id
	)
	as lapp_meta
	ON update_table.lapp_id=lapp_meta.object_id
	SET update_table.lapp_meta=lapp_meta.js, 
	update_table.is_lapp_meta=1; 
    
	#END IF;
	
	#Step 4: Once we create json and insert is_archived key in lapp_meta then delete from lapp_meta
	delete loantap_in.lapp_meta from loantap_in.lapp_meta join t_data on lapp_meta.object_id=t_data.object_id where meta_key not in ('lapp_id');
    delete loantap_in.lapp_meta from loantap_in.lapp_meta join t_data on lapp_meta.object_id=t_data.object_id where  coll_id not in ('lapp');
	
	#Step 5:Update lapp_archive with deleted as 1
	UPDATE data_store.lapp_archive la
	JOIN t_data
	ON la.lapp_id = t_data.object_id
	SET  la.lapp_meta_deleted = 1;

    #Step 6 insert into lapp_meta as lapp is archived
	insert into lapp_meta (object_id,coll_id,coll_type,meta_key,meta_value,updated_by)
	select t_data.object_id,'lapp','application_details','is_archived','yes','nishant@loantap.in' 
	from 
	data_store.lapp_archive 
	join 
	t_data on t_data.object_id=lapp_archive.lapp_id;
    
    UPDATE data_store.all_apps_dataset  as all_apps
	JOIN `t_data`
	ON all_apps.lapp_id= t_data.object_id 
	SET  all_apps.is_archived= 'yes';
    
    commit;
END$$
DELIMITER ;

DELIMITER $$
CREATE DEFINER=`root`@`localhost` FUNCTION `add_days_yyyymmdd`(`p_date` VARCHAR(8), `p_days` INT) RETURNS varchar(8) CHARSET utf8mb4 COLLATE utf8mb4_unicode_ci
    DETERMINISTIC
BEGIN 
	DECLARE reply varchar(8);
	set reply=date_format(date_add(date(p_date),interval p_days day),'%Y%m%d');
	RETURN reply;
END$$
DELIMITER ;

DELIMITER $$
CREATE DEFINER=`root`@`10.102.50.174` PROCEDURE `archive_lead_meta`(IN `archive_date` DATE)
    NO SQL
BEGIN
    SET NAMES utf8mb4 COLLATE utf8mb4_unicode_ci;
    SET SESSION TRANSACTION ISOLATION LEVEL READ COMMITTED;
    start transaction; 
       
    select @archive_date:='2020-07-01';
	#Step 0 : Create temporary table which will have all the archive data
	DROP temporary TABLE IF EXISTS t_data;
	CREATE temporary TABLE t_data (
	object_id varchar(100) NOT NULL,
        lead_date date,
        stamp datetime
	) ENGINE=InnoDB;

	ALTER TABLE t_data
	ADD PRIMARY KEY object_id (object_id);
	
    #Step 1 : Push all the data into t_data which needs to be archived
	insert into t_data(object_id,lead_date,stamp)
	select all_leads.lead_id as object_id,lead_date,stamp
	FROM data_store.lead_dataset as all_leads join loantap_in.lead_meta on  all_leads.lead_id=lead_meta.object_id and coll_id='lead' and meta_key='lead_id'  
	where all_leads .lead_date <=@archive_date and all_leads .is_archived='no' group by lead_meta.meta_value limit 2500;
	
	
    DELETE t_data 
    FROM t_data 
    JOIN  data_store.lead_archive
    on t_data.object_id= lead_archive.lead_id;

	#Step 2: Dump all the archive application into lead_archive table
	insert ignore into data_store.lead_archive(lead_id,lead_date,stamp)
	select * from t_data;

	#select * from t_data;
	#select * from data_store.lead_archive where lead_meta_deleted=0;
    
	#Step 3: Creating temporary table for updating the archive table so that we can create json
	UPDATE data_store.lead_archive AS update_table
	join
	(
	SELECT  CONCAT
	(
	'[',
	GROUP_CONCAT(
	JSON_OBJECT('coll_id',coll_id,'coll_type',coll_type,'meta_key',meta_key, 'meta_value', meta_value)
	),
	']'
	) as js,t_data.object_id from loantap_in.lead_meta  
	join t_data on lead_meta.object_id=t_data.object_id
	group by loantap_in.lead_meta.object_id
	)
	as lead_meta
	ON update_table.lead_id=lead_meta.object_id
	SET update_table.lead_meta=lead_meta.js, 
	update_table.is_lead_meta=1; 

	 #Step 4: Once we create json and insert is_archived key in lead_meta then delete from lead_meta
	delete loantap_in.lead_meta from loantap_in.lead_meta join t_data on lead_meta.object_id=t_data.object_id where meta_key not in ('lead_id');
        delete loantap_in.lead_meta from loantap_in.lead_meta join t_data on lead_meta.object_id=t_data.object_id where  coll_id not in ('lead');

	
	#Step 5:Update lead_archive with deleted as 1
	UPDATE data_store.lead_archive la
	JOIN t_data
	ON la.lead_id = t_data.object_id
	SET  la.lead_meta_deleted = 1;

    #Step 6 insert into lead_meta as lapp is archived
	insert into lead_meta (object_id,coll_id,coll_type,meta_key,meta_value,updated_by)
	select t_data.object_id,'lead','application_details','is_archived','yes','nishant@loantap.in' 
	from 
	data_store.lead_archive 
	join 
	t_data on t_data.object_id=lead_archive.lead_id;
      
	UPDATE data_store.lead_dataset as ld
	JOIN t_data
	ON ld.lead_id = t_data.object_id
	SET  ld.is_archived= 'yes';
  commit;
  
  END$$
DELIMITER ;

DELIMITER $$
CREATE DEFINER=`root`@`10.102.50.174` PROCEDURE `archive_rejected_lapp_meta`(IN `lapp_stage` VARCHAR(100), IN `archive_date` DATE, IN `rejected_stage` VARCHAR(100))
    NO SQL
BEGIN
    SET NAMES utf8mb4 COLLATE utf8mb4_unicode_ci;
	SET SESSION TRANSACTION ISOLATION LEVEL READ COMMITTED;
	start transaction;
	
    #Initial Setup
	select @lapp_stage:=lapp_stage;
	select @archive_date:=archive_date;
	select @rejected_stage:=rejected_stage;
      
     #Step 0 : Create temporary table which will have all the archive data
	DROP temporary TABLE IF EXISTS t_data;
	CREATE temporary TABLE t_data (
	object_id varchar(100) NOT NULL,lapp_date date,stamp datetime,lapp_stage varchar(50)
	) ENGINE=InnoDB;

	ALTER TABLE t_data
	ADD PRIMARY KEY object_id (object_id);
	
    #Step 1 : Push all the data into t_data which needs to be archived
	insert into t_data(object_id,lapp_date,stamp,lapp_stage)
	select DISTINCT all_lapps.lapp_id as object_id,lapp_date,dataset_timestamp,@lapp_stage
	FROM data_store.all_apps_dataset as all_lapps join loantap_in.lapp_meta on  all_lapps.lapp_id=lapp_meta.object_id and meta_key='lapp_stage' and meta_value=@lapp_stage 
	and lapp_date <=@archive_date  and current_stage=@lapp_stage and  rejected_stage=@rejected_stage and is_archived='no'  and date(last_activity_dt) <= @archive_date limit 2500;

    DELETE t_data 
    FROM t_data 
    JOIN  data_store.lapp_archive
    on t_data.object_id= lapp_archive.lapp_id;

	#Step 2: Dump all the archive application into lapp_archive table
	insert ignore into data_store.lapp_archive(lapp_id ,lapp_date,stamp,lapp_stage)
	select * from t_data;

	#select * from t_data;
	#select * from data_store.lapp_archive where lapp_meta_deleted=0;
    
	#Step 3: Creating temporary table for updating the archive table so that we can create json
	UPDATE data_store.lapp_archive AS update_table
	join
	(
	SELECT  CONCAT
	(
	'[',
	GROUP_CONCAT(
	JSON_OBJECT('coll_id',coll_id,'coll_type',coll_type,'meta_key',meta_key, 'meta_value', meta_value)
	),
	']'
	) as js,t_data.object_id from loantap_in.lapp_meta  
	join t_data on lapp_meta.object_id=t_data.object_id
	group by loantap_in.lapp_meta.object_id
	)
	as lapp_meta
	ON update_table.lapp_id=lapp_meta.object_id
	SET update_table.lapp_meta=lapp_meta.js, 
	update_table.is_lapp_meta=1; 

	 #Step 4: Once we create json and insert is_archived key in lapp_meta then delete from lapp_meta
	delete loantap_in.lapp_meta from loantap_in.lapp_meta join t_data on lapp_meta.object_id=t_data.object_id where meta_key not in ('lapp_id');
    delete loantap_in.lapp_meta from loantap_in.lapp_meta join t_data on lapp_meta.object_id=t_data.object_id where  coll_id not in ('lapp');

	
	#Step 5:Update lapp_archive with deleted as 1
	UPDATE data_store.lapp_archive la
	JOIN t_data
	ON la.lapp_id = t_data.object_id
	SET  la.lapp_meta_deleted = 1;

    #Step 6 insert into lapp_meta as lapp is archived
	insert into lapp_meta (object_id,coll_id,coll_type,meta_key,meta_value,updated_by)
	select t_data.object_id,'lapp','application_details','is_archived','yes','nishant@loantap.in' 
	from 
	data_store.lapp_archive 
	join 
	t_data on t_data.object_id=lapp_archive.lapp_id;

    commit;
END$$
DELIMITER ;

DELIMITER $$
CREATE DEFINER=`root`@`localhost` FUNCTION `select_squad`(`type` VARCHAR(50), `city` VARCHAR(50), `scheme` VARCHAR(50)) RETURNS varchar(50) CHARSET utf8mb4 COLLATE utf8mb4_unicode_ci
    DETERMINISTIC
BEGIN 

        DECLARE final_value varchar(50);
        DECLARE squad varchar(50);
				set final_value='';
        # when type is topup, set final_value as topup
	if type='appraisal-topup' then
		set final_value =  'topup';
	end if;
	
	if type='topup-uncalled' then
		set final_value =  'topup';
	end if;

       	# when final_value is empty, check against scheme
	if final_value ='' then
		select meta_value into squad from loan_scheme where object_id=scheme and meta_key='squad';
                 if squad<>'digital' then
			set final_value = squad;
		end if;
	end if;

	# when @final_value is empty, check against scheme
	if final_value ='' then
		with
		q0 as (select term_id from `wp_terms` where slug=city),
		q1 as (select q0.* from q0 join wp_term_taxonomy using (term_id) where taxonomy='loan_city' ),
		q2 as (select meta_value as squad_name from q1 join wp_termmeta on q1.term_id = wp_termmeta.term_id where meta_key='squad')
		select squad_name into final_value from q2;
	end if;
       
      # return the final value
      RETURN  final_value;

END$$
DELIMITER ;

DELIMITER $$
CREATE DEFINER=`root`@`localhost` FUNCTION `get_date_yyyymmdd`(`p_days` INT) RETURNS varchar(8) CHARSET utf8mb4 COLLATE utf8mb4_unicode_ci
    DETERMINISTIC
BEGIN 
	DECLARE reply varchar(8);
	set reply=date_format(date_sub(curdate(),interval p_days day),'%Y%m%d');
	RETURN reply;
END$$
DELIMITER ;

DELIMITER $$
CREATE DEFINER=`root`@`10.102.50.174` PROCEDURE `delete_junk_app`(IN `lapp_stage` VARCHAR(100), IN `archive_date` DATE)
    NO SQL
BEGIN
   SET NAMES utf8mb4 COLLATE utf8mb4_unicode_ci;
SET SESSION TRANSACTION ISOLATION LEVEL READ COMMITTED;
start transaction;

#Initial Setup
select @lapp_stage:='junk';
select @archive_date:=DATE_SUB(curdate(), INTERVAL 5 Day);
  
 #Step 0 : Create temporary table which will have all the archive data
DROP temporary TABLE IF EXISTS t_data;
CREATE temporary TABLE t_data (
object_id varchar(100) NOT NULL,lapp_date date,stamp datetime,lapp_stage varchar(50)
) ENGINE=InnoDB;

ALTER TABLE t_data
ADD PRIMARY KEY object_id (object_id);

#Step 1 : Push all the data into t_data which needs to be archived
insert into t_data(object_id,lapp_date,stamp,lapp_stage)
select all_lapps.lapp_id as object_id,lapp_date,dataset_timestamp,@lapp_stage
FROM data_store.all_apps_dataset as all_lapps join loantap_in.lapp_meta on  all_lapps.lapp_id=lapp_meta.object_id and meta_key='lapp_stage' and meta_value=@lapp_stage 
and lapp_date <=@archive_date  and current_stage=@lapp_stage and is_archived='no'  and date(last_activity_dt) <= @archive_date limit 2500;

DELETE t_data 
FROM t_data 
JOIN  data_store.lapp_archive
on t_data.object_id= lapp_archive.lapp_id;	

#Step 2: Dump all the archive application into lapp_archive table
insert ignore into data_store.lapp_archive(lapp_id ,lapp_date,stamp,lapp_stage)
select * from t_data;

#Step 4: Once we create json and insert is_archived key in lapp_meta then delete from lapp_meta
delete loantap_in.lapp_meta from loantap_in.lapp_meta join t_data on lapp_meta.object_id=t_data.object_id ;

	 delete data_store.all_apps_dataset from data_store.all_apps_dataset 
JOIN t_data
ON all_apps_dataset.lapp_id= t_data.object_id;

commit;
END$$
DELIMITER ;

DELIMITER $$
CREATE DEFINER=`root`@`localhost` PROCEDURE `filldates`(IN `dateStart` DATE, IN `dateEnd` DATE)
BEGIN
      WHILE dateStart <= dateEnd DO
        INSERT INTO obs_dates (obs_end,obs_month,obs_start) VALUES (LAST_DAY(dateStart),EXTRACT(YEAR_MONTH FROM dateStart),date_add(dateStart,interval -DAY(dateStart)+1 DAY));
        SET dateStart = date_add(dateStart, INTERVAL 1 MONTH);
      END WHILE;
    END$$
DELIMITER ;

DELIMITER $$
CREATE DEFINER=`root`@`10.102.50.174` PROCEDURE `sp_loan_entries`(IN `sp_sublan_id` VARCHAR(100))
    NO SQL
select entry_date,txn_set,txn_set_id,entry_set,entry_set_id,debit,credit,account,subgroup,head,reason,due_date,txn_ref,success_ref from loan_entries where sublan_id=sp_sublan_id$$
DELIMITER ;

DELIMITER $$
CREATE DEFINER=`root`@`10.102.50.174` PROCEDURE `sp_clear_marketing_db`()
    NO SQL
BEGIN
SET SESSION TRANSACTION ISOLATION LEVEL READ COMMITTED;
#DELIMITER //
#get table name
SELECT @marketing_tbl_name:=table_name FROM delete_marketing_tables WHERE expiry_date < now() LIMIT 1;

#drop table
SET @sql := CONCAT('DROP TABLE IF EXISTS ', @marketing_tbl_name);
PREPARE stmt FROM @sql;
EXECUTE stmt;

#delete from delete_marketing_tables
DELETE FROM delete_marketing_tables WHERE table_name=@marketing_tbl_name;
#//
END$$
DELIMITER ;

DELIMITER $$
CREATE DEFINER=`root`@`10.102.50.174` PROCEDURE `sp_loan_dataset_v4`(IN `sp_sublan_id` VARCHAR(64), IN `singleQuery` INT(0))
    COMMENT 'This sp will except two paramets sp_sublan_id and singleQuery'
BEGIN
SET SESSION TRANSACTION ISOLATION LEVEL READ COMMITTED;

SET NAMES utf8mb4 COLLATE utf8mb4_unicode_ci;
	
	Drop temporary table if exists t_loan_monthly_obs;
	CREATE temporary TABLE t_loan_monthly_obs like data_store.loan_monthly_obs;
	Drop temporary table if exists entries;
	CREATE temporary TABLE entries like loantap_in.loan_entries;

	Drop temporary table if exists t_loan_daily_obs;
	/*Create skeleton for daily obs*/
	CREATE TEMPORARY TABLE t_loan_daily_obs (
		row_num bigint(20) NOT NULL,
		ID bigint(20) unsigned NOT NULL AUTO_INCREMENT,
		obs_month char(6) NULL,
		obs_date date NOT NULL,
		added_primary_dues decimal(12,2)  DEFAULT 0,
		added_instalment_interest decimal(12,2)  DEFAULT 0,
		added_instalment_principal decimal(12,2)  DEFAULT 0,
		added_instalment decimal(12,2) DEFAULT 0,
		added_penalty_dues decimal(12,2) DEFAULT 0,
		added_penalty_waiver decimal(12,2)  DEFAULT 0,
		added_eom_adjustment decimal(12,2)  DEFAULT 0,
		added_all_dues decimal(12,2) GENERATED ALWAYS AS (added_primary_dues + added_instalment + added_penalty_dues + added_eom_adjustment) STORED,
		closing_dues_account decimal(12,2)  DEFAULT 0,
		unadjusted_penalty_dues decimal(12,2)  DEFAULT 0, 
		opening_primary_dues decimal(12,2)  DEFAULT 0,
		opening_instalment_interest decimal(12,2)  DEFAULT 0,
		opening_instalment_principal decimal(12,2)  DEFAULT 0,
		opening_instalment decimal(12,2) GENERATED ALWAYS AS (opening_instalment_interest + opening_instalment_principal) STORED,
		opening_penalty_dues decimal(12,2) DEFAULT 0,
		opening_eom_adjustment decimal(12,2) DEFAULT 0,
		opening_all_dues decimal(12,2) GENERATED ALWAYS AS (opening_primary_dues + opening_instalment + opening_penalty_dues + opening_eom_adjustment) STORED,
		knocked_off	decimal(12,2) GENERATED ALWAYS AS (opening_all_dues + added_all_dues + added_eom_adjustment - closing_dues_account) STORED,
		closing_penalty_dues decimal(12,2) DEFAULT 0,
		closing_instalment_principal decimal(12,2)  DEFAULT 0,
		closing_instalment_interest decimal(12,2)  DEFAULT 0,
		closing_primary_dues decimal(12,2)  DEFAULT 0,
		closing_eom_adjustment decimal(12,2)  DEFAULT 0,
		closing_instalment decimal(12,2) DEFAULT 0,
		closing_all_dues decimal(12,2) GENERATED ALWAYS AS (closing_primary_dues + closing_instalment + closing_penalty_dues + closing_eom_adjustment) STORED,
		moratorium_month tinyint(1) Default 0,
		eom_moratorium tinyint(1) Default 0,
		is_dpd_day tinyint(1) Default 1,
		dpd_days int(5) Default 0,
		dpd_start date NULL,
		next_dpd int(5) Default NULL,
	    next_closing_instalment_principal decimal(12,2)  DEFAULT NULL,
	    next_closing_instalment_interest decimal(12,2)  DEFAULT NULL,
	    next_closing_instalment_instalment decimal(12,2)  DEFAULT NULL,
        next_closing_instalment decimal(12,2)  DEFAULT NULL,
   	    closing_principal decimal(12,2)  DEFAULT 0,	
	    next_closing_principal decimal(12,2)  DEFAULT NULL,	  		
		PRIMARY KEY id (id),
		KEY row_num (row_num),
		KEY obs_date (obs_date)
	) ENGINE=InnoDB AUTO_INCREMENT=32 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

	Drop temporary table if exists t_adjustments;
	CREATE TEMPORARY TABLE t_adjustments (
		ID bigint(20) unsigned NOT NULL AUTO_INCREMENT,
		obs_date date NOT NULL,
		adj_type varchar(255) NOT NULL,	
		amount decimal(12,2)  DEFAULT 0,
		cum_amount decimal(12,2)  DEFAULT 0,
		PRIMARY KEY id (id),
		KEY obs_date (obs_date)
	) ENGINE=InnoDB AUTO_INCREMENT=32 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;


	#dpd example SUB16092981338054039851345
	#moratorium example SUB16251785799280472479762
	#select @object_id:='SUB16092981338054039851345';
	select @object_id:=sp_sublan_id;

	#select * from data_store.loan_dataset where sublan_id=@object_id;
	#select * from data_store.loan_monthly_obs where sublan_id=@object_id;

	insert into entries select * from loantap_in.loan_entries where sublan_id=@object_id;

	select @max_date:=ifnull(max(entry_date),curdate()) from entries where account='closed';

	insert into t_loan_monthly_obs(sublan_id,lan_id,obs_start,obs_end,obs_month)	
	with 
		q0 as (
		#first loan entry date
		select lan_id,sublan_id,min(entry_date) as min_date from entries
		),
		q1 as (
		select q0.*,obs_dates.* from loantap_in.obs_dates join q0 
		on obs_month >=EXTRACT(YEAR_MONTH FROM min_date) and  obs_month <=EXTRACT(YEAR_MONTH FROM @max_date)
		)
	select sublan_id,lan_id,obs_start,obs_end,obs_month from q1; 

	#update the last month to obs_end=@max_date
	UPDATE t_loan_monthly_obs set obs_end=@max_date where obs_month=EXTRACT(YEAR_MONTH FROM @max_date);


	#update sublan id
	#UPDATE t_loan_monthly_obs as update_table
	#SET update_table.sublan_id=(select sublan_id from entries limit 1);


	################new and old section#######################################
	# Setup sublan
	Drop temporary table if exists t_sublan_entries;
	CREATE temporary TABLE t_sublan_entries like loantap_in.sublan;

	#lapp_meta 
	Drop temporary table if exists t_lapp_meta;
	CREATE temporary TABLE t_lapp_meta like loantap_in.lapp_meta;

	insert into t_sublan_entries 
	select * from loantap_in.sublan where object_id=@object_id;

	Drop temporary table if exists t_sublan_collection_entries;
	CREATE temporary TABLE t_sublan_collection_entries like loantap_in.sublan_collection;

	insert into t_sublan_collection_entries 
	select * from loantap_in.sublan_collection where reference_id=@object_id;      


	#product_or_scheme
	select @product_or_scheme:=IF(count(1)>0, 'product', 'scheme') from t_sublan_entries where coll_id='sublan' and coll_type='core' and meta_key='product_id';

	UPDATE t_loan_monthly_obs as update_table
	SET update_table.product_or_scheme=@product_or_scheme;

	#current meta
	UPDATE t_loan_monthly_obs as update_table
	join 
	(
		with q1 as (
		SELECT obs_month,max(entries.ID) as ID FROM t_loan_monthly_obs 
		JOIN entries ON entry_date<=obs_end AND account='Current Meta' 
		GROUP BY obs_end
		)
	select obs_month,current_meta from entries  join q1  on entries.ID=q1.ID 
	) as data
	on update_table.obs_month=data.obs_month
	set update_table.current_meta=data.current_meta;

	#delimiter //
	IF @product_or_scheme='product' 
		THEN
		#product JSON (@product_json)
		select @product_json:=meta_value from sublan where object_id=@object_id and coll_id='sublan' and coll_type='loan_details' and meta_key='product_json';

		UPDATE t_loan_monthly_obs as update_table
		SET update_table.loan_id=sublan_id;

		#product_id -> loan_product
		UPDATE t_loan_monthly_obs as update_table
		join
		(
			select meta_value as loan_product from t_sublan_entries where coll_id='sublan' and coll_type='core' and meta_key='product_id'
		) as data
		SET update_table.loan_product=data.loan_product;

		#loan_product_label
		UPDATE t_loan_monthly_obs as update_table
		SET update_table.loan_product_label=json_value(@product_json, '$.base.label');

		#bureau_account_type
		UPDATE t_loan_monthly_obs as update_table
		SET update_table.bureau_account_type=json_value(@product_json, '$.bureau.account_type');

		#sublan_instalment_method  from product
		UPDATE t_loan_monthly_obs as update_table
		SET update_table.sublan_instalment_method=json_value(@product_json, '$.instalment.instalment_method');

		#disbursal_beneficiary
		UPDATE t_loan_monthly_obs as update_table
		SET update_table.disbursal_beneficiary=json_value(@product_json, '$.disbursal.beneficiary');


		#lapp_id from sublan
		UPDATE t_loan_monthly_obs as update_table
		join 
		(
			select meta_value as lapp_id from t_sublan_entries where coll_id='sublan' and coll_type='core' and meta_key='lapp_id'
		) as data
		SET update_table.lapp_id=data.lapp_id;

		#insert data into t_lapp_meta
		insert into t_lapp_meta (updated_by,object_id,coll_id,coll_type,meta_key,meta_value)
		with 
		q0 as (select lapp_id  from t_loan_monthly_obs group by lapp_id),
		q1 as (select q0.*,updated_by,object_id,coll_id,coll_type,meta_key,meta_value from q0 join lapp_meta on lapp_meta.object_id=q0.lapp_id )
		select updated_by,object_id,coll_id,coll_type,meta_key,meta_value from q1;

		#customer_id from sublan
		#get the parent lan from sublan table and take customer id
		UPDATE t_loan_monthly_obs as update_table
		join
		(
			with q0 as (select meta_value as lan_id from t_sublan_entries where coll_id='sublan' and meta_key="lan_id"),
			q1 as (select q0.*,meta_value as customer_id from q0 join sublan on sublan.object_id=q0.lan_id and  coll_id='lan' and meta_key='customer_id')        
			select * from q1
		) as data
		SET update_table.customer_id=data.customer_id;

		#nbfc
		UPDATE t_loan_monthly_obs as update_table
		join 
		(
			select meta_value as nbfc from t_sublan_entries where coll_id='sublan' and coll_type='core' and meta_key='nbfc'
		) as data
		SET update_table.nbfc=data.nbfc;

		#sublan_loan_tenure
		UPDATE t_loan_monthly_obs as update_table
		join 
		(
			select meta_value as sublan_loan_tenure from t_sublan_entries where coll_id='sublan' and coll_type='loan_details' and meta_key='loan_tenure'
		) as data
		SET update_table.sublan_loan_tenure=data.sublan_loan_tenure;

		#sublan_loan_interest_rate
		UPDATE t_loan_monthly_obs as update_table
		join 
		(
			select meta_value as sublan_loan_interest_rate from t_sublan_entries where coll_id='sublan' and coll_type='loan_details' and meta_key='interest_rate'
		) as data
		SET update_table.sublan_loan_interest_rate=data.sublan_loan_interest_rate;

		#sublan_loan_amount
		UPDATE t_loan_monthly_obs as update_table
		join 
		(
			select meta_value as sanction_amount from t_sublan_entries where coll_id='sublan' and coll_type='loan_details' and meta_key='sanction_amount'
		) as data
		SET update_table.sublan_loan_amount=data.sanction_amount;

		#sublan_setup_date
		UPDATE t_loan_monthly_obs as update_table
		join 
		(
			select meta_value as sublan_setup_date from t_sublan_entries where coll_id='sublan' and coll_type='loan_details' and meta_key='setup_date'
		) as data
		SET update_table.sublan_setup_date=data.sublan_setup_date;

		#sublan_advance_instalments
		UPDATE t_loan_monthly_obs as update_table
		join 
		(
			select  if(meta_value IS NULL or meta_value = '', 0, meta_value) as sublan_advance_instalments from t_sublan_entries where coll_id='sublan' and coll_type='loan_details' and meta_key='advance_instalments'
		) as data
		SET update_table.sublan_advance_instalments=data.sublan_advance_instalments;

		#sublan_virtual_account
		UPDATE t_loan_monthly_obs as update_table
		join 
		(
			select meta_value as sublan_virtual_account from t_sublan_entries where coll_id='sublan' and coll_type='core' and meta_key='virtual_account'
		) as data
		SET update_table.sublan_virtual_account=data.sublan_virtual_account;

		#sublan_loan_end_date
		UPDATE t_loan_monthly_obs as update_table
		join 
		(
			select meta_value as sublan_loan_end_date from t_sublan_entries where coll_id='sublan' and coll_type='loan_details' and meta_key='loan_end_date'
		) as data
		SET update_table.sublan_loan_end_date=data.sublan_loan_end_date;

		#sublan_end_date

		#loan_end_use
		UPDATE t_loan_monthly_obs as update_table
		SET update_table.loan_end_use=json_value(@product_json, '$.base.end_use');

		#sublan_dealer_code
		UPDATE t_loan_monthly_obs as update_table
		SET update_table.sublan_dealer_code=json_value(@product_json, '$.dealer.type');

		#loan_end_date
		UPDATE t_loan_monthly_obs as update_table
		join 
		(
		with q1 as (
			SELECT max(ID) as id,due_date FROM t_loan_monthly_obs JOIN entries on entry_date < obs_end where account ='Loan End Date'  group by obs_end
		)
		select q1.due_date as loan_end_date from q1 join entries on  entries.ID=q1.id
		)as data
		SET update_table.loan_end_date=data.loan_end_date;

		#loan_end_date
		UPDATE t_loan_monthly_obs as update_table
		SET update_table.loan_end_date=IF(json_value(current_meta, '$.end_date'),json_value(current_meta, '$.end_date'),DATE_ADD(update_table.sublan_setup_date, INTERVAL update_table.sublan_loan_tenure MONTH));

	ELSE
		UPDATE t_loan_monthly_obs as update_table
		SET update_table.loan_id=lan_id;

		#scheme id -> loan_product
		#bureau_account_type from scheme
		#sublan_instalment_method from scheme (doubt)
		UPDATE t_loan_monthly_obs as update_table
		join
		(
			with 
			q0 as (select meta_value as loan_product from t_sublan_entries where coll_id='sublan' and coll_type='sublan' and meta_key='scheme_id'),
			q1 as (select q0.*,meta_value as bureau_account_type from q0 left join loan_scheme on q0.loan_product=loan_scheme.object_id and coll_id='scheme' and meta_key='bureau_account_type'),		
			q2 as (select q1.*,meta_value as sublan_instalment_method from q1 left join loan_scheme on q1.loan_product=loan_scheme.object_id  and coll_id='instalment_method' and coll_type='scheme' and meta_key='default'),
			q3 as (select q2.*,meta_value as disbursal_beneficiary from q2 left join loan_scheme on q2.loan_product=loan_scheme.object_id  and coll_id='disbursal_beneficiary' and coll_type='scheme' and meta_key='default'),	
			q4 as (select q3.*,meta_value as product_category from q3 left join loan_scheme on q3.loan_product=loan_scheme.object_id and coll_id='scheme' and meta_key='product_category'),
			q5 as (select q4.*,meta_value as loan_product_label from q4 left join loan_scheme on q4.loan_product=loan_scheme.object_id and coll_id='scheme' and meta_key='scheme_label')
			select * from q5
		) as data
		SET update_table.loan_product=data.loan_product,
		update_table.loan_product_label=data.loan_product_label,
		update_table.bureau_account_type=data.bureau_account_type,
		update_table.sublan_instalment_method=data.sublan_instalment_method,
		update_table.product_category =data.product_category,
		update_table.disbursal_beneficiary =data.disbursal_beneficiary;



		#lapp_id from sublan
		UPDATE t_loan_monthly_obs as update_table
		join 
		(
			select meta_value as lapp_id from t_sublan_entries where coll_id='sublan' and coll_type='sublan' and meta_key='lapp_id'
		) as data
		SET update_table.lapp_id=data.lapp_id;

		insert into t_lapp_meta (updated_by,object_id,coll_id,coll_type,meta_key,meta_value)
		with 
		q0 as (select lapp_id  from t_loan_monthly_obs group by lapp_id),
		q1 as (select q0.*,updated_by,object_id,coll_id,coll_type,meta_key,meta_value from q0 join lapp_meta on lapp_meta.object_id=q0.lapp_id )
		select updated_by,object_id,coll_id,coll_type,meta_key,meta_value from q1;

		#customer_id from lapp
		UPDATE t_loan_monthly_obs as update_table
		join 
		(
			select lapp_id,meta_value as customer_id from t_loan_monthly_obs join t_lapp_meta on t_loan_monthly_obs.lapp_id=t_lapp_meta.object_id and coll_id='lapp' and meta_key='customer_id'
		) as data
		SET update_table.customer_id=data.customer_id;

		#nbfc
		UPDATE t_loan_monthly_obs as update_table
		join 
		(
			select meta_value as nbfc from t_sublan_entries where coll_id='sublan' and coll_type='sublan' and meta_key='nbfc'
		) as data
		SET update_table.nbfc=data.nbfc;

		#sublan_loan_tenure
		UPDATE t_loan_monthly_obs as update_table
		join 
		(
			select meta_value as sublan_loan_tenure from t_sublan_entries where coll_id='sublan' and coll_type='sublan' and meta_key='tenure'
		) as data
		SET update_table.sublan_loan_tenure=data.sublan_loan_tenure;

		#sublan_loan_interest_rate
		UPDATE t_loan_monthly_obs as update_table
		join 
		(
			select meta_value as sublan_loan_interest_rate from t_sublan_entries where coll_id='sublan' and coll_type='sublan' and meta_key='interest_rate'
		) as data
		SET update_table.sublan_loan_interest_rate=data.sublan_loan_interest_rate;


		#sublan_loan_amount from sublan
		UPDATE t_loan_monthly_obs as update_table
		join 
		(
			select meta_value as sanction_amount from t_sublan_entries where coll_id='sublan' and coll_type='sublan' and meta_key='sanction_amount'
		) as data
		SET update_table.sublan_loan_amount=data.sanction_amount;


		#sublan_setup_date
		UPDATE t_loan_monthly_obs as update_table
		join 
		(
			select meta_value as sublan_setup_date from t_sublan_entries where coll_id='sublan' and coll_type='sublan' and meta_key='setup_date'
		) as data
		SET update_table.sublan_setup_date=data.sublan_setup_date;

		#sublan_advance_instalments
		UPDATE t_loan_monthly_obs as update_table
		join 
		(
			select  if(meta_value IS NULL or meta_value = '', 0, meta_value) as sublan_advance_instalments from t_sublan_entries where coll_id='sublan' and coll_type='sublan' and meta_key='advance_instalments'
		) as data
		SET update_table.sublan_advance_instalments=data.sublan_advance_instalments;

		#sublan_virtual_account
		UPDATE t_loan_monthly_obs as update_table
		join 
		(
			select meta_value as sublan_virtual_account from t_sublan_entries where coll_id='sublan' and coll_type='sublan' and meta_key='virtual_account'
		) as data
		SET update_table.sublan_virtual_account=data.sublan_virtual_account;

		#sublan_end_date (DOUBT)

		#sublan_loan_end_date 
		UPDATE t_loan_monthly_obs as update_table
		join
		(
			select object_id as sublan_id,DATE_FORMAT(STR_TO_DATE(meta_value, '%d %M,%Y'), '%Y%m%d') as sublan_loan_end_date  from t_sublan_entries where coll_id='txn' and coll_type='txn' and meta_key='emi_end_date'
		) as data
		SET update_table.sublan_loan_end_date= DATE_FORMAT(IF(data.sublan_loan_end_date,data.sublan_loan_end_date,DATE_ADD(update_table.sublan_setup_date, INTERVAL update_table.sublan_loan_tenure MONTH)),'%Y%m%d');

		#sublan_loan_end_date_label
		UPDATE t_loan_monthly_obs as update_table
		SET update_table.sublan_loan_end_date_label=DATE_FORMAT(update_table.sublan_loan_end_date,'%d %M, %Y');



		#loan_end_use  lapp.decision_end_use
		UPDATE t_loan_monthly_obs as update_table
		join
		(
			select lapp_id,meta_value as decision_end_use from t_loan_monthly_obs join t_lapp_meta on t_loan_monthly_obs.lapp_id=t_lapp_meta.object_id and coll_id='lapp' and meta_key='decision_end_use'
		) as data
		SET update_table.loan_end_use=data.decision_end_use;


		#sublan_dealer_code not in new???????
		UPDATE t_loan_monthly_obs as update_table
		join 
		(
		select meta_value as sublan_dealer_code from t_sublan_entries where coll_id='sublan' and coll_type='dealer' and meta_key='dealer_code'
		) as data
		SET update_table.sublan_dealer_code=data.sublan_dealer_code;


	END IF;#//
	#delimiter ;

	##########################################################################

	#tenure
	UPDATE t_loan_monthly_obs as update_table
	SET 
	update_table.loan_tenure=json_value(current_meta, '$.tenure'),
	update_table.interest_rate=json_value(current_meta, '$.interest_rate');

	#loan_status
	UPDATE t_loan_monthly_obs as update_table
	join 
	(
		with q1 as (
		SELECT max(ID) as id,obs_month FROM t_loan_monthly_obs JOIN entries ON entry_date<=obs_end 
		where head ='Loan Status' group by obs_end
	)
	select obs_month ,account as loan_status from entries join q1 on entries.id=q1.ID 
	) as data
	on update_table.obs_month=data.obs_month
	SET update_table.loan_status=data.loan_status;

	#final_loan_status
	UPDATE t_loan_monthly_obs as update_table
	join 
	(
		with q1 as (
		SELECT max(ID) as id,obs_month FROM t_loan_monthly_obs JOIN entries where head ='Loan Status'  group by obs_end
		)
	select account as loan_status,obs_month from q1  join entries on  entries.ID=q1.id
	)as data
	SET update_table.final_loan_status=data.loan_status;

	#loan_city_label 
	UPDATE t_loan_monthly_obs as update_table
	join
	(
		select t_lapp_meta.meta_value as loan_city_label from t_loan_monthly_obs join t_lapp_meta on t_lapp_meta.object_id=t_loan_monthly_obs.lapp_id and coll_id='lapp' and meta_key='loan_city_label'
	) as data
	SET update_table.loan_city_label=data.loan_city_label;

	#loan_closed_date
	UPDATE t_loan_monthly_obs as update_table
	join 
	(
		select obs_month,entry_date as loan_closed_date from t_loan_monthly_obs join entries 
		on entry_date<=obs_end and account ='closed' group by obs_month
	) as data
	on update_table.obs_month=data.obs_month
	SET update_table.loan_closed_date=data.loan_closed_date;

	#loan_amount
	UPDATE t_loan_monthly_obs as update_table
	join 
	(
		select obs_month,ifnull(sum(debit-credit),0) as loan_amount from t_loan_monthly_obs join entries 
		on entry_date<=obs_end and account ='Loan Sanction' group by obs_month
	) as data
	on update_table.obs_month=data.obs_month
	SET update_table.loan_amount=data.loan_amount;

	#sanction_date
	UPDATE t_loan_monthly_obs as update_table
	join 
	(
		select obs_month,DATE_FORMAT(min(entry_date), '%Y%m%d') as sanction_date from t_loan_monthly_obs join entries 
		on entry_date<=obs_end and account ='Loan Sanction' group by obs_month
	) as data
	on update_table.obs_month=data.obs_month
	SET update_table.sanction_date=data.sanction_date;


	#loan_advance_instalments
	UPDATE t_loan_monthly_obs as update_table
	join 
	(
		select  if(meta_value IS NULL or meta_value = '', 0, meta_value) as loan_advance_instalments from t_sublan_entries where coll_id='sublan' and  meta_key='advance_instalments'
	) as data
	SET update_table.loan_advance_instalments=data.loan_advance_instalments;

	#instalment details
	#next_instalment_date,next_instalment_due_date,next_instalment_amount
	
	UPDATE t_loan_monthly_obs as update_table
	SET
	update_table.next_instalment_date=json_value(current_meta, '$.next_instalment_date'),
	update_table.next_instalment_due_date=json_value(current_meta, '$.next_instalment_due_date'),
	update_table.next_instalment_amount=json_value(current_meta, '$.next_instalment_amount'),
	update_table.instalments_left=json_value(current_meta, '$.instalments_left'),
	update_table.instalments_total=json_value(current_meta, '$.instalments_total'),
	update_table.instalment_end_date=json_value(current_meta, '$.end_date');

	#loan_line_utilized -> disbursal_amount
	UPDATE t_loan_monthly_obs as update_table
	join
	(
		select obs_month,ifnull(sum(credit - debit),0) as loan_line_utilized from t_loan_monthly_obs join entries 
		on entry_date>=obs_start and entry_date<=obs_end and account ='Loan Disbursed' group by obs_month
	) as data
	on update_table.obs_month=data.obs_month
	SET update_table.loan_line_utilized=data.loan_line_utilized;

	#pending_disbursal
	UPDATE t_loan_monthly_obs as update_table
	join 
	(
		select obs_month, ifnull(sum(credit-debit),0) as pending_disbursal from t_loan_monthly_obs join entries 
		on entry_date<=obs_end and account ='Loan Account Pending Disbursement' and head='Loan Account' group by obs_month
	) as data
	on update_table.obs_month=data.obs_month
	SET update_table.pending_disbursal=data.pending_disbursal;

	#first_loan_line_utilization_date -> disbursal_date
	UPDATE t_loan_monthly_obs as update_table
	join 
	(
		select obs_month,DATE_FORMAT(min(entry_date), '%Y%m%d') as first_loan_line_utilization_date from t_loan_monthly_obs join entries 
	on entry_date<=obs_end and account ='Loan Disbursed' group by obs_month
	) as data
	on update_table.obs_month=data.obs_month
	SET update_table.first_loan_line_utilization_date=data.first_loan_line_utilization_date;

	#computed - first_loan_line_utilization_month
	#cum_loan_line_utilized
	UPDATE t_loan_monthly_obs as update_table
	join 
	(
		select obs_month,ifnull(sum(credit - debit),0) as cum_loan_line_utilized from t_loan_monthly_obs join entries 
		on entry_date<=obs_end and account ='Loan Disbursed' group by obs_month
	) as data
	on update_table.obs_month=data.obs_month
	SET update_table.cum_loan_line_utilized=data.cum_loan_line_utilized;

/*

	# Closing Principal
	UPDATE t_loan_monthly_obs as update_table
	join 
	(
		select obs_month,ifnull(sum(debit-credit),0) as closing_principal from t_loan_monthly_obs join entries 
		on entry_date<=obs_end and account ='Loan Account Principal' group by obs_month
	) as data
	on update_table.obs_month=data.obs_month
	SET update_table.closing_principal=data.closing_principal;
*/

	#cum_bank_receipts
	UPDATE t_loan_monthly_obs as update_table
	join 
	(
		select obs_month,ifnull(sum(debit),0) as cum_bank_receipts from t_loan_monthly_obs join entries 
		on entry_date<=obs_end and head='Bank' group by obs_month
	) as data
	on update_table.obs_month=data.obs_month
	SET update_table.cum_bank_receipts=data.cum_bank_receipts;

	#cum_excess_amount
	UPDATE t_loan_monthly_obs as update_table
	join 
	(
		select obs_month, sum(credit-debit) as cum_excess_amount from t_loan_monthly_obs join entries 
		on entry_date<=obs_end and account ='Loan Account Excess' and head='Loan Account' group by obs_month
	) as data
	on update_table.obs_month=data.obs_month
	SET update_table.cum_excess_amount=data.cum_excess_amount;

	#bank_receipts
	UPDATE t_loan_monthly_obs as update_table
	join 
	(
		select obs_month,ifnull(sum(debit),0) as bank_receipts from t_loan_monthly_obs join entries 
		on entry_date>=obs_start and entry_date<=obs_end and head='Bank' group by obs_month
	) as data
	on update_table.obs_month=data.obs_month
	SET update_table.bank_receipts=data.bank_receipts;

	#last_bank_receipt_date
	UPDATE t_loan_monthly_obs as update_table
	join 
	(
		select obs_month,max(entry_date) as last_bank_receipt_date from t_loan_monthly_obs join entries 
		on entry_date<=obs_end and head='Bank' and debit>0 group by obs_month
	) as data
	on update_table.obs_month=data.obs_month
	SET update_table.last_bank_receipt_date=data.last_bank_receipt_date;


	#Cumulative Processing Fees
	UPDATE t_loan_monthly_obs as update_table
	join 
	(
		select obs_month,ifnull(sum(debit-credit),0) as cum_processing_fees from t_loan_monthly_obs join entries 
		on entry_date<=obs_end and account = 'Loan Account Processing Fees' and entry_set in ('GST Processing Fees','Processing Fees') group by obs_month
	) as data
	on update_table.obs_month=data.obs_month
	SET update_table.cum_processing_fees=data.cum_processing_fees;

	# Cumulative Broken Period Interest
	UPDATE t_loan_monthly_obs as update_table
	join 
	(
		select obs_month,ifnull(sum(credit-debit),0) as cum_bpi from t_loan_monthly_obs join entries 
		on entry_date<=obs_end and account = 'Broken Period Interest' group by obs_month
	) as data
	on update_table.obs_month=data.obs_month
	SET update_table.cum_bpi=data.cum_bpi;

	# Cumulative Insurance Fees
	UPDATE t_loan_monthly_obs as update_table
	join 
	(
		select obs_month,ifnull(sum(credit-debit),0) as cum_insurance_fees from t_loan_monthly_obs join entries 
		on entry_date<=obs_end and head ='Insurance Dealer' group by obs_month
	) as data
	on update_table.obs_month=data.obs_month
	SET update_table.cum_insurance_fees=data.cum_insurance_fees;


	# Cumulative Foreclosure Fees
	UPDATE t_loan_monthly_obs as update_table
	join 
	(
		select obs_month,ifnull(sum(debit-credit),0) as cum_foreclosure_fees from t_loan_monthly_obs join entries 
		on entry_date<=obs_end and account = 'Loan Account Foreclosure Fees' and entry_set in ('Foreclosure Fees','GST Foreclosure Fees') group by obs_month
	) as data
	on update_table.obs_month=data.obs_month
	SET update_table.cum_foreclosure_fees=data.cum_foreclosure_fees;


	#closure_amount
	UPDATE t_loan_monthly_obs as update_table
	join 
	(
		select obs_month,ifnull(credit,0) as closure_amount from t_loan_monthly_obs join entries 
		on entry_date<=obs_end AND entry_set = 'Close' AND account ='Loan Account Principal' group by obs_month
	) as data
	on update_table.obs_month=data.obs_month
	SET update_table.closure_amount=data.closure_amount;

	# Cumulative Other Interest
	UPDATE t_loan_monthly_obs as update_table
	join 
	(
		select obs_month,ifnull(sum(credit-debit),0) as cum_other_interest from t_loan_monthly_obs join entries 
		on entry_date<=obs_end and account = 'Days Interest' group by obs_month
	) as data
	on update_table.obs_month=data.obs_month
	SET update_table.cum_other_interest=data.cum_other_interest;


	# Cumulative Primary Dues - Calculated


	# Cumulative Late Payment Fees
	UPDATE t_loan_monthly_obs as update_table
	join 
	(
		select obs_month,ifnull(sum(debit-credit),0) as cum_late_payment_fees from t_loan_monthly_obs join entries 
		on entry_date<=obs_end and account = 'Loan Account Late Payment Fees' and entry_set in ('GST Late Payment Fees','Late Payment Fees') group by obs_month
	) as data
	on update_table.obs_month=data.obs_month
	SET update_table.cum_late_payment_fees=data.cum_late_payment_fees;

	# Cumulative Penalty Interest
	UPDATE t_loan_monthly_obs as update_table
	join 
	(
		select obs_month,ifnull(sum(credit-debit),0) as cum_penalty_interest from t_loan_monthly_obs join entries 
		on entry_date<=obs_end and account = 'Penalty Interest' group by obs_month
	) as data
	on update_table.obs_month=data.obs_month
	SET update_table.cum_penalty_interest=data.cum_penalty_interest;

	# Cumulative Penalty Dues
	UPDATE t_loan_monthly_obs as update_table
	SET update_table.cum_penalty_dues=cum_penalty_interest + cum_late_payment_fees;

	#Processing Fees
	UPDATE t_loan_monthly_obs as update_table
	join 
	(
		select obs_month,ifnull(sum(debit-credit),0) as processing_fees from t_loan_monthly_obs join entries 
		on entry_date>=obs_start and entry_date<=obs_end and account = 'Loan Account Processing Fees' and entry_set in ('GST Processing Fees','Processing Fees') group by obs_month
	) as data
	on update_table.obs_month=data.obs_month
	SET update_table.processing_fees=data.processing_fees;

	#Penalty Dues
	UPDATE t_loan_monthly_obs as update_table
	join 
	(
		select obs_month, ifnull(cum_penalty_dues - LAG(cum_penalty_dues) OVER (ORDER BY obs_month),0) AS penalty_dues  from t_loan_monthly_obs
	) as data
	on update_table.obs_month=data.obs_month
	SET update_table.penalty_dues=data.penalty_dues;

	#moratorium month
	UPDATE t_loan_monthly_obs as update_table
	join 
	(
		select obs_month,if(count(1)>0,1,0) as moratorium_month from t_loan_monthly_obs join entries 
		on entry_date>=obs_start and entry_date<=obs_end and head='Interest Income' and subgroup='moratorium' group by obs_month
	) as data
	on update_table.obs_month=data.obs_month
	SET update_table.moratorium_month=data.moratorium_month;

	#eom_moratorium
	UPDATE t_loan_monthly_obs as update_table
	join
	(
	select obs_month,1 as moratorium_month from t_loan_monthly_obs join entries
	on due_date>=obs_start and due_date<=obs_end and  txn_set='EOM Moratorium' and subgroup='eom_moratorium' group by obs_month having count(1)>0
	) as data
	on update_table.obs_month=data.obs_month
	SET update_table.moratorium_month=data.moratorium_month;

	#moratorium
	UPDATE t_loan_monthly_obs as update_table
	join
	(
	select if(sum(moratorium_month)>=1,1,0) as moratorium  from t_loan_monthly_obs
	) as data
	SET update_table.moratorium=data.moratorium;

	#Instalment
	UPDATE t_loan_monthly_obs as update_table
	join 
	(
	select obs_month,ifnull(sum(debit-credit),0) as instalment from t_loan_monthly_obs join entries 
	on entry_date>=obs_start and entry_date<=obs_end and entry_set in ('Monthly Instalment','Instalment','Advance EMI','Advance Instalments','Advance Credit') and account ='Loan Account Monthly Instalment' group by obs_month
	) as data
	on update_table.obs_month=data.obs_month
	SET update_table.instalment=data.instalment;

	#previous instalment
	UPDATE t_loan_monthly_obs as update_table
	join
	(
	select obs_month,LAG(instalment) OVER (ORDER BY obs_month) as previous_instalment
	from t_loan_monthly_obs
	) as data
	on update_table.obs_month=data.obs_month
	SET update_table.previous_instalment=data.previous_instalment;

	#Instalment Interest
	UPDATE t_loan_monthly_obs as update_table
	join
	(
	select obs_month,ifnull(sum(credit-debit),0) as instalment_interest from t_loan_monthly_obs join entries
	on entry_date>=obs_start and entry_date<=obs_end and entry_set in ('Monthly Instalment','Instalment','Advance EMI','Advance Instalments','Advance Credit') and head='Interest Income' group by obs_month
	) as data
	on update_table.obs_month=data.obs_month
	SET update_table.instalment_interest=data.instalment_interest;

	#Set the interest to 0 if instalment=0 
	UPDATE t_loan_monthly_obs set instalment_interest=0 where instalment=0;

	#Instalment Principal
	UPDATE t_loan_monthly_obs as update_table
	join
	(
		select obs_month,ifnull(sum(credit-debit),0) as instalment_principal from t_loan_monthly_obs join entries
		on entry_date>=obs_start and entry_date<=obs_end and entry_set in ('Monthly Instalment','Instalment','Advance EMI','Advance Instalments','Advance Credit') and account ='Loan Account Principal' group by obs_month
	) as data
	on update_table.obs_month=data.obs_month
	SET update_table.instalment_principal=data.instalment_principal;

	#Set the principal to 0 if instalment=0 
	UPDATE t_loan_monthly_obs set instalment_principal=0 where instalment=0;


	# Cumulative Instalment
	/*
	UPDATE t_loan_monthly_obs as update_table
	join 
	(
		select obs_month,ifnull(sum(debit-credit),0) as cum_instalment from t_loan_monthly_obs  join entries 
		on entry_date<=obs_end and entry_set in ('Monthly Instalment','Instalment','Advance EMI','Advance Instalments','Advance Credit') and account ='Loan Account Monthly Instalment' group by obs_month
	) as data
	on update_table.obs_month=data.obs_month
	SET update_table.cum_instalment=data.cum_instalment;
	*/

	# Cumulative Instalment
	UPDATE t_loan_monthly_obs as update_table
	join 
	(
		SELECT obs_month,instalment,SUM(instalment) OVER (ORDER BY obs_month ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as cum_instalment
	FROM t_loan_monthly_obs
	) as data
	on update_table.obs_month=data.obs_month
	SET update_table.cum_instalment=data.cum_instalment;

	# Cumulative Instalment Interest
	/*
	UPDATE t_loan_monthly_obs as update_table
	join 
	(
		select obs_month,ifnull(sum(credit-debit),0) as cum_instalment_interest from t_loan_monthly_obs join entries 
		on entry_date<=obs_end and entry_set in ('Monthly Instalment','Instalment','Advance EMI','Advance Instalments','Advance Credit') and head='Interest Income' group by obs_month
	) as data
	on update_table.obs_month=data.obs_month
	SET update_table.cum_instalment_interest=data.cum_instalment_interest;
	*/

	UPDATE t_loan_monthly_obs as update_table
	join 
	(
		SELECT obs_month,instalment_interest,SUM(instalment_interest) OVER (ORDER BY obs_month ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as cum_instalment_interest FROM t_loan_monthly_obs
	) as data
	on update_table.obs_month=data.obs_month
	SET update_table.cum_instalment_interest=data.cum_instalment_interest;

	# Cumulative Instalment Principal
	/*
	UPDATE t_loan_monthly_obs as update_table
	join 
	(
		select obs_month,ifnull(sum(credit-debit),0) as cum_instalment_principal from t_loan_monthly_obs join entries 
		on entry_date<=obs_end and entry_set in ('Monthly Instalment','Instalment','Advance EMI','Advance Instalments','Advance Credit') and account ='Loan Account Principal' group by obs_month
	) as data
	on update_table.obs_month=data.obs_month
	SET update_table.cum_instalment_principal=data.cum_instalment_principal;
	*/

	UPDATE t_loan_monthly_obs as update_table
	join 
	(
		SELECT obs_month,instalment_principal,SUM(instalment_principal) OVER (ORDER BY obs_month ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as cum_instalment_principal FROM t_loan_monthly_obs
	) as data
	on update_table.obs_month=data.obs_month
	SET update_table.cum_instalment_principal=data.cum_instalment_principal;


	#instalment_start_date
	UPDATE t_loan_monthly_obs as update_table
	join 
	(
		select obs_month,min(entries.entry_date) as instalment_start_date  from t_loan_monthly_obs join entries 
		on entry_date<=obs_end and txn_set='Monthly Instalment' group by obs_month 
	) as data
	on update_table.obs_month=data.obs_month
	SET update_table.instalment_start_date=data.instalment_start_date,
	first_instalment_month=EXTRACT(YEAR_MONTH FROM data.instalment_start_date);


	#first_bounce_month
	UPDATE t_loan_monthly_obs as update_table
	join 
	(
		select obs_month,min(entries.entry_date) as first_bounce_month  from t_loan_monthly_obs join entries 
		on entry_date<=obs_end and  account = 'Loan Account Returns' or account='Loan Account Late Payment Fees' group by obs_month 
	) as data
	on update_table.obs_month=data.obs_month
	SET update_table.first_bounce_month=EXTRACT(YEAR_MONTH FROM data.first_bounce_month);


	insert into t_loan_daily_obs(obs_date,row_num)	
	with 
	q0 as (
	select min(entry_date) as min_date from entries
	),
	q1 as (
	select obs_daily.obs_date from loantap_in.obs_daily join q0 
	on obs_daily.obs_date >=min_date and  obs_daily.obs_date <=@max_date
	),
	q2 as (
	select obs_date,ROW_NUMBER() OVER (ORDER BY obs_date ASC) AS row_num from q1
	)
	select obs_date,row_num from q2;


	# obs_month
	UPDATE t_loan_daily_obs as update_table
	join 
	(
	select obs_date,t_loan_monthly_obs.obs_month from t_loan_daily_obs join t_loan_monthly_obs 
	on t_loan_daily_obs.obs_date>=t_loan_monthly_obs.obs_start 
	and 
	t_loan_daily_obs.obs_date<=t_loan_monthly_obs.obs_end
	) as data
	on update_table.obs_date=data.obs_date
	SET update_table.obs_month=data.obs_month;


	#add moratorium data to daily obs
	UPDATE t_loan_daily_obs as update_table
	join 
	t_loan_monthly_obs
	on update_table.obs_month=t_loan_monthly_obs.obs_month
	SET update_table.moratorium_month=t_loan_monthly_obs.moratorium_month;


	#set up is_dpd_day;
	UPDATE t_loan_daily_obs set is_dpd_day=if(moratorium_month=1,0,1);


	#Added Penalty Waiver
	UPDATE t_loan_daily_obs as update_table
	join
	(
	select obs_date,ifnull(sum(debit-credit),0) as added_penalty_waiver from t_loan_daily_obs join entries
	on entry_date=obs_date and entry_set in ('Receipt') and account ='Penalty Waiver' group by obs_date
	) as data
	on update_table.obs_date=data.obs_date
	SET update_table.added_penalty_waiver=data.added_penalty_waiver;
	

	#Daily Instalment
	UPDATE t_loan_daily_obs as update_table
	join 
	(
	select obs_date,ifnull(sum(debit-credit),0) as added_instalment from t_loan_daily_obs join entries 
	on entry_date=obs_date and entry_set in ('Monthly Instalment','Instalment','Advance EMI','Advance Instalments','Advance Credit') and account ='Loan Account Monthly Instalment' group by obs_date
	) as data
	on update_table.obs_date=data.obs_date
	SET update_table.added_instalment=data.added_instalment;

	#Instalment Interest
	UPDATE t_loan_daily_obs as update_table
	join 
	(
	select obs_date,ifnull(sum(credit-debit),0) as added_instalment_interest from t_loan_daily_obs join entries 
	on entry_date=obs_date and entry_set in ('Monthly Instalment','Instalment','Advance EMI','Advance Instalments','Advance Credit') and head='Interest Income' group by obs_date
	) as data
	on update_table.obs_date=data.obs_date
	SET update_table.added_instalment_interest=data.added_instalment_interest;

	#sort out moratium issue
	UPDATE t_loan_daily_obs set added_instalment_interest=0 where added_instalment=0;

	#Instalment Principal
	UPDATE t_loan_daily_obs as update_table
	join 
	(
	select obs_date,ifnull(sum(credit-debit),0) as added_instalment_principal from t_loan_daily_obs join entries 
	on entry_date=obs_date and entry_set in ('Monthly Instalment','Instalment','Advance EMI','Advance Instalments','Advance Credit') and account ='Loan Account Principal' group by obs_date
	) as data
	on update_table.obs_date=data.obs_date
	SET update_table.added_instalment_principal=data.added_instalment_principal;

	#sort out moratium issue
	UPDATE t_loan_daily_obs set added_instalment_principal=0 where added_instalment=0;

	#sort out EOM Moratorium

	UPDATE t_loan_daily_obs as update_table
	join 
	(
	select obs_date,1 as eom_moratorium from t_loan_daily_obs join entries 
	on entry_date=obs_date and txn_set='EOM Moratorium' group by obs_date
	) as data
	on update_table.obs_date=data.obs_date
	SET update_table.eom_moratorium=data.eom_moratorium;
	
	UPDATE t_loan_daily_obs 
	set 
	added_eom_adjustment=added_instalment,
	added_instalment_principal=0,
	added_instalment_interest=0,
	added_instalment=0
	where moratorium_month=1 and added_instalment>0;
	
	
	# Penalty Dues
	UPDATE t_loan_daily_obs as update_table
	join 
	(
	with q0 as (
	select obs_date,ifnull(sum(debit-credit),0) as penalty_dues from t_loan_daily_obs join entries 
	on entry_date=t_loan_daily_obs.obs_date and account = 'Loan Account Late Payment Fees' and entry_set in ('GST Late Payment Fees','Late Payment Fees') group by obs_date
	UNION All
	select obs_date,ifnull(sum(credit-debit),0) as penalty_dues from t_loan_daily_obs join entries 
	on entry_date=t_loan_daily_obs.obs_date and account = 'Penalty Interest' group by obs_date
	UNION All
	select obs_date,ifnull(sum(debit-credit),0) as penalty_dues from t_loan_daily_obs join entries 
	on entry_date=t_loan_daily_obs.obs_date and account = 'Loan Account Processing Fees' and entry_set in ('GST Processing Fees','Processing Fees') group by obs_date
	UNION All
	select obs_date,ifnull(sum(credit-debit),0) as penalty_dues from t_loan_daily_obs join entries 
	on entry_date=t_loan_daily_obs.obs_date and account = 'Broken Period Interest' group by obs_date
	UNION All
	select obs_date,ifnull(sum(credit-debit),0) as penalty_dues from t_loan_daily_obs join entries 
	on entry_date=t_loan_daily_obs.obs_date and head ='Insurance Dealer' group by obs_date
	UNION All
	select obs_date,ifnull(sum(debit-credit),0) as penalty_dues from t_loan_daily_obs join entries 
	on entry_date=t_loan_daily_obs.obs_date and account = 'Loan Account Foreclosure Fees' and entry_set in ('Foreclosure Fees','GST Foreclosure Fees') group by obs_date
	UNION All
	select obs_date,ifnull(sum(credit-debit),0) as penalty_dues from t_loan_daily_obs join entries 
	on entry_date=t_loan_daily_obs.obs_date and account = 'Days Interest' group by obs_date
	
	),
	q1 as (
	select obs_date,sum(penalty_dues) as added_penalty_dues from q0 group by obs_date
	)
	select * from q1
	) as data
	on update_table.obs_date=data.obs_date
	SET update_table.added_penalty_dues=data.added_penalty_dues;


	# Closing Dues Account
/*	
	UPDATE t_loan_daily_obs as update_table
	join 
	(
	select obs_date,ifnull(sum(debit-credit),0) as closing_dues_account from t_loan_daily_obs join entries 
	on entry_date<=obs_date and account in ('Loan Account Dues','Loan Account Instalment Dues','Loan Account Future Dues') group by obs_date
	) as data
	on update_table.obs_date=data.obs_date
	SET update_table.closing_dues_account=data.closing_dues_account;

	# Closing Principal Account
	UPDATE t_loan_daily_obs as update_table
	join 
	(
	select obs_date,ifnull(sum(debit-credit),0) as closing_principal from t_loan_daily_obs join entries 
	on entry_date<=obs_date and account in ('Loan Account Principal') group by obs_date
	) as data
	on update_table.obs_date=data.obs_date
	SET update_table.closing_principal=data.closing_principal;

*/

	select @daily_total:=count(1) from t_loan_daily_obs;

	/*
		Jan 1: P:1400 , I:200
		Feb 1: P:600, I:100

		Feb 10 : Paid:1500

		Adjust with all interest first and then principal
		Left::
		P:800, I:0

		Feb 20: Paid: 200
		Left::
		P:600, I:0


		New Approach , knock of FIFO basis, with principal first

		Jan 1: P:1400 , I:200
		Feb 1: P:600, I:100

		Feb 10 : Paid:1500

		Adjust in z fashion
		Left::
		for Jan 1: P:0 , I:100
		for Feb 1: P:600, I:100

		Feb 20: Paid: 200
		Left::
		for Jan 1: P:0 , I:0
		for Feb 1: P:500, I:100
	*/

	/*
	Take the closing_dues_account for the day from Loan Entries (instalment + Penalty Dues)

	So @adjustment= closing_dues_account is what is left to be apportioned between penalty, primary, instalment_interest, instalment_principal


	Adjust against penalty: get the closing penalty account
	------------------------------------------------------
	@unadjusted_penalty_dues = previous closing penalty + added_penalty_dues - added_penalty_waiver
	@closing_penalty_dues=least(@unadjusted_penalty_dues,@adjustment)


	Left to adjust = @adjustment - (adjusted against penalty dues)

	Adjust against Interest and Principal
	-------------------------------------------
	Create a series of all Principal and Interest posted till that day
	Go backwards till you can adjust what is left to adjust

	This gives you the closing Principal and Closing Interest



	closing_dues_account :: 10000

	adjust with @unadjusted_penalty_dues say 1000
	Left with 9000

	Go Backwards of all Principal and Interest posted till day till you can gather Rs 9000.
	This will give you outstanding Principal and Outstanding


	Adjust against primary dues
	----------------------------
	if closing_dues_account is still not adjusted then mark it as closing primary dues
	*/


	UPDATE t_loan_daily_obs as update_table
	SET 
	update_table.opening_primary_dues=0,
	update_table.opening_penalty_dues=0,
	update_table.opening_eom_adjustment=0,
	update_table.opening_instalment_principal=0,
	update_table.opening_instalment_interest=0,
	update_table.opening_instalment=0;
	
	#delimiter //
	FOR i IN 1..@daily_total
	DO


	#Update Closing Dues Account
	select @obs_date:=obs_date from t_loan_daily_obs where row_num=i;


	UPDATE t_loan_daily_obs as update_table
	join 
	(
	select ifnull(sum(debit-credit),0) as closing_dues_account from entries 
	where entry_date<=@obs_date and account in ('Loan Account Dues','Loan Account Instalment Dues','Loan Account Future Dues','Loan Account Monthly Instalment','Loan Account Prepayment')
	) as data
	SET update_table.closing_dues_account=data.closing_dues_account
	where row_num=i;


	# Update Closing Principal Account
	UPDATE t_loan_daily_obs as update_table
	join 
	(
	select ifnull(sum(debit-credit),0) as closing_principal from entries 
	where entry_date<=@obs_date and account in ('Loan Account Principal')

	) as data
	SET update_table.closing_principal=data.closing_principal
	where row_num=i;

	
	#setup opening by taking previous data
	
	if i>0
	THEN
	
		UPDATE t_loan_daily_obs as update_table
		join 
		(
		select obs_date,closing_primary_dues,closing_penalty_dues,closing_eom_adjustment,closing_instalment_principal,closing_instalment_interest,closing_instalment
		from t_loan_daily_obs where row_num=i-1	
		) as data
		SET 
		update_table.opening_primary_dues=data.closing_primary_dues,
		update_table.opening_penalty_dues=data.closing_penalty_dues,
		update_table.opening_eom_adjustment=data.closing_eom_adjustment,
		update_table.opening_instalment_principal=data.closing_instalment_principal,
		update_table.opening_instalment_interest=data.closing_instalment_interest,
		update_table.opening_instalment=data.closing_instalment
		where row_num=i;
	END IF;

	#Destroy all EOM Adjustment if EOM Moratorium=1 eom_moratorium
	UPDATE t_loan_daily_obs
	SET 
	added_eom_adjustment=-1 * opening_eom_adjustment
	where row_num=i and eom_moratorium=1;	

	


	#select * from t_loan_daily_obs where row_num=i;
	#get the closing dues
	select @obs_date:=obs_date,@closing_dues_account:=closing_dues_account,@available_penalty_dues:=opening_penalty_dues + added_penalty_dues - added_penalty_waiver,@available_instalment:= opening_instalment + added_instalment,@available_eom_adjustment:=opening_eom_adjustment + added_eom_adjustment from t_loan_daily_obs where row_num=i;
		
	#first take the maximum in penalty_dues
	set @closing_penalty_dues=least(@available_penalty_dues,@closing_dues_account);
	set @closing_dues_account=@closing_dues_account - @closing_penalty_dues;
	
	#then take in EOM Adjustment
	set @closing_eom_adjustment=least(@available_eom_adjustment,@closing_dues_account);
	set @closing_dues_account=@closing_dues_account - @closing_eom_adjustment;
	
	#what is left adjust with @available_instalment. @adjustment is to broken into instalment and principal
	set @adjustment=least(@available_instalment,@closing_dues_account);
	set @closing_dues_account=@closing_dues_account - @adjustment;

	#what is left is primary dues
	Set @closing_primary_dues=@closing_dues_account;

	#Find the breakup of @adjustment
	#adjust interest and principal
	insert into t_adjustments(obs_date,adj_type,amount)
	select obs_date,'principal',added_instalment_principal from t_loan_daily_obs where row_num=i and added_instalment_principal>0;

	insert into t_adjustments(obs_date,adj_type,amount)
	select obs_date,'interest',added_instalment_interest from t_loan_daily_obs where row_num=i and added_instalment_interest>0;

	UPDATE t_adjustments as update_table
	join
	(
	SELECT ID,SUM(amount) OVER (ORDER BY ID DESC RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as cum_amount
	FROM t_adjustments ORDER BY ID ASC
	) as data
	on update_table.ID=data.ID
	set update_table.cum_amount=data.cum_amount;	
	
	set @closing_instalment_principal=0;
	set @closing_instalment_interest=0;
	set @dpd_days=0;
	set @dpd_start=NULL;
	if @adjustment>0
	THEN

		select @match_id:=ifnull(min(ID),0) from t_adjustments where cum_amount<=@adjustment ;

		IF @match_id>0 
		THEN
			select @dpd_start:=obs_date from t_adjustments where ID=@match_id;

			select @closing_instalment_principal:=ifnull(sum(amount),0) from t_adjustments where adj_type='principal' and ID>=@match_id ;
			set @adjustment=@adjustment - @closing_instalment_principal;

			select @closing_instalment_interest:=ifnull(sum(amount),0) from t_adjustments where adj_type='interest' and ID>=@match_id ;
			set @adjustment=@adjustment - @closing_instalment_interest;

		END IF;	

		#finding incremental amount
		#Find the entry immediately before the @match_id


		IF @adjustment>0
		THEN
			select @difference_id:=max(ID)  from t_adjustments where ID<@match_id;
			
			IF @difference_id is NULL
			THEN
			select @difference_id:=max(ID)  from t_adjustments;
			
			END IF;	
			
			
			select @dpd_start:=obs_date,@adj_type:=adj_type from t_adjustments where ID=@difference_id;

			select @closing_instalment_principal:=@closing_instalment_principal + @adjustment from dual where @adj_type='principal';
			
			select @closing_instalment_interest:=@closing_instalment_interest + @adjustment from dual where @adj_type='interest';

		END IF;	


		#calculate the dpd	
		#sum from > @dpd_start to the current obs_date 

		IF @dpd_start is not NULL
		THEN
			select @dpd_days:=ifnull(sum(is_dpd_day),0) from t_loan_daily_obs where obs_date>@dpd_start and obs_date<=@obs_date;
		END IF;

	END IF;
	
	update t_loan_daily_obs
	set closing_penalty_dues=@closing_penalty_dues,
	closing_eom_adjustment=@closing_eom_adjustment,
	closing_instalment_principal=@closing_instalment_principal,
	closing_instalment_interest=@closing_instalment_interest,
	closing_primary_dues = @closing_primary_dues,
	closing_instalment=@closing_instalment_principal + @closing_instalment_interest,	
	dpd_days=@dpd_days,
	dpd_start=@dpd_start
	where row_num=i;	

	END FOR;#//
	#delimiter ;

	# find the 3rd of the next month
	# if dpd days is lesser use that

	UPDATE t_loan_daily_obs as update_table
	join
	(
		with q0 as
		( select distinct obs_month as obs_month from t_loan_monthly_obs
		),
		q1 as (
			#get the lead month
			select obs_month,LEAD(obs_month) OVER (ORDER BY obs_month) as next_obs_month
			from q0
		),
		q2 as (
			select obs_month,next_obs_month,date(concat(next_obs_month,'02')) as next_obs_date from q1
		),
		q3 as (
			select q2.obs_month,dpd_days as next_dpd,closing_principal as next_closing_principal,closing_instalment_principal as next_closing_instalment_principal,closing_instalment_interest as next_closing_instalment_interest,closing_instalment as next_closing_instalment
			from t_loan_daily_obs join q2 on q2.next_obs_date=t_loan_daily_obs.obs_date
		)
		select q3.obs_month,q3.next_dpd,q3.next_closing_principal,q3.next_closing_instalment_principal,q3.next_closing_instalment_interest,q3.next_closing_instalment from q3 join t_loan_daily_obs on q3.obs_month=t_loan_daily_obs.obs_month
	) as data
	on update_table.obs_month=data.obs_month
	SET
	update_table.next_dpd=data.next_dpd,
	update_table.next_closing_principal=data.next_closing_principal,
	update_table.next_closing_instalment_principal=data.next_closing_instalment_principal,
	update_table.next_closing_instalment_interest=data.next_closing_instalment_interest,
	update_table.next_closing_instalment=data.next_closing_instalment;

	/*
	loan_quality

	-----------------
	Standard			dpd_days<=30
	SMA1					dpd_days>=31 and dpd_days<=60
	SMA2					dpd_days>=61 and dpd_days<=90
	SubStandard		dpd_days>=91 and dpd_days<=365
	Doubtful			dpd_days>=366

	loan_quality_days
	---------------------
	000						dpd_days<=30
	030						dpd_days>=31 and dpd_days<=60
	060						dpd_days>=61 and dpd_days<=90
	090						dpd_days>=91 and dpd_days<=365
	365+					dpd_days>=366

	dpd
	-------------------
	Regular				dpd_days=0
	1-30
	31-60
	61-90
	91-120
	121-150
	151-180
	181-270
	271-365
	365-450
	451-540
	541+


	*/


	UPDATE t_loan_monthly_obs as update_table
	join 
	(
	select obs_date as obs_start,
	t_loan_daily_obs.opening_primary_dues,
	t_loan_daily_obs.opening_instalment_interest,
	t_loan_daily_obs.opening_instalment_principal,
	t_loan_daily_obs.opening_instalment,
	t_loan_daily_obs.opening_penalty_dues,
	t_loan_daily_obs.opening_all_dues
	from t_loan_daily_obs join t_loan_monthly_obs
	on t_loan_daily_obs.obs_date=t_loan_monthly_obs.obs_start
	) as data
	on update_table.obs_start=data.obs_start
	SET update_table.opening_primary_dues=data.opening_primary_dues,
	update_table.opening_instalment_interest=data.opening_instalment_interest,
	update_table.opening_instalment_principal=data.opening_instalment_principal,
	update_table.opening_instalment=data.opening_instalment,
	update_table.opening_penalty_dues=data.opening_penalty_dues,
	update_table.opening_all_dues=data.opening_all_dues;

	/*knocked off should be sum across the month */
	UPDATE t_loan_monthly_obs as update_table
	join (
	select sum(knocked_off) as cum_knocked_off, obs_month 
	from t_loan_daily_obs group by obs_month
	) as data
	on update_table.obs_month=data.obs_month 
	set update_table.knocked_off=ifnull(data.cum_knocked_off,0);


	/*Closing calculated bases on end date of each month*/  
	UPDATE t_loan_monthly_obs as update_table
	join
	(
	select obs_date as obs_end,
	t_loan_daily_obs.closing_dues_account,
	t_loan_daily_obs.closing_penalty_dues,
	t_loan_daily_obs.closing_instalment_principal,
	t_loan_daily_obs.closing_instalment_interest,
	t_loan_daily_obs.closing_instalment,
	t_loan_daily_obs.closing_primary_dues,
	t_loan_daily_obs.dpd_days,
	t_loan_daily_obs.next_dpd,
	t_loan_daily_obs.closing_all_dues,
	t_loan_daily_obs.closing_principal,
    t_loan_daily_obs.next_closing_principal,
    t_loan_daily_obs.next_closing_instalment_principal,
    t_loan_daily_obs.next_closing_instalment_Interest,
    t_loan_daily_obs.next_closing_instalment
	from t_loan_daily_obs join t_loan_monthly_obs
	on t_loan_daily_obs.obs_date=t_loan_monthly_obs.obs_end
	) as data
	on update_table.obs_end=data.obs_end
	SET update_table.closing_dues_account=data.closing_dues_account,
	update_table.closing_penalty_dues=data.closing_penalty_dues,
	update_table.closing_primary_dues=data.closing_primary_dues,
	update_table.closing_all_dues=ifnull(data.closing_all_dues,0),
	
	update_table.eom_dpd_days=ifnull(data.dpd_days,0),
	update_table.eom_closing_principal=data.closing_principal,
	update_table.eom_closing_instalment_principal=data.closing_instalment_principal,
update_table.eom_closing_instalment_interest=data.closing_instalment_interest,
update_table.eom_closing_instalment=data.closing_instalment,
update_table.dpd_days=least(ifnull(data.dpd_days,0),ifnull(data.next_dpd,10000)),
update_table.closing_principal=least(data.closing_principal, ifnull(data.next_closing_principal,100000000)),	update_table.closing_instalment_principal=data.closing_instalment_principal,update_table.closing_instalment_interest=data.closing_instalment_interest,
update_table.closing_instalment=data.closing_instalment;
	

	/*update instalment to next if required*/  
	UPDATE t_loan_monthly_obs as update_table
	join
	(
	select obs_date as obs_end,
    t_loan_daily_obs.next_closing_instalment_principal,
    t_loan_daily_obs.next_closing_instalment_Interest,
    t_loan_daily_obs.next_closing_instalment

	from t_loan_daily_obs join t_loan_monthly_obs
	on t_loan_daily_obs.obs_date=t_loan_monthly_obs.obs_end
	) as data
	on update_table.obs_end=data.obs_end
	SET 
	update_table.closing_instalment_principal=data.next_closing_instalment_principal,
	update_table.closing_instalment_interest=data.next_closing_instalment_interest,
	update_table.closing_instalment=data.next_closing_instalment
	where data.next_closing_instalment<update_table.closing_instalment;



	

	#dpd 
	UPDATE t_loan_monthly_obs as update_table
	join 
	(
	select case when `dpd_days` = 0 then 'Regular' when (`dpd_days` >= 1 and `dpd_days` <= 30) then '1-30' when (`dpd_days` >= 31 and `dpd_days` <= 60) then '31-60' 
	when (`dpd_days` >= 61 and `dpd_days` <= 90) then '61-90' when (`dpd_days` >= 91 and `dpd_days` <= 120) then '91-120'
	when (`dpd_days` >= 121 and `dpd_days` <= 150) then '121-150' when (`dpd_days` >= 151 and `dpd_days` <= 180) then '151-180' 
	when (`dpd_days` >= 181 and `dpd_days` <= 270) then '181-270' when (`dpd_days` >= 271 and `dpd_days` <= 365) then '271-365'    when (`dpd_days` >= 366 and `dpd_days` <= 450) then '366-450' 
	when (`dpd_days` >= 451 and `dpd_days` <= 540) then '451-540' when `dpd_days` >= 541 then '541+' end as dpd,obs_month from t_loan_monthly_obs 
	) as data
	on update_table.obs_month=data.obs_month
	SET update_table.dpd=data.dpd;

	#previous dpd
	UPDATE t_loan_monthly_obs as update_table
	join 
	(
	select obs_month,LAG(dpd) OVER (ORDER BY obs_month) as previous_dpd 
	from t_loan_monthly_obs
	) as data
	on update_table.obs_month=data.obs_month
	SET update_table.previous_dpd=ifnull(data.previous_dpd,'');


	/*dpd movements */
	UPDATE t_loan_monthly_obs as update_table
	join 
	(
	select obs_end,
	case 
	when LAG(bucket_movement_index) OVER (ORDER BY obs_date) is null then 'stable' 
	when bucket_movement_index=LAG(bucket_movement_index) OVER (ORDER BY obs_date) then 'stable'
	when bucket_movement_index>LAG(bucket_movement_index) OVER (ORDER BY obs_date) then 'flow' 
	when ((bucket_movement_index<LAG(bucket_movement_index) OVER (ORDER BY obs_date)) and bucket_movement_index=0) then 'normalize'
	when bucket_movement_index<LAG(bucket_movement_index) OVER (ORDER BY obs_date) then 'Roll Back' 
	end as dpd_movement from t_loan_daily_obs  join t_loan_monthly_obs
	on t_loan_daily_obs.obs_date=t_loan_monthly_obs.obs_end
	) as data
	on update_table.obs_end=data.obs_end
	SET update_table.dpd_movement=data.dpd_movement;


	#bounce_month
	UPDATE t_loan_monthly_obs as update_table
	join
	(
	select obs_month,if(count(1)>0,1,0) as bounce_month from t_loan_monthly_obs join entries
	on entry_date>=obs_start and entry_date<=obs_end  and (entry_set='Bounce' or entry_set in ('GST Late Payment Fees','Late Payment Fees'))  and 
	(account='Loan Account Returns' or account='Loan Account Late Payment Fees') group by entries.lan_id,obs_end
	) as data
	on update_table.obs_month=data.obs_month
	SET update_table.bounce_month=data.bounce_month ;

	#no_of_bounces
	UPDATE t_loan_monthly_obs as update_table
	join
	(
	select obs_month,count(entry_set_id) as no_of_bounces from t_loan_monthly_obs join entries
	on entry_date>=obs_start and entry_date<=obs_end  and (entry_set='Bounce' or entry_set in ('GST Late Payment Fees','Late Payment Fees'))  and 
	(account='Loan Account Returns' or account='Loan Account Late Payment Fees') group by entry_set_id,obs_end
	) as data
	on update_table.obs_month=data.obs_month
	SET update_table.no_of_bounces=data.no_of_bounces;

	#cum_no_of_bounces
	UPDATE t_loan_monthly_obs as update_table
	join
	(
	select obs_month,count(entries.lan_id) as cum_no_of_bounces from t_loan_monthly_obs join entries
	on  entry_date<=obs_end  and (entry_set='Bounce' or entry_set in ('GST Late Payment Fees','Late Payment Fees'))  and 
	(account='Loan Account Returns' or account='Loan Account Late Payment Fees') 
	group by entries.lan_id,obs_end
	) as data
	on update_table.obs_month=data.obs_month
	SET update_table.cum_no_of_bounces=data.cum_no_of_bounces;


	#loan_line_available
	UPDATE t_loan_monthly_obs as update_table
	join
	(
	select obs_month,ifnull(sum(credit - debit),0) as loan_line_available from t_loan_monthly_obs join entries
	on  entry_date<=obs_end  and account='Loan Line'  
	group by obs_end
	) as data
	on update_table.obs_month=data.obs_month
	SET update_table.loan_line_available=data.loan_line_available;

	#hypothecation lapp.debt_hypothecation
	UPDATE t_loan_monthly_obs as update_table
	join
	(
	select t_lapp_meta.meta_value as debt_hypothecation from t_loan_monthly_obs join t_lapp_meta on t_lapp_meta.object_id=t_loan_monthly_obs.lapp_id and coll_id='lapp' and meta_key='debt_hypothecation'
	) as data
	SET update_table.hypothecation=data.debt_hypothecation;

	#dealer_code
	UPDATE t_loan_monthly_obs as update_table
	join 
	(
	select meta_value as sublan_dealer_code from t_sublan_entries where coll_id='sublan' and coll_type='dealer' and meta_key='dealer_code'
	) as data
	SET update_table.dealer_code=data.sublan_dealer_code;


	#select nach_id
	select @nach_id:= (select meta_value as nach_id from t_sublan_entries where object_id=@object_id and meta_key='nach_id' limit 1) ;

	#nach_status
	UPDATE t_loan_monthly_obs as update_table
	join
	(
	select obs_month,meta_value as nach_status from t_loan_monthly_obs join t_sublan_collection_entries as sublan_collection
	on t_loan_monthly_obs.sublan_id = sublan_collection.reference_id where object_id=@nach_id and meta_key='nach_status'
	) as data
	on update_table.obs_month=data.obs_month
	SET update_table.nach_status=data.nach_status;

	#nach_umrn
	UPDATE t_loan_monthly_obs as update_table
	join
	(
	select obs_month,meta_value as nach_umrn from t_loan_monthly_obs join t_sublan_collection_entries as sublan_collection
	on t_loan_monthly_obs.sublan_id = sublan_collection.reference_id where object_id=@nach_id and meta_key='nach_umrn'
	) as data
	on update_table.obs_month=data.obs_month
	SET update_table.nach_umrn=data.nach_umrn;

	#nach_mandate_id
	UPDATE t_loan_monthly_obs as update_table
	join
	(
	select obs_month,meta_value as nach_mandate_id from t_loan_monthly_obs join t_sublan_collection_entries as sublan_collection
	on t_loan_monthly_obs.sublan_id = sublan_collection.reference_id where object_id=@nach_id and meta_key='nach_mandate_id'
	) as data
	on update_table.obs_month=data.obs_month
	SET update_table.nach_mandate_id=data.nach_mandate_id;

	#nach_max_amount
	UPDATE t_loan_monthly_obs as update_table
	join
	(
	select obs_month,meta_value as nach_max_amount from t_loan_monthly_obs join t_sublan_entries
	on t_loan_monthly_obs.sublan_id = t_sublan_entries.object_id and coll_id='sublan' and meta_key='nach_max_amount'
	) as data
	on update_table.obs_month=data.obs_month
	SET update_table.nach_max_amount=data.nach_max_amount;

	#nach_process
	UPDATE t_loan_monthly_obs as update_table
	join
	(
	select obs_month,meta_value as nach_process from t_loan_monthly_obs join t_sublan_collection_entries as sublan_collection
	on t_loan_monthly_obs.sublan_id = sublan_collection.reference_id where object_id=@nach_id and meta_key='nach_process'
	) as data
	on update_table.obs_month=data.obs_month
	SET update_table.nach_process=data.nach_process;

	#nach_frequency
	UPDATE t_loan_monthly_obs as update_table
	join
	(
	select obs_month,meta_value as nach_frequency from t_loan_monthly_obs join t_sublan_collection_entries as sublan_collection
	on t_loan_monthly_obs.sublan_id = sublan_collection.reference_id where object_id=@nach_id and meta_key='nach_frequency'
	) as data
	on update_table.obs_month=data.obs_month
	SET update_table.nach_frequency=data.nach_frequency;

	#loan_end_date_label
	UPDATE t_loan_monthly_obs as update_table
	SET update_table.loan_end_date_label=DATE_FORMAT(update_table.loan_end_date,'%d %M, %Y');

	#last_nach_status and date
	UPDATE t_loan_monthly_obs as update_table
	join
	(
	with q0 as(	
	select max(txn_set_id) as last_entry,obs_month from  entries join t_loan_monthly_obs on entry_date>=obs_start and entry_date<=obs_end 
	where txn_set='NACH Update'  group by obs_month 
	),
	q1 as (
	select q0.* ,entry_set,entry_date as last_nach_date from q0 join entries on q0.last_entry =entries.txn_set_id   where head in ('bank','NACH','Not Presented') or account in ('Loan Account Excess' ,'Loan Account Returns') 
	),
	q2 as (
	select obs_month ,last_entry,last_nach_date ,
	case when q1.entry_set='Bounce' then 'Bounce'
	when (q1.entry_set='NACH Not Presented' or  q1.entry_set='Drawdown NACH Not Presented') then 'Not Presented'
	else 'success' end as 'last_nach_status' from q1
	)
	select * from q2
	) as data
	on update_table.obs_month=data.obs_month
	SET update_table.last_nach_status=data.last_nach_status,
	update_table.last_nach_date=data.last_nach_date;
	
	
	#for old cases we dont store agreement_id in sublan so need to get the process id and then check the agreement process from sublan collection.
	select @agreement_id:= (select meta_value as agreement_id from t_sublan_entries where object_id=@object_id and  (meta_key='agreement_id' or meta_key='ops_process_id') limit 1);
	#agreement_process
	UPDATE t_loan_monthly_obs as update_table
	join
	(
	select obs_month,meta_value as agreement_process from t_loan_monthly_obs join t_sublan_collection_entries as sublan_collection
	on t_loan_monthly_obs.sublan_id = sublan_collection.reference_id where object_id=@agreement_id and coll_type='agreement' and meta_key='agreement_process'
	) as data
	on update_table.obs_month=data.obs_month
	SET update_table.agreement_process=data.agreement_process;
	
	#agreement_created_date
	UPDATE t_loan_monthly_obs as update_table
	join
	(
	select obs_month,meta_value as agreement_created_date from 
	t_loan_monthly_obs join t_sublan_collection_entries as sublan_collection
	on t_loan_monthly_obs.sublan_id = sublan_collection.reference_id 
	where object_id=@agreement_id and coll_type='agreement' and meta_key='agreement_created_date'
	) as data
	on update_table.obs_month=data.obs_month
	SET update_table.agreement_created_date=data.agreement_created_date;
	
	#######################################################
	/*
	closing_all_dues decimal(12,2) GENERATED ALWAYS AS (closing_primary_dues + closing_instalment + closing_penalty_dues) STORED,
	closing_instalment decimal(12,2) GENERATED ALWAYS AS (closing_instalment_principal + closing_instalment_interest) STORED,
	*/	

	#instalment_consumed  formula

	# Instalment Consumed - formula 	

	#Relevant Principal
	UPDATE t_loan_monthly_obs as update_table
	join 
	(

	with q0 as (
	select obs_month,instalment_consumed from t_loan_monthly_obs
	),
	q1 as (
	select  q0.obs_month,t_loan_monthly_obs.obs_month as relevant_month from q0 join t_loan_monthly_obs on
	t_loan_monthly_obs.cum_instalment<=q0.instalment_consumed
	),
	q2 as (
	select obs_month,max(relevant_month) as max_month from q1 group by obs_month
	)
	select q2.obs_month,t_loan_monthly_obs.closing_principal as relevant_principal from q2 join t_loan_monthly_obs 
	on q2.max_month=t_loan_monthly_obs.obs_month
	) as data
	on update_table.obs_month=data.obs_month
	SET update_table.relevant_principal=data.relevant_principal;

	#closure_mode
	UPDATE t_loan_monthly_obs as update_table
	join 
	(
	select 
	case 
	when account='Principal Transfer'  then 'Principal Transfer' 
	when account='Transfer within Limit'  then 'Transfer within' 
	when (head='Bank' or account='Loan Account Excess')  then 'Bank' 
	else 'Bank' end as closure_mode,obs_month from t_loan_monthly_obs join entries 
	on entry_date<=obs_end and entry_set='close' group by obs_month
	) as data
	on update_table.obs_month=data.obs_month
	SET update_table.closure_mode=data.closure_mode;

	#interest_income_dealer
	UPDATE t_loan_monthly_obs as update_table
	join
	(
	select obs_month,ifnull(sum(credit-debit),0) as interest_income_dealer from t_loan_monthly_obs join entries
	on entry_date>=obs_start and entry_date<=obs_end and account='Dealer Interest'  and head='Interest Income' group by obs_month
	) as data
	on update_table.obs_month=data.obs_month
	SET update_table.interest_income_dealer=data.interest_income_dealer;

	#interest_income_bpi
	UPDATE t_loan_monthly_obs as update_table
	join
	(
	select obs_month,ifnull(sum(credit-debit),0) as interest_income_bpi from t_loan_monthly_obs join entries
	on entry_date>=obs_start and entry_date<=obs_end and account='Broken Period Interest' and head='Interest Income' group by obs_month
	) as data
	on update_table.obs_month=data.obs_month
	SET update_table.interest_income_bpi=data.interest_income_bpi;

	#interest_income_days
	UPDATE t_loan_monthly_obs as update_table
	join
	(
	select obs_month,ifnull(sum(credit-debit),0) as interest_income_days from t_loan_monthly_obs join entries
	on entry_date>=obs_start and entry_date<=obs_end and account='Days Interest' and head='Interest Income' group by obs_month
	) as data
	on update_table.obs_month=data.obs_month
	SET update_table.interest_income_days=data.interest_income_days;

	#fees_income_pf
	UPDATE t_loan_monthly_obs as update_table
	join
	(
	select obs_month,ifnull(sum(credit-debit),0) as fees_income_pf from t_loan_monthly_obs join entries
	on entry_date>=obs_start and entry_date<=obs_end and account='Processing Fees' and head='Fee Income'group by obs_month
	) as data
	on update_table.obs_month=data.obs_month
	SET update_table.fees_income_pf=data.fees_income_pf;

	#fees_income_foreclosure
	UPDATE t_loan_monthly_obs as update_table
	join
	(
	select obs_month,ifnull(sum(credit-debit),0) as fees_income_foreclosure from t_loan_monthly_obs join entries
	on entry_date>=obs_start and entry_date<=obs_end and account='Foreclosure Fees' and head='Fee Income' group by obs_month
	) as data
	on update_table.obs_month=data.obs_month
	SET update_table.fees_income_foreclosure=data.fees_income_foreclosure;

	#interest_income_monthly_interest
	UPDATE t_loan_monthly_obs as update_table
	join
	(
	select obs_month,ifnull(sum(credit-debit),0) as interest_income_monthly_interest from t_loan_monthly_obs join entries
	on entry_date>=obs_start and entry_date<=obs_end and account in ('Monthly Interest(EMI)','Monthly Interest(Interest Only)','Monthly Interest(Flat)','Monthly Interest(ADB)') and head='Interest Income' group by obs_month
	) as data
	on update_table.obs_month=data.obs_month
	SET update_table.interest_income_monthly_interest=data.interest_income_monthly_interest;

	#fees_income_insurance
	UPDATE t_loan_monthly_obs as update_table
	join
	(
	select obs_month,ifnull(sum(credit-debit),0) * 0.15 as fees_income_insurance from t_loan_monthly_obs join entries
	on entry_date<=obs_end and account='Insurance Fees' and head='Fee Income' group by obs_month
	) as data
	on update_table.obs_month=data.obs_month
	SET update_table.fees_income_insurance=data.fees_income_insurance;

	# opening_principal
	UPDATE t_loan_monthly_obs as update_table
	join 
	(
	select obs_month,ifnull(sum(debit-credit),0) as opening_principal from t_loan_monthly_obs join entries 
	on entry_date<obs_start and account ='Loan Account Principal' group by obs_month
	) as data
	on update_table.obs_month=data.obs_month
	SET update_table.opening_principal=data.opening_principal;

	#dealer_discount
	UPDATE t_loan_monthly_obs as update_table
	join
	(
	select meta_value as dealer_discount from t_sublan_entries where coll_id='sublan' and coll_type='sublan' and meta_key='dealer_discount'
	) as data
	SET update_table.dealer_discount=data.dealer_discount;

	delete from data_store.loan_monthly_obs where sublan_id=@object_id;



	insert into data_store.loan_monthly_obs(lapp_id,
	loan_id,
	lan_id,
	customer_id,
	sublan_id,
	nbfc,
	product_or_scheme,
	product_category,
	obs_start,
	obs_end,
	obs_month,
	sublan_loan_tenure,
	sublan_loan_interest_rate,
	sublan_loan_amount,
	sublan_setup_date,
	sublan_advance_instalments,
	sublan_dealer_code,
	sublan_instalment_method,
	sublan_virtual_account,
	sublan_loan_end_date,
	sublan_loan_end_date_label,
	loan_amount,
	loan_tenure,
	interest_rate,
	loan_status,
	final_loan_status,
	loan_city_label,
	loan_product,
	loan_product_label,
	bureau_account_type,
	closing_principal,
	cum_bank_receipts,
	bank_receipts,
	last_bank_receipt_date,
	cum_excess_amount,
	cum_processing_fees,
	cum_bpi,
	cum_insurance_fees,
	cum_foreclosure_fees,
	cum_other_interest,
	cum_instalment,
	cum_instalment_interest,
	cum_instalment_principal,
	cum_late_payment_fees,
	cum_penalty_interest,
	cum_penalty_dues,
	penalty_dues,
	moratorium_month,
	instalment,
	instalment_interest,
	instalment_principal,
	closing_dues_account,
	opening_primary_dues,
	opening_instalment_interest,
	opening_instalment_principal,
	opening_instalment,
	opening_penalty_dues,
	opening_all_dues,
	knocked_off,
	closing_all_dues,
	closing_penalty_dues,
	closing_instalment_principal,
	closing_instalment_interest,
	closing_primary_dues,
	relevant_principal,
	dpd_days,
	dpd_movement,
	dpd,
	previous_dpd,
	loan_line_utilized,
	pending_disbursal,
	loan_line_available,
	first_loan_line_utilization_date,
	cum_loan_line_utilized,
	cum_principal_receipt_count,
	sanction_date,
	first_instalment_month,
	instalment_start_date,
	first_bounce_month,
	loan_closed_date,
	no_of_bounces,
	cum_no_of_bounces,
	loan_end_date,
	loan_end_date_label,
	loan_advance_instalments,
	loan_end_use,
	hypothecation,
	dealer_code,
	nach_status,
	nach_umrn,
	nach_mandate_id,
	disbursal_beneficiary,
	instalments_left,
	instalments_total,
	next_instalment_date,
	next_instalment_due_date,
	next_instalment_amount,
	instalment_end_date,
	closure_mode,
	interest_income_dealer,
	interest_income_bpi,
	interest_income_days,
	fees_income_pf,
	fees_income_foreclosure,
	interest_income_monthly_interest,
	opening_principal,
	closure_amount,
	bounce_month,
	last_nach_status,
	last_nach_date,
	moratorium,
	previous_instalment,
	nach_max_amount,
	nach_process,
	processing_fees,
	nach_frequency,
	dealer_discount,
	eom_dpd_days,
	eom_closing_instalment_principal,
	eom_closing_principal,
	agreement_process,
	agreement_created_date
	) 
	select lapp_id,
	loan_id,
	lan_id,
	customer_id,
	sublan_id,
	nbfc,
	product_or_scheme,
	product_category,
	obs_start,
	obs_end,
	obs_month,
	sublan_loan_tenure,
	sublan_loan_interest_rate,
	sublan_loan_amount,
	sublan_setup_date,
	sublan_advance_instalments,
	sublan_dealer_code,
	sublan_instalment_method,
	sublan_virtual_account,
	sublan_loan_end_date,
	sublan_loan_end_date_label,
	loan_amount,
	loan_tenure,
	interest_rate,
	loan_status,
	final_loan_status,
	loan_city_label,
	loan_product,
	loan_product_label,
	bureau_account_type,
	closing_principal,
	cum_bank_receipts,
	bank_receipts,
	last_bank_receipt_date,
	cum_excess_amount,
	cum_processing_fees,
	cum_bpi,
	cum_insurance_fees,
	cum_foreclosure_fees,
	cum_other_interest,
	cum_instalment,
	cum_instalment_interest,
	cum_instalment_principal,
	cum_late_payment_fees,
	cum_penalty_interest,
	cum_penalty_dues,
	penalty_dues,
	moratorium_month,
	instalment,
	instalment_interest,
	instalment_principal,
	closing_dues_account,
	opening_primary_dues,
	opening_instalment_interest,
	opening_instalment_principal,
	opening_instalment,
	opening_penalty_dues,
	opening_all_dues,
	knocked_off,
	closing_all_dues,
	closing_penalty_dues,
	closing_instalment_principal,
	closing_instalment_interest,
	closing_primary_dues,
	relevant_principal,
	dpd_days,
	dpd_movement,
	dpd,
	previous_dpd,
	loan_line_utilized,
	pending_disbursal,
	loan_line_available,
	first_loan_line_utilization_date,
	cum_loan_line_utilized,
	cum_principal_receipt_count,
	sanction_date,
	first_instalment_month,
	instalment_start_date,
	first_bounce_month,
	loan_closed_date,
	no_of_bounces,
	cum_no_of_bounces,
	loan_end_date,
	loan_end_date_label,
	loan_advance_instalments,
	loan_end_use,
	hypothecation,
	dealer_code,
	nach_status,
	nach_umrn,
	nach_mandate_id,
	disbursal_beneficiary,
	instalments_left,
	instalments_total,
	next_instalment_date,
	next_instalment_due_date,
	next_instalment_amount,
	instalment_end_date,
	closure_mode,
	interest_income_dealer,
	interest_income_bpi,
	interest_income_days,
	fees_income_pf,
	fees_income_foreclosure,
	interest_income_monthly_interest,
	opening_principal,
	closure_amount,
	bounce_month,
	last_nach_status,
	last_nach_date,
	moratorium,
	previous_instalment,
	nach_max_amount,
	nach_process,
	processing_fees,
	nach_frequency,
	dealer_discount,
	eom_dpd_days,
	eom_closing_instalment_principal,
	eom_closing_principal,
	agreement_process,
	agreement_created_date
	from t_loan_monthly_obs;	

	delete from data_store.loan_dataset
	where sublan_id=@object_id;

	insert into data_store.loan_dataset(lapp_id,
	loan_id,
	lan_id,
	customer_id,
	sublan_id,
	nbfc,
	product_or_scheme,
	product_category,
	obs_start,
	obs_end,
	obs_month,
	sublan_loan_tenure,
	sublan_loan_interest_rate,
	sublan_loan_amount,
	sublan_setup_date,
	sublan_advance_instalments,
	sublan_dealer_code,
	sublan_instalment_method,
	sublan_virtual_account,
	sublan_loan_end_date,
	sublan_loan_end_date_label,
	loan_amount,
	loan_tenure,
	interest_rate,
	loan_status,
	final_loan_status,
	loan_city_label,
	loan_product,
	loan_product_label,
	bureau_account_type,
	closing_principal,
	cum_bank_receipts,
	bank_receipts,
	last_bank_receipt_date,
	cum_excess_amount,
	cum_processing_fees,
	cum_bpi,
	cum_insurance_fees,
	cum_foreclosure_fees,
	cum_other_interest,
	cum_instalment,
	cum_instalment_interest,
	cum_instalment_principal,
	cum_late_payment_fees,
	cum_penalty_interest,
	cum_penalty_dues,
	penalty_dues,
	moratorium_month,
	instalment,
	instalment_interest,
	instalment_principal,
	closing_dues_account,
	opening_primary_dues,
	opening_instalment_interest,
	opening_instalment_principal,
	opening_instalment,
	opening_penalty_dues,
	opening_all_dues,
	knocked_off,
	closing_all_dues,
	closing_penalty_dues,
	closing_instalment_principal,
	closing_instalment_interest,
	closing_primary_dues,
	relevant_principal,
	dpd_days,
	dpd_movement,
	dpd,
	previous_dpd,
	loan_line_utilized,
	pending_disbursal,
	loan_line_available,
	first_loan_line_utilization_date,
	cum_loan_line_utilized,
	cum_principal_receipt_count,
	sanction_date,
	first_instalment_month,
	instalment_start_date,
	first_bounce_month,
	loan_closed_date,
	no_of_bounces,
	cum_no_of_bounces,
	loan_end_date,
	loan_end_date_label,
	loan_advance_instalments,
	loan_end_use,
	hypothecation,
	dealer_code,
	nach_status,
	nach_umrn,
	nach_mandate_id,
	disbursal_beneficiary,
	instalments_left,
	instalments_total,
	next_instalment_date,
	next_instalment_due_date,
	next_instalment_amount,
	instalment_end_date,
	closure_mode,
	interest_income_dealer,
	interest_income_bpi,
	interest_income_days,
	fees_income_pf,
	fees_income_foreclosure,
	interest_income_monthly_interest,
	opening_principal,
	closure_amount,
	bounce_month,
	last_nach_status,
	last_nach_date,
	moratorium,
	previous_instalment,
	nach_max_amount,
	nach_process,
	processing_fees,
	nach_frequency,
	dealer_discount,
	eom_dpd_days,
	eom_closing_instalment_principal,
	eom_closing_principal,
	agreement_process,
	agreement_created_date
	)
	select lapp_id,
	loan_id,
	lan_id,
	customer_id,
	sublan_id,
	nbfc,
	product_or_scheme,
	product_category,
	obs_start,
	obs_end,
	obs_month,
	sublan_loan_tenure,
	sublan_loan_interest_rate,
	sublan_loan_amount,
	sublan_setup_date,
	sublan_advance_instalments,
	sublan_dealer_code,
	sublan_instalment_method,
	sublan_virtual_account,
	sublan_loan_end_date,
	sublan_loan_end_date_label,
	loan_amount,
	loan_tenure,
	interest_rate,
	loan_status,
	final_loan_status,
	loan_city_label,
	loan_product,
	loan_product_label,
	bureau_account_type,
	closing_principal,
	cum_bank_receipts,
	bank_receipts,
	last_bank_receipt_date,
	cum_excess_amount,
	cum_processing_fees,
	cum_bpi,
	cum_insurance_fees,
	cum_foreclosure_fees,
	cum_other_interest,
	cum_instalment,
	cum_instalment_interest,
	cum_instalment_principal,
	cum_late_payment_fees,
	cum_penalty_interest,
	cum_penalty_dues,
	penalty_dues,
	moratorium_month,
	instalment,
	instalment_interest,
	instalment_principal,
	closing_dues_account,
	opening_primary_dues,
	opening_instalment_interest,
	opening_instalment_principal,
	opening_instalment,
	opening_penalty_dues,
	opening_all_dues,
	knocked_off,
	closing_all_dues,
	closing_penalty_dues,
	closing_instalment_principal,
	closing_instalment_interest,
	closing_primary_dues,
	relevant_principal,
	dpd_days,
	dpd_movement,
	dpd,
	previous_dpd,
	loan_line_utilized,
	pending_disbursal,
	loan_line_available,
	first_loan_line_utilization_date,
	cum_loan_line_utilized,
	cum_principal_receipt_count,
	sanction_date,
	first_instalment_month,
	instalment_start_date,
	first_bounce_month,
	loan_closed_date,
	no_of_bounces,
	cum_no_of_bounces,
	loan_end_date,
	loan_end_date_label,
	loan_advance_instalments,
	loan_end_use,
	hypothecation,
	dealer_code,
	nach_status,
	nach_umrn,
	nach_mandate_id,
	disbursal_beneficiary,
	instalments_left,
	instalments_total,
	next_instalment_date,
	next_instalment_due_date,
	next_instalment_amount,
	instalment_end_date,
	closure_mode,
	interest_income_dealer,
	interest_income_bpi,
	interest_income_days,
	fees_income_pf,
	fees_income_foreclosure,
	interest_income_monthly_interest,
	opening_principal,
	closure_amount,
	bounce_month,
	last_nach_status,
	last_nach_date,
	moratorium,
	previous_instalment,
	nach_max_amount,
	nach_process,
	processing_fees,
	nach_frequency,
	dealer_discount,
	eom_dpd_days,
	eom_closing_instalment_principal,
	eom_closing_principal,
	agreement_process,
	agreement_created_date
	from t_loan_monthly_obs order by obs_month desc limit 1;
    IF singleQuery=1  THEN
		drop table if exists data_store.loan_daily_obs;
		create table data_store.loan_daily_obs select * from t_loan_daily_obs;
	END IF;
    delete from data_changes.sublan_data_changes where object_id=@object_id;
END$$
DELIMITER ;

DELIMITER $$
CREATE DEFINER=`root`@`10.102.50.174` PROCEDURE `sp_instalments_left`(IN `sp_sublan_id` VARCHAR(255))
    NO SQL
BEGIN
#Adjustments:

SET SESSION TRANSACTION ISOLATION LEVEL READ COMMITTED; 
SET collation_connection = 'utf8_general_ci';
#SET @sublan_id:='SLAN1701367429144555';
SET @sublan_id:=sp_sublan_id;


drop table if exists loan_restructure.all_instalments;
create table loan_restructure.all_instalments
select * from loan_entries where sublan_id=@sublan_id and txn_set='Monthly Instalment' and entry_set='Instalment' and account='Loan Account Monthly Instalment' and debit>0;

ALTER TABLE loan_restructure.all_instalments
ADD running_total decimal(12,2);

UPDATE loan_restructure.all_instalments 
join
(
SELECT ID,debit,sum(debit) OVER (ORDER BY ID ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as cum_instalment from loan_restructure.all_instalments
) as data
on  loan_restructure.all_instalments.ID=data.ID
SET loan_restructure.all_instalments.running_total=data.cum_instalment;





drop table if exists loan_restructure.all_receipts;
create table loan_restructure.all_receipts
select * from loan_entries where sublan_id=@sublan_id and entry_set='Excess Receipt' and debit>0;


drop table if exists loan_restructure.all_receipts_dues;
create table loan_restructure.all_receipts_dues
select * from loan_entries where sublan_id=@sublan_id and entry_set='Receipt' and account='Loan Account Instalment Dues' and credit>0;
ALTER TABLE loan_restructure.all_receipts_dues
ADD running_total decimal(12,2);

UPDATE loan_restructure.all_receipts_dues 
join
(
SELECT ID,credit,sum(credit) OVER (ORDER BY ID ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as cum_instalment from loan_restructure.all_receipts_dues
) as data
on  loan_restructure.all_receipts_dues.ID=data.ID
SET loan_restructure.all_receipts_dues.running_total=data.cum_instalment;



#select id,credit,debit,running_total  from loan_restructure.all_instalments;

SET @total_receipt = (select sum(credit) from loan_restructure.all_receipts_dues);

SET @total_adjusted =(select sum(debit) from loan_restructure.all_instalments where running_total<=@total_receipt);

SET @total_diff=@total_receipt-@total_adjusted; 
select @total_receipt, @total_adjusted,@total_diff ;

drop table if exists loan_restructure.instalments_left;

create table loan_restructure.instalments_left 
select *,debit as left_amount from loan_restructure.all_instalments where running_total>@total_receipt;

UPDATE loan_restructure.instalments_left SET left_amount= left_amount-@total_diff LIMIT 1;	
select id,credit,debit from loan_restructure.instalments_left;
select sum(debit) from loan_restructure.instalments_left;
END$$
DELIMITER ;

DELIMITER $$
CREATE DEFINER=`root`@`10.102.50.174` PROCEDURE `sp_daily_balances_v2`(IN `sp_sublan_id` VARCHAR(100))
    NO SQL
BEGIN
SET NAMES utf8mb4 COLLATE utf8mb4_unicode_ci;

SET @sublan=sp_sublan_id;

Drop temporary TABLE IF EXISTS daily_entries;
CREATE temporary TABLE daily_entries like loantap_in.loan_entries;
insert into daily_entries 
select * from loan_entries where sublan_id=@sublan;
ALTER TABLE daily_entries DROP COLUMN current_meta;

#principal balances
INSERT INTO daily_entries (`ID`, `entry_date`, `txn_set`, `txn_set_id`, `entry_set`, `entry_set_id`, `entry_timestamp`, `debit`, `credit`, `account`, `head`, `reason`, `lan_id`, `sublan_id`, `user`, `voucher_no`) 
with dates as (
	select distinct entry_date as entry_date from daily_entries
),
q1 as (
	select dates.entry_date,ifnull(SUM(debit-credit),0) as balance from dates left join daily_entries 
	on daily_entries.entry_date<=dates.entry_date and account in ('Loan Account Principal')  group by dates.entry_date
)
select concat('9999999990' ,DATE_FORMAT(entry_date,'%Y%m%d')), entry_date, 'Principal Summary', 'ZZZ00000000', 'Principal Summary', 'ZZZ00000000', current_timestamp(), balance, 0, 'Principal Summary', 'Principal Summary', 'Total Outstanding Principal', 'ZZZZ', @sublan, 'ZZZZ', 'ZZZZZ' from q1;


#Instalment Dues balances
INSERT INTO daily_entries (`ID`, `entry_date`, `txn_set`, `txn_set_id`, `entry_set`, `entry_set_id`, `entry_timestamp`, `debit`, `credit`, `account`, `head`, `reason`, `lan_id`, `sublan_id`, `user`, `voucher_no`) 
with dates as (
	select distinct entry_date as entry_date from daily_entries
),
q1 as (
	select dates.entry_date,ifnull(SUM(debit-credit),0) as balance from dates left join daily_entries 
	on daily_entries.entry_date<=dates.entry_date and account in ('Loan Account Instalment Dues')  group by dates.entry_date
)
select concat('9999999991' ,DATE_FORMAT(entry_date,'%Y%m%d')), entry_date, 'Instalment Dues Summary', 'ZZZ00000000', 'Instalment Dues Summary', 'ZZZ00000000', current_timestamp(), balance, 0, 'Instalment Dues Summary', 'Instalment Dues Summary', 'Total Outstanding Instalment Dues', 'ZZZZ', @sublan, 'ZZZZ', 'ZZZZZ' from q1;


#Dues balances
INSERT INTO daily_entries (`ID`, `entry_date`, `txn_set`, `txn_set_id`, `entry_set`, `entry_set_id`, `entry_timestamp`, `debit`, `credit`, `account`, `head`, `reason`, `lan_id`, `sublan_id`, `user`, `voucher_no`) 
with dates as (
	select distinct entry_date as entry_date from daily_entries
),
q1 as (
	select dates.entry_date,ifnull(SUM(debit-credit),0) as balance from dates left join daily_entries 
	on daily_entries.entry_date<=dates.entry_date and account in ('Loan Account Dues')  group by dates.entry_date
)
select concat('9999999992' ,DATE_FORMAT(entry_date,'%Y%m%d')), entry_date, 'Dues Summary', 'ZZZ00000000', 'Dues Summary', 'ZZZ00000000', current_timestamp(), balance, 0, 'Dues Summary', 'Dues Summary', 'Total Outstanding Dues', 'ZZZZ', @sublan, 'ZZZZ', 'ZZZZZ' from q1;


#Excess balances
INSERT INTO daily_entries (`ID`, `entry_date`, `txn_set`, `txn_set_id`, `entry_set`, `entry_set_id`, `entry_timestamp`, `debit`, `credit`, `account`, `head`, `reason`, `lan_id`, `sublan_id`, `user`, `voucher_no`) 
with dates as (
	select distinct entry_date as entry_date from daily_entries
),
q1 as (
	select dates.entry_date,ifnull(SUM(credit-debit),0) as balance from dates left join daily_entries 
	on daily_entries.entry_date<=dates.entry_date and account in ('Loan Account Excess')  group by dates.entry_date
)
select concat('9999999993' ,DATE_FORMAT(entry_date,'%Y%m%d')), entry_date, 'Excess Summary', 'ZZZ00000000', 'Excess Summary', 'ZZZ00000000', current_timestamp(), 0,balance, 'Excess Summary', 'Excess Summary', 'Total Outstanding Excess', 'ZZZZ', @sublan, 'ZZZZ', 'ZZZZZ' from q1;

#select * from daily_entries order by entry_date,ID
END$$
DELIMITER ;

DELIMITER $$
CREATE DEFINER=`root`@`localhost` FUNCTION `validate_mobile_number`(
mobile_num varchar(20)
) RETURNS varchar(20) CHARSET utf8mb4 COLLATE utf8mb4_unicode_ci
    DETERMINISTIC
BEGIN
 DECLARE flag varchar(20); #declare flag to store result
 set flag='invalid';
     if (mobile_num REGEXP '^\\+91[6-9]{1}[0-9]{9}$|^[6-9]{1}[0-9]{9}$|^0[6-9]{1}[0-9]{9}$')  then
              set flag='valid';
      end if ;
 RETURN flag;
END$$
DELIMITER ;

DELIMITER $$
CREATE DEFINER=`root`@`10.102.50.174` PROCEDURE `sp_drawdown_data`(IN `sp_sublan_id` VARCHAR(64), IN `do_insert` INT(1))
    NO SQL
BEGIN
#delimiter //
	SET SESSION TRANSACTION ISOLATION LEVEL READ COMMITTED; 
	start transaction; 
	SET NAMES utf8mb4 COLLATE utf8mb4_unicode_ci; 

	Drop temporary table if exists t_strcl_dataset; 
	CREATE temporary TABLE t_strcl_dataset like data_store.strcl_dataset; 

        Drop temporary table if exists drawdown_entries; 
	create temporary  table drawdown_entries(
	row_num bigint(20) NOT NULL,
	subgroup varchar(50),
	lan_id varchar(50),
	sublan_id varchar(50)
	); 

	Drop temporary table if exists entries; 
	CREATE temporary TABLE entries like loan_entries; 

	Drop temporary table if exists single_drawdown_entries; 
	CREATE temporary TABLE single_drawdown_entries like loan_entries; 

	set @object_id = sp_sublan_id; 
        set @do_insert = do_insert;

	insert into entries select * from loan_entries where sublan_id=@object_id; 
	
        #select * from entries;
       

	#insert all the open onhold subgroup 
	insert into t_strcl_dataset(drawdown_id,lan_id,sublan_id,drawdown_open_date,drawdown_open_amount) 
	with q0 as (
			select * from entries where  account='Open Drawdown' and head='Drawdown' 
	),
	q1 as (
		select min(ID) as ID from q0 group by subgroup
	)
	select  subgroup,lan_id,sublan_id,entry_date as drawdown_open_date,credit as drawdown_open_amount   from entries join q1 on entries.ID=q1.ID group by subgroup;

	
	insert ignore into t_strcl_dataset(drawdown_id,lan_id,sublan_id,drawdown_open_date) 
	with q0 as (
			select * from entries where  account='Open Drawdown Instalment' and head='Drawdown Instalment'  group by subgroup
	),
	q1 as (
		select min(ID) as ID from q0 group by subgroup
	)
	select subgroup,lan_id,sublan_id,entry_date as drawdown_open_date  from entries join q1 on entries.ID=q1.ID group by subgroup;
        
        #select * from t_strcl_dataset;

	UPDATE t_strcl_dataset as update_table 
	join 
	(  
	SELECT drawdown_id,
	ROW_NUMBER() OVER (ORDER BY drawdown_id ASC) AS rn from t_strcl_dataset 
	) as data 
	SET update_table.row_num=data.rn
	where update_table .drawdown_id=data.drawdown_id ;


	select * from t_strcl_dataset;

	set @total = (select count(1) from t_strcl_dataset); 
	
	select * from t_strcl_dataset;
	
	#Now Processing all the drawdown one by one 
	#delimiter // 
	FOR i IN 1..@total 
	DO
	truncate single_drawdown_entries; 
	set @subgroup= (select drawdown_id from t_strcl_dataset where row_num=i);

	insert into single_drawdown_entries 
	select * from entries where txn_set_id in (select distinct txn_set_id from entries  where subgroup=@subgroup); 

	select * from single_drawdown_entries;
	
	#accrual_date
	UPDATE t_strcl_dataset as update_table 
	join 
	(  
		with q0 as (
			select * from single_drawdown_entries where txn_set='drawdown' and entry_set='Drawdown Accrual' and account='Open Drawdown Accrual' and head='Drawdown Accrual'
		),
		q1 as (
			select max(ID) as ID from q0
		)
		select due_date as accrual_date from single_drawdown_entries join q1 on single_drawdown_entries.ID=q1.ID
	) as data 
	SET update_table.accrual_date=data.accrual_date
        where drawdown_id=@subgroup; 
		
	#instalment_posted_date
	UPDATE t_strcl_dataset as update_table 
	join 
	(  
		with q0 as (
			select * from single_drawdown_entries where account='Close Drawdown Instalment' and head='Drawdown Instalment' and reason='Closed an instalment'
	),
	q1 as (
		select max(ID) as ID from q0
	)
		select entry_date as instalment_posted_date,credit as instalment_amount from single_drawdown_entries join q1 on single_drawdown_entries.ID=q1.ID
	) as data 
	SET update_table.instalment_posted_date=data.instalment_posted_date,
	update_table.instalment_amount=data.instalment_amount
	where drawdown_id=@subgroup; 

	#drawdown_disbursal_amount and drawdown_disbursal_date
	UPDATE t_strcl_dataset as update_table 
	join 
	(  
		with q0 as (
			select * from single_drawdown_entries where txn_set='drawdown' and entry_set ='Beneficiary Disbursal'  and account='Loan Account Principal' and head='Loan Account' 
		),
		q1 as (
			select max(ID) as ID from q0
		)
		select entry_date as drawdown_disbursal_date,debit as drawdown_disbursal_amount  from single_drawdown_entries join q1 on single_drawdown_entries.ID=q1.ID
	
	) as data 
	SET 
	update_table.drawdown_disbursal_amount=data.drawdown_disbursal_amount,
	update_table.drawdown_disbursal_date=data.drawdown_disbursal_date
	where drawdown_id=@subgroup; 

        update t_strcl_dataset  set drawdown_open_amount =drawdown_disbursal_amount where drawdown_open_amount is null and drawdown_id=@subgroup; 
	
	#dealer,success_ref and beneficiary
	UPDATE t_strcl_dataset as update_table 
	join 
	(  
		with q0 as (
			select * from single_drawdown_entries where entry_set ='Beneficiary Disbursal'  and head='Dealer' 
		),
		q1 as (
			select max(ID) as ID from q0
		)
		select account as dealer,head as beneficiary,success_ref    from single_drawdown_entries join q1 on single_drawdown_entries.ID=q1.ID
	
	) as data 
	SET 
	update_table.dealer=data.dealer,
	update_table.beneficiary=data.beneficiary,
	update_table.success_ref=data.success_ref
	where drawdown_id=@subgroup; 	
	
	#dealer subvention
	UPDATE t_strcl_dataset as update_table 
	join 
	(  
		with q0 as (
			select * from single_drawdown_entries where entry_set ='Beneficiary Disbursal'  and head='Interest Income' 
		),
		q1 as (
			select max(ID) as ID from q0
		)
		select credit as dealer_subvention from single_drawdown_entries join q1 on single_drawdown_entries.ID=q1.ID
	
	) as data 
	SET 
	update_table.dealer_subvention=data.dealer_subvention
	where drawdown_id=@subgroup; 	
		
	#nach_date and nach_max_date
	UPDATE t_strcl_dataset as update_table 
	join 
	(  
		with q0 as (
			select * from single_drawdown_entries where account='Register Drawdown NACH' and head='Drawdown NACH' 
		),
		q1 as (
			select max(ID) as ID from q0
		)
		select due_date as nach_date from single_drawdown_entries join q1 on single_drawdown_entries.ID=q1.ID

	) as data 
	SET update_table.nach_date=data.nach_date,
	update_table.nach_max_date=DATE_ADD(data.nach_date, INTERVAL 5 Day)
	where drawdown_id=@subgroup; 
	
	/*Disbursal Status*/
	#Open Onhold Status
	UPDATE t_strcl_dataset as update_table 
	join 
	(  
		with q0 as (
			select * from single_drawdown_entries where account='Open Drawdown Onhold' and head='Drawdown Onhold'
		),
		q1 as (
			select max(ID) as ID from q0
		)
		select 1 from single_drawdown_entries join q1 on single_drawdown_entries.ID=q1.ID	
	) as data 
	SET 
	update_table.drawdown_status='Onhold'
	where drawdown_id=@subgroup ; 

	#Closed Status
	UPDATE t_strcl_dataset as update_table 
	join 
	(  
		with q0 as (
			select * from single_drawdown_entries where account='Close Drawdown' and head='Drawdown'
		),
		q1 as (
			select max(ID) as ID from q0
		)
		select 1 from single_drawdown_entries join q1 on single_drawdown_entries.ID=q1.ID	
	) as data 
	SET 
	update_table.drawdown_status='Closed'
	where drawdown_id=@subgroup ; 


	#drawdown_status
	UPDATE t_strcl_dataset as update_table 
	join 
	(  
	with q0 as (
		select max(ID) as ID from single_drawdown_entries where head='Drawdown NACH'
	)
	select (case 
			when account ='Register Drawdown NACH' then 'Disbursed' 
			when account ='Open Drawdown NACH' then 'Instalment Posted'
			when account ='Close Drawdown NACH' then 'NACH Presented'  end
			) as drawdown_status                         
	from single_drawdown_entries join q0 on single_drawdown_entries.ID=q0.ID
	) as data 
	SET 
	update_table.drawdown_status=data.drawdown_status
	where drawdown_id=@subgroup; 
	
	
	/*
	NULL if Not Presented
	Soft Bounce if NACH Not Presented
	
	if txn_set='NACH Update' and entry_set='Receipt' then success
	if txn_set='NACH Update' and entry_set='Drawdown NACH Not Presented'
	if txn_set='NACH Update' and entry_set='Bounce'
	
	*/

	#NACH Status
	UPDATE t_strcl_dataset as update_table 
	join 
	(  
		select 1 from single_drawdown_entries where txn_set='NACH Update' and entry_set='Receipt'
	) as data 
	SET 
	update_table.nach_status='Success'
	where drawdown_id=@subgroup and nach_status ='Not Processed'; 

	#NACH Status
	UPDATE t_strcl_dataset as update_table 
	join 
	(  
		select 1 from single_drawdown_entries where txn_set='NACH Update' and entry_set='Drawdown NACH Not Presented'
	) as data 
	SET 
	update_table.nach_status='Soft Bounce'
	where drawdown_id=@subgroup and nach_status ='Not Processed'; 

	#NACH Status
	UPDATE t_strcl_dataset as update_table 
	join 
	(  
		 select 1 from single_drawdown_entries where txn_set='NACH Update' and entry_set='Bounce'
	) as data 
	SET 
	update_table.nach_status='Bounce'
	where drawdown_id=@subgroup and nach_status ='Not Processed'; 
	select * from t_strcl_dataset ;
	END FOR;

	IF @do_insert=0 THEN
    delete from data_store.strcl_dataset  where sublan_id=@object_id;
	insert into  data_store.strcl_dataset
	select * from t_strcl_dataset;
    END IF;
END$$
DELIMITER ;

DELIMITER $$
CREATE DEFINER=`root`@`10.102.50.174` PROCEDURE `sp_populate_call_center_queue`()
    NO SQL
BEGIN
    SET SESSION TRANSACTION ISOLATION LEVEL READ COMMITTED;
    start transaction;
    set @activity_start_date=get_date_yyyymmdd(30);
    set @buffer_time=30;

    DROP temporary TABLE IF EXISTS t1;
    CREATE temporary TABLE t1 (
    object_id varchar(100) NOT NULL,
    object_type varchar(10) COLLATE utf8mb4_unicode_ci DEFAULT '' COMMENT 'Lead or Lapp',
    mobile_number varchar(100) COLLATE utf8mb4_unicode_ci DEFAULT '',
    open_count int  DEFAULT 0,
    agency varchar(100) COLLATE utf8mb4_unicode_ci DEFAULT '') ENGINE=InnoDB;

    ALTER TABLE t1
    ADD PRIMARY KEY object_id (object_id),
    ADD KEY object_type (object_type),
    ADD KEY mobile_number (mobile_number);

    #Insert the LAPP data
    INSERT IGNORE INTO t1 (object_id, object_type, mobile_number, open_count, agency)

    #selected the object ids which will have cc_queue as initiated
    with q0 as (
        select object_id from lapp_meta where coll_id='lapp' and meta_key='cc_queue' and meta_value='initiated' 
    ),
    #filer the object ids with current stage as incomplete-application
    q1 as (
        Select q0.*,meta_value as lapp_stage from lapp_meta join q0 on lapp_meta.object_id=q0.object_id where coll_id = 'lapp' and meta_key = 'lapp_stage' and meta_value IN ('incomplete-application','channel-app')
    ),
        #filter the applications having lapp_status NOT EQUAL TO 'no-offers'
    q2 as (
        select q1.*,meta_value as lapp_status from q1 join lapp_meta on q1.object_id=lapp_meta.object_id where coll_id='lapp' and meta_key='lapp_status' and meta_value <> 'no-offers'
    ),
    #select and filter the applications having mobile_number 
    q3 as (
        select q2.*,meta_value as mobile_number from q2 join lapp_meta on q2.object_id=lapp_meta.object_id where coll_id='lapp' and meta_key='mobile_number' and meta_value<>'' AND validate_mobile_number(meta_value)='valid'
    ),
    #select and filter the applications having mobile_number_verified as yes
    #q3 as (
    #   select q2.* from q2 join lapp_meta on q2.object_id=lapp_meta.object_id where  IF(lapp_stage='channel-app',(coll_id='lapp' and meta_key='mobile_number_verified' and meta_value='no'),meta_key='mobile_number_verified' and meta_value='yes')
    #),
    #filter the application who's last activity is not beyond given criteria 
    q4 as (
        select q3.*, meta_value as last_activity_date from q3 join lapp_meta on q3.object_id=lapp_meta.object_id where coll_id='lapp' and meta_key='turnaround_end'  and date_format(meta_value, '%Y%m%d')>=@activity_start_date and TIMESTAMPDIFF(MINUTE, meta_value, NOW()) >  @buffer_time 
    ),
    #filter the application who are already present in callcenter_queue
    q5 as (
        select q4.* from q4 left join callcenter_queue on q4.object_id=callcenter_queue.object_id and callcenter_queue.object_type='lapp' where callcenter_queue.object_id is null
    ),
    q6 as (
        Select q5.*,  count(callcenter_queue.mobile_number) as cnt from q5 left join callcenter_queue on q5.mobile_number=callcenter_queue.mobile_number and stage='open' group by q5.mobile_number
    ),
    q7 as (
        Select q6.*, agency from q6 join call_center_agencies on q6.object_id=call_center_agencies.object_id 
    )
    Select object_id, 'lapp', mobile_number, cnt, agency from q7;

    #Insert the LEAD data
    INSERT IGNORE INTO t1 (object_id, object_type, mobile_number, open_count, agency)
    #selected the object ids which will have is_callcentre_queue as yes
    with q0 as (
        select object_id from lead_meta where coll_id = 'lead' and meta_key='cc_queue' and meta_value='initiated'
    ),
    #filer the object ids with current stage as lead
    q1 as (
        select q0.* from q0 join lead_meta on q0.object_id=lead_meta.object_id where coll_id='lead' and meta_key = 'lead_stage' and meta_value = 'lead'
    ),
    #select and filter the application having mobile_number 
    q2 as (
        select q1.*,meta_value as mobile_number from q1 join lead_meta on q1.object_id=lead_meta.object_id where coll_id='lead' and meta_key='mobile_number' and meta_value<>'' AND  validate_mobile_number(meta_value)='valid'
    ),
    #filter the application who's last activity is not beyond given criteria 
    q3 as (
        select q2.*, meta_value as last_activity_date from q2 join lead_meta on q2.object_id=lead_meta.object_id where coll_id='lead' and meta_key='turnaround_end' and date_format(meta_value, '%Y%m%d')>=@activity_start_date 
    ),
    #filter the application who are already present in callcenter_queue
    q4 as (
        select q3.* from q3 left join callcenter_queue on q3.object_id=callcenter_queue.object_id and callcenter_queue.object_type='lead' where callcenter_queue.object_id is null
    ),
    #Discard: Any lead_id already there in lapp table
    q5 as (
        Select q4.* from q4 left join lapp_meta on q4.object_id=lapp_meta.meta_value  and coll_id='lapp' and meta_key='lead_id' where meta_value is null
    ),
    q6 as (
        Select q5.*, count(callcenter_queue.mobile_number) as cnt from q5 left join callcenter_queue on q5.mobile_number=callcenter_queue.mobile_number and stage='open' group by q5.mobile_number 
    ),
    q7 as (
        Select q6.*, agency from q6 join call_center_agencies on q6.object_id=call_center_agencies.object_id 
    )
    Select object_id, 'lead', mobile_number, cnt, agency from q7;

    #remove those objects with same mobile_number and lapp_stage has advanced ahead of incomplete OR channel-app and not rejected in lapp_meta

    create temporary table delete_data
    with q1 as (
        select lapp_meta.object_id,mobile_number from t1 join lapp_meta on t1.mobile_number=lapp_meta.meta_value
    where coll_id='lapp' and meta_key='mobile_number' ),
    q2 as  (
        select q1.* from q1 join lapp_meta on q1.object_id=lapp_meta.object_id where coll_id='lapp' and meta_key='lapp_stage' and meta_value in ('credit-appraisal', 'cpv', 'credit-decision', 'credit-approval')
    ) 
    select mobile_number from q2;

    delete t1 from t1 join delete_data on t1.mobile_number=delete_data.mobile_number;
    
    INSERT IGNORE INTO callcenter_queue(object_id, mobile_number, object_type, status, reason, stage) 
    SELECT object_id, mobile_number, object_type, 'Discarded', 'Duplicate Entry', 'closed' from t1 where object_type='lead' and open_count>1;

    #update the objects in lead_meta
    UPDATE lead_meta AS update_table
    join
    (
        select lead_meta.object_id from t1 join lead_meta on t1.mobile_number=lead_meta.meta_value where coll_id='lead' and meta_key='mobile_number' AND t1.object_type='lead' AND t1.open_count>1
    ) as cc_data
    ON
    update_table.object_id=cc_data.object_id
    SET update_table.meta_value='closed'
    WHERE update_table.meta_key='cc_queue';

    #remove: Those objects which were marked 'Discarded'
    DELETE from t1 where object_type='lead' and open_count>1;

    #Update the records in callcenter_queue for 'lapp' whose mobile_number already exists
    UPDATE callcenter_queue as update_table
    join t1 ON update_table.mobile_number=t1.mobile_number AND t1.object_type='lapp' AND t1.open_count>1
    SET status='Discarded', reason='New Application', stage='closed';

    #update the objects in lapp_meta
    #UPDATE lapp_meta AS update_table
    #join
    #(
    #    select lapp_meta.object_id from t1 join lapp_meta on t1.mobile_number=lapp_meta.meta_value where     #coll_id='lapp' and meta_key='mobile_number'  AND t1.object_type='lapp' AND t1.open_count>1
    #   ) as cc_data
    #ON
    #update_table.object_id=cc_data.object_id
    #SET update_table.meta_value='closed'
    #WHERE update_table.meta_key='cc_queue';*/
    
    UPDATE lapp_meta AS update_table
    join
    (
        select lapp_meta.object_id from t1 join lapp_meta on t1.mobile_number=lapp_meta.meta_value where coll_id='lapp' and meta_key='mobile_number'  AND t1.object_type='lapp' AND t1.open_count>1 order by object_id
    ) as cc_data
    ON
    update_table.object_id=cc_data.object_id
    SET update_table.meta_value='closed'
    WHERE update_table.meta_key='cc_queue';
    

    #remove: Those objects which were marked 'Discarded'
    DELETE FROM t1 WHERE object_type='lapp' AND open_count>1;

    ########### (create fresh entries) Inserting the final set of data #################

    INSERT IGNORE INTO callcenter_queue(object_id, mobile_number, object_type, agency) 
    SELECT object_id , mobile_number, object_type, agency from t1 where open_count=0;

    #update the objects in lapp_meta
    UPDATE lapp_meta as update_table
    join t1 ON update_table.object_id=t1.object_id and t1.open_count=0 AND t1.object_type='lapp'
    SET update_table.meta_value='open'
    WHERE update_table.meta_key='cc_queue' ;

    #update columns in callcenter_queue for LAPPS
    #personal_email
    UPDATE callcenter_queue AS update_table
    join
    (
        select t1.object_id, ifnull(meta_value,'') as meta_value from t1 join lapp_meta on t1.object_id=lapp_meta.object_id where coll_id='lapp' and meta_key='personal_email'
    ) as cc_data
    ON
    update_table.object_id=cc_data.object_id
    SET update_table.personal_email=cc_data.meta_value;

    #channel_code
    UPDATE callcenter_queue AS update_table
    join
    (
        select t1.object_id, ifnull(meta_value,'') as meta_value from t1 join lapp_meta on t1.object_id=lapp_meta.object_id where coll_id='lapp' AND meta_key='channel_code'
    ) as cc_data
    ON
    update_table.object_id=cc_data.object_id
    SET update_table.channel_code=cc_data.meta_value;

    #req_scheme_id
    UPDATE callcenter_queue AS update_table
    join
    (
        select t1.object_id, ifnull(meta_value,'') as meta_value from t1 join lapp_meta on t1.object_id=lapp_meta.object_id where coll_id='lapp' AND meta_key='req_scheme_id'
    ) as cc_data
    ON
    update_table.object_id=cc_data.object_id
    SET update_table.product=cc_data.meta_value;

    #loan_city 
    UPDATE callcenter_queue AS update_table
    join
    (
        select t1.object_id, ifnull(meta_value,'') as meta_value from t1 join lapp_meta on t1.object_id=lapp_meta.object_id where coll_id='lapp' AND meta_key='loan_city'
    ) as cc_data
    ON
    update_table.object_id=cc_data.object_id
    SET update_table.loan_city =cc_data.meta_value;

    #object_date 
    UPDATE callcenter_queue AS update_table
    join
    (
        select t1.object_id, ifnull(meta_value,'') as meta_value from t1 join lapp_meta on t1.object_id=lapp_meta.object_id where coll_id='lapp' AND meta_key='lapp_date'
    ) as cc_data
    ON
    update_table.object_id=cc_data.object_id
    SET update_table.object_date =cc_data.meta_value;

    #update the objects in lead_meta
    UPDATE lead_meta as update_table
    join t1 ON update_table.object_id=t1.object_id AND t1.object_type='lead' and t1.open_count=0
    SET update_table.meta_value='open'
    WHERE update_table.meta_key='cc_queue' ;

    #update columns in callcenter_queue for LEADS
    #personal_email
    UPDATE callcenter_queue AS update_table
    join
    (
        select t1.object_id, ifnull(meta_value,'') as meta_value from t1 join lead_meta on t1.object_id=lead_meta.object_id where coll_id='lead' and meta_key='personal_email'
    ) as cc_data
    ON
    update_table.object_id=cc_data.object_id
    SET update_table.personal_email=cc_data.meta_value;

    #loan_city 
    UPDATE callcenter_queue AS update_table
    join
    (
        select t1.object_id, ifnull(meta_value,'') as meta_value from t1 join lead_meta on t1.object_id=lead_meta.object_id where coll_id='lead' AND meta_key='loan_city'
    ) as cc_data
    ON
    update_table.object_id=cc_data.object_id
    SET update_table.loan_city =cc_data.meta_value;

    #object_date 
    UPDATE callcenter_queue AS update_table
    join
    (
        select t1.object_id, ifnull(meta_value,'') as meta_value from t1 join lead_meta on t1.object_id=lead_meta.object_id where coll_id='lead' AND meta_key='lead_date'
    ) as cc_data
    ON
    update_table.object_id=cc_data.object_id
    SET update_table.object_date =cc_data.meta_value;




    ###########################################################################
    #Discard redundant entries
    ###########################################################################
    DROP TEMPORARY TABLE IF EXISTS temp1;
    CREATE TEMPORARY TABLE temp1 
    with q0 as (
        select object_id from lapp_meta where coll_id='lapp' and meta_key='cc_queue' and meta_value <> 'closed'
    ),
    #filer the object ids with current stage NOT as incomplete-application OR channel-app
    q1 as (
        Select q0.* from lapp_meta join q0 on lapp_meta.object_id=q0.object_id where coll_id = 'lapp' and meta_key = 'lapp_stage' and meta_value NOT IN ('incomplete-application','channel-app')
    )
    select * from q1;

    UPDATE lapp_meta as update_table
    join 
    (
    select object_id from temp1
    ) as data
    on data.object_id=update_table.object_id and update_table.meta_key='cc_queue'
    SET update_table.meta_value='closed';
    
    ###Marking those applications whose cc_queue is 'closed' in lapp_meta but not 'closed' in callcenter_queue
    DROP TEMPORARY TABLE IF EXISTS temp1;
    CREATE TEMPORARY TABLE temp1 
    with q0 as (
        select object_id from lapp_meta where coll_id='lapp' and meta_key='cc_queue' and meta_value='closed'
    ),
    #filer the object ids with current stage NOT as incomplete-application OR channel-app
    q1 as (
        Select q0.* from lapp_meta join q0 on lapp_meta.object_id=q0.object_id where coll_id = 'lapp' and meta_key = 'lapp_stage' and meta_value NOT IN ('incomplete-application','channel-app')
    ),
    q2 as (
        select q1.* from callcenter_queue join q1 on callcenter_queue.object_id=q1.object_id where callcenter_queue.stage <> 'closed'
    )
    select * from q2;

    UPDATE callcenter_queue as update_table
    join 
    (
    select object_id from temp1
    ) as data
    ON data.object_id=update_table.object_id
    SET update_table.stage='closed', update_table.status='do-not-call', update_table.reason='Application moved ahead';
    commit;
    
    END$$
DELIMITER ;

DELIMITER $$
CREATE DEFINER=`root`@`10.102.50.174` PROCEDURE `sp_process_dataset`()
    NO SQL
BEGIN
	#Create temp table full_data
	DROP temporary TABLE IF EXISTS full_data;
	
	#Create temp table full_data
	CREATE temporary TABLE full_data (
		object_id varchar(100) NOT NULL,
		id bigint NOT NULL
	) ENGINE=InnoDB;

	#Alter full_data table add primary key
  	ALTER TABLE full_data
  		ADD PRIMARY KEY object_id (object_id),
  		ADD KEY id (id);
  
  	#Insert the lapp_id into full_data which are updated in lapp_meta
  	INSERT IGNORE INTO full_data (object_id,id)
  		select distinct object_id,id 
  		from data_changes.lapp_data_changes limit 5000;
	
	#Drop t_data temp table if exists
	DROP temporary TABLE IF EXISTS t_data;

	#create new temp table t_data
	CREATE temporary TABLE t_data (
		object_id varchar(100) NOT NULL,
		id bigint NOT NULL
	) ENGINE=InnoDB;

	#update temp table t_data to set primery key
	ALTER TABLE t_data
		ADD PRIMARY KEY object_id (object_id),
		ADD KEY id (id);

	#data into temp table t_data from full_data which are updated and not archived
	INSERT IGNORE INTO t_data (object_id) 
	SELECT full_data.object_id 
  	FROM 
  	full_data left join lapp_meta 
  	ON full_data.object_id=lapp_meta.object_id and meta_key='is_archived' where (meta_value is null or meta_value <> 'yes');

	#----------------------------------------------------------------------------------------------------------
	#Not Undersantd
	DELETE data_store.process_dataset 
	FROM data_store.process_dataset
	JOIN t_data
	on process_dataset.object_id=t_data.object_id where process_name in ('cibil','crif','cibil_trace','crif_trace','EXTCibil','Appraisal','Decisioning','Ops','Approval');

	
	#cibil
	insert ignore into data_store.process_dataset (
		object_id,
		lapp_id,
		process_id,
		user,
		process_name,
		process_type,
		process_subtype,
		completed,
		cost,
		vendor,
		initiated_dt,
		work_started_dt,
		completion_dt
	)
	select 
		t_data.object_id,
		IF(t_data.object_id IS NULL or t_data.object_id = '', 'external', t_data.object_id) as lapp_id,
		process_id,
		updated_by as user,
		process_type as process_name,
		'Bureau' as process_type, 
		NULL as process_subtype, 
		'1' as completed, 
		'49' as cost,
		'cibil' as vendor,
		process_dt as initiated_dt,
		process_dt as work_started_dt,
		process_dt as completion_dt
	from  t_data join process_meta on t_data.object_id=process_meta.lapp_id where process_type='cibil';
		
		
	#crif
	insert ignore into data_store.process_dataset (
		object_id,
		lapp_id,
		process_id,
		user,
		process_name,
		process_type,
		process_subtype,
		completed,
		cost,
		vendor,
		initiated_dt,
		work_started_dt,
		completion_dt
	)
	select 
		t_data.object_id,
		IF(t_data.object_id IS NULL or t_data.object_id = '', 'external', t_data.object_id) as lapp_id,
		process_id,
		updated_by as user,
		process_type as process_name,
		'Bureau' as process_type, 
		NULL as process_subtype, 
		'1' as completed, 
		'22' as cost,
		'crif' as vendor,
		process_dt as initiated_dt,
		process_dt as work_started_dt,
		process_dt as completion_dt
	from  t_data join process_meta on t_data.object_id=process_meta.lapp_id where process_type='crif';
		
	#appraisal
	insert ignore into data_store.process_dataset (
		object_id,
		lapp_id,
		process_id,
		user,
		process_name,
        process_type,
		process_subtype,
		completed,
		initiated_dt,
		work_started_dt,
		completion_dt
	)
	with q1 as 
	(
		select lapp_id as object_id,process_id,updated_by as user,'Appraisal' as process_name, min(process_dt) as initiated_dt,max(process_dt) as last_process_date,process_stage as process_type,process_status as process_subtype from t_data join process_turnaround on process_turnaround.lapp_id=t_data.object_id  and process_stage='credit-appraisal' and process_type='lapp_status_change'  group by object_id having initiated_dt is not null 
	),
	q2 as (
		select q1.*,ifnull(min(process_dt),initiated_dt) as work_started_dt,lapp_id from q1 left join process_turnaround  on process_turnaround.lapp_id=q1.object_id  and process_stage='credit-appraisal' and process_turnaround.process_type='lapp_stage_work_started'  group by q1.object_id 
	),
	q3 as (select q2.*, min(process_dt) as completion_dt, process_status, process_stage from q2 left join process_turnaround on q2.object_id=process_turnaround.lapp_id and process_turnaround.process_type='lapp_status_change' and process_dt > last_process_date  group by q2.object_id )
	select 
		object_id,
		lapp_id,
		process_id,
		user,
		process_name,
        process_type,
		process_subtype,
		IF(completion_dt IS NULL,0,1) AS completed,
		initiated_dt,
		work_started_dt,
		completion_dt
	from q3;
		
	#decision
	insert ignore into data_store.process_dataset (
		object_id,
		lapp_id,
		process_id,
		user,
		process_name,
        process_type,
		process_subtype,
		completed,
		initiated_dt,
		work_started_dt,
		completion_dt
	)
	with q1 as 
    (
        select lapp_id as object_id,process_id,updated_by as user,'Decisioning' as process_name, min(process_dt) as initiated_dt,max(process_dt) as last_process_date,process_stage as process_type,process_status as process_subtype from t_data join process_turnaround on process_turnaround.lapp_id=t_data.object_id  and process_stage='credit-decision' and process_type='lapp_status_change'  group by object_id having initiated_dt is not null 
    ),
    q2 as (
        select q1.*,ifnull(min(process_dt),initiated_dt) as work_started_dt,lapp_id from q1 left join process_turnaround  on process_turnaround.lapp_id=q1.object_id  and process_stage='credit-decision' and process_turnaround.process_type='lapp_stage_work_started'  group by q1.object_id 
    ),
    q3 as (select q2.*, min(process_dt) as completion_dt, process_status, process_stage from q2 left join process_turnaround on q2.object_id=process_turnaround.lapp_id and process_turnaround.process_type='lapp_status_change' and process_dt > last_process_date  group by q2.object_id)
	select 
		object_id,
		lapp_id,
		process_id,
		user,
		process_name,
        process_type,
		process_subtype,
		IF(completion_dt IS NULL,0,1) AS completed,
		initiated_dt,
		work_started_dt,
		completion_dt
	from q3;
		
	#Ops
	insert ignore into data_store.process_dataset(
		object_id,
		lapp_id,
		process_id,
		user,
		process_name,
        process_type,
		process_subtype,
		completed,
		initiated_dt,
		work_started_dt,
		completion_dt
	)
	with q1 as 
	(
		select lapp_id as object_id,process_id,updated_by as user,'Ops' as process_name, min(process_dt) as initiated_dt,max(process_dt) as last_process_date,process_stage as process_type,process_status as process_subtype from t_data join process_turnaround on process_turnaround.lapp_id=t_data.object_id  and process_stage='cpv' and process_type='lapp_status_change'  group by object_id having initiated_dt is not null 
	),
	q2 as (
		select q1.*,ifnull(min(process_dt),initiated_dt) as work_started_dt,lapp_id from q1 left join process_turnaround  on process_turnaround.lapp_id=q1.object_id  and process_stage='cpv' and process_turnaround.process_type='lapp_stage_work_started'  group by q1.object_id 
	),
	q3 as (select q2.*, min(process_dt) as completion_dt, process_status, process_stage from q2 left join process_turnaround on q2.object_id=process_turnaround.lapp_id and process_turnaround.process_type='lapp_status_change' and process_dt > last_process_date  group by q2.object_id )
	select 
		object_id,
		lapp_id,
		process_id,
		user,
		process_name,
        process_type,
		process_subtype,
		IF(completion_dt IS NULL,0,1) AS completed,
		initiated_dt,
		work_started_dt,
		completion_dt
		from q3;
		
		#Approval
		insert ignore into data_store.process_dataset (
		object_id,
		lapp_id,
		process_id,
		user,
		process_name,
        process_type,
		process_subtype,
		completed,
		initiated_dt,
		work_started_dt,
		completion_dt
		)
		with q1 as 
		(
		select lapp_id as object_id,process_id,updated_by as user,'Approval' as process_name, min(process_dt) as initiated_dt,max(process_dt) as last_process_date,process_stage as process_type,process_status as process_subtype from t_data join process_turnaround on process_turnaround.lapp_id=t_data.object_id  and process_stage='credit-approval' and process_type='lapp_status_change'  group by object_id having initiated_dt is not null 
		),
		q2 as (
		select q1.*,ifnull(min(process_dt),initiated_dt) as work_started_dt,lapp_id from q1 left join process_turnaround  on process_turnaround.lapp_id=q1.object_id  and process_stage='credit-approval' and process_turnaround.process_type='lapp_stage_work_started'  group by q1.object_id 
		),
		q3 as (select q2.*, min(process_dt) as completion_dt, process_status, process_stage from q2 left join process_turnaround on q2.object_id=process_turnaround.lapp_id and process_turnaround.process_type='lapp_status_change' and process_dt > last_process_date  group by q2.object_id )
		select 
		object_id,
		lapp_id,
		process_id,
		user,
		process_name,
        process_type,
		process_subtype,
		IF(completion_dt IS NULL,0,1) AS completed,
		initiated_dt,
		work_started_dt,
		completion_dt
		from q3;
#Approval
	insert ignore into data_store.process_dataset (
		object_id,
		lapp_id,
		process_id,
		user,
		process_name,
        process_type,
		process_subtype,
		completed,
		initiated_dt,
		work_started_dt,
		completion_dt
		)
		with q1 as 
		(
		select lapp_id as object_id,process_id,updated_by as user,'Approval' as process_name, min(process_dt) as initiated_dt,max(process_dt) as last_process_date,process_stage as process_type,process_status as process_subtype from t_data join process_turnaround on process_turnaround.lapp_id=t_data.object_id  and process_stage='credit-approval' and process_type='lapp_status_change'  group by object_id having initiated_dt is not null 
		),
		q2 as (
		select q1.*,ifnull(min(process_dt),initiated_dt) as work_started_dt,lapp_id from q1 left join process_turnaround  on process_turnaround.lapp_id=q1.object_id  and process_stage='credit-approval' and process_turnaround.process_type='lapp_stage_work_started'  group by q1.object_id 
		),
		q3 as (select q2.*, min(process_dt) as completion_dt, process_status, process_stage from q2 left join process_turnaround on q2.object_id=process_turnaround.lapp_id and process_turnaround.process_type='lapp_status_change' and process_dt > last_process_date  group by q2.object_id )
		select 
		object_id,
		lapp_id,
		process_id,
		user,
		process_name,
        process_type,
		process_subtype,
		IF(completion_dt IS NULL,0,1) AS completed,
		initiated_dt,
		work_started_dt,
		completion_dt
		from q3;
END$$
DELIMITER ;

DELIMITER $$
CREATE DEFINER=`root`@`10.102.50.174` PROCEDURE `sp_all_apps_dataset`()
BEGIN
 DROP temporary TABLE IF EXISTS full_data;
  CREATE temporary TABLE full_data (
	object_id varchar(100) NOT NULL,
	id bigint NOT NULL
  ) ENGINE=InnoDB;

  ALTER TABLE full_data
  ADD PRIMARY KEY object_id (object_id),
  ADD KEY id (id);

  /*
  Move 5000 Records to the temporary table.
  No point putting duplicate records
  */

 
  INSERT IGNORE INTO full_data (object_id,id)
  select distinct object_id,id 
  from data_changes.lapp_data_changes limit 500;

  /*Create temporary table t_lapp_meta*/
  DROP temporary TABLE IF EXISTS t_lapp_meta;
  CREATE temporary TABLE t_lapp_meta LIKE loantap_in.lapp_meta;

  DROP temporary TABLE IF EXISTS t_data;
  CREATE temporary TABLE t_data (
	object_id varchar(100) NOT NULL,
	lan_id varchar(50) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT '',
	process_id varchar(50) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT '',
	id bigint NOT NULL
  ) ENGINE=InnoDB;

  ALTER TABLE t_data
  ADD PRIMARY KEY object_id (object_id),
  ADD KEY id (id),
  ADD KEY lan_id (lan_id),
  ADD KEY process_id (process_id);

  /*
  Move 1000 Records to the temporary table.
  No point putting duplicate records
  */

  INSERT IGNORE INTO t_data (object_id) 
  SELECT full_data.object_id 
  FROM 
  full_data left join lapp_meta 
  ON full_data.object_id=lapp_meta.object_id AND meta_key='is_archived' WHERE (meta_value is null or meta_value <> 'yes');

  /*Insert data into t_lapp_meta*/
  INSERT INTO t_lapp_meta SELECT lapp_meta.* FROM loantap_in.lapp_meta 
  JOIN t_data ON lapp_meta.object_id = t_data.object_id;

  #Lan ID
  UPDATE t_data
	JOIN (
		SELECT t_data.object_id, ifnull(meta_value,'') AS lan_id FROM t_data 
		JOIN t_lapp_meta ON t_data.object_id=t_lapp_meta.object_id 
		WHERE coll_id='lapp' AND meta_key='lan_id' 
	) AS q1
	ON t_data.object_id=q1.object_id
	set t_data.lan_id=q1.lan_id;

	#Process ID
	UPDATE t_data AS update_table
	JOIN(
		WITH q0 AS (
			SELECT lan_id FROM t_data WHERE lan_id <> ''
		),
		q1 AS (
			SELECT q0.lan_id,sublan.object_id AS sublan_id FROM q0 
			JOIN sublan ON q0.lan_id=sublan.meta_value WHERE coll_id='sublan' AND meta_key='lan_id'
		),
		q2 AS (
			SELECT q1.* FROM q1 JOIN sublan 
			ON q1.sublan_id=sublan.object_id 
			WHERE coll_id='sublan' AND meta_key='ops_process' AND meta_value='active'
		),
		q3 AS (
			SELECT q2.*, meta_value AS process_id FROM q2 
			JOIN sublan ON q2.sublan_id=sublan.object_id WHERE coll_id='sublan' AND meta_key='ops_process_id'
		)

		SELECT lan_id, process_id FROM q3
	) AS lapp_meta
	ON
	update_table.lan_id=lapp_meta.lan_id
	SET update_table.process_id=lapp_meta.process_id;


	INSERT IGNORE INTO data_store.all_apps_dataset(lapp_id, lan_id, process_id) 
	SELECT t_data.object_id, t_data.lan_id, t_data.process_id FROM t_data;
	
	INSERT IGNORE INTO data_store.app_personal_dataset(lapp_id)  
	SELECT t_data.object_id from t_data;

	#Run the template based on activity
	#Lan ID
	UPDATE data_store.all_apps_dataset AS update_table
	JOIN(
		SELECT t_data.object_id, lan_id from t_data
	) as lapp_meta
	ON
	update_table.lapp_id=lapp_meta.object_id
	SET update_table.lan_id=left(lapp_meta.lan_id,50);

	#process_id
	UPDATE data_store.all_apps_dataset AS update_table
	JOIN(
	SELECT t_data.object_id, process_id FROM t_data
	) AS lapp_meta
	ON
	update_table.lapp_id=lapp_meta.object_id
	SET update_table.process_id=left(lapp_meta.process_id,50);

	#channel code
	UPDATE data_store.all_apps_dataset AS update_table
	JOIN(
		SELECT t_data.object_id, ifnull(meta_value,'') AS meta_value FROM t_data 
		JOIN t_lapp_meta using(object_id) 
		WHERE coll_id='lapp' AND meta_key='channel_code'
	) AS lapp_meta
	ON
	update_table.lapp_id=lapp_meta.object_id
	SET update_table.channel_code=left(lapp_meta.meta_value,50);

	#rcode
	UPDATE data_store.all_apps_dataset AS update_table
	JOIN(
		SELECT t_data.object_id, ifnull(meta_value,'') AS meta_value FROM t_data JOIN t_lapp_meta using(object_id) WHERE coll_id='lapp' AND meta_key='rcode'
	) as lapp_meta
	ON
	update_table.lapp_id=lapp_meta.object_id
	SET update_table.referral_code=left(lapp_meta.meta_value,50);

	# requested amount
	UPDATE data_store.all_apps_dataset AS update_table
	JOIN(
		SELECT t_data.object_id, ifnull(meta_value,'') AS meta_value FROM t_data JOIN t_lapp_meta using(object_id) WHERE coll_id='lapp' AND meta_key='req_amount' AND meta_value <> ''
	) as lapp_meta
	ON
	update_table.lapp_id=lapp_meta.object_id
	SET update_table.req_amount=CAST(lapp_meta.meta_value AS DECIMAL(11,0)) ; 


	# requested tenure
	UPDATE data_store.all_apps_dataset AS update_table
	JOIN(
		SELECT t_data.object_id, ifnull(meta_value,'') AS meta_value FROM t_data JOIN t_lapp_meta using(object_id) where coll_id='lapp' AND meta_key='req_tenure' AND meta_value <> ''
	) as lapp_meta
	ON
	update_table.lapp_id=lapp_meta.object_id
	SET update_table.requested_tenure=if(CAST(lapp_meta.meta_value AS UNSIGNED)<240, CAST(lapp_meta.meta_value AS UNSIGNED),0);

	#requested scheme
	UPDATE data_store.all_apps_dataset AS update_table
	join(
		select t_data.object_id, ifnull(meta_value,'') as meta_value from t_data join t_lapp_meta using(object_id) where coll_id='lapp' AND meta_key='req_scheme_id'
	) as lapp_meta

	ON
	update_table.lapp_id=lapp_meta.object_id
	SET update_table.requested_scheme=left(lapp_meta.meta_value,50);
	
	#requested product
	UPDATE data_store.all_apps_dataset AS update_table
	join
	(
	select t_data.object_id, ifnull(meta_value,'') as meta_value from t_data join t_lapp_meta using(object_id) where coll_id='lapp' AND meta_key='req_product_id'
	) as lapp_meta

	ON
	update_table.lapp_id=lapp_meta.object_id
	SET update_table.req_product_id=left(lapp_meta.meta_value,100);

	#loan city
	UPDATE data_store.all_apps_dataset AS update_table
	join
	(
	select t_data.object_id, ifnull(meta_value,'') as meta_value from t_data join t_lapp_meta using(object_id) where coll_id='lapp' AND meta_key='loan_city'
	) as lapp_meta
	ON
	update_table.lapp_id=lapp_meta.object_id
	SET update_table.loan_city=lapp_meta.meta_value;

	#source
	UPDATE data_store.all_apps_dataset AS update_table
	join
	(
	select t_data.object_id, ifnull(meta_value,'') as meta_value from t_data join t_lapp_meta using(object_id) where coll_id='lapp' AND meta_key='utm_source'
	) as lapp_meta
	ON update_table.lapp_id=lapp_meta.object_id
	SET update_table.utm_source=left(lapp_meta.meta_value,50); 

	#medium
	UPDATE data_store.all_apps_dataset AS update_table
	join
	(
	select t_data.object_id, ifnull(meta_value,'') as meta_value from t_data join t_lapp_meta using(object_id) where coll_id='lapp' AND meta_key='utm_medium'
	) as lapp_meta
	ON update_table.lapp_id=lapp_meta.object_id
	SET update_table.utm_medium=left(lapp_meta.meta_value,50); 

	#sourcemedium
	UPDATE data_store.all_apps_dataset AS update_table
	join
	(
	select t_data.object_id, ifnull(meta_value,'') as meta_value from t_data join t_lapp_meta using(object_id) where coll_id='lapp' AND meta_key='utm_source_medium'
	) as lapp_meta
	ON update_table.lapp_id=lapp_meta.object_id
	SET update_table.utm_source_medium=left(lapp_meta.meta_value,50);

	#utm_referrer
	UPDATE data_store.all_apps_dataset AS update_table
	join
	(
	select t_data.object_id, ifnull(meta_value,'') as meta_value from t_data join t_lapp_meta using(object_id) where coll_id='lapp' AND meta_key='utm_referrer'
	) as lapp_meta
	ON update_table.lapp_id=lapp_meta.object_id
	SET update_table.utm_referrer=lapp_meta.meta_value; 

	#utmcampaign
	UPDATE data_store.all_apps_dataset AS update_table
	join
	(
	select t_data.object_id, ifnull(meta_value,'') as meta_value from t_data join t_lapp_meta using(object_id) where coll_id='lapp' AND meta_key='utm_campaign'
	) as lapp_meta
	ON update_table.lapp_id=lapp_meta.object_id
	SET update_table.utm_campaign=left(lapp_meta.meta_value,50); 


	#utm campaign id
	UPDATE data_store.all_apps_dataset AS update_table
	join
	(
	select t_data.object_id, ifnull(meta_value,'') as meta_value from t_data join t_lapp_meta using(object_id) where coll_id='lapp' AND meta_key='utm_campaign_id'
	) as lapp_meta
	ON update_table.lapp_id=lapp_meta.object_id
	SET update_table.utm_campaign_id=left(lapp_meta.meta_value,100);

	#current_stage
	UPDATE data_store.all_apps_dataset AS update_table
	join
	(
	select t_data.object_id, ifnull(meta_value,'') as meta_value from t_data join t_lapp_meta using(object_id) where coll_id='lapp' AND meta_key='lapp_stage'
	) as lapp_meta

	ON
	update_table.lapp_id=lapp_meta.object_id
	SET update_table.current_stage=left(lapp_meta.meta_value,50); 

	#current_status
	UPDATE data_store.all_apps_dataset AS update_table 
	join 
	( 
	select t_data.object_id, ifnull(meta_value,'') as meta_value from t_data left join t_lapp_meta on t_lapp_meta.object_id=t_data.object_id and coll_id='lapp' AND meta_key='lapp_status' 
	) as lapp_meta  
	ON 
	update_table.lapp_id=lapp_meta.object_id 
	SET update_table.current_status=meta_value;

	#rejected_reason
	UPDATE data_store.all_apps_dataset AS update_table
	join
	(
	select t_data.object_id, ifnull(meta_value,'') as meta_value from t_data join t_lapp_meta using(object_id) where coll_id='lapp' AND meta_key='lapp_status_label' AND meta_value <> ''
	) as lapp_meta
	ON
	update_table.lapp_id=lapp_meta.object_id
	SET update_table.rejected_reason=left(lapp_meta.meta_value,100) 
	WHERE update_table.current_stage='rejected';


	#rejected_dt
	UPDATE data_store.all_apps_dataset AS update_table
	join
	(  
	with q0 as (
		select min(process_turnaround.id) as row_id from process_turnaround join t_data on t_data.object_id=process_turnaround.lapp_id and process_type='lapp_status_change' and process_stage='rejected' group by lapp_id
	),
	q1 as (
		select q0.*, process_dt as rejected_date, lapp_id from q0 join process_turnaround on row_id=id
	)
	select rejected_date, lapp_id from q1

	)as lapp_meta
	ON
	update_table.lapp_id=lapp_meta.lapp_id 
	SET update_table.rejected_dt=ifnull(lapp_meta.rejected_date,'');

	#first_activity_dt
	UPDATE data_store.all_apps_dataset AS update_table
	join
	(
	select process_dt
	as first_activity_dt,t_data.object_id from process_turnaround join t_data on process_turnaround.lapp_id=t_data.object_id
	) as lapp_meta
	ON
	update_table.lapp_id=lapp_meta.object_id
	SET update_table.first_activity_dt=ifnull(lapp_meta.first_activity_dt,'');


	#form-completed
	UPDATE data_store.all_apps_dataset AS update_table
	join
	(
	with q0 as (
		select min(process_turnaround.id) as row_id from process_turnaround join t_data on t_data.object_id=process_turnaround.lapp_id and process_type='lapp_status_change' and process_status='form-completed' group by lapp_id
	),
	q1 as (
		select q0.*, process_dt as form_completed_dt, lapp_id from q0 join process_turnaround on row_id=id
	)
	select form_completed_dt, lapp_id from q1
	)as lapp_meta
	ON
	update_table.lapp_id=lapp_meta.lapp_id 
	SET update_table.form_completed_dt=ifnull(lapp_meta.form_completed_dt,'');

	#last_activity_dt
	UPDATE data_store.all_apps_dataset AS update_table
	join
	(
	select process_dt as last_activity_dt,t_data.object_id from process_turnaround join t_data on process_turnaround.lapp_id=t_data.object_id
	) as lapp_meta
	ON
	update_table.lapp_id=lapp_meta.object_id
	SET update_table.last_activity_dt=ifnull(lapp_meta.last_activity_dt,'');

	#orig_login_dt
	UPDATE data_store.all_apps_dataset AS update_table
	join
	(
	with q0 as (select min(process_turnaround.id) as row_id from process_turnaround join t_data on t_data.object_id=process_turnaround.lapp_id and process_type='lapp_status_change' and process_stage='credit-appraisal' group by lapp_id),
		q1 as (select q0.*, process_dt as orig_login_dt, lapp_id from q0 join process_turnaround on row_id=id)
		select orig_login_dt, lapp_id from q1
	)as lapp_meta
	ON
	update_table.lapp_id=lapp_meta.lapp_id
	SET update_table.orig_login_dt=ifnull(lapp_meta.orig_login_dt,'');

	#decision_init_dt
	UPDATE data_store.all_apps_dataset AS update_table
	join
	(
	with q0 as (
		select min(process_turnaround.id) as row_id from process_turnaround join t_data on t_data.object_id=process_turnaround.lapp_id and process_type='lapp_status_change' and process_stage='credit-decision' group by lapp_id
	),
	q1 as (
		select q0.*, process_dt as decision_init_dt, lapp_id from q0 join process_turnaround on row_id=id
	)
	select decision_init_dt, lapp_id from q1		
	)as lapp_meta
	ON
	update_table.lapp_id=lapp_meta.lapp_id 
	SET update_table.decision_init_dt=ifnull(lapp_meta.decision_init_dt,'');

	#ops_init_dt
	UPDATE data_store.all_apps_dataset AS update_table
	join
	(
	with q0 as (
		select min(process_turnaround.id) as row_id from process_turnaround join t_data on t_data.object_id=process_turnaround.lapp_id and process_type='lapp_status_change' and process_stage='cpv' group by lapp_id
	),
	q1 as (
		select q0.*, process_dt as ops_init_dt, lapp_id from q0 join process_turnaround on row_id=id
	)
	select ops_init_dt, lapp_id from q1	
	
	)as lapp_meta
	ON
	update_table.lapp_id=lapp_meta.lapp_id 
	SET update_table.ops_init_dt=ifnull(lapp_meta.ops_init_dt,'');

	#approval_init_dt
	UPDATE data_store.all_apps_dataset AS update_table
	join
	(
	with q0 as (
		select min(process_turnaround.id) as row_id from process_turnaround join t_data on t_data.object_id=process_turnaround.lapp_id and process_type='lapp_status_change' and process_stage='credit-approval' group by lapp_id
	),
	q1 as (
		select q0.*, process_dt as approval_init_dt, lapp_id from q0 join process_turnaround on row_id=id
	)
	select approval_init_dt, lapp_id from q1
	)as lapp_meta
	ON
	update_table.lapp_id=lapp_meta.lapp_id 
	SET update_table.approval_init_dt=ifnull(lapp_meta.approval_init_dt,'');
	
	#decision_completion_time
	UPDATE data_store.all_apps_dataset AS update_table
	join
	(
	with q0 as (
		select min(process_turnaround.id) as row_id from process_turnaround join t_data on t_data.object_id=process_turnaround.lapp_id and process_type='lapp_status_change' and process_status='credit-documentation-initiated' group by lapp_id
	),
	q1 as (
		select q0.*, process_dt as decision_completion_time, lapp_id from q0 join process_turnaround on row_id=id
	)
	select decision_completion_time, lapp_id from q1
	)as lapp_meta
	ON
	update_table.lapp_id=lapp_meta.lapp_id 
	SET update_table.decision_completion_time=ifnull(lapp_meta.decision_completion_time,'');

	#documentation_init_time
	UPDATE data_store.all_apps_dataset AS update_table
	join
	(
	with q0 as (
		select min(process_turnaround.id) as row_id from process_turnaround join t_data on t_data.object_id=process_turnaround.lapp_id and process_type='lapp_status_change' and process_status='waiting-for-documentation' group by lapp_id
	),
	q1 as (
		select q0.*, process_dt as documentation_init_time, lapp_id from q0 join process_turnaround on row_id=id
	)
	select documentation_init_time, lapp_id from q1
	)as lapp_meta
	ON
	update_table.lapp_id=lapp_meta.lapp_id 
	SET update_table.documentation_init_time=ifnull(lapp_meta.documentation_init_time,'');
	
	#approval_done_dt
	UPDATE data_store.all_apps_dataset AS update_table
	join
	(
	with q0 as (
		select min(process_turnaround.id) as row_id from process_turnaround join t_data on t_data.object_id=process_turnaround.lapp_id and process_type='lapp_status_change' and process_status='active' group by lapp_id
	),
	q1 as (
		select q0.*, process_dt as approval_done_dt, lapp_id from q0 join process_turnaround on row_id=id
	)
	select approval_done_dt, lapp_id from q1	
	)as lapp_meta
	ON
	update_table.lapp_id=lapp_meta.lapp_id 
	SET update_table.approval_done_dt=ifnull(lapp_meta.approval_done_dt,'');

	#bv_init_dt
	UPDATE data_store.all_apps_dataset AS update_table
	join
	(
	SELECT t_data.process_id, meta_value FROM t_data JOIN sublan_collection ON t_data.process_id=sublan_collection.object_id WHERE coll_type='bv' AND meta_key='bv_created_date' AND meta_value<>''
	) as process_meta
	ON
	update_table.process_id=process_meta.process_id
	SET update_table.bv_init_dt=process_meta.meta_value;

	#bv_done_dt
	UPDATE data_store.all_apps_dataset AS update_table
	join
	(
	SELECT t_data.process_id, meta_value FROM t_data JOIN sublan_collection ON t_data.process_id=sublan_collection.object_id WHERE coll_type='bv' AND meta_key='bv_closed_date' and meta_value<>''
	) as process_meta

	ON
	update_table.process_id=process_meta.process_id
	SET update_table.bv_done_dt=process_meta.meta_value;

	#bv_vendor
	UPDATE data_store.all_apps_dataset AS update_table
	join
	(
	SELECT t_data.process_id, meta_value FROM t_data JOIN sublan_collection ON t_data.process_id=sublan_collection.object_id WHERE coll_type='bv' AND meta_key='bv_vendor'
	) as process_meta

	ON
	update_table.process_id=process_meta.process_id
	SET update_table.bv_vendor=left(process_meta.meta_value,100);

	#cpv_office_init_dt
	UPDATE data_store.all_apps_dataset AS update_table
	join
	(
	SELECT t_data.process_id, meta_value FROM t_data JOIN sublan_collection ON t_data.process_id=sublan_collection.object_id WHERE coll_type='cpv_office' AND meta_key='cpv_office_created_date'
	) as process_meta

	ON
	update_table.process_id=process_meta.process_id
	SET update_table.cpv_office_init_dt=process_meta.meta_value;

	#cpv_office_done_dt
	UPDATE data_store.all_apps_dataset AS update_table
	join
	(
	SELECT t_data.process_id, meta_value FROM t_data JOIN sublan_collection ON t_data.process_id=sublan_collection.object_id WHERE coll_type='cpv_office' AND meta_key='cpv_office_closed_date'
	) as process_meta

	ON
	update_table.process_id=process_meta.process_id
	SET update_table.cpv_office_done_dt=process_meta.meta_value;


	#cpv_office_vendor
	UPDATE data_store.all_apps_dataset AS update_table
	join
	(
	SELECT t_data.process_id, meta_value FROM t_data JOIN sublan_collection ON t_data.process_id=sublan_collection.object_id WHERE coll_type='cpv_office' AND meta_key='cpv_office_vendor'
	) as process_meta

	ON
	update_table.process_id=process_meta.process_id
	SET update_table.cpv_office_vendor=left(process_meta.meta_value,100);


	#cpv_resi_init_dt
	UPDATE data_store.all_apps_dataset AS update_table
	join
	(
	SELECT t_data.process_id, ifnull(meta_value,'') AS meta_value FROM t_data JOIN sublan_collection ON t_data.process_id=sublan_collection.object_id WHERE coll_type='cpv_resi' AND meta_key='cpv_resi_created_date'
	) as process_meta

	ON
	update_table.process_id=process_meta.process_id
	SET update_table.cpv_resi_init_dt=process_meta.meta_value;

	#cpv_resi_done_dt
	UPDATE data_store.all_apps_dataset AS update_table
	join
	(
	SELECT t_data.process_id, ifnull(meta_value,'') AS meta_value FROM t_data JOIN sublan_collection ON t_data.process_id=sublan_collection.object_id WHERE coll_type='cpv_resi' AND meta_key='cpv_resi_closed_date'
	) as process_meta

	ON
	update_table.process_id=process_meta.process_id
	SET update_table.cpv_resi_done_dt=process_meta.meta_value;

	#cpv_resi_vendor
	UPDATE data_store.all_apps_dataset AS update_table
	join
	(
	SELECT t_data.process_id, ifnull(meta_value,'') AS meta_value FROM t_data JOIN sublan_collection ON t_data.process_id=sublan_collection.object_id WHERE coll_type='cpv_resi' AND meta_key='cpv_resi_vendor'
	) as process_meta

	ON
	update_table.process_id=process_meta.process_id
	SET update_table.cpv_resi_vendor=left(process_meta.meta_value,100);

	#nbfc
	UPDATE data_store.all_apps_dataset AS update_table
	join
	(
	select t_data.object_id, ifnull(meta_value,'') as meta_value from t_data join t_lapp_meta using(object_id) where coll_id='lapp' AND meta_key='nbfc'
	) as lapp_meta
	ON update_table.lapp_id=lapp_meta.object_id
	SET update_table.nbfc=left(lapp_meta.meta_value,100); 

	

	#age
	UPDATE data_store.all_apps_dataset AS update_table
	join
	(
	select t_data.object_id, ifnull(date_format(CONVERT(meta_value, DATE),'%Y%m%d'),'') as meta_value from t_data join t_lapp_meta using(object_id) where coll_id='lapp' AND meta_key='dob' and meta_value <> ''
	) as lapp_meta
	ON update_table.lapp_id=lapp_meta.object_id
	SET update_table.age = if(TIMESTAMPDIFF(YEAR, date(lapp_meta.meta_value), CURDATE())>=21 and TIMESTAMPDIFF(YEAR, date(lapp_meta.meta_value), CURDATE())<100,TIMESTAMPDIFF(YEAR, date(lapp_meta.meta_value), CURDATE()),0);


	#educational qualification
	UPDATE data_store.all_apps_dataset AS update_table
	join
	(
	select t_data.object_id, ifnull(meta_value,'') as meta_value from t_data join t_lapp_meta using(object_id) where coll_id='lapp' AND meta_key='educational_qualification'
	) as lapp_meta

	ON
	update_table.lapp_id=lapp_meta.object_id
	SET update_table.educational_qualification=left(lapp_meta.meta_value,50); 


	#employer name
	UPDATE data_store.all_apps_dataset AS update_table
	join
	(
	select t_data.object_id, ifnull(meta_value,'') as meta_value from t_data join t_lapp_meta using(object_id) where coll_id='lapp' AND meta_key='employer_name'
	) as lapp_meta
	ON update_table.lapp_id=lapp_meta.object_id
	SET update_table.employer_name=left(lapp_meta.meta_value,255); 


	# employment year
	UPDATE data_store.all_apps_dataset AS update_table
	join
	(
	select t_data.object_id, ifnull(meta_value,'') as meta_value from t_data join t_lapp_meta using(object_id) where coll_id='lapp' AND meta_key='employment_year'
	) as lapp_meta
	ON update_table.lapp_id=lapp_meta.object_id
	SET update_table.employment_year=left(lapp_meta.meta_value,50); 


	# fixed income
	UPDATE data_store.all_apps_dataset AS update_table
	join
	(
	select t_data.object_id, ifnull(meta_value,'') as meta_value from t_data join t_lapp_meta using(object_id) where coll_id='lapp' AND meta_key='fixed_income' AND meta_value <> ''
	) as lapp_meta
	ON update_table.lapp_id=lapp_meta.object_id
	SET update_table.fixed_income=CAST(left(lapp_meta.meta_value,6) AS DECIMAL(8,0)) ; 

	# gender
	UPDATE data_store.all_apps_dataset AS update_table
	join
	(
	select t_data.object_id, ifnull(meta_value,'') as meta_value from t_data join t_lapp_meta using(object_id) where coll_id='lapp' AND meta_key='gender'
	) as lapp_meta

	ON
	update_table.lapp_id=lapp_meta.object_id
	SET update_table.gender=left(lapp_meta.meta_value,50);


	# marital status
	UPDATE data_store.all_apps_dataset AS update_table
	join
	(
	select t_data.object_id, ifnull(meta_value,'') as meta_value from t_data join t_lapp_meta using(object_id) where coll_id='lapp' AND meta_key='marital_status'
	) as lapp_meta

	ON
	update_table.lapp_id=lapp_meta.object_id
	SET update_table.marital_status=left(lapp_meta.meta_value,50); 


	# job type
	UPDATE data_store.all_apps_dataset AS update_table
	join
	(
	select t_data.object_id, ifnull(meta_value,'') as meta_value from t_data join t_lapp_meta using(object_id) where coll_id='lapp' AND meta_key='job_type'
	) as lapp_meta

	ON
	update_table.lapp_id=lapp_meta.object_id
	SET update_table.job_type=left(lapp_meta.meta_value,50);

	# cibil score
	UPDATE data_store.all_apps_dataset AS update_table
	join
	(
	select t_data.object_id, ifnull(left(meta_value,3),'') as meta_value from t_data join t_lapp_meta using(object_id) where coll_id='lapp' AND meta_key='cibil_score' AND meta_value <> ''
	) as lapp_meta

	ON
	update_table.lapp_id=lapp_meta.object_id
	SET update_table.cibil_score=if(CAST(lapp_meta.meta_value AS UNSIGNED)<999, CAST(lapp_meta.meta_value AS UNSIGNED),0);

	# crif score
	UPDATE data_store.all_apps_dataset AS update_table
	join
	(
	select t_data.object_id, ifnull(meta_value,'') as meta_value from t_data join t_lapp_meta using(object_id) where coll_id='lapp' AND meta_key='crif_score' AND meta_value <> ''
	) as lapp_meta

	ON
	update_table.lapp_id=lapp_meta.object_id
	SET update_table.crif_score=if(CAST(lapp_meta.meta_value AS UNSIGNED)<999, CAST(lapp_meta.meta_value AS UNSIGNED),0);

	# bank statement read
	UPDATE data_store.all_apps_dataset AS update_table
	join
	(
	select t_data.object_id, ifnull(meta_value,'') as meta_value from t_data join t_lapp_meta using(object_id) where coll_id='lapp' AND meta_key='read_bank_statement'
	) as lapp_meta
	ON
	update_table.lapp_id=lapp_meta.object_id
	SET update_table.bank_statement_read= if( lapp_meta.meta_value = 'full', 1 , 0);


	#auto_reject
	UPDATE data_store.all_apps_dataset AS update_table
	join
	(
	select t_data.object_id, ifnull(meta_value,'') as meta_value from t_data join t_lapp_meta using(object_id) where coll_id='lapp' AND meta_key='auto_rejection_rule'
	) as lapp_meta

	ON
	update_table.lapp_id=lapp_meta.object_id
	SET update_table.auto_reject= if( lapp_meta.meta_value != '', 1 , 0),
	update_table.auto_reject_reason= lapp_meta.meta_value;

	# auto_approval
	UPDATE data_store.all_apps_dataset AS update_table
	join
	(
	select t_data.object_id, ifnull(meta_value,'') as meta_value from t_data join t_lapp_meta using(object_id) where coll_id='lapp' AND meta_key='auto_approval'
	) as lapp_meta

	ON
	update_table.lapp_id=lapp_meta.object_id
	SET update_table.auto_approval= if( lapp_meta.meta_value = 'yes', 1 , 0);

	# squad
	UPDATE data_store.all_apps_dataset AS update_table
	join
	(
	select t_data.object_id, ifnull(meta_value,'') as meta_value from t_data join t_lapp_meta using(object_id) where coll_id='lapp' AND meta_key='delivery_squad' and meta_value <> ''
	) as lapp_meta
	ON
	update_table.lapp_id=lapp_meta.object_id
	SET update_table.squad=left(lapp_meta.meta_value,100); 

	#lead_id
	UPDATE data_store.all_apps_dataset AS update_table
	join
	(
	select t_data.object_id, ifnull(meta_value,'') as meta_value from t_data join t_lapp_meta using(object_id) where coll_id='lapp' AND meta_key='lead_id' and meta_value <> ''
	) as lapp_meta
	ON
	update_table.lapp_id=lapp_meta.object_id
	SET update_table.lead_id=left(lapp_meta.meta_value,100);

	#rate_checked_dt
	UPDATE data_store.all_apps_dataset AS update_table
	join
	(
	select process_dt as rate_checked_dt,t_data.object_id from process_turnaround join t_data on process_turnaround.lapp_id=t_data.object_id and process_status='rate-checked' and process_type='lapp_status_change'
	)as lapp_meta
	ON
	update_table.lapp_id=lapp_meta.object_id
	SET update_table.rate_checked_dt=ifnull(lapp_meta.rate_checked_dt,'');


	
	
	
	
  # utm_device
	UPDATE data_store.all_apps_dataset AS update_table
	join
	(
	select t_data.object_id, ifnull(meta_value,'') as meta_value from t_data join t_lapp_meta using(object_id) where coll_id='lapp' AND meta_key='utm_device'
	) as lapp_meta
	ON
	update_table.lapp_id=lapp_meta.object_id
	SET update_table.utm_device=left(lapp_meta.meta_value,10); 


	#utm_mobile_os
	UPDATE data_store.all_apps_dataset AS update_table
	join
	(
	select t_data.object_id, ifnull(meta_value,'') as meta_value from t_data join t_lapp_meta using(object_id) where coll_id='lapp' AND meta_key='utm_mobile_os'
	) as lapp_meta
	ON
	update_table.lapp_id=lapp_meta.object_id
	SET update_table.utm_mobile_os=left(lapp_meta.meta_value,10); 
  
	#rejected_by
	UPDATE data_store.all_apps_dataset AS update_table
	join
	(
	select updated_by as rejected_by,t_data.object_id from process_turnaround join t_data on process_turnaround.lapp_id=t_data.object_id and process_stage='rejected' and process_type='lapp_status_change'
	)as lapp_meta
	ON
	update_table.lapp_id=lapp_meta.object_id
	SET update_table.rejected_by=ifnull(lapp_meta.rejected_by,'');
   

	#utm_term
	UPDATE data_store.all_apps_dataset AS update_table
	join
	(
	select t_data.object_id, ifnull(meta_value,'') as meta_value from t_data join t_lapp_meta using(object_id) where coll_id='lapp' AND meta_key='utm_term'
	) as lapp_meta
	ON
	update_table.lapp_id=lapp_meta.object_id
	SET update_table.utm_term=lapp_meta.meta_value;
	
	#gclid
	UPDATE data_store.all_apps_dataset AS update_table
	join
	(
	select t_data.object_id, ifnull(meta_value,'') as meta_value from t_data join t_lapp_meta using(object_id) where coll_id='lapp' AND meta_key='gclid'
	) as lapp_meta
	ON
	update_table.lapp_id=lapp_meta.object_id
	SET update_table.gclid=lapp_meta.meta_value;
	
	
	#customer_id
	UPDATE data_store.all_apps_dataset AS update_table
	join
	(
	select t_data.object_id, ifnull(meta_value,'') as meta_value from t_data join t_lapp_meta using(object_id) where coll_id='lapp' AND meta_key='customer_id'
	) as lapp_meta
	ON
	update_table.lapp_id=lapp_meta.object_id
	SET update_table.customer_id=left(lapp_meta.meta_value,50); 
	
	#dob 
	UPDATE data_store.all_apps_dataset AS update_table 
	join
	(
	select t_data.object_id, ifnull(meta_value,'') as meta_value from t_data join t_lapp_meta using(object_id) where coll_id='lapp' AND meta_key='dob' and meta_value<>''
	) as lapp_meta 
	ON update_table.lapp_id=lapp_meta.object_id 
	SET update_table.dob=lapp_meta.meta_value; 

	#employment_duration 
	UPDATE data_store.all_apps_dataset AS update_table 
	join
	(
	select t_data.object_id, ifnull(meta_value,'') as meta_value from t_data join t_lapp_meta using(object_id) where coll_id='lapp' AND meta_key='employment_duration' 
	) as lapp_meta 
	ON update_table.lapp_id=lapp_meta.object_id 
	SET update_table.employment_duration=lapp_meta.meta_value;  

	#emi_outflow 
	UPDATE data_store.all_apps_dataset AS update_table 
	join
	(
	select t_data.object_id, ifnull(meta_value,'') as meta_value from t_data join t_lapp_meta using(object_id) where coll_id='lapp' AND meta_key='emi_outflow' 
	) as lapp_meta 
	ON update_table.lapp_id=lapp_meta.object_id 
	SET update_table.emi_outflow=lapp_meta.meta_value;   

	#rent_outflow 
	UPDATE data_store.all_apps_dataset AS update_table 
	join
	(
	select t_data.object_id, ifnull(meta_value,'') as meta_value from t_data join t_lapp_meta using(object_id) where coll_id='lapp' AND meta_key='rent_outflow' 
	) as lapp_meta 
	ON update_table.lapp_id=lapp_meta.object_id 
	SET update_table.rent_outflow=lapp_meta.meta_value;   

	#utm_ad 
	UPDATE data_store.all_apps_dataset AS update_table 
	join
	(
	select t_data.object_id, ifnull(meta_value,'') as meta_value from t_data join t_lapp_meta using(object_id) where coll_id='lapp' AND meta_key='utm_ad' and meta_value<>''
	) as lapp_meta 
	ON update_table.lapp_id=lapp_meta.object_id 
	SET update_table.utm_ad=left(lapp_meta.meta_value,200);   

	#utm_adset 
	UPDATE data_store.all_apps_dataset AS update_table 
	join
	(
	select t_data.object_id, ifnull(meta_value,'') as meta_value from t_data join t_lapp_meta using(object_id) where coll_id='lapp' AND meta_key='utm_adset' and meta_value<>''
	) as lapp_meta 
	ON update_table.lapp_id=lapp_meta.object_id 
	SET update_table.utm_adset=left(lapp_meta.meta_value,200);   


	#utm_ad_id 
	UPDATE data_store.all_apps_dataset AS update_table 
	join
	(
	select t_data.object_id, ifnull(meta_value,'') as meta_value from t_data join t_lapp_meta using(object_id) where coll_id='lapp' AND meta_key='utm_ad_id' 
	) as lapp_meta 
	ON update_table.lapp_id=lapp_meta.object_id 
	SET update_table.utm_ad_id=lapp_meta.meta_value;   

	#utm_time 
	UPDATE data_store.all_apps_dataset AS update_table 
	join( select t_data.object_id, ifnull(meta_value,'') as meta_value from t_data join t_lapp_meta using(object_id) where coll_id='lapp' AND meta_key='utm_time' 
	) as lapp_meta 
	ON update_table.lapp_id=lapp_meta.object_id 
	SET update_table.utm_time=lapp_meta.meta_value; 
	
	#lapp_datetime
	UPDATE IGNORE data_store.all_apps_dataset AS update_table
	join
	(
	select t_data.object_id, ifnull(meta_value,'') as meta_value from t_data join t_lapp_meta using(object_id) where coll_id='lapp' AND meta_key='lapp_datetime' and meta_value <> ''
	) as lapp_meta
	ON update_table.lapp_id=lapp_meta.object_id
	SET update_table.lapp_datetime=ifnull(STR_TO_DATE(STR_TO_DATE(lapp_meta.meta_value,'%d-%m-%Y %H:%i:%s'),'%Y-%m-%d %H:%i:%s'),lapp_meta.meta_value); 

	#dataset_timestamp
	UPDATE data_store.all_apps_dataset AS update_table
	join
	(
	select t_data.object_id, max(stamp) as meta_value from t_data join t_lapp_meta on t_data.object_id=t_lapp_meta.object_id group by t_lapp_meta.object_id
	) as lapp_meta
	ON update_table.lapp_id=lapp_meta.object_id
	SET update_table.dataset_timestamp=lapp_meta.meta_value; 
	
	#is_archived
	UPDATE data_store.all_apps_dataset AS update_table
	join
	(
	select t_data.object_id, ifnull(meta_value,'no') as meta_value from  t_data left join t_lapp_meta on t_data.object_id=t_lapp_meta.object_id and coll_id='lapp' AND meta_key='is_archived' 
	) as lapp_meta
	ON update_table.lapp_id=lapp_meta.object_id
	SET update_table.is_archived=lapp_meta.meta_value; 
	
	#relationship_manager 
	UPDATE data_store.all_apps_dataset AS update_table
	join
	(
	select t_data.object_id,meta_value from  t_data left join t_lapp_meta on t_data.object_id=t_lapp_meta.object_id and coll_id='lapp' AND meta_key='relationship_manager'  and meta_value <>''
	) as lapp_meta
	ON update_table.lapp_id=lapp_meta.object_id
	SET update_table.relationship_manager=lapp_meta.meta_value; 


	
	
	#kyc_completed_dt
	UPDATE data_store.all_apps_dataset AS update_table
	join
	(
	  select stamp as kyc_completed_dt,t_data.object_id as lapp_id from t_data join t_lapp_meta on t_data.object_id=t_lapp_meta.object_id where coll_id='lapp' and meta_key='kyc_verified' and meta_value = 'yes'
	)as lapp_meta
	ON
	update_table.lapp_id=lapp_meta.lapp_id
	SET update_table.kyc_completed_dt=lapp_meta.kyc_completed_dt;

	#offer_availed_dt
	UPDATE data_store.all_apps_dataset AS update_table
	join
	(
	  select process_turnaround.process_dt as process_dt,process_turnaround.lapp_id from process_turnaround join t_data on t_data.object_id=process_turnaround.lapp_id where process_stage='incomplete-application' and process_type='lapp_status_change' and process_status='availed-offer'
	)as lapp_meta
	ON
	update_table.lapp_id=lapp_meta.lapp_id
	SET update_table.offer_availed_dt=lapp_meta.process_dt;


	#doc_uploaded_dt
	UPDATE data_store.all_apps_dataset AS update_table
	join
	(
	  select process_turnaround.process_dt as process_dt,process_turnaround.lapp_id from process_turnaround join t_data on t_data.object_id=process_turnaround.lapp_id where process_stage='incomplete-application' and process_type='lapp_status_change' and process_status='docs-uploaded'
	)as lapp_meta
	ON
	update_table.lapp_id=lapp_meta.lapp_id
	SET update_table.doc_uploaded_dt=lapp_meta.process_dt;


	#utm_origin
	UPDATE data_store.all_apps_dataset AS update_table
	join
	(
	select t_data.object_id, meta_value from t_data join t_lapp_meta  on t_lapp_meta.object_id=t_data.object_id where coll_id='lapp' AND meta_key='utm_origin' and meta_value  <> ''
	) as lapp_meta

	ON
	update_table.lapp_id=lapp_meta.object_id
	SET update_table.utm_origin=lapp_meta.meta_value;

	#utm_origin_flow
	UPDATE data_store.all_apps_dataset AS update_table
	join
	(
	select t_data.object_id, meta_value from t_data join t_lapp_meta on t_lapp_meta.object_id=t_data.object_id where coll_id='lapp' 		AND meta_key='utm_origin_flow' and meta_value  <> ''
	) as lapp_meta
	ON
	update_table.lapp_id=lapp_meta.object_id
	SET update_table.utm_origin_flow=lapp_meta.meta_value;


	#job_subtype
	UPDATE data_store.all_apps_dataset AS update_table
	join
	(
	select t_data.object_id, meta_value from t_data join t_lapp_meta on  t_lapp_meta.object_id=t_data.object_id  where coll_id='lapp' and meta_key='job_subtype' and meta_value<>''
	) as lapp_meta
	ON
	update_table.lapp_id=lapp_meta.object_id
	SET update_table.job_subtype=lapp_meta.meta_value;

	#end_use
	UPDATE data_store.all_apps_dataset AS update_table
	join
	(
	select t_data.object_id, meta_value from t_data join t_lapp_meta  on  t_lapp_meta.object_id=t_data.object_id where coll_id='lapp' AND meta_key='end_use' and meta_value<>''
	) as lapp_meta
	ON
	update_table.lapp_id=lapp_meta.object_id
	SET update_table.end_use=lapp_meta.meta_value;
  
	#doc_combo
	UPDATE data_store.all_apps_dataset AS update_table
	join
	(
	select t_data.object_id,stamp, meta_value from t_data join t_lapp_meta  on  t_lapp_meta.object_id=t_data.object_id where coll_id='lapp' AND meta_key='doc_combo' and meta_value<>''
	) as lapp_meta
	ON
	update_table.lapp_id=lapp_meta.object_id
	SET update_table.doc_combo=lapp_meta.meta_value;

	#kyc_method
	UPDATE data_store.all_apps_dataset AS update_table
	join
	(
	select t_data.object_id,  meta_value from t_data join t_lapp_meta  on  t_lapp_meta.object_id=t_data.object_id where coll_id='lapp' AND meta_key='kyc_method' and meta_value<>''
	) as lapp_meta
	ON
	update_table.lapp_id=lapp_meta.object_id
	SET update_table.kyc_method=lapp_meta.meta_value;
	
	#outbound_email_count
	UPDATE data_store.all_apps_dataset AS update_table
	join
	(
	select t_data.object_id, email_count as outbound_email_count from t_data join callcenter_queue  on  callcenter_queue.object_id=t_data.object_id where object_type='lapp'
	) as callcenter_queue
	ON update_table.lapp_id=callcenter_queue.object_id
	SET update_table.outbound_email_count=callcenter_queue.outbound_email_count; 

	#outbound_sms_count
	UPDATE data_store.all_apps_dataset AS update_table
	join
	(
	select t_data.object_id, sms_count as outbound_sms_count from t_data join callcenter_queue  on  callcenter_queue.object_id=t_data.object_id where object_type='lapp'
	) as callcenter_queue
	ON update_table.lapp_id=callcenter_queue.object_id
	SET update_table.outbound_sms_count=callcenter_queue.outbound_sms_count; 
	
	
	#outbound_calling_count
	UPDATE data_store.all_apps_dataset AS update_table
	join
	(
	select t_data.object_id, calling_count as outbound_calling_count from t_data join callcenter_queue  on  callcenter_queue.object_id=t_data.object_id where object_type='lapp'
	) as callcenter_queue
	ON update_table.lapp_id=callcenter_queue.object_id
	SET update_table.outbound_calling_count=callcenter_queue.outbound_calling_count; 

	#outbound_calling_start_dt
	UPDATE data_store.all_apps_dataset AS update_table
	join
	(
	select t_data.object_id, ifnull(calling_start_date,'') as outbound_calling_start_dt from t_data join callcenter_queue  on  callcenter_queue.object_id=t_data.object_id where object_type='lapp' and calling_start_date is not NULL
	) as callcenter_queue
	ON update_table.lapp_id=callcenter_queue.object_id
	SET update_table.outbound_calling_start_dt=callcenter_queue.outbound_calling_start_dt; 

	#outbound_calling_end_dt
	UPDATE data_store.all_apps_dataset AS update_table
	join
	(
	select t_data.object_id, ifnull(calling_end_date,'') as outbound_calling_end_dt from t_data join callcenter_queue on  callcenter_queue.object_id=t_data.object_id where object_type='lapp' and calling_end_date is not NULL
	) as callcenter_queue
	ON update_table.lapp_id=callcenter_queue.object_id
	SET update_table.outbound_calling_end_dt=callcenter_queue.outbound_calling_end_dt; 

	#hl_current_outstanding 
	UPDATE data_store.all_apps_dataset AS update_table 
	join(  
	select t_data.object_id,ifnull(sum(current_bal),0) as hl_current_outstanding from t_data join loantap_in.credit_accounts as ca on t_data.object_id=ca.lapp_id 
	where acct_type in ('Housing Loan','Microfinance – Housing Loan','Microfinance Housing Loan') and current_bal<>'' group by ca.lapp_id
	) as credit_accounts 
	ON update_table.lapp_id=credit_accounts.object_id 
	SET update_table.hl_current_outstanding=credit_accounts.hl_current_outstanding;  

	#non_hl_current_outstanding 
	UPDATE data_store.all_apps_dataset AS update_table 
	join(  
	select t_data.object_id,ifnull(sum(current_bal),0) as non_hl_current_outstanding from t_data join loantap_in.credit_accounts as ca on t_data.object_id=ca.lapp_id 
	where current_bal<>'' and acct_type not in ('Housing Loan','Microfinance – Housing Loan','Microfinance Housing Loan', 'Credit Card','Non-Funded Credit Facility','Kisan Credit Card','Secured Credit Card','Corporate Credit Card','Loan on Credit Card') group by ca.lapp_id
	) as credit_accounts 
	ON update_table.lapp_id=credit_accounts.object_id 
	SET update_table.non_hl_current_outstanding=credit_accounts.non_hl_current_outstanding;  

	#cc_current_outstanding 
	UPDATE data_store.all_apps_dataset AS update_table 
	join(  
	select t_data.object_id,ifnull(sum(current_bal),0) as cc_current_outstanding from t_data join loantap_in.credit_accounts as ca on t_data.object_id=ca.lapp_id 
	where current_bal<>'' and acct_type in ('Credit Card','Non-Funded Credit Facility','Kisan Credit Card','Secured Credit Card','Corporate Credit Card','Loan on Credit Card') group by ca.lapp_id
	) as credit_accounts 
	ON update_table.lapp_id=credit_accounts.object_id 
	SET update_table.cc_current_outstanding=credit_accounts.cc_current_outstanding;  
	
	#no_offers_reason
	UPDATE data_store.all_apps_dataset AS update_table
	join
	(
	select t_data.object_id, meta_value from t_data join t_lapp_meta  on  t_lapp_meta.object_id=t_data.object_id where coll_id='lapp' AND meta_key='no_offers_reason' and meta_value<>''
	) as lapp_meta
	ON
	update_table.lapp_id=lapp_meta.object_id
	SET update_table.no_offers_reason=lapp_meta.meta_value;
	
	#no_offers_id
	UPDATE data_store.all_apps_dataset AS update_table
	join
	(
	select t_data.object_id, meta_value from t_data join t_lapp_meta  on  t_lapp_meta.object_id=t_data.object_id where coll_id='lapp' AND meta_key='no_offers_id' and meta_value<>''
	) as lapp_meta
	ON
	update_table.lapp_id=lapp_meta.object_id
	SET update_table.no_offers_id=lapp_meta.meta_value;
	
	#equifax_score
	UPDATE data_store.all_apps_dataset AS update_table
	join
	(
	select t_data.object_id, meta_value from t_data join t_lapp_meta  on  t_lapp_meta.object_id=t_data.object_id where coll_id='lapp' AND meta_key='equifax_score' and meta_value<>''
	) as lapp_meta
	ON
	update_table.lapp_id=lapp_meta.object_id
	SET update_table.equifax_score=lapp_meta.meta_value;
	
	#equifax_writeoff
	UPDATE data_store.all_apps_dataset AS update_table
	join
	(
	select t_data.object_id, meta_value from t_data join t_lapp_meta  on  t_lapp_meta.object_id=t_data.object_id where coll_id='lapp' AND meta_key='equifax_writeoff' and meta_value<>''
	) as lapp_meta
	ON
	update_table.lapp_id=lapp_meta.object_id
	SET update_table.equifax_writeoff=lapp_meta.meta_value;
	
	#equifax_overdue
	UPDATE data_store.all_apps_dataset AS update_table
	join
	(
	select t_data.object_id, meta_value from t_data join t_lapp_meta  on  t_lapp_meta.object_id=t_data.object_id where coll_id='lapp' AND meta_key='equifax_overdue' and meta_value<>''
	) as lapp_meta
	ON
	update_table.lapp_id=lapp_meta.object_id
	SET update_table.equifax_overdue=lapp_meta.meta_value;
	
	#equifax_temp_rejection
	UPDATE data_store.all_apps_dataset AS update_table
	join
	(
	select t_data.object_id, meta_value from t_data join t_lapp_meta  on  t_lapp_meta.object_id=t_data.object_id where coll_id='lapp' AND meta_key='equifax_temp_rejection' and meta_value<>''
	) as lapp_meta
	ON
	update_table.lapp_id=lapp_meta.object_id
	SET update_table.equifax_temp_rejection=lapp_meta.meta_value;
	
	#equifax_temp_reason
	UPDATE data_store.all_apps_dataset AS update_table
	join
	(
	select t_data.object_id, meta_value from t_data join t_lapp_meta  on  t_lapp_meta.object_id=t_data.object_id where coll_id='lapp' AND meta_key='equifax_temp_reason' and meta_value<>''
	) as lapp_meta
	ON
	update_table.lapp_id=lapp_meta.object_id
	SET update_table.equifax_temp_reason=lapp_meta.meta_value;

	
	#mapp_id
	UPDATE data_store.all_apps_dataset AS update_table
	join
	(
	select t_data.object_id, ifnull(meta_value,'') as meta_value from t_data join t_lapp_meta on t_data.object_id=t_lapp_meta.object_id where coll_id='lapp' AND meta_key='mapp_id'
	) as lapp_meta
	ON update_table.lapp_id=lapp_meta.object_id
	SET update_table.mapp_id=left(lapp_meta.meta_value,50);
	
    #credit_score
    UPDATE data_store.all_apps_dataset AS update_table
    join
    (
        select t_data.object_id, meta_value from t_data join t_lapp_meta on t_data.object_id=t_lapp_meta.object_id where coll_id='lapp' AND meta_key='credit_score' AND meta_value <> ''
    ) as lapp_meta
    ON
    update_table.lapp_id=lapp_meta.object_id
    SET update_table.credit_score=ifnull(lapp_meta.meta_value,0);
    
    #non_financial_credit_score
    UPDATE data_store.all_apps_dataset AS update_table
    join
    (
        select t_data.object_id, meta_value from t_data join t_lapp_meta on t_data.object_id=t_lapp_meta.object_id where coll_id='lapp' AND meta_key='non_financial_credit_score' AND meta_value <> ''
    ) as lapp_meta
    ON
    update_table.lapp_id=lapp_meta.object_id
    SET update_table.non_financial_credit_score=ifnull(lapp_meta.meta_value,0);
    
    #preferred_agreement_method
    UPDATE data_store.all_apps_dataset AS update_table
    join
    (
        select t_data.object_id, meta_value from t_data join t_lapp_meta on t_data.object_id=t_lapp_meta.object_id where coll_id='lapp' AND meta_key='preferred_agreement_method'
    ) as lapp_meta
    ON
    update_table.lapp_id=lapp_meta.object_id
    SET update_table.preferred_agreement_method=ifnull(lapp_meta.meta_value,'');
	
	#lt_generic_fail 
    UPDATE data_store.all_apps_dataset AS update_table
    join
    (
        select t_data.object_id, meta_value from t_data join t_lapp_meta on t_data.object_id=t_lapp_meta.object_id where coll_id='lapp' AND meta_key='lt_generic_fail '
    ) as lapp_meta
    ON
    update_table.lapp_id=lapp_meta.object_id
    SET update_table.lt_generic_fail =ifnull(lapp_meta.meta_value,'');
	
	#full_name 
	UPDATE data_store.app_personal_dataset AS update_table 
	join(  
	select t_data.object_id, ifnull(meta_value,'') as meta_value from t_data join t_lapp_meta using(object_id) where coll_id='lapp' AND meta_key='full_name' 
	) as lapp_meta 
	ON update_table.lapp_id=lapp_meta.object_id 
	SET update_table.full_name=lapp_meta.meta_value;  

	#personal_email 
	UPDATE data_store.app_personal_dataset AS update_table 
	join
	(
	select t_data.object_id, ifnull(meta_value,'') as meta_value from t_data join t_lapp_meta using(object_id) where coll_id='lapp' AND meta_key='personal_email' 
	) as lapp_meta 
	ON update_table.lapp_id=lapp_meta.object_id 
	SET update_table.personal_email=lapp_meta.meta_value;  

	#mobile_number 
	UPDATE data_store.app_personal_dataset AS update_table 
	join
	(
	select t_data.object_id, ifnull(meta_value,'') as meta_value from t_data join t_lapp_meta using(object_id) where coll_id='lapp' AND meta_key='mobile_number' 
	) as lapp_meta 
	ON update_table.lapp_id=lapp_meta.object_id 
	SET update_table.mobile_number=left(lapp_meta.meta_value,15); 

	#pan_card 
	UPDATE data_store.app_personal_dataset AS update_table 
	join( select t_data.object_id, ifnull(meta_value,'') as meta_value from t_data join t_lapp_meta using(object_id) where coll_id='lapp' AND meta_key='pan_card' and meta_value<>'' 
	) as lapp_meta 
	ON update_table.lapp_id=lapp_meta.object_id 
	SET update_table.pan_card=left(lapp_meta.meta_value,10); 

	#home_addr_line1 
	UPDATE data_store.app_personal_dataset AS update_table 
	join
	(
	select t_data.object_id, ifnull(meta_value,'') as meta_value from t_data join t_lapp_meta using(object_id) where coll_id='lapp' AND meta_key='home_addr_line1' 
	) as lapp_meta 
	ON update_table.lapp_id=lapp_meta.object_id 
	SET update_table.home_addr_line1=lapp_meta.meta_value; 

	#home_addr_line2 
	UPDATE data_store.app_personal_dataset AS update_table 
	join
	(
	select t_data.object_id, ifnull(meta_value,'') as meta_value from t_data join t_lapp_meta using(object_id) where coll_id='lapp' AND meta_key='home_addr_line2' 
	) as lapp_meta 
	ON update_table.lapp_id=lapp_meta.object_id 
	SET update_table.home_addr_line2=lapp_meta.meta_value;  

	#home_city 
	UPDATE data_store.app_personal_dataset AS update_table 
	join
	(
	select t_data.object_id, ifnull(meta_value,'') as meta_value from t_data join t_lapp_meta using(object_id) where coll_id='lapp' AND meta_key='home_city' 
	) as lapp_meta 
	ON update_table.lapp_id=lapp_meta.object_id 
	SET update_table.home_city=lapp_meta.meta_value;  
	
	#home_zipcode 
	UPDATE data_store.app_personal_dataset AS update_table 
	join
	(
	select t_data.object_id, ifnull(meta_value,'') as meta_value from t_data join t_lapp_meta using(object_id) where coll_id='lapp' AND meta_key='home_zipcode' and meta_value<>''
	) as lapp_meta 
	ON update_table.lapp_id=lapp_meta.object_id 
	SET update_table.home_zipcode=if(CHAR_LENGTH(lapp_meta.meta_value)<=6,CAST(lapp_meta.meta_value AS UNSIGNED),0);  


	#office_addr_line1 
	UPDATE data_store.app_personal_dataset AS update_table 
	join( select t_data.object_id, ifnull(meta_value,'') as meta_value from t_data join t_lapp_meta using(object_id) where coll_id='lapp' AND meta_key='office_addr_line1' 
	) as lapp_meta 
	ON update_table.lapp_id=lapp_meta.object_id 
	SET update_table.office_addr_line1=lapp_meta.meta_value;  

	#office_addr_line2 
	UPDATE data_store.app_personal_dataset AS update_table 
	join
	(
	select t_data.object_id, ifnull(meta_value,'') as meta_value from t_data join t_lapp_meta using(object_id) where coll_id='lapp' AND meta_key='office_addr_line2' 
	) as lapp_meta 
	ON update_table.lapp_id=lapp_meta.object_id 
	SET update_table.office_addr_line2=lapp_meta.meta_value;  

	#office_city 
	UPDATE data_store.app_personal_dataset AS update_table 
	join
	(
	select t_data.object_id, ifnull(meta_value,'') as meta_value from t_data join t_lapp_meta using(object_id) where coll_id='lapp' AND meta_key='office_city' 
	) as lapp_meta 
	ON update_table.lapp_id=lapp_meta.object_id 
	SET update_table.office_city=lapp_meta.meta_value;  

	#office_zipcode 
	UPDATE data_store.app_personal_dataset AS update_table 
	join( select t_data.object_id, ifnull(meta_value,'') as meta_value from t_data join t_lapp_meta using(object_id) where coll_id='lapp' AND meta_key='office_zipcode' and meta_value<>''
	) as lapp_meta 
	ON update_table.lapp_id=lapp_meta.object_id 
	SET update_table.office_zipcode=if(CHAR_LENGTH(lapp_meta.meta_value)<=6,CAST(lapp_meta.meta_value AS UNSIGNED),0);  

	#office_landline_no 
	UPDATE data_store.app_personal_dataset AS update_table 
	join
	(
	select t_data.object_id, ifnull(meta_value,'') as meta_value from t_data join t_lapp_meta using(object_id) where coll_id='lapp' AND meta_key='office_landline_no' 
	) as lapp_meta 
	ON update_table.lapp_id=lapp_meta.object_id 
	SET update_table.office_landline_no=lapp_meta.meta_value;  
	
	#official_email 
	UPDATE data_store.app_personal_dataset AS update_table 
	join
	(
	select t_data.object_id, ifnull(meta_value,'') as meta_value from t_data join t_lapp_meta using(object_id) where coll_id='lapp' AND meta_key='official_email' 
	) as lapp_meta 
	ON update_table.lapp_id=lapp_meta.object_id 
	SET update_table.official_email=lapp_meta.meta_value;
	
	#company_mca_id
	UPDATE data_store.app_personal_dataset AS update_table 
	join
	(
	select t_data.object_id, ifnull(meta_value,'') as meta_value from t_data join t_lapp_meta using(object_id) where coll_id='lapp' AND meta_key='company_mca_id' 
	) as lapp_meta 
	ON update_table.lapp_id=lapp_meta.object_id 
	SET update_table.company_mca_id=left(lapp_meta.meta_value,255);
	
	#ip 
	UPDATE data_store.app_personal_dataset AS update_table 
	join
	(
	select t_data.object_id, ifnull(meta_value,'') as meta_value from t_data join t_lapp_meta using(object_id) where coll_id='lapp' AND meta_key='ip' 
	) as lapp_meta 
	ON update_table.lapp_id=lapp_meta.object_id 
	SET update_table.ip=lapp_meta.meta_value;

	#salary_account_no 
	UPDATE data_store.app_personal_dataset AS update_table 
	join
	(
	select t_data.object_id, ifnull(meta_value,'') as meta_value from t_data join t_lapp_meta using(object_id) where coll_id='lapp' AND meta_key='salary_account_no' 
	) as lapp_meta 
	ON update_table.lapp_id=lapp_meta.object_id 
	SET update_table.salary_account_no=left(lapp_meta.meta_value,20);  

	#salary_bank_name 
	UPDATE data_store.app_personal_dataset AS update_table 
	join
	(
	select t_data.object_id, ifnull(meta_value,'') as meta_value from t_data join t_lapp_meta using(object_id) where coll_id='lapp' AND meta_key='salary_bank_name' 
	) as lapp_meta 
	ON update_table.lapp_id=lapp_meta.object_id 
	SET update_table.salary_bank_name=lapp_meta.meta_value;  


	#ecs_cust_name 
	UPDATE data_store.app_personal_dataset AS update_table 
	join
	(
	select t_data.object_id, ifnull(meta_value,'') as meta_value from t_data join t_lapp_meta using(object_id) where coll_id='lapp' AND meta_key='ecs_cust_name' 
	) as lapp_meta 
	ON update_table.lapp_id=lapp_meta.object_id 
	SET update_table.ecs_cust_name=lapp_meta.meta_value;  

	#ecs_bank 
	UPDATE data_store.app_personal_dataset AS update_table 
	join
	(
	select t_data.object_id, ifnull(meta_value,'') as meta_value from t_data join t_lapp_meta using(object_id) where coll_id='lapp' AND meta_key='ecs_bank' 
	) as lapp_meta 
	ON update_table.lapp_id=lapp_meta.object_id 
	SET update_table.ecs_bank=lapp_meta.meta_value;  

	#ecs_bank_acc_no 
	UPDATE data_store.app_personal_dataset AS update_table 
	join( select t_data.object_id, ifnull(meta_value,'') as meta_value from t_data join t_lapp_meta using(object_id) where coll_id='lapp' AND meta_key='ecs_bank_acc_no' and meta_value<>''
	) as lapp_meta 
	ON update_table.lapp_id=lapp_meta.object_id 
	SET update_table.ecs_bank_acc_no=lapp_meta.meta_value;  

	#ecs_ifsc_code 
	UPDATE data_store.app_personal_dataset AS update_table 
	join( select t_data.object_id, ifnull(meta_value,'') as meta_value from t_data join t_lapp_meta using(object_id) where coll_id='lapp' AND meta_key='ecs_ifsc_code' and meta_value<>''
	) as lapp_meta 
	ON update_table.lapp_id=lapp_meta.object_id 
	SET update_table.ecs_ifsc_code=left(lapp_meta.meta_value,20); 

	#business_name 
	UPDATE data_store.app_personal_dataset AS update_table 
	join( select t_data.object_id, ifnull(meta_value,'') as meta_value from t_data join t_lapp_meta using(object_id) where coll_id='lapp' AND meta_key='business_name' and meta_value<>''
	) as lapp_meta 
	ON update_table.lapp_id=lapp_meta.object_id 
	SET update_table.business_name=lapp_meta.meta_value;   

	#business_address 
	UPDATE data_store.app_personal_dataset AS update_table 
	join( select t_data.object_id, ifnull(meta_value,'') as meta_value from t_data join t_lapp_meta using(object_id) where coll_id='lapp' AND meta_key='business_address' 
	) as lapp_meta 
	ON update_table.lapp_id=lapp_meta.object_id 
	SET update_table.business_address=lapp_meta.meta_value;   

	#business_addr_line1 
	UPDATE data_store.app_personal_dataset AS update_table 
	join( select t_data.object_id, ifnull(meta_value,'') as meta_value from t_data join t_lapp_meta using(object_id) where coll_id='lapp' AND meta_key='business_addr_line1' 
	) as lapp_meta 
	ON update_table.lapp_id=lapp_meta.object_id 
	SET update_table.business_addr_line1=lapp_meta.meta_value;   

	#business_addr_line2 
	UPDATE data_store.app_personal_dataset AS update_table 
	join( select t_data.object_id, ifnull(meta_value,'') as meta_value from t_data join t_lapp_meta using(object_id) where coll_id='lapp' AND meta_key='business_addr_line2' 
	) as lapp_meta 
	ON update_table.lapp_id=lapp_meta.object_id 
	SET update_table.business_addr_line2=lapp_meta.meta_value;   

	#business_city 
	UPDATE data_store.app_personal_dataset AS update_table 
	join( select t_data.object_id, ifnull(meta_value,'') as meta_value from t_data join t_lapp_meta using(object_id) where coll_id='lapp' AND meta_key='business_city' 
	) as lapp_meta 
	ON update_table.lapp_id=lapp_meta.object_id 
	SET update_table.business_city=lapp_meta.meta_value;

	#business_zipcode 
	UPDATE data_store.app_personal_dataset AS update_table 
	join( select t_data.object_id, ifnull(meta_value,'') as meta_value from t_data join t_lapp_meta using(object_id) where coll_id='lapp' AND meta_key='business_zipcode' and meta_value<>''
	) as lapp_meta 
	ON update_table.lapp_id=lapp_meta.object_id 
	SET update_table.business_zipcode=if(CHAR_LENGTH(lapp_meta.meta_value)<=6,CAST(lapp_meta.meta_value AS UNSIGNED),0);   

	#business_monthly_sales 
	UPDATE data_store.app_personal_dataset AS update_table 
	join( select t_data.object_id, ifnull(meta_value,'') as meta_value from t_data join t_lapp_meta using(object_id) where coll_id='lapp' AND meta_key='business_monthly_sales' 
	) as lapp_meta 
	ON update_table.lapp_id=lapp_meta.object_id 
	SET update_table.business_monthly_sales=lapp_meta.meta_value;   

	#business_monthly_gp 
	UPDATE data_store.app_personal_dataset AS update_table 
	join( select t_data.object_id, ifnull(meta_value,'') as meta_value from t_data join t_lapp_meta using(object_id) where coll_id='lapp' AND meta_key='business_monthly_gp' 
	) as lapp_meta 
	ON update_table.lapp_id=lapp_meta.object_id 
	SET update_table.business_monthly_gp=lapp_meta.meta_value;   

	#business_monthly_np 
	UPDATE data_store.app_personal_dataset AS update_table 
	join( select t_data.object_id, ifnull(meta_value,'') as meta_value from t_data join t_lapp_meta using(object_id) where coll_id='lapp' AND meta_key='business_monthly_np' 
	) as lapp_meta 
	ON update_table.lapp_id=lapp_meta.object_id 
	SET update_table.business_monthly_np=lapp_meta.meta_value;  
	
	#fathers_name 
	UPDATE data_store.app_personal_dataset AS update_table
	join
	(
	select t_data.object_id,meta_value from  t_data left join t_lapp_meta on t_data.object_id=t_lapp_meta.object_id and coll_id='lapp' AND meta_key='fathers_name'  and meta_value <>''
	) as lapp_meta
	ON update_table.lapp_id=lapp_meta.object_id
	SET update_table.fathers_name=lapp_meta.meta_value; 
	
	#mothers_maiden_name 
	UPDATE data_store.app_personal_dataset AS update_table
	join
	(
	select t_data.object_id,meta_value from  t_data left join t_lapp_meta on t_data.object_id=t_lapp_meta.object_id and coll_id='lapp' AND meta_key='mothers_maiden_name'  and meta_value <>''
	) as lapp_meta
	ON update_table.lapp_id=lapp_meta.object_id
	SET update_table.mothers_maiden_name=lapp_meta.meta_value; 
	
	#aadhar_uid  
	UPDATE data_store.app_personal_dataset AS update_table
	join
	(
	select t_data.object_id,meta_value from  t_data left join t_lapp_meta on t_data.object_id=t_lapp_meta.object_id and coll_id='lapp' AND meta_key='aadhar_uid '  and meta_value <>''
	) as lapp_meta
	ON update_table.lapp_id=lapp_meta.object_id
	SET update_table.aadhar_uid=lapp_meta.meta_value; 
	
	#permanent_address1  
	UPDATE data_store.app_personal_dataset AS update_table
	join
	(
	select t_data.object_id,meta_value from  t_data left join t_lapp_meta on t_data.object_id=t_lapp_meta.object_id and coll_id='lapp' AND meta_key='permanent_addr_line1'  and meta_value <>''
	) as lapp_meta
	ON update_table.lapp_id=lapp_meta.object_id
	SET update_table.permanent_address1=lapp_meta.meta_value; 
	
	#permanent_address2  
	UPDATE data_store.app_personal_dataset AS update_table
	join
	(
	select t_data.object_id,meta_value from  t_data left join t_lapp_meta on t_data.object_id=t_lapp_meta.object_id and coll_id='lapp' AND meta_key='permanent_addr_line2'  and meta_value <>''
	) as lapp_meta
	ON update_table.lapp_id=lapp_meta.object_id
	SET update_table.permanent_address2=lapp_meta.meta_value; 
	
	#permanent_city  
	UPDATE data_store.app_personal_dataset AS update_table
	join
	(
	select t_data.object_id,meta_value from  t_data left join t_lapp_meta on t_data.object_id=t_lapp_meta.object_id and coll_id='lapp' AND meta_key='permanent_city'  and meta_value <>''
	) as lapp_meta
	ON update_table.lapp_id=lapp_meta.object_id
	SET update_table.permanent_city=lapp_meta.meta_value; 
	
	#permanent_zipcode  
	UPDATE data_store.app_personal_dataset AS update_table
	join
	(
	select t_data.object_id,meta_value from  t_data left join t_lapp_meta on t_data.object_id=t_lapp_meta.object_id and coll_id='lapp' AND meta_key='permanent_zipcode'  and meta_value <>''
	) as lapp_meta
	ON update_table.lapp_id=lapp_meta.object_id
	SET update_table.permanent_zipcode=lapp_meta.meta_value;
    
  delete data_changes.lapp_data_changes from data_changes.lapp_data_changes join full_data on lapp_data_changes.object_id=full_data.object_id;
    
    delete data_changes.lapp_data_changes from data_changes.lapp_data_changes join full_data on lapp_data_changes.object_id=full_data.object_id;

END$$
DELIMITER ;

DELIMITER $$
CREATE DEFINER=`root`@`10.102.50.174` PROCEDURE `sp_loan_dataset_vikas`(IN `sp_sublan_id` VARCHAR(64), IN `singleQuery` INT(0))
    COMMENT 'This sp will except two paramets sp_sublan_id and singleQuery'
BEGIN
SET SESSION TRANSACTION ISOLATION LEVEL READ COMMITTED;
start transaction;
SET NAMES utf8mb4 COLLATE utf8mb4_unicode_ci;
	
	Drop temporary table if exists t_loan_monthly_obs;
	CREATE temporary TABLE t_loan_monthly_obs like data_store.loan_monthly_obs;
	Drop temporary table if exists entries;
	CREATE temporary TABLE entries like loantap_in.loan_entries;

	Drop temporary table if exists t_loan_daily_obs;
	/*Create skeleton for daily obs*/
	CREATE TEMPORARY TABLE t_loan_daily_obs (
		row_num bigint(20) NOT NULL,
		ID bigint(20) unsigned NOT NULL AUTO_INCREMENT,
		obs_month char(6) NULL,
		obs_date date NOT NULL,
		added_primary_dues decimal(12,2)  DEFAULT 0,
		added_instalment_interest decimal(12,2)  DEFAULT 0,
		added_instalment_principal decimal(12,2)  DEFAULT 0,
		added_instalment decimal(12,2) DEFAULT 0,
		added_penalty_dues decimal(12,2) DEFAULT 0,
		added_penalty_waiver decimal(12,2)  DEFAULT 0,
		added_eom_adjustment decimal(12,2)  DEFAULT 0,
		added_all_dues decimal(12,2) GENERATED ALWAYS AS (added_primary_dues + added_instalment + added_penalty_dues + added_eom_adjustment) STORED,
		closing_dues_account decimal(12,2)  DEFAULT 0,
		unadjusted_penalty_dues decimal(12,2)  DEFAULT 0, 
		opening_primary_dues decimal(12,2)  DEFAULT 0,
		opening_instalment_interest decimal(12,2)  DEFAULT 0,
		opening_instalment_principal decimal(12,2)  DEFAULT 0,
		opening_instalment decimal(12,2) GENERATED ALWAYS AS (opening_instalment_interest + opening_instalment_principal) STORED,
		opening_penalty_dues decimal(12,2) DEFAULT 0,
		opening_eom_adjustment decimal(12,2) DEFAULT 0,
		opening_all_dues decimal(12,2) GENERATED ALWAYS AS (opening_primary_dues + opening_instalment + opening_penalty_dues + opening_eom_adjustment) STORED,
		knocked_off	decimal(12,2) GENERATED ALWAYS AS (opening_all_dues + added_all_dues + added_eom_adjustment - closing_dues_account) STORED,
		closing_penalty_dues decimal(12,2) DEFAULT 0,
		closing_instalment_principal decimal(12,2)  DEFAULT 0,
		closing_instalment_interest decimal(12,2)  DEFAULT 0,
		closing_primary_dues decimal(12,2)  DEFAULT 0,
		closing_eom_adjustment decimal(12,2)  DEFAULT 0,
		closing_instalment decimal(12,2) DEFAULT 0,
		closing_all_dues decimal(12,2) GENERATED ALWAYS AS (closing_primary_dues + closing_instalment + closing_penalty_dues + closing_eom_adjustment) STORED,
		moratorium_month tinyint(1) Default 0,
		eom_moratorium tinyint(1) Default 0,
		is_dpd_day tinyint(1) Default 1,
		dpd_days int(5) Default 0,
		dpd_start date NULL,
		next_dpd int(5) Default NULL,
	    next_closing_instalment_principal decimal(12,2)  DEFAULT NULL,
	    next_closing_instalment_interest decimal(12,2)  DEFAULT NULL,
	    next_closing_instalment_instalment decimal(12,2)  DEFAULT NULL,
        next_closing_instalment decimal(12,2)  DEFAULT NULL,
   	    closing_principal decimal(12,2)  DEFAULT 0,	
	    next_closing_principal decimal(12,2)  DEFAULT NULL,	  		
		PRIMARY KEY id (id),
		KEY row_num (row_num),
		KEY obs_date (obs_date)
	) ENGINE=InnoDB AUTO_INCREMENT=32 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

	Drop temporary table if exists t_adjustments;
	CREATE TEMPORARY TABLE t_adjustments (
		ID bigint(20) unsigned NOT NULL AUTO_INCREMENT,
		obs_date date NOT NULL,
		adj_type varchar(255) NOT NULL,	
		amount decimal(12,2)  DEFAULT 0,
		cum_amount decimal(12,2)  DEFAULT 0,
		PRIMARY KEY id (id),
		KEY obs_date (obs_date)
	) ENGINE=InnoDB AUTO_INCREMENT=32 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;


	#dpd example SUB16092981338054039851345
	#moratorium example SUB16251785799280472479762
	#select @object_id:='SUB16092981338054039851345';
	select @object_id:=sp_sublan_id;

	#select * from data_store.loan_dataset where sublan_id=@object_id;
	#select * from data_store.loan_monthly_obs where sublan_id=@object_id;

	insert into entries select * from loantap_in.loan_entries where sublan_id=@object_id;

	select @max_date:=ifnull(max(entry_date),curdate()) from entries where account='closed';

	insert into t_loan_monthly_obs(sublan_id,lan_id,obs_start,obs_end,obs_month)	
	with 
		q0 as (
		#first loan entry date
		select lan_id,sublan_id,min(entry_date) as min_date from entries
		),
		q1 as (
		select q0.*,obs_dates.* from loantap_in.obs_dates join q0 
		on obs_month >=EXTRACT(YEAR_MONTH FROM min_date) and  obs_month <=EXTRACT(YEAR_MONTH FROM @max_date)
		)
	select sublan_id,lan_id,obs_start,obs_end,obs_month from q1; 

	#update the last month to obs_end=@max_date
	UPDATE t_loan_monthly_obs set obs_end=@max_date where obs_month=EXTRACT(YEAR_MONTH FROM @max_date);


	#update sublan id
	#UPDATE t_loan_monthly_obs as update_table
	#SET update_table.sublan_id=(select sublan_id from entries limit 1);


	################new and old section#######################################
	# Setup sublan
	Drop temporary table if exists t_sublan_entries;
	CREATE temporary TABLE t_sublan_entries like loantap_in.sublan;

	#lapp_meta 
	Drop temporary table if exists t_lapp_meta;
	CREATE temporary TABLE t_lapp_meta like loantap_in.lapp_meta;

	insert into t_sublan_entries 
	select * from loantap_in.sublan where object_id=@object_id;

	Drop temporary table if exists t_sublan_collection_entries;
	CREATE temporary TABLE t_sublan_collection_entries like loantap_in.sublan_collection;

	insert into t_sublan_collection_entries 
	select * from loantap_in.sublan_collection where reference_id=@object_id;      


	#product_or_scheme
	select @product_or_scheme:=IF(count(1)>0, 'product', 'scheme') from t_sublan_entries where coll_id='sublan' and coll_type='core' and meta_key='product_id';

	UPDATE t_loan_monthly_obs as update_table
	SET update_table.product_or_scheme=@product_or_scheme;

	#current meta
	UPDATE t_loan_monthly_obs as update_table
	join 
	(
		with q1 as (
		SELECT obs_month,max(entries.ID) as ID FROM t_loan_monthly_obs 
		JOIN entries ON entry_date<=obs_end AND account='Current Meta' 
		GROUP BY obs_end
		)
	select obs_month,current_meta from entries  join q1  on entries.ID=q1.ID 
	) as data
	on update_table.obs_month=data.obs_month
	set update_table.current_meta=data.current_meta;

	#delimiter //
	IF @product_or_scheme='product' 
		THEN
		#product JSON (@product_json)
		select @product_json:=meta_value from sublan where object_id=@object_id and coll_id='sublan' and coll_type='loan_details' and meta_key='product_json';

		UPDATE t_loan_monthly_obs as update_table
		SET update_table.loan_id=sublan_id;

		#product_id -> loan_product
		UPDATE t_loan_monthly_obs as update_table
		join
		(
			select meta_value as loan_product from t_sublan_entries where coll_id='sublan' and coll_type='core' and meta_key='product_id'
		) as data
		SET update_table.loan_product=data.loan_product;

		#loan_product_label
		UPDATE t_loan_monthly_obs as update_table
		SET update_table.loan_product_label=json_value(@product_json, '$.base.label');

		#bureau_account_type
		UPDATE t_loan_monthly_obs as update_table
		SET update_table.bureau_account_type=json_value(@product_json, '$.bureau.account_type');

		#sublan_instalment_method  from product
		UPDATE t_loan_monthly_obs as update_table
		SET update_table.sublan_instalment_method=json_value(@product_json, '$.instalment.instalment_method');

		#disbursal_beneficiary
		UPDATE t_loan_monthly_obs as update_table
		SET update_table.disbursal_beneficiary=json_value(@product_json, '$.disbursal.beneficiary');


		#lapp_id from sublan
		UPDATE t_loan_monthly_obs as update_table
		join 
		(
			select meta_value as lapp_id from t_sublan_entries where coll_id='sublan' and coll_type='core' and meta_key='lapp_id'
		) as data
		SET update_table.lapp_id=data.lapp_id;

		#insert data into t_lapp_meta
		insert into t_lapp_meta (updated_by,object_id,coll_id,coll_type,meta_key,meta_value)
		with 
		q0 as (select lapp_id  from t_loan_monthly_obs group by lapp_id),
		q1 as (select q0.*,updated_by,object_id,coll_id,coll_type,meta_key,meta_value from q0 join lapp_meta on lapp_meta.object_id=q0.lapp_id )
		select updated_by,object_id,coll_id,coll_type,meta_key,meta_value from q1;

		#customer_id from sublan
		#get the parent lan from sublan table and take customer id
		UPDATE t_loan_monthly_obs as update_table
		join
		(
			with q0 as (select meta_value as lan_id from t_sublan_entries where coll_id='sublan' and meta_key="lan_id"),
			q1 as (select q0.*,meta_value as customer_id from q0 join sublan on sublan.object_id=q0.lan_id and  coll_id='lan' and meta_key='customer_id')        
			select * from q1
		) as data
		SET update_table.customer_id=data.customer_id;

		#nbfc
		UPDATE t_loan_monthly_obs as update_table
		join 
		(
			select meta_value as nbfc from t_sublan_entries where coll_id='sublan' and coll_type='core' and meta_key='nbfc'
		) as data
		SET update_table.nbfc=data.nbfc;

		#sublan_loan_tenure
		UPDATE t_loan_monthly_obs as update_table
		join 
		(
			select meta_value as sublan_loan_tenure from t_sublan_entries where coll_id='sublan' and coll_type='loan_details' and meta_key='loan_tenure'
		) as data
		SET update_table.sublan_loan_tenure=data.sublan_loan_tenure;

		#sublan_loan_interest_rate
		UPDATE t_loan_monthly_obs as update_table
		join 
		(
			select meta_value as sublan_loan_interest_rate from t_sublan_entries where coll_id='sublan' and coll_type='loan_details' and meta_key='interest_rate'
		) as data
		SET update_table.sublan_loan_interest_rate=data.sublan_loan_interest_rate;

		#sublan_loan_amount
		UPDATE t_loan_monthly_obs as update_table
		join 
		(
			select meta_value as sanction_amount from t_sublan_entries where coll_id='sublan' and coll_type='loan_details' and meta_key='sanction_amount'
		) as data
		SET update_table.sublan_loan_amount=data.sanction_amount;

		#sublan_setup_date
		UPDATE t_loan_monthly_obs as update_table
		join 
		(
			select meta_value as sublan_setup_date from t_sublan_entries where coll_id='sublan' and coll_type='loan_details' and meta_key='setup_date'
		) as data
		SET update_table.sublan_setup_date=data.sublan_setup_date;

		#sublan_advance_instalments
		UPDATE t_loan_monthly_obs as update_table
		join 
		(
			select  if(meta_value IS NULL or meta_value = '', 0, meta_value) as sublan_advance_instalments from t_sublan_entries where coll_id='sublan' and coll_type='loan_details' and meta_key='advance_instalments'
		) as data
		SET update_table.sublan_advance_instalments=data.sublan_advance_instalments;

		#sublan_virtual_account
		UPDATE t_loan_monthly_obs as update_table
		join 
		(
			select meta_value as sublan_virtual_account from t_sublan_entries where coll_id='sublan' and coll_type='core' and meta_key='virtual_account'
		) as data
		SET update_table.sublan_virtual_account=data.sublan_virtual_account;

		#sublan_loan_end_date
		UPDATE t_loan_monthly_obs as update_table
		join 
		(
			select meta_value as sublan_loan_end_date from t_sublan_entries where coll_id='sublan' and coll_type='loan_details' and meta_key='loan_end_date'
		) as data
		SET update_table.sublan_loan_end_date=data.sublan_loan_end_date;

		#sublan_end_date

		#loan_end_use
		UPDATE t_loan_monthly_obs as update_table
		SET update_table.loan_end_use=json_value(@product_json, '$.base.end_use');

		#sublan_dealer_code
		UPDATE t_loan_monthly_obs as update_table
		SET update_table.sublan_dealer_code=json_value(@product_json, '$.dealer.type');

		#loan_end_date
		UPDATE t_loan_monthly_obs as update_table
		join 
		(
		with q1 as (
			SELECT max(ID) as id,due_date FROM t_loan_monthly_obs JOIN entries on entry_date < obs_end where account ='Loan End Date'  group by obs_end
		)
		select q1.due_date as loan_end_date from q1 join entries on  entries.ID=q1.id
		)as data
		SET update_table.loan_end_date=data.loan_end_date;

		#loan_end_date
		UPDATE t_loan_monthly_obs as update_table
		SET update_table.loan_end_date=IF(json_value(current_meta, '$.end_date'),json_value(current_meta, '$.end_date'),DATE_ADD(update_table.sublan_setup_date, INTERVAL update_table.sublan_loan_tenure MONTH));

	ELSE
		UPDATE t_loan_monthly_obs as update_table
		SET update_table.loan_id=lan_id;

		#scheme id -> loan_product
		#bureau_account_type from scheme
		#sublan_instalment_method from scheme (doubt)
		UPDATE t_loan_monthly_obs as update_table
		join
		(
			with 
			q0 as (select meta_value as loan_product from t_sublan_entries where coll_id='sublan' and coll_type='sublan' and meta_key='scheme_id'),
			q1 as (select q0.*,meta_value as bureau_account_type from q0 left join loan_scheme on q0.loan_product=loan_scheme.object_id and coll_id='scheme' and meta_key='bureau_account_type'),		
			q2 as (select q1.*,meta_value as sublan_instalment_method from q1 left join loan_scheme on q1.loan_product=loan_scheme.object_id  and coll_id='instalment_method' and coll_type='scheme' and meta_key='default'),
			q3 as (select q2.*,meta_value as disbursal_beneficiary from q2 left join loan_scheme on q2.loan_product=loan_scheme.object_id  and coll_id='disbursal_beneficiary' and coll_type='scheme' and meta_key='default'),	
			q4 as (select q3.*,meta_value as product_category from q3 left join loan_scheme on q3.loan_product=loan_scheme.object_id and coll_id='scheme' and meta_key='product_category'),
			q5 as (select q4.*,meta_value as loan_product_label from q4 left join loan_scheme on q4.loan_product=loan_scheme.object_id and coll_id='scheme' and meta_key='scheme_label')
			select * from q5
		) as data
		SET update_table.loan_product=data.loan_product,
		update_table.loan_product_label=data.loan_product_label,
		update_table.bureau_account_type=data.bureau_account_type,
		update_table.sublan_instalment_method=data.sublan_instalment_method,
		update_table.product_category =data.product_category,
		update_table.disbursal_beneficiary =data.disbursal_beneficiary;



		#lapp_id from sublan
		UPDATE t_loan_monthly_obs as update_table
		join 
		(
			select meta_value as lapp_id from t_sublan_entries where coll_id='sublan' and coll_type='sublan' and meta_key='lapp_id'
		) as data
		SET update_table.lapp_id=data.lapp_id;

		insert into t_lapp_meta (updated_by,object_id,coll_id,coll_type,meta_key,meta_value)
		with 
		q0 as (select lapp_id  from t_loan_monthly_obs group by lapp_id),
		q1 as (select q0.*,updated_by,object_id,coll_id,coll_type,meta_key,meta_value from q0 join lapp_meta on lapp_meta.object_id=q0.lapp_id )
		select updated_by,object_id,coll_id,coll_type,meta_key,meta_value from q1;

		#customer_id from lapp
		UPDATE t_loan_monthly_obs as update_table
		join 
		(
			select lapp_id,meta_value as customer_id from t_loan_monthly_obs join t_lapp_meta on t_loan_monthly_obs.lapp_id=t_lapp_meta.object_id and coll_id='lapp' and meta_key='customer_id'
		) as data
		SET update_table.customer_id=data.customer_id;

		#nbfc
		UPDATE t_loan_monthly_obs as update_table
		join 
		(
			select meta_value as nbfc from t_sublan_entries where coll_id='sublan' and coll_type='sublan' and meta_key='nbfc'
		) as data
		SET update_table.nbfc=data.nbfc;

		#sublan_loan_tenure
		UPDATE t_loan_monthly_obs as update_table
		join 
		(
			select meta_value as sublan_loan_tenure from t_sublan_entries where coll_id='sublan' and coll_type='sublan' and meta_key='tenure'
		) as data
		SET update_table.sublan_loan_tenure=data.sublan_loan_tenure;

		#sublan_loan_interest_rate
		UPDATE t_loan_monthly_obs as update_table
		join 
		(
			select meta_value as sublan_loan_interest_rate from t_sublan_entries where coll_id='sublan' and coll_type='sublan' and meta_key='interest_rate'
		) as data
		SET update_table.sublan_loan_interest_rate=data.sublan_loan_interest_rate;


		#sublan_loan_amount from sublan
		UPDATE t_loan_monthly_obs as update_table
		join 
		(
			select meta_value as sanction_amount from t_sublan_entries where coll_id='sublan' and coll_type='sublan' and meta_key='sanction_amount'
		) as data
		SET update_table.sublan_loan_amount=data.sanction_amount;


		#sublan_setup_date
		UPDATE t_loan_monthly_obs as update_table
		join 
		(
			select meta_value as sublan_setup_date from t_sublan_entries where coll_id='sublan' and coll_type='sublan' and meta_key='setup_date'
		) as data
		SET update_table.sublan_setup_date=data.sublan_setup_date;

		#sublan_advance_instalments
		UPDATE t_loan_monthly_obs as update_table
		join 
		(
			select  if(meta_value IS NULL or meta_value = '', 0, meta_value) as sublan_advance_instalments from t_sublan_entries where coll_id='sublan' and coll_type='sublan' and meta_key='advance_instalments'
		) as data
		SET update_table.sublan_advance_instalments=data.sublan_advance_instalments;

		#sublan_virtual_account
		UPDATE t_loan_monthly_obs as update_table
		join 
		(
			select meta_value as sublan_virtual_account from t_sublan_entries where coll_id='sublan' and coll_type='sublan' and meta_key='virtual_account'
		) as data
		SET update_table.sublan_virtual_account=data.sublan_virtual_account;

		#sublan_end_date (DOUBT)

		#sublan_loan_end_date 
		UPDATE t_loan_monthly_obs as update_table
		join
		(
			select object_id as sublan_id,DATE_FORMAT(STR_TO_DATE(meta_value, '%d %M,%Y'), '%Y%m%d') as sublan_loan_end_date  from t_sublan_entries where coll_id='txn' and coll_type='txn' and meta_key='emi_end_date'
		) as data
		SET update_table.sublan_loan_end_date= DATE_FORMAT(IF(data.sublan_loan_end_date,data.sublan_loan_end_date,DATE_ADD(update_table.sublan_setup_date, INTERVAL update_table.sublan_loan_tenure MONTH)),'%Y%m%d');

		#sublan_loan_end_date_label
		UPDATE t_loan_monthly_obs as update_table
		SET update_table.sublan_loan_end_date_label=DATE_FORMAT(update_table.sublan_loan_end_date,'%d %M, %Y');



		#loan_end_use  lapp.decision_end_use
		UPDATE t_loan_monthly_obs as update_table
		join
		(
			select lapp_id,meta_value as decision_end_use from t_loan_monthly_obs join t_lapp_meta on t_loan_monthly_obs.lapp_id=t_lapp_meta.object_id and coll_id='lapp' and meta_key='decision_end_use'
		) as data
		SET update_table.loan_end_use=data.decision_end_use;


		#sublan_dealer_code not in new???????
		UPDATE t_loan_monthly_obs as update_table
		join 
		(
		select meta_value as sublan_dealer_code from t_sublan_entries where coll_id='sublan' and coll_type='dealer' and meta_key='dealer_code'
		) as data
		SET update_table.sublan_dealer_code=data.sublan_dealer_code;


	END IF;#//
	#delimiter ;

	##########################################################################

	#tenure
	UPDATE t_loan_monthly_obs as update_table
	SET 
	update_table.loan_tenure=json_value(current_meta, '$.tenure'),
	update_table.interest_rate=json_value(current_meta, '$.interest_rate');

	#loan_status
	UPDATE t_loan_monthly_obs as update_table
	join 
	(
		with q1 as (
		SELECT max(ID) as id,obs_month FROM t_loan_monthly_obs JOIN entries ON entry_date<=obs_end 
		where head ='Loan Status' group by obs_end
	)
	select obs_month ,account as loan_status from entries join q1 on entries.id=q1.ID 
	) as data
	on update_table.obs_month=data.obs_month
	SET update_table.loan_status=data.loan_status;

	#final_loan_status
	UPDATE t_loan_monthly_obs as update_table
	join 
	(
		with q1 as (
		SELECT max(ID) as id,obs_month FROM t_loan_monthly_obs JOIN entries where head ='Loan Status'  group by obs_end
		)
	select account as loan_status,obs_month from q1  join entries on  entries.ID=q1.id
	)as data
	SET update_table.final_loan_status=data.loan_status;

	#loan_city_label 
	UPDATE t_loan_monthly_obs as update_table
	join
	(
		select t_lapp_meta.meta_value as loan_city_label from t_loan_monthly_obs join t_lapp_meta on t_lapp_meta.object_id=t_loan_monthly_obs.lapp_id and coll_id='lapp' and meta_key='loan_city_label'
	) as data
	SET update_table.loan_city_label=data.loan_city_label;

	#loan_closed_date
	UPDATE t_loan_monthly_obs as update_table
	join 
	(
		select obs_month,entry_date as loan_closed_date from t_loan_monthly_obs join entries 
		on entry_date<=obs_end and account ='closed' group by obs_month
	) as data
	on update_table.obs_month=data.obs_month
	SET update_table.loan_closed_date=data.loan_closed_date;

	#loan_amount
	UPDATE t_loan_monthly_obs as update_table
	join 
	(
		select obs_month,ifnull(sum(debit-credit),0) as loan_amount from t_loan_monthly_obs join entries 
		on entry_date<=obs_end and account ='Loan Sanction' group by obs_month
	) as data
	on update_table.obs_month=data.obs_month
	SET update_table.loan_amount=data.loan_amount;

	#sanction_date
	UPDATE t_loan_monthly_obs as update_table
	join 
	(
		select obs_month,DATE_FORMAT(min(entry_date), '%Y%m%d') as sanction_date from t_loan_monthly_obs join entries 
		on entry_date<=obs_end and account ='Loan Sanction' group by obs_month
	) as data
	on update_table.obs_month=data.obs_month
	SET update_table.sanction_date=data.sanction_date;


	#loan_advance_instalments
	UPDATE t_loan_monthly_obs as update_table
	join 
	(
		select  if(meta_value IS NULL or meta_value = '', 0, meta_value) as loan_advance_instalments from t_sublan_entries where coll_id='sublan' and  meta_key='advance_instalments'
	) as data
	SET update_table.loan_advance_instalments=data.loan_advance_instalments;

	#instalment details
	#next_instalment_date,next_instalment_due_date,next_instalment_amount
	
	UPDATE t_loan_monthly_obs as update_table
	SET
	update_table.next_instalment_date=json_value(current_meta, '$.next_instalment_date'),
	update_table.next_instalment_due_date=json_value(current_meta, '$.next_instalment_due_date'),
	update_table.next_instalment_amount=json_value(current_meta, '$.next_instalment_amount'),
	update_table.instalments_left=json_value(current_meta, '$.instalments_left'),
	update_table.instalments_total=json_value(current_meta, '$.instalments_total'),
	update_table.instalment_end_date=json_value(current_meta, '$.end_date');

	#loan_line_utilized -> disbursal_amount
	UPDATE t_loan_monthly_obs as update_table
	join
	(
		select obs_month,ifnull(sum(credit - debit),0) as loan_line_utilized from t_loan_monthly_obs join entries 
		on entry_date>=obs_start and entry_date<=obs_end and account ='Loan Disbursed' group by obs_month
	) as data
	on update_table.obs_month=data.obs_month
	SET update_table.loan_line_utilized=data.loan_line_utilized;

	#pending_disbursal
	UPDATE t_loan_monthly_obs as update_table
	join 
	(
		select obs_month, ifnull(sum(credit-debit),0) as pending_disbursal from t_loan_monthly_obs join entries 
		on entry_date<=obs_end and account ='Loan Account Pending Disbursement' and head='Loan Account' group by obs_month
	) as data
	on update_table.obs_month=data.obs_month
	SET update_table.pending_disbursal=data.pending_disbursal;

	#first_loan_line_utilization_date -> disbursal_date
	UPDATE t_loan_monthly_obs as update_table
	join 
	(
		select obs_month,DATE_FORMAT(min(entry_date), '%Y%m%d') as first_loan_line_utilization_date from t_loan_monthly_obs join entries 
	on entry_date<=obs_end and account ='Loan Disbursed' group by obs_month
	) as data
	on update_table.obs_month=data.obs_month
	SET update_table.first_loan_line_utilization_date=data.first_loan_line_utilization_date;

	#computed - first_loan_line_utilization_month
	#cum_loan_line_utilized
	UPDATE t_loan_monthly_obs as update_table
	join 
	(
		select obs_month,ifnull(sum(credit - debit),0) as cum_loan_line_utilized from t_loan_monthly_obs join entries 
		on entry_date<=obs_end and account ='Loan Disbursed' group by obs_month
	) as data
	on update_table.obs_month=data.obs_month
	SET update_table.cum_loan_line_utilized=data.cum_loan_line_utilized;

/*

	# Closing Principal
	UPDATE t_loan_monthly_obs as update_table
	join 
	(
		select obs_month,ifnull(sum(debit-credit),0) as closing_principal from t_loan_monthly_obs join entries 
		on entry_date<=obs_end and account ='Loan Account Principal' group by obs_month
	) as data
	on update_table.obs_month=data.obs_month
	SET update_table.closing_principal=data.closing_principal;
*/

	#cum_bank_receipts
	UPDATE t_loan_monthly_obs as update_table
	join 
	(
		select obs_month,ifnull(sum(debit),0) as cum_bank_receipts from t_loan_monthly_obs join entries 
		on entry_date<=obs_end and head='Bank' group by obs_month
	) as data
	on update_table.obs_month=data.obs_month
	SET update_table.cum_bank_receipts=data.cum_bank_receipts;

	#cum_excess_amount
	UPDATE t_loan_monthly_obs as update_table
	join 
	(
		select obs_month, sum(credit-debit) as cum_excess_amount from t_loan_monthly_obs join entries 
		on entry_date<=obs_end and account ='Loan Account Excess' and head='Loan Account' group by obs_month
	) as data
	on update_table.obs_month=data.obs_month
	SET update_table.cum_excess_amount=data.cum_excess_amount;

	#bank_receipts
	UPDATE t_loan_monthly_obs as update_table
	join 
	(
		select obs_month,ifnull(sum(debit),0) as bank_receipts from t_loan_monthly_obs join entries 
		on entry_date>=obs_start and entry_date<=obs_end and head='Bank' group by obs_month
	) as data
	on update_table.obs_month=data.obs_month
	SET update_table.bank_receipts=data.bank_receipts;

	#last_bank_receipt_date
	UPDATE t_loan_monthly_obs as update_table
	join 
	(
		select obs_month,max(entry_date) as last_bank_receipt_date from t_loan_monthly_obs join entries 
		on entry_date<=obs_end and head='Bank' and debit>0 group by obs_month
	) as data
	on update_table.obs_month=data.obs_month
	SET update_table.last_bank_receipt_date=data.last_bank_receipt_date;


	#Cumulative Processing Fees
	UPDATE t_loan_monthly_obs as update_table
	join 
	(
		select obs_month,ifnull(sum(debit-credit),0) as cum_processing_fees from t_loan_monthly_obs join entries 
		on entry_date<=obs_end and account = 'Loan Account Processing Fees' and entry_set in ('GST Processing Fees','Processing Fees') group by obs_month
	) as data
	on update_table.obs_month=data.obs_month
	SET update_table.cum_processing_fees=data.cum_processing_fees;

	# Cumulative Broken Period Interest
	UPDATE t_loan_monthly_obs as update_table
	join 
	(
		select obs_month,ifnull(sum(credit-debit),0) as cum_bpi from t_loan_monthly_obs join entries 
		on entry_date<=obs_end and account = 'Broken Period Interest' group by obs_month
	) as data
	on update_table.obs_month=data.obs_month
	SET update_table.cum_bpi=data.cum_bpi;

	# Cumulative Insurance Fees
	UPDATE t_loan_monthly_obs as update_table
	join 
	(
		select obs_month,ifnull(sum(credit-debit),0) as cum_insurance_fees from t_loan_monthly_obs join entries 
		on entry_date<=obs_end and head ='Insurance Dealer' group by obs_month
	) as data
	on update_table.obs_month=data.obs_month
	SET update_table.cum_insurance_fees=data.cum_insurance_fees;


	# Cumulative Foreclosure Fees
	UPDATE t_loan_monthly_obs as update_table
	join 
	(
		select obs_month,ifnull(sum(debit-credit),0) as cum_foreclosure_fees from t_loan_monthly_obs join entries 
		on entry_date<=obs_end and account = 'Loan Account Foreclosure Fees' and entry_set in ('Foreclosure Fees','GST Foreclosure Fees') group by obs_month
	) as data
	on update_table.obs_month=data.obs_month
	SET update_table.cum_foreclosure_fees=data.cum_foreclosure_fees;


	#closure_amount
	UPDATE t_loan_monthly_obs as update_table
	join 
	(
		select obs_month,ifnull(credit,0) as closure_amount from t_loan_monthly_obs join entries 
		on entry_date<=obs_end AND entry_set = 'Close' AND account ='Loan Account Principal' group by obs_month
	) as data
	on update_table.obs_month=data.obs_month
	SET update_table.closure_amount=data.closure_amount;

	# Cumulative Other Interest
	UPDATE t_loan_monthly_obs as update_table
	join 
	(
		select obs_month,ifnull(sum(credit-debit),0) as cum_other_interest from t_loan_monthly_obs join entries 
		on entry_date<=obs_end and account = 'Days Interest' group by obs_month
	) as data
	on update_table.obs_month=data.obs_month
	SET update_table.cum_other_interest=data.cum_other_interest;


	# Cumulative Primary Dues - Calculated


	# Cumulative Late Payment Fees
	UPDATE t_loan_monthly_obs as update_table
	join 
	(
		select obs_month,ifnull(sum(debit-credit),0) as cum_late_payment_fees from t_loan_monthly_obs join entries 
		on entry_date<=obs_end and account = 'Loan Account Late Payment Fees' and entry_set in ('GST Late Payment Fees','Late Payment Fees') group by obs_month
	) as data
	on update_table.obs_month=data.obs_month
	SET update_table.cum_late_payment_fees=data.cum_late_payment_fees;

	# Cumulative Penalty Interest
	UPDATE t_loan_monthly_obs as update_table
	join 
	(
		select obs_month,ifnull(sum(credit-debit),0) as cum_penalty_interest from t_loan_monthly_obs join entries 
		on entry_date<=obs_end and account = 'Penalty Interest' group by obs_month
	) as data
	on update_table.obs_month=data.obs_month
	SET update_table.cum_penalty_interest=data.cum_penalty_interest;

	# Cumulative Penalty Dues
	UPDATE t_loan_monthly_obs as update_table
	SET update_table.cum_penalty_dues=cum_penalty_interest + cum_late_payment_fees;

	#Processing Fees
	UPDATE t_loan_monthly_obs as update_table
	join 
	(
		select obs_month,ifnull(sum(debit-credit),0) as processing_fees from t_loan_monthly_obs join entries 
		on entry_date>=obs_start and entry_date<=obs_end and account = 'Loan Account Processing Fees' and entry_set in ('GST Processing Fees','Processing Fees') group by obs_month
	) as data
	on update_table.obs_month=data.obs_month
	SET update_table.processing_fees=data.processing_fees;

	#Penalty Dues
	UPDATE t_loan_monthly_obs as update_table
	join 
	(
		select obs_month, ifnull(cum_penalty_dues - LAG(cum_penalty_dues) OVER (ORDER BY obs_month),0) AS penalty_dues  from t_loan_monthly_obs
	) as data
	on update_table.obs_month=data.obs_month
	SET update_table.penalty_dues=data.penalty_dues;

	#moratorium month
	UPDATE t_loan_monthly_obs as update_table
	join 
	(
		select obs_month,if(count(1)>0,1,0) as moratorium_month from t_loan_monthly_obs join entries 
		on entry_date>=obs_start and entry_date<=obs_end and head='Interest Income' and subgroup='moratorium' group by obs_month
	) as data
	on update_table.obs_month=data.obs_month
	SET update_table.moratorium_month=data.moratorium_month;

	#eom_moratorium
	UPDATE t_loan_monthly_obs as update_table
	join
	(
	select obs_month,1 as moratorium_month from t_loan_monthly_obs join entries
	on due_date>=obs_start and due_date<=obs_end and  txn_set='EOM Moratorium' and subgroup='eom_moratorium' group by obs_month having count(1)>0
	) as data
	on update_table.obs_month=data.obs_month
	SET update_table.moratorium_month=data.moratorium_month;

	#moratorium
	UPDATE t_loan_monthly_obs as update_table
	join
	(
	select if(sum(moratorium_month)>=1,1,0) as moratorium  from t_loan_monthly_obs
	) as data
	SET update_table.moratorium=data.moratorium;

	#Instalment
	UPDATE t_loan_monthly_obs as update_table
	join 
	(
	select obs_month,ifnull(sum(debit-credit),0) as instalment from t_loan_monthly_obs join entries 
	on entry_date>=obs_start and entry_date<=obs_end and entry_set in ('Monthly Instalment','Instalment','Advance EMI','Advance Instalments','Advance Credit') and account ='Loan Account Monthly Instalment' group by obs_month
	) as data
	on update_table.obs_month=data.obs_month
	SET update_table.instalment=data.instalment;

	#previous instalment
	UPDATE t_loan_monthly_obs as update_table
	join
	(
	select obs_month,LAG(instalment) OVER (ORDER BY obs_month) as previous_instalment
	from t_loan_monthly_obs
	) as data
	on update_table.obs_month=data.obs_month
	SET update_table.previous_instalment=data.previous_instalment;

	#Instalment Interest
	UPDATE t_loan_monthly_obs as update_table
	join
	(
	select obs_month,ifnull(sum(credit-debit),0) as instalment_interest from t_loan_monthly_obs join entries
	on entry_date>=obs_start and entry_date<=obs_end and entry_set in ('Monthly Instalment','Instalment','Advance EMI','Advance Instalments','Advance Credit') and head='Interest Income' group by obs_month
	) as data
	on update_table.obs_month=data.obs_month
	SET update_table.instalment_interest=data.instalment_interest;

	#Set the interest to 0 if instalment=0 
	UPDATE t_loan_monthly_obs set instalment_interest=0 where instalment=0;

	#Instalment Principal
	UPDATE t_loan_monthly_obs as update_table
	join
	(
		select obs_month,ifnull(sum(credit-debit),0) as instalment_principal from t_loan_monthly_obs join entries
		on entry_date>=obs_start and entry_date<=obs_end and entry_set in ('Monthly Instalment','Instalment','Advance EMI','Advance Instalments','Advance Credit') and account ='Loan Account Principal' group by obs_month
	) as data
	on update_table.obs_month=data.obs_month
	SET update_table.instalment_principal=data.instalment_principal;

	#Set the principal to 0 if instalment=0 
	UPDATE t_loan_monthly_obs set instalment_principal=0 where instalment=0;


	# Cumulative Instalment
	/*
	UPDATE t_loan_monthly_obs as update_table
	join 
	(
		select obs_month,ifnull(sum(debit-credit),0) as cum_instalment from t_loan_monthly_obs  join entries 
		on entry_date<=obs_end and entry_set in ('Monthly Instalment','Instalment','Advance EMI','Advance Instalments','Advance Credit') and account ='Loan Account Monthly Instalment' group by obs_month
	) as data
	on update_table.obs_month=data.obs_month
	SET update_table.cum_instalment=data.cum_instalment;
	*/

	# Cumulative Instalment
	UPDATE t_loan_monthly_obs as update_table
	join 
	(
		SELECT obs_month,instalment,SUM(instalment) OVER (ORDER BY obs_month ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as cum_instalment
	FROM t_loan_monthly_obs
	) as data
	on update_table.obs_month=data.obs_month
	SET update_table.cum_instalment=data.cum_instalment;

	# Cumulative Instalment Interest
	/*
	UPDATE t_loan_monthly_obs as update_table
	join 
	(
		select obs_month,ifnull(sum(credit-debit),0) as cum_instalment_interest from t_loan_monthly_obs join entries 
		on entry_date<=obs_end and entry_set in ('Monthly Instalment','Instalment','Advance EMI','Advance Instalments','Advance Credit') and head='Interest Income' group by obs_month
	) as data
	on update_table.obs_month=data.obs_month
	SET update_table.cum_instalment_interest=data.cum_instalment_interest;
	*/

	UPDATE t_loan_monthly_obs as update_table
	join 
	(
		SELECT obs_month,instalment_interest,SUM(instalment_interest) OVER (ORDER BY obs_month ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as cum_instalment_interest FROM t_loan_monthly_obs
	) as data
	on update_table.obs_month=data.obs_month
	SET update_table.cum_instalment_interest=data.cum_instalment_interest;

	# Cumulative Instalment Principal
	/*
	UPDATE t_loan_monthly_obs as update_table
	join 
	(
		select obs_month,ifnull(sum(credit-debit),0) as cum_instalment_principal from t_loan_monthly_obs join entries 
		on entry_date<=obs_end and entry_set in ('Monthly Instalment','Instalment','Advance EMI','Advance Instalments','Advance Credit') and account ='Loan Account Principal' group by obs_month
	) as data
	on update_table.obs_month=data.obs_month
	SET update_table.cum_instalment_principal=data.cum_instalment_principal;
	*/

	UPDATE t_loan_monthly_obs as update_table
	join 
	(
		SELECT obs_month,instalment_principal,SUM(instalment_principal) OVER (ORDER BY obs_month ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as cum_instalment_principal FROM t_loan_monthly_obs
	) as data
	on update_table.obs_month=data.obs_month
	SET update_table.cum_instalment_principal=data.cum_instalment_principal;


	#instalment_start_date
	UPDATE t_loan_monthly_obs as update_table
	join 
	(
		select obs_month,min(entries.entry_date) as instalment_start_date  from t_loan_monthly_obs join entries 
		on entry_date<=obs_end and txn_set='Monthly Instalment' group by obs_month 
	) as data
	on update_table.obs_month=data.obs_month
	SET update_table.instalment_start_date=data.instalment_start_date,
	first_instalment_month=EXTRACT(YEAR_MONTH FROM data.instalment_start_date);


	#first_bounce_month
	UPDATE t_loan_monthly_obs as update_table
	join 
	(
		select obs_month,min(entries.entry_date) as first_bounce_month  from t_loan_monthly_obs join entries 
		on entry_date<=obs_end and  account = 'Loan Account Returns' or account='Loan Account Late Payment Fees' group by obs_month 
	) as data
	on update_table.obs_month=data.obs_month
	SET update_table.first_bounce_month=EXTRACT(YEAR_MONTH FROM data.first_bounce_month);


	insert into t_loan_daily_obs(obs_date,row_num)	
	with 
	q0 as (
	select min(entry_date) as min_date from entries
	),
	q1 as (
	select obs_daily.obs_date from loantap_in.obs_daily join q0 
	on obs_daily.obs_date >=min_date and  obs_daily.obs_date <=@max_date
	),
	q2 as (
	select obs_date,ROW_NUMBER() OVER (ORDER BY obs_date ASC) AS row_num from q1
	)
	select obs_date,row_num from q2;


	# obs_month
	UPDATE t_loan_daily_obs as update_table
	join 
	(
	select obs_date,t_loan_monthly_obs.obs_month from t_loan_daily_obs join t_loan_monthly_obs 
	on t_loan_daily_obs.obs_date>=t_loan_monthly_obs.obs_start 
	and 
	t_loan_daily_obs.obs_date<=t_loan_monthly_obs.obs_end
	) as data
	on update_table.obs_date=data.obs_date
	SET update_table.obs_month=data.obs_month;


	#add moratorium data to daily obs
	UPDATE t_loan_daily_obs as update_table
	join 
	t_loan_monthly_obs
	on update_table.obs_month=t_loan_monthly_obs.obs_month
	SET update_table.moratorium_month=t_loan_monthly_obs.moratorium_month;


	#set up is_dpd_day;
	UPDATE t_loan_daily_obs set is_dpd_day=if(moratorium_month=1,0,1);



	#Daily Instalment
	UPDATE t_loan_daily_obs as update_table
	join 
	(
	select obs_date,ifnull(sum(debit-credit),0) as added_instalment from t_loan_daily_obs join entries 
	on entry_date=obs_date and entry_set in ('Monthly Instalment','Instalment','Advance EMI','Advance Instalments','Advance Credit') and account ='Loan Account Monthly Instalment' group by obs_date
	) as data
	on update_table.obs_date=data.obs_date
	SET update_table.added_instalment=data.added_instalment;

	#Instalment Interest
	UPDATE t_loan_daily_obs as update_table
	join 
	(
	select obs_date,ifnull(sum(credit-debit),0) as added_instalment_interest from t_loan_daily_obs join entries 
	on entry_date=obs_date and entry_set in ('Monthly Instalment','Instalment','Advance EMI','Advance Instalments','Advance Credit') and head='Interest Income' group by obs_date
	) as data
	on update_table.obs_date=data.obs_date
	SET update_table.added_instalment_interest=data.added_instalment_interest;

	#sort out moratium issue
	UPDATE t_loan_daily_obs set added_instalment_interest=0 where added_instalment=0;

	#Instalment Principal
	UPDATE t_loan_daily_obs as update_table
	join 
	(
	select obs_date,ifnull(sum(credit-debit),0) as added_instalment_principal from t_loan_daily_obs join entries 
	on entry_date=obs_date and entry_set in ('Monthly Instalment','Instalment','Advance EMI','Advance Instalments','Advance Credit') and account ='Loan Account Principal' group by obs_date
	) as data
	on update_table.obs_date=data.obs_date
	SET update_table.added_instalment_principal=data.added_instalment_principal;

	#sort out moratium issue
	UPDATE t_loan_daily_obs set added_instalment_principal=0 where added_instalment=0;

	#sort out EOM Moratorium

	UPDATE t_loan_daily_obs as update_table
	join 
	(
	select obs_date,1 as eom_moratorium from t_loan_daily_obs join entries 
	on entry_date=obs_date and txn_set='EOM Moratorium' group by obs_date
	) as data
	on update_table.obs_date=data.obs_date
	SET update_table.eom_moratorium=data.eom_moratorium;
	
	UPDATE t_loan_daily_obs 
	set 
	added_eom_adjustment=added_instalment,
	added_instalment_principal=0,
	added_instalment_interest=0,
	added_instalment=0
	where moratorium_month=1 and added_instalment>0;
	
	
	# Penalty Dues
	UPDATE t_loan_daily_obs as update_table
	join 
	(
	with q0 as (
	select obs_date,ifnull(sum(debit-credit),0) as penalty_dues from t_loan_daily_obs join entries 
	on entry_date=t_loan_daily_obs.obs_date and account = 'Loan Account Late Payment Fees' and entry_set in ('GST Late Payment Fees','Late Payment Fees') group by obs_date
	UNION All
	select obs_date,ifnull(sum(credit-debit),0) as penalty_dues from t_loan_daily_obs join entries 
	on entry_date=t_loan_daily_obs.obs_date and account = 'Penalty Interest' group by obs_date
	UNION All
	select obs_date,ifnull(sum(debit-credit),0) as penalty_dues from t_loan_daily_obs join entries 
	on entry_date=t_loan_daily_obs.obs_date and account = 'Loan Account Processing Fees' and entry_set in ('GST Processing Fees','Processing Fees') group by obs_date
	UNION All
	select obs_date,ifnull(sum(credit-debit),0) as penalty_dues from t_loan_daily_obs join entries 
	on entry_date=t_loan_daily_obs.obs_date and account = 'Broken Period Interest' group by obs_date
	UNION All
	select obs_date,ifnull(sum(credit-debit),0) as penalty_dues from t_loan_daily_obs join entries 
	on entry_date=t_loan_daily_obs.obs_date and head ='Insurance Dealer' group by obs_date
	UNION All
	select obs_date,ifnull(sum(debit-credit),0) as penalty_dues from t_loan_daily_obs join entries 
	on entry_date=t_loan_daily_obs.obs_date and account = 'Loan Account Foreclosure Fees' and entry_set in ('Foreclosure Fees','GST Foreclosure Fees') group by obs_date
	UNION All
	select obs_date,ifnull(sum(credit-debit),0) as penalty_dues from t_loan_daily_obs join entries 
	on entry_date=t_loan_daily_obs.obs_date and account = 'Days Interest' group by obs_date
	
	),
	q1 as (
	select obs_date,sum(penalty_dues) as added_penalty_dues from q0 group by obs_date
	)
	select * from q1
	) as data
	on update_table.obs_date=data.obs_date
	SET update_table.added_penalty_dues=data.added_penalty_dues;


	# Closing Dues Account
/*	
	UPDATE t_loan_daily_obs as update_table
	join 
	(
	select obs_date,ifnull(sum(debit-credit),0) as closing_dues_account from t_loan_daily_obs join entries 
	on entry_date<=obs_date and account in ('Loan Account Dues','Loan Account Instalment Dues','Loan Account Future Dues') group by obs_date
	) as data
	on update_table.obs_date=data.obs_date
	SET update_table.closing_dues_account=data.closing_dues_account;

	# Closing Principal Account
	UPDATE t_loan_daily_obs as update_table
	join 
	(
	select obs_date,ifnull(sum(debit-credit),0) as closing_principal from t_loan_daily_obs join entries 
	on entry_date<=obs_date and account in ('Loan Account Principal') group by obs_date
	) as data
	on update_table.obs_date=data.obs_date
	SET update_table.closing_principal=data.closing_principal;

*/

	select @daily_total:=count(1) from t_loan_daily_obs;


	/*
		Jan 1: P:1400 , I:200
		Feb 1: P:600, I:100

		Feb 10 : Paid:1500

		Adjust with all interest first and then principal
		Left::
		P:800, I:0

		Feb 20: Paid: 200
		Left::
		P:600, I:0


		New Approach , knock of FIFO basis, with principal first

		Jan 1: P:1400 , I:200
		Feb 1: P:600, I:100

		Feb 10 : Paid:1500

		Adjust in z fashion
		Left::
		for Jan 1: P:0 , I:100
		for Feb 1: P:600, I:100

		Feb 20: Paid: 200
		Left::
		for Jan 1: P:0 , I:0
		for Feb 1: P:500, I:100
	*/

	/*
	Take the closing_dues_account for the day from Loan Entries (instalment + Penalty Dues)

	So @adjustment= closing_dues_account is what is left to be apportioned between penalty, primary, instalment_interest, instalment_principal


	Adjust against penalty: get the closing penalty account
	------------------------------------------------------
	@unadjusted_penalty_dues = previous closing penalty + added_penalty_dues - added_penalty_waiver
	@closing_penalty_dues=least(@unadjusted_penalty_dues,@adjustment)


	Left to adjust = @adjustment - (adjusted against penalty dues)

	Adjust against Interest and Principal
	-------------------------------------------
	Create a series of all Principal and Interest posted till that day
	Go backwards till you can adjust what is left to adjust

	This gives you the closing Principal and Closing Interest



	closing_dues_account :: 10000

	adjust with @unadjusted_penalty_dues say 1000
	Left with 9000

	Go Backwards of all Principal and Interest posted till day till you can gather Rs 9000.
	This will give you outstanding Principal and Outstanding


	Adjust against primary dues
	----------------------------
	if closing_dues_account is still not adjusted then mark it as closing primary dues
	*/


	UPDATE t_loan_daily_obs as update_table
	SET 
	update_table.opening_primary_dues=0,
	update_table.opening_penalty_dues=0,
	update_table.opening_eom_adjustment=0,
	update_table.opening_instalment_principal=0,
	update_table.opening_instalment_interest=0,
	update_table.opening_instalment=0;
	
	#delimiter //
	FOR i IN 1..@daily_total
	DO


	#Update Closing Dues Account
	select @obs_date:=obs_date from t_loan_daily_obs where row_num=i;
	
	UPDATE t_loan_daily_obs as update_table
	join 
	(
	select ifnull(sum(debit-credit),0) as closing_dues_account from entries 
	where entry_date<=@obs_date and account in ('Loan Account Dues','Loan Account Instalment Dues','Loan Account Future Dues')
	) as data
	SET update_table.closing_dues_account=data.closing_dues_account
	where row_num=i;


	# Update Closing Principal Account
	UPDATE t_loan_daily_obs as update_table
	join 
	(
	select ifnull(sum(debit-credit),0) as closing_principal from entries 
	where entry_date<=@obs_date and account in ('Loan Account Principal')

	) as data
	SET update_table.closing_principal=data.closing_principal
	where row_num=i;

	
	#setup opening by taking previous data
	
	if i>0
	THEN
	
		UPDATE t_loan_daily_obs as update_table
		join 
		(
		select obs_date,closing_primary_dues,closing_penalty_dues,closing_eom_adjustment,closing_instalment_principal,closing_instalment_interest,closing_instalment
		from t_loan_daily_obs where row_num=i-1	
		) as data
		SET 
		update_table.opening_primary_dues=data.closing_primary_dues,
		update_table.opening_penalty_dues=data.closing_penalty_dues,
		update_table.opening_eom_adjustment=data.closing_eom_adjustment,
		update_table.opening_instalment_principal=data.closing_instalment_principal,
		update_table.opening_instalment_interest=data.closing_instalment_interest,
		update_table.opening_instalment=data.closing_instalment
		where row_num=i;
	END IF;

	#Destroy all EOM Adjustment if EOM Moratorium=1 eom_moratorium
	UPDATE t_loan_daily_obs
	SET 
	added_eom_adjustment=-1 * opening_eom_adjustment
	where row_num=i and eom_moratorium=1;	

	


	#select * from t_loan_daily_obs where row_num=i;
	#get the closing dues
	select @obs_date:=obs_date,@closing_dues_account:=closing_dues_account,@available_penalty_dues:=opening_penalty_dues + added_penalty_dues - added_penalty_waiver,@available_instalment:= opening_instalment + added_instalment,@available_eom_adjustment:=opening_eom_adjustment + added_eom_adjustment from t_loan_daily_obs where row_num=i;
		
	#first take the maximum in penalty_dues
	set @closing_penalty_dues=least(@available_penalty_dues,@closing_dues_account);
	set @closing_dues_account=@closing_dues_account - @closing_penalty_dues;
	
	#then take in EOM Adjustment
	set @closing_eom_adjustment=least(@available_eom_adjustment,@closing_dues_account);
	set @closing_dues_account=@closing_dues_account - @closing_eom_adjustment;
	
	#what is left adjust with @available_instalment. @adjustment is to broken into instalment and principal
	set @adjustment=least(@available_instalment,@closing_dues_account);
	set @closing_dues_account=@closing_dues_account - @adjustment;

	#what is left is primary dues
	Set @closing_primary_dues=@closing_dues_account;

	#Find the breakup of @adjustment
	#adjust interest and principal
	insert into t_adjustments(obs_date,adj_type,amount)
	select obs_date,'principal',added_instalment_principal from t_loan_daily_obs where row_num=i and added_instalment_principal>0;

	insert into t_adjustments(obs_date,adj_type,amount)
	select obs_date,'interest',added_instalment_interest from t_loan_daily_obs where row_num=i and added_instalment_interest>0;

	UPDATE t_adjustments as update_table
	join
	(
	SELECT ID,SUM(amount) OVER (ORDER BY ID DESC RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as cum_amount
	FROM t_adjustments ORDER BY ID ASC
	) as data
	on update_table.ID=data.ID
	set update_table.cum_amount=data.cum_amount;	
	
	set @closing_instalment_principal=0;
	set @closing_instalment_interest=0;
	set @dpd_days=0;
	set @dpd_start=NULL;
	if @adjustment>0
	THEN

		select @match_id:=ifnull(min(ID),0) from t_adjustments where cum_amount<=@adjustment ;

		IF @match_id>0 
		THEN
			select @dpd_start:=obs_date from t_adjustments where ID=@match_id;

			select @closing_instalment_principal:=ifnull(sum(amount),0) from t_adjustments where adj_type='principal' and ID>=@match_id ;
			set @adjustment=@adjustment - @closing_instalment_principal;

			select @closing_instalment_interest:=ifnull(sum(amount),0) from t_adjustments where adj_type='interest' and ID>=@match_id ;
			set @adjustment=@adjustment - @closing_instalment_interest;

		END IF;	

		#finding incremental amount
		#Find the entry immediately before the @match_id


		IF @adjustment>0
		THEN
			select @difference_id:=max(ID)  from t_adjustments where ID<@match_id;
			
			IF @difference_id is NULL
			THEN
			select @difference_id:=max(ID)  from t_adjustments;
			
			END IF;	
			
			
			select @dpd_start:=obs_date,@adj_type:=adj_type from t_adjustments where ID=@difference_id;

			select @closing_instalment_principal:=@closing_instalment_principal + @adjustment from dual where @adj_type='principal';
			
			select @closing_instalment_interest:=@closing_instalment_interest + @adjustment from dual where @adj_type='interest';

		END IF;	


		#calculate the dpd	
		#sum from > @dpd_start to the current obs_date 

		IF @dpd_start is not NULL
		THEN
			select @dpd_days:=ifnull(sum(is_dpd_day),0) from t_loan_daily_obs where obs_date>@dpd_start and obs_date<=@obs_date;
		END IF;

	END IF;
	
	update t_loan_daily_obs
	set closing_penalty_dues=@closing_penalty_dues,
	closing_eom_adjustment=@closing_eom_adjustment,
	closing_instalment_principal=@closing_instalment_principal,
	closing_instalment_interest=@closing_instalment_interest,
	closing_primary_dues = @closing_primary_dues,
	closing_instalment=@closing_instalment_principal + @closing_instalment_interest,	
	dpd_days=@dpd_days,
	dpd_start=@dpd_start
	where row_num=i;	

	END FOR;#//
	#delimiter ;

	# find the 3rd of the next month
	# if dpd days is lesser use that

	UPDATE t_loan_daily_obs as update_table
	join
	(
		with q0 as
		( select distinct obs_month as obs_month from t_loan_monthly_obs
		),
		q1 as (
			#get the lead month
			select obs_month,LEAD(obs_month) OVER (ORDER BY obs_month) as next_obs_month
			from q0
		),
		q2 as (
			select obs_month,next_obs_month,date(concat(next_obs_month,'02')) as next_obs_date from q1
		),
		q3 as (
			select q2.obs_month,dpd_days as next_dpd,closing_principal as next_closing_principal,closing_instalment_principal as next_closing_instalment_principal,closing_instalment_interest as next_closing_instalment_interest,closing_instalment as next_closing_instalment
			from t_loan_daily_obs join q2 on q2.next_obs_date=t_loan_daily_obs.obs_date
		)
		select q3.obs_month,q3.next_dpd,q3.next_closing_principal,q3.next_closing_instalment_principal,q3.next_closing_instalment_interest,q3.next_closing_instalment from q3 join t_loan_daily_obs on q3.obs_month=t_loan_daily_obs.obs_month
	) as data
	on update_table.obs_month=data.obs_month
	SET
	update_table.next_dpd=data.next_dpd,
	update_table.next_closing_principal=data.next_closing_principal,
	update_table.next_closing_instalment_principal=data.next_closing_instalment_principal,
	update_table.next_closing_instalment_interest=data.next_closing_instalment_interest,
	update_table.next_closing_instalment=data.next_closing_instalment;

	/*
	loan_quality

	-----------------
	Standard			dpd_days<=30
	SMA1					dpd_days>=31 and dpd_days<=60
	SMA2					dpd_days>=61 and dpd_days<=90
	SubStandard		dpd_days>=91 and dpd_days<=365
	Doubtful			dpd_days>=366

	loan_quality_days
	---------------------
	000						dpd_days<=30
	030						dpd_days>=31 and dpd_days<=60
	060						dpd_days>=61 and dpd_days<=90
	090						dpd_days>=91 and dpd_days<=365
	365+					dpd_days>=366

	dpd
	-------------------
	Regular				dpd_days=0
	1-30
	31-60
	61-90
	91-120
	121-150
	151-180
	181-270
	271-365
	365-450
	451-540
	541+


	*/


	UPDATE t_loan_monthly_obs as update_table
	join 
	(
	select obs_date as obs_start,
	t_loan_daily_obs.opening_primary_dues,
	t_loan_daily_obs.opening_instalment_interest,
	t_loan_daily_obs.opening_instalment_principal,
	t_loan_daily_obs.opening_instalment,
	t_loan_daily_obs.opening_penalty_dues,
	t_loan_daily_obs.opening_all_dues
	from t_loan_daily_obs join t_loan_monthly_obs
	on t_loan_daily_obs.obs_date=t_loan_monthly_obs.obs_start
	) as data
	on update_table.obs_start=data.obs_start
	SET update_table.opening_primary_dues=data.opening_primary_dues,
	update_table.opening_instalment_interest=data.opening_instalment_interest,
	update_table.opening_instalment_principal=data.opening_instalment_principal,
	update_table.opening_instalment=data.opening_instalment,
	update_table.opening_penalty_dues=data.opening_penalty_dues,
	update_table.opening_all_dues=data.opening_all_dues;

	/*knocked off should be sum across the month */
	UPDATE t_loan_monthly_obs as update_table
	join (
	select sum(knocked_off) as cum_knocked_off, obs_month 
	from t_loan_daily_obs group by obs_month
	) as data
	on update_table.obs_month=data.obs_month 
	set update_table.knocked_off=ifnull(data.cum_knocked_off,0);


	/*Closing calculated bases on end date of each month*/  
	UPDATE t_loan_monthly_obs as update_table
	join
	(
	select obs_date as obs_end,
	t_loan_daily_obs.closing_dues_account,
	t_loan_daily_obs.closing_penalty_dues,
	t_loan_daily_obs.closing_instalment_principal,
	t_loan_daily_obs.closing_instalment_interest,
	t_loan_daily_obs.closing_instalment,
	t_loan_daily_obs.closing_primary_dues,
	t_loan_daily_obs.dpd_days,
	t_loan_daily_obs.next_dpd,
	t_loan_daily_obs.closing_all_dues,
	t_loan_daily_obs.closing_principal,
    t_loan_daily_obs.next_closing_principal,
    t_loan_daily_obs.next_closing_instalment_principal,
    t_loan_daily_obs.next_closing_instalment_Interest,
    t_loan_daily_obs.next_closing_instalment
	from t_loan_daily_obs join t_loan_monthly_obs
	on t_loan_daily_obs.obs_date=t_loan_monthly_obs.obs_end
	) as data
	on update_table.obs_end=data.obs_end
	SET update_table.closing_dues_account=data.closing_dues_account,
	update_table.closing_penalty_dues=data.closing_penalty_dues,
	update_table.closing_primary_dues=data.closing_primary_dues,
	update_table.closing_all_dues=ifnull(data.closing_all_dues,0),
	
	update_table.eom_dpd_days=ifnull(data.dpd_days,0),
	update_table.eom_closing_principal=data.closing_principal,
	update_table.eom_closing_instalment_principal=data.closing_instalment_principal,
update_table.eom_closing_instalment_interest=data.closing_instalment_interest,
update_table.eom_closing_instalment=data.closing_instalment,
update_table.dpd_days=least(ifnull(data.dpd_days,0),ifnull(data.next_dpd,10000)),
update_table.closing_principal=least(data.closing_principal, ifnull(data.next_closing_principal,100000000)),	update_table.closing_instalment_principal=data.closing_instalment_principal,update_table.closing_instalment_interest=data.closing_instalment_interest,
update_table.closing_instalment=data.closing_instalment;
	

	/*update instalment to next if required*/  
	UPDATE t_loan_monthly_obs as update_table
	join
	(
	select obs_date as obs_end,
    t_loan_daily_obs.next_closing_instalment_principal,
    t_loan_daily_obs.next_closing_instalment_Interest,
    t_loan_daily_obs.next_closing_instalment

	from t_loan_daily_obs join t_loan_monthly_obs
	on t_loan_daily_obs.obs_date=t_loan_monthly_obs.obs_end
	) as data
	on update_table.obs_end=data.obs_end
	SET 
	update_table.closing_instalment_principal=data.next_closing_instalment_principal,
	update_table.closing_instalment_interest=data.next_closing_instalment_interest,
	update_table.closing_instalment=data.next_closing_instalment
	where data.next_closing_instalment<update_table.closing_instalment;



	

	#dpd 
	UPDATE t_loan_monthly_obs as update_table
	join 
	(
	select case when `dpd_days` = 0 then 'Regular' when (`dpd_days` >= 1 and `dpd_days` <= 30) then '1-30' when (`dpd_days` >= 31 and `dpd_days` <= 60) then '31-60' 
	when (`dpd_days` >= 61 and `dpd_days` <= 90) then '61-90' when (`dpd_days` >= 91 and `dpd_days` <= 120) then '91-120'
	when (`dpd_days` >= 121 and `dpd_days` <= 150) then '121-150' when (`dpd_days` >= 151 and `dpd_days` <= 180) then '151-180' 
	when (`dpd_days` >= 181 and `dpd_days` <= 270) then '181-270' when (`dpd_days` >= 271 and `dpd_days` <= 365) then '271-365'    when (`dpd_days` >= 366 and `dpd_days` <= 450) then '366-450' 
	when (`dpd_days` >= 451 and `dpd_days` <= 540) then '451-540' when `dpd_days` >= 541 then '541+' end as dpd,obs_month from t_loan_monthly_obs 
	) as data
	on update_table.obs_month=data.obs_month
	SET update_table.dpd=data.dpd;

	#previous dpd
	UPDATE t_loan_monthly_obs as update_table
	join 
	(
	select obs_month,LAG(dpd) OVER (ORDER BY obs_month) as previous_dpd 
	from t_loan_monthly_obs
	) as data
	on update_table.obs_month=data.obs_month
	SET update_table.previous_dpd=ifnull(data.previous_dpd,'');


	/*dpd movements */
	UPDATE t_loan_monthly_obs as update_table
	join 
	(
	select obs_end,
	case 
	when LAG(bucket_movement_index) OVER (ORDER BY obs_date) is null then 'stable' 
	when bucket_movement_index=LAG(bucket_movement_index) OVER (ORDER BY obs_date) then 'stable'
	when bucket_movement_index>LAG(bucket_movement_index) OVER (ORDER BY obs_date) then 'flow' 
	when ((bucket_movement_index<LAG(bucket_movement_index) OVER (ORDER BY obs_date)) and bucket_movement_index=0) then 'normalize'
	when bucket_movement_index<LAG(bucket_movement_index) OVER (ORDER BY obs_date) then 'Roll Back' 
	end as dpd_movement from t_loan_daily_obs  join t_loan_monthly_obs
	on t_loan_daily_obs.obs_date=t_loan_monthly_obs.obs_end
	) as data
	on update_table.obs_end=data.obs_end
	SET update_table.dpd_movement=data.dpd_movement;


	#bounce_month
	UPDATE t_loan_monthly_obs as update_table
	join
	(
	select obs_month,if(count(1)>0,1,0) as bounce_month from t_loan_monthly_obs join entries
	on entry_date>=obs_start and entry_date<=obs_end  and (entry_set='Bounce' or entry_set in ('GST Late Payment Fees','Late Payment Fees'))  and 
	(account='Loan Account Returns' or account='Loan Account Late Payment Fees') group by entries.lan_id,obs_end
	) as data
	on update_table.obs_month=data.obs_month
	SET update_table.bounce_month=data.bounce_month ;

	#no_of_bounces
	UPDATE t_loan_monthly_obs as update_table
	join
	(
	select obs_month,count(entry_set_id) as no_of_bounces from t_loan_monthly_obs join entries
	on entry_date>=obs_start and entry_date<=obs_end  and (entry_set='Bounce' or entry_set in ('GST Late Payment Fees','Late Payment Fees'))  and 
	(account='Loan Account Returns' or account='Loan Account Late Payment Fees') group by entry_set_id,obs_end
	) as data
	on update_table.obs_month=data.obs_month
	SET update_table.no_of_bounces=data.no_of_bounces;

	#cum_no_of_bounces
	UPDATE t_loan_monthly_obs as update_table
	join
	(
	select obs_month,count(entries.lan_id) as cum_no_of_bounces from t_loan_monthly_obs join entries
	on  entry_date<=obs_end  and (entry_set='Bounce' or entry_set in ('GST Late Payment Fees','Late Payment Fees'))  and 
	(account='Loan Account Returns' or account='Loan Account Late Payment Fees') 
	group by entries.lan_id,obs_end
	) as data
	on update_table.obs_month=data.obs_month
	SET update_table.cum_no_of_bounces=data.cum_no_of_bounces;


	#loan_line_available
	UPDATE t_loan_monthly_obs as update_table
	join
	(
	select obs_month,ifnull(sum(credit - debit),0) as loan_line_available from t_loan_monthly_obs join entries
	on  entry_date<=obs_end  and account='Loan Line'  
	group by obs_end
	) as data
	on update_table.obs_month=data.obs_month
	SET update_table.loan_line_available=data.loan_line_available;

	#hypothecation lapp.debt_hypothecation
	UPDATE t_loan_monthly_obs as update_table
	join
	(
	select t_lapp_meta.meta_value as debt_hypothecation from t_loan_monthly_obs join t_lapp_meta on t_lapp_meta.object_id=t_loan_monthly_obs.lapp_id and coll_id='lapp' and meta_key='debt_hypothecation'
	) as data
	SET update_table.hypothecation=data.debt_hypothecation;

	#dealer_code
	UPDATE t_loan_monthly_obs as update_table
	join 
	(
	select meta_value as sublan_dealer_code from t_sublan_entries where coll_id='sublan' and coll_type='dealer' and meta_key='dealer_code'
	) as data
	SET update_table.dealer_code=data.sublan_dealer_code;


	#select nach_id
	select @nach_id:= (select meta_value as nach_id from t_sublan_entries where object_id=@object_id and meta_key='nach_id' limit 1) ;

	#nach_status
	UPDATE t_loan_monthly_obs as update_table
	join
	(
	select obs_month,meta_value as nach_status from t_loan_monthly_obs join t_sublan_collection_entries as sublan_collection
	on t_loan_monthly_obs.sublan_id = sublan_collection.reference_id where object_id=@nach_id and meta_key='nach_status'
	) as data
	on update_table.obs_month=data.obs_month
	SET update_table.nach_status=data.nach_status;

	#nach_umrn
	UPDATE t_loan_monthly_obs as update_table
	join
	(
	select obs_month,meta_value as nach_umrn from t_loan_monthly_obs join t_sublan_collection_entries as sublan_collection
	on t_loan_monthly_obs.sublan_id = sublan_collection.reference_id where object_id=@nach_id and meta_key='nach_umrn'
	) as data
	on update_table.obs_month=data.obs_month
	SET update_table.nach_umrn=data.nach_umrn;

	#nach_mandate_id
	UPDATE t_loan_monthly_obs as update_table
	join
	(
	select obs_month,meta_value as nach_mandate_id from t_loan_monthly_obs join t_sublan_collection_entries as sublan_collection
	on t_loan_monthly_obs.sublan_id = sublan_collection.reference_id where object_id=@nach_id and meta_key='nach_mandate_id'
	) as data
	on update_table.obs_month=data.obs_month
	SET update_table.nach_mandate_id=data.nach_mandate_id;

	#nach_max_amount
	UPDATE t_loan_monthly_obs as update_table
	join
	(
	select obs_month,meta_value as nach_max_amount from t_loan_monthly_obs join t_sublan_entries
	on t_loan_monthly_obs.sublan_id = t_sublan_entries.object_id and coll_id='sublan' and meta_key='nach_max_amount'
	) as data
	on update_table.obs_month=data.obs_month
	SET update_table.nach_max_amount=data.nach_max_amount;

	#nach_process
	UPDATE t_loan_monthly_obs as update_table
	join
	(
	select obs_month,meta_value as nach_process from t_loan_monthly_obs join t_sublan_collection_entries as sublan_collection
	on t_loan_monthly_obs.sublan_id = sublan_collection.reference_id where object_id=@nach_id and meta_key='nach_process'
	) as data
	on update_table.obs_month=data.obs_month
	SET update_table.nach_process=data.nach_process;

	#nach_frequency
	UPDATE t_loan_monthly_obs as update_table
	join
	(
	select obs_month,meta_value as nach_frequency from t_loan_monthly_obs join t_sublan_collection_entries as sublan_collection
	on t_loan_monthly_obs.sublan_id = sublan_collection.reference_id where object_id=@nach_id and meta_key='nach_frequency'
	) as data
	on update_table.obs_month=data.obs_month
	SET update_table.nach_frequency=data.nach_frequency;

	#loan_end_date_label
	UPDATE t_loan_monthly_obs as update_table
	SET update_table.loan_end_date_label=DATE_FORMAT(update_table.loan_end_date,'%d %M, %Y');

	#last_nach_status and date
	UPDATE t_loan_monthly_obs as update_table
	join
	(
	with q0 as(	
	select max(txn_set_id) as last_entry,obs_month from  entries join t_loan_monthly_obs on entry_date>=obs_start and entry_date<=obs_end 
	where txn_set='NACH Update'  group by obs_month 
	),
	q1 as (
	select q0.* ,entry_set,entry_date as last_nach_date from q0 join entries on q0.last_entry =entries.txn_set_id   where head in ('bank','NACH','Not Presented') or account in ('Loan Account Excess' ,'Loan Account Returns') 
	),
	q2 as (
	select obs_month ,last_entry,last_nach_date ,
	case when q1.entry_set='Bounce' then 'Bounce'
	when (q1.entry_set='NACH Not Presented' or  q1.entry_set='Drawdown NACH Not Presented') then 'Not Presented'
	else 'success' end as 'last_nach_status' from q1
	)
	select * from q2
	) as data
	on update_table.obs_month=data.obs_month
	SET update_table.last_nach_status=data.last_nach_status,
	update_table.last_nach_date=data.last_nach_date;
	
	
	#for old cases we dont store agreement_id in sublan so need to get the process id and then check the agreement process from sublan collection.
	select @agreement_id:= (select meta_value as agreement_id from t_sublan_entries where object_id=@object_id and  (meta_key='agreement_id' or meta_key='ops_process_id') limit 1);
	#agreement_process
	UPDATE t_loan_monthly_obs as update_table
	join
	(
	select obs_month,meta_value as agreement_process from t_loan_monthly_obs join t_sublan_collection_entries as sublan_collection
	on t_loan_monthly_obs.sublan_id = sublan_collection.reference_id where object_id=@agreement_id and coll_type='agreement' and meta_key='agreement_process'
	) as data
	on update_table.obs_month=data.obs_month
	SET update_table.agreement_process=data.agreement_process;
	
	#agreement_created_date
	UPDATE t_loan_monthly_obs as update_table
	join
	(
	select obs_month,meta_value as agreement_created_date from 
	t_loan_monthly_obs join t_sublan_collection_entries as sublan_collection
	on t_loan_monthly_obs.sublan_id = sublan_collection.reference_id 
	where object_id=@agreement_id and coll_type='agreement' and meta_key='agreement_created_date'
	) as data
	on update_table.obs_month=data.obs_month
	SET update_table.agreement_created_date=data.agreement_created_date;
	
	#######################################################
	/*
	closing_all_dues decimal(12,2) GENERATED ALWAYS AS (closing_primary_dues + closing_instalment + closing_penalty_dues) STORED,
	closing_instalment decimal(12,2) GENERATED ALWAYS AS (closing_instalment_principal + closing_instalment_interest) STORED,
	*/	

	#instalment_consumed  formula

	# Instalment Consumed - formula 	

	#Relevant Principal
	UPDATE t_loan_monthly_obs as update_table
	join 
	(

	with q0 as (
	select obs_month,instalment_consumed from t_loan_monthly_obs
	),
	q1 as (
	select  q0.obs_month,t_loan_monthly_obs.obs_month as relevant_month from q0 join t_loan_monthly_obs on
	t_loan_monthly_obs.cum_instalment<=q0.instalment_consumed
	),
	q2 as (
	select obs_month,max(relevant_month) as max_month from q1 group by obs_month
	)
	select q2.obs_month,t_loan_monthly_obs.closing_principal as relevant_principal from q2 join t_loan_monthly_obs 
	on q2.max_month=t_loan_monthly_obs.obs_month
	) as data
	on update_table.obs_month=data.obs_month
	SET update_table.relevant_principal=data.relevant_principal;

	#closure_mode
	UPDATE t_loan_monthly_obs as update_table
	join 
	(
	select 
	case 
	when account='Principal Transfer'  then 'Principal Transfer' 
	when account='Transfer within Limit'  then 'Transfer within' 
	when (head='Bank' or account='Loan Account Excess')  then 'Bank' 
	else 'Bank' end as closure_mode,obs_month from t_loan_monthly_obs join entries 
	on entry_date<=obs_end and entry_set='close' group by obs_month
	) as data
	on update_table.obs_month=data.obs_month
	SET update_table.closure_mode=data.closure_mode;

	#interest_income_dealer
	UPDATE t_loan_monthly_obs as update_table
	join
	(
	select obs_month,ifnull(sum(credit-debit),0) as interest_income_dealer from t_loan_monthly_obs join entries
	on entry_date>=obs_start and entry_date<=obs_end and account='Dealer Interest'  and head='Interest Income' group by obs_month
	) as data
	on update_table.obs_month=data.obs_month
	SET update_table.interest_income_dealer=data.interest_income_dealer;

	#interest_income_bpi
	UPDATE t_loan_monthly_obs as update_table
	join
	(
	select obs_month,ifnull(sum(credit-debit),0) as interest_income_bpi from t_loan_monthly_obs join entries
	on entry_date>=obs_start and entry_date<=obs_end and account='Broken Period Interest' and head='Interest Income' group by obs_month
	) as data
	on update_table.obs_month=data.obs_month
	SET update_table.interest_income_bpi=data.interest_income_bpi;

	#interest_income_days
	UPDATE t_loan_monthly_obs as update_table
	join
	(
	select obs_month,ifnull(sum(credit-debit),0) as interest_income_days from t_loan_monthly_obs join entries
	on entry_date>=obs_start and entry_date<=obs_end and account='Days Interest' and head='Interest Income' group by obs_month
	) as data
	on update_table.obs_month=data.obs_month
	SET update_table.interest_income_days=data.interest_income_days;

	#fees_income_pf
	UPDATE t_loan_monthly_obs as update_table
	join
	(
	select obs_month,ifnull(sum(credit-debit),0) as fees_income_pf from t_loan_monthly_obs join entries
	on entry_date>=obs_start and entry_date<=obs_end and account='Processing Fees' and head='Fee Income'group by obs_month
	) as data
	on update_table.obs_month=data.obs_month
	SET update_table.fees_income_pf=data.fees_income_pf;

	#fees_income_foreclosure
	UPDATE t_loan_monthly_obs as update_table
	join
	(
	select obs_month,ifnull(sum(credit-debit),0) as fees_income_foreclosure from t_loan_monthly_obs join entries
	on entry_date>=obs_start and entry_date<=obs_end and account='Foreclosure Fees' and head='Fee Income' group by obs_month
	) as data
	on update_table.obs_month=data.obs_month
	SET update_table.fees_income_foreclosure=data.fees_income_foreclosure;

	#interest_income_monthly_interest
	UPDATE t_loan_monthly_obs as update_table
	join
	(
	select obs_month,ifnull(sum(credit-debit),0) as interest_income_monthly_interest from t_loan_monthly_obs join entries
	on entry_date>=obs_start and entry_date<=obs_end and account in ('Monthly Interest(EMI)','Monthly Interest(Interest Only)','Monthly Interest(Flat)','Monthly Interest(ADB)') and head='Interest Income' group by obs_month
	) as data
	on update_table.obs_month=data.obs_month
	SET update_table.interest_income_monthly_interest=data.interest_income_monthly_interest;

	#fees_income_insurance
	UPDATE t_loan_monthly_obs as update_table
	join
	(
	select obs_month,ifnull(sum(credit-debit),0) * 0.15 as fees_income_insurance from t_loan_monthly_obs join entries
	on entry_date<=obs_end and account='Insurance Fees' and head='Fee Income' group by obs_month
	) as data
	on update_table.obs_month=data.obs_month
	SET update_table.fees_income_insurance=data.fees_income_insurance;

	# opening_principal
	UPDATE t_loan_monthly_obs as update_table
	join 
	(
	select obs_month,ifnull(sum(debit-credit),0) as opening_principal from t_loan_monthly_obs join entries 
	on entry_date<obs_start and account ='Loan Account Principal' group by obs_month
	) as data
	on update_table.obs_month=data.obs_month
	SET update_table.opening_principal=data.opening_principal;

	#dealer_discount
	UPDATE t_loan_monthly_obs as update_table
	join
	(
	select meta_value as dealer_discount from t_sublan_entries where coll_id='sublan' and coll_type='sublan' and meta_key='dealer_discount'
	) as data
	SET update_table.dealer_discount=data.dealer_discount;

	delete from data_store.loan_monthly_obs where sublan_id=@object_id;



	insert into data_store.loan_monthly_obs(lapp_id,
	loan_id,
	lan_id,
	customer_id,
	sublan_id,
	nbfc,
	product_or_scheme,
	product_category,
	obs_start,
	obs_end,
	obs_month,
	sublan_loan_tenure,
	sublan_loan_interest_rate,
	sublan_loan_amount,
	sublan_setup_date,
	sublan_advance_instalments,
	sublan_dealer_code,
	sublan_instalment_method,
	sublan_virtual_account,
	sublan_loan_end_date,
	sublan_loan_end_date_label,
	loan_amount,
	loan_tenure,
	interest_rate,
	loan_status,
	final_loan_status,
	loan_city_label,
	loan_product,
	loan_product_label,
	bureau_account_type,
	closing_principal,
	cum_bank_receipts,
	bank_receipts,
	last_bank_receipt_date,
	cum_excess_amount,
	cum_processing_fees,
	cum_bpi,
	cum_insurance_fees,
	cum_foreclosure_fees,
	cum_other_interest,
	cum_instalment,
	cum_instalment_interest,
	cum_instalment_principal,
	cum_late_payment_fees,
	cum_penalty_interest,
	cum_penalty_dues,
	penalty_dues,
	moratorium_month,
	instalment,
	instalment_interest,
	instalment_principal,
	closing_dues_account,
	opening_primary_dues,
	opening_instalment_interest,
	opening_instalment_principal,
	opening_instalment,
	opening_penalty_dues,
	opening_all_dues,
	knocked_off,
	closing_all_dues,
	closing_penalty_dues,
	closing_instalment_principal,
	closing_instalment_interest,
	closing_primary_dues,
	relevant_principal,
	dpd_days,
	dpd_movement,
	dpd,
	previous_dpd,
	loan_line_utilized,
	pending_disbursal,
	loan_line_available,
	first_loan_line_utilization_date,
	cum_loan_line_utilized,
	cum_principal_receipt_count,
	sanction_date,
	first_instalment_month,
	instalment_start_date,
	first_bounce_month,
	loan_closed_date,
	no_of_bounces,
	cum_no_of_bounces,
	loan_end_date,
	loan_end_date_label,
	loan_advance_instalments,
	loan_end_use,
	hypothecation,
	dealer_code,
	nach_status,
	nach_umrn,
	nach_mandate_id,
	disbursal_beneficiary,
	instalments_left,
	instalments_total,
	next_instalment_date,
	next_instalment_due_date,
	next_instalment_amount,
	instalment_end_date,
	closure_mode,
	interest_income_dealer,
	interest_income_bpi,
	interest_income_days,
	fees_income_pf,
	fees_income_foreclosure,
	interest_income_monthly_interest,
	opening_principal,
	closure_amount,
	bounce_month,
	last_nach_status,
	last_nach_date,
	moratorium,
	previous_instalment,
	nach_max_amount,
	nach_process,
	processing_fees,
	nach_frequency,
	dealer_discount,
	eom_dpd_days,
	eom_closing_instalment_principal,
	eom_closing_principal,
	agreement_process,
	agreement_created_date
	) 
	select lapp_id,
	loan_id,
	lan_id,
	customer_id,
	sublan_id,
	nbfc,
	product_or_scheme,
	product_category,
	obs_start,
	obs_end,
	obs_month,
	sublan_loan_tenure,
	sublan_loan_interest_rate,
	sublan_loan_amount,
	sublan_setup_date,
	sublan_advance_instalments,
	sublan_dealer_code,
	sublan_instalment_method,
	sublan_virtual_account,
	sublan_loan_end_date,
	sublan_loan_end_date_label,
	loan_amount,
	loan_tenure,
	interest_rate,
	loan_status,
	final_loan_status,
	loan_city_label,
	loan_product,
	loan_product_label,
	bureau_account_type,
	closing_principal,
	cum_bank_receipts,
	bank_receipts,
	last_bank_receipt_date,
	cum_excess_amount,
	cum_processing_fees,
	cum_bpi,
	cum_insurance_fees,
	cum_foreclosure_fees,
	cum_other_interest,
	cum_instalment,
	cum_instalment_interest,
	cum_instalment_principal,
	cum_late_payment_fees,
	cum_penalty_interest,
	cum_penalty_dues,
	penalty_dues,
	moratorium_month,
	instalment,
	instalment_interest,
	instalment_principal,
	closing_dues_account,
	opening_primary_dues,
	opening_instalment_interest,
	opening_instalment_principal,
	opening_instalment,
	opening_penalty_dues,
	opening_all_dues,
	knocked_off,
	closing_all_dues,
	closing_penalty_dues,
	closing_instalment_principal,
	closing_instalment_interest,
	closing_primary_dues,
	relevant_principal,
	dpd_days,
	dpd_movement,
	dpd,
	previous_dpd,
	loan_line_utilized,
	pending_disbursal,
	loan_line_available,
	first_loan_line_utilization_date,
	cum_loan_line_utilized,
	cum_principal_receipt_count,
	sanction_date,
	first_instalment_month,
	instalment_start_date,
	first_bounce_month,
	loan_closed_date,
	no_of_bounces,
	cum_no_of_bounces,
	loan_end_date,
	loan_end_date_label,
	loan_advance_instalments,
	loan_end_use,
	hypothecation,
	dealer_code,
	nach_status,
	nach_umrn,
	nach_mandate_id,
	disbursal_beneficiary,
	instalments_left,
	instalments_total,
	next_instalment_date,
	next_instalment_due_date,
	next_instalment_amount,
	instalment_end_date,
	closure_mode,
	interest_income_dealer,
	interest_income_bpi,
	interest_income_days,
	fees_income_pf,
	fees_income_foreclosure,
	interest_income_monthly_interest,
	opening_principal,
	closure_amount,
	bounce_month,
	last_nach_status,
	last_nach_date,
	moratorium,
	previous_instalment,
	nach_max_amount,
	nach_process,
	processing_fees,
	nach_frequency,
	dealer_discount,
	eom_dpd_days,
	eom_closing_instalment_principal,
	eom_closing_principal,
	agreement_process,
	agreement_created_date
	from t_loan_monthly_obs;	

	delete from data_store.loan_dataset
	where sublan_id=@object_id;

	insert into data_store.loan_dataset(lapp_id,
	loan_id,
	lan_id,
	customer_id,
	sublan_id,
	nbfc,
	product_or_scheme,
	product_category,
	obs_start,
	obs_end,
	obs_month,
	sublan_loan_tenure,
	sublan_loan_interest_rate,
	sublan_loan_amount,
	sublan_setup_date,
	sublan_advance_instalments,
	sublan_dealer_code,
	sublan_instalment_method,
	sublan_virtual_account,
	sublan_loan_end_date,
	sublan_loan_end_date_label,
	loan_amount,
	loan_tenure,
	interest_rate,
	loan_status,
	final_loan_status,
	loan_city_label,
	loan_product,
	loan_product_label,
	bureau_account_type,
	closing_principal,
	cum_bank_receipts,
	bank_receipts,
	last_bank_receipt_date,
	cum_excess_amount,
	cum_processing_fees,
	cum_bpi,
	cum_insurance_fees,
	cum_foreclosure_fees,
	cum_other_interest,
	cum_instalment,
	cum_instalment_interest,
	cum_instalment_principal,
	cum_late_payment_fees,
	cum_penalty_interest,
	cum_penalty_dues,
	penalty_dues,
	moratorium_month,
	instalment,
	instalment_interest,
	instalment_principal,
	closing_dues_account,
	opening_primary_dues,
	opening_instalment_interest,
	opening_instalment_principal,
	opening_instalment,
	opening_penalty_dues,
	opening_all_dues,
	knocked_off,
	closing_all_dues,
	closing_penalty_dues,
	closing_instalment_principal,
	closing_instalment_interest,
	closing_primary_dues,
	relevant_principal,
	dpd_days,
	dpd_movement,
	dpd,
	previous_dpd,
	loan_line_utilized,
	pending_disbursal,
	loan_line_available,
	first_loan_line_utilization_date,
	cum_loan_line_utilized,
	cum_principal_receipt_count,
	sanction_date,
	first_instalment_month,
	instalment_start_date,
	first_bounce_month,
	loan_closed_date,
	no_of_bounces,
	cum_no_of_bounces,
	loan_end_date,
	loan_end_date_label,
	loan_advance_instalments,
	loan_end_use,
	hypothecation,
	dealer_code,
	nach_status,
	nach_umrn,
	nach_mandate_id,
	disbursal_beneficiary,
	instalments_left,
	instalments_total,
	next_instalment_date,
	next_instalment_due_date,
	next_instalment_amount,
	instalment_end_date,
	closure_mode,
	interest_income_dealer,
	interest_income_bpi,
	interest_income_days,
	fees_income_pf,
	fees_income_foreclosure,
	interest_income_monthly_interest,
	opening_principal,
	closure_amount,
	bounce_month,
	last_nach_status,
	last_nach_date,
	moratorium,
	previous_instalment,
	nach_max_amount,
	nach_process,
	processing_fees,
	nach_frequency,
	dealer_discount,
	eom_dpd_days,
	eom_closing_instalment_principal,
	eom_closing_principal,
	agreement_process,
	agreement_created_date
	)
	select lapp_id,
	loan_id,
	lan_id,
	customer_id,
	sublan_id,
	nbfc,
	product_or_scheme,
	product_category,
	obs_start,
	obs_end,
	obs_month,
	sublan_loan_tenure,
	sublan_loan_interest_rate,
	sublan_loan_amount,
	sublan_setup_date,
	sublan_advance_instalments,
	sublan_dealer_code,
	sublan_instalment_method,
	sublan_virtual_account,
	sublan_loan_end_date,
	sublan_loan_end_date_label,
	loan_amount,
	loan_tenure,
	interest_rate,
	loan_status,
	final_loan_status,
	loan_city_label,
	loan_product,
	loan_product_label,
	bureau_account_type,
	closing_principal,
	cum_bank_receipts,
	bank_receipts,
	last_bank_receipt_date,
	cum_excess_amount,
	cum_processing_fees,
	cum_bpi,
	cum_insurance_fees,
	cum_foreclosure_fees,
	cum_other_interest,
	cum_instalment,
	cum_instalment_interest,
	cum_instalment_principal,
	cum_late_payment_fees,
	cum_penalty_interest,
	cum_penalty_dues,
	penalty_dues,
	moratorium_month,
	instalment,
	instalment_interest,
	instalment_principal,
	closing_dues_account,
	opening_primary_dues,
	opening_instalment_interest,
	opening_instalment_principal,
	opening_instalment,
	opening_penalty_dues,
	opening_all_dues,
	knocked_off,
	closing_all_dues,
	closing_penalty_dues,
	closing_instalment_principal,
	closing_instalment_interest,
	closing_primary_dues,
	relevant_principal,
	dpd_days,
	dpd_movement,
	dpd,
	previous_dpd,
	loan_line_utilized,
	pending_disbursal,
	loan_line_available,
	first_loan_line_utilization_date,
	cum_loan_line_utilized,
	cum_principal_receipt_count,
	sanction_date,
	first_instalment_month,
	instalment_start_date,
	first_bounce_month,
	loan_closed_date,
	no_of_bounces,
	cum_no_of_bounces,
	loan_end_date,
	loan_end_date_label,
	loan_advance_instalments,
	loan_end_use,
	hypothecation,
	dealer_code,
	nach_status,
	nach_umrn,
	nach_mandate_id,
	disbursal_beneficiary,
	instalments_left,
	instalments_total,
	next_instalment_date,
	next_instalment_due_date,
	next_instalment_amount,
	instalment_end_date,
	closure_mode,
	interest_income_dealer,
	interest_income_bpi,
	interest_income_days,
	fees_income_pf,
	fees_income_foreclosure,
	interest_income_monthly_interest,
	opening_principal,
	closure_amount,
	bounce_month,
	last_nach_status,
	last_nach_date,
	moratorium,
	previous_instalment,
	nach_max_amount,
	nach_process,
	processing_fees,
	nach_frequency,
	dealer_discount,
	eom_dpd_days,
	eom_closing_instalment_principal,
	eom_closing_principal,
	agreement_process,
	agreement_created_date
	from t_loan_monthly_obs order by obs_month desc limit 1;
    IF singleQuery=1  THEN
		drop table if exists data_store.loan_daily_obs;
		create table data_store.loan_daily_obs select * from t_loan_daily_obs;
	END IF;
    delete from data_changes.sublan_data_changes where object_id=@object_id;
	commit;
END$$
DELIMITER ;
