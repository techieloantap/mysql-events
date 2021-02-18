CREATE  EVENT `archive_incomplete_lapp` ON SCHEDULE EVERY 1 DAY STARTS '2021-01-18 03:00:00' ON COMPLETION PRESERVE ENABLE DO BEGIN
SET @p0='incomplete-application';
SET @p1=DATE_SUB(curdate(), INTERVAL 4 MONTH);  
CALL `archive_lapp_meta`(@p0, @p1);  

SET @p3='junk';
SET @p4=DATE_SUB(curdate(), INTERVAL 15 Day);  
CALL `archive_lapp_meta`(@p3, @p4);  

END

CREATE DEFINER=`root`@`localhost` EVENT `all_apps_dataset` ON SCHEDULE EVERY 10 MINUTE STARTS '2021-02-11 18:06:22' ON COMPLETION PRESERVE ENABLE DO BEGIN
	select @free:=get_lock('all_apps_dataset',5);
	insert into data_changes.all_apps_log(last_run,is_lock) values(now(),@free);
	IF @free=1
	THEN
		start transaction;
			select @max:=max(id) from data_changes.lapp_data_changes;
			set @cnt=@max;
			set @limit=1;
			#DELIMITER //
			while @cnt>0 and @limit<=1000
			do
				call loantap_in.sp_all_apps_dataset();
				select @cnt:=count(1) from data_changes.lapp_data_changes where id<@max;
				set @limit=@limit + 1;
			end while;
		commit;
		select release_lock('all_apps_dataset');
	END IF;
END

CREATE  EVENT `archive_lead_meta` ON SCHEDULE EVERY 1 DAY STARTS '2021-01-26 04:30:00' ON COMPLETION PRESERVE ENABLE DO BEGIN
SET @p0=DATE_SUB(curdate(), INTERVAL 2 MONTH);
CALL archive_lead_meta(@p0);
END

CREATE  EVENT `seed_all_apps_dataset` ON SCHEDULE EVERY 1 DAY STARTS '2021-01-19 05:00:00' ON COMPLETION PRESERVE ENABLE DO BEGIN
    SET TRANSACTION ISOLATION LEVEL READ COMMITTED;
    start transaction ;
    DROP temporary TABLE IF EXISTS delete_data;

    create temporary table delete_data
    select lapp_id from data_store.all_apps_dataset app
    left join lapp_meta on app.lapp_id=lapp_meta.object_id and coll_id='lapp' and meta_key='lapp_id' where object_id is null and is_archived='no'; 

    DELETE data_store.all_apps_dataset
    FROM data_store.all_apps_dataset
    JOIN delete_data
    on all_apps_dataset.lapp_id=delete_data.lapp_id;

    DELETE data_store.app_personal_dataset
    FROM data_store.app_personal_dataset
    JOIN delete_data
    on app_personal_dataset.lapp_id=delete_data.lapp_id;


    #Step 2 : Now Insert all the application which are not archived into lapp data changes
    drop  TEMPORARY table IF EXISTS t_objects;

    CREATE TEMPORARY TABLE t_objects 
    with q1 as (
    select object_id from lapp_meta where coll_id='lapp' and meta_key='lapp_id'
    )
    select q1.* from q1 left join lapp_meta on q1.object_id=lapp_meta.object_id and meta_key='is_archived' where meta_value is null ;

    insert into data_changes.lapp_data_changes(stamp,object_id,activity,object_type)
    select RIGHT(CONCAT('00000000000', row_number() over()),12) as stamp,object_id,'changed','lapp' from t_objects; 

	delete  data_changes.lapp_data_changes from  data_changes.lapp_data_changes join data_store.lapp_archive on lapp_archive .lapp_id=lapp_data_changes.object_id;

commit;
END

CREATE  EVENT `archive_rejected_lapp_meta` ON SCHEDULE EVERY 1 DAY STARTS '2020-12-27 06:00:00' ON COMPLETION PRESERVE ENABLE DO BEGIN
SET @p0='rejected';
SET @p1=DATE_SUB(curdate(), INTERVAL 4 MONTH);
SET @p2='incomplete-application';
CALL archive_rejected_lapp_meta(@p0, @p1,@p2);
END

CREATE  EVENT `loan_dataset_async5` ON SCHEDULE EVERY 10 MINUTE STARTS '2021-02-12 19:35:00' ON COMPLETION NOT PRESERVE ENABLE DO BEGIN
select @free:=get_lock('loan_dataset_async5',-1);
IF @free=1
	THEN
		set @max =(select max(id) from data_changes.sublan_data_changes );
		WHILE @max  is not NULL DO
			set @sublanid=NULL;
			select get_lock('update_loan_dataset_select',-1);
			select  @sublanid:=object_id from data_changes.sublan_data_changes where ID<=@max limit 1;
			delete from data_changes.sublan_data_changes where object_id=@sublanid;
			select release_lock('update_loan_dataset_select');		
			IF @sublanid is not null
			THEN
				call sp_loan_dataset(@sublanid);
			END IF;
		END WHILE;
		select release_lock('loan_dataset_async5');
	END IF;
END

CREATE  EVENT `seed_loan_dataset` ON SCHEDULE EVERY 1 DAY STARTS '2021-02-19 03:00:00' ON COMPLETION PRESERVE ENABLE DO BEGIN 

  truncate table data_changes.sublan_data_changes;
  set session transaction isolation level read committed;
  drop  TEMPORARY table IF EXISTS t_objects;

  CREATE TEMPORARY TABLE t_objects (
  object_id varchar(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL
  ) ENGINE=InnoDB;

  insert into t_objects
  select distinct sublan_id from loan_entries where loan_entries.sublan_id !='lan' ;

  truncate table data_changes.sublan_data_changes;

  insert ignore into data_changes.sublan_data_changes(stamp,object_id,activity,object_type)
  select RIGHT(CONCAT('00000000000', row_number() over()),12) as stamp,object_id,'changed','sublan' from t_objects;

END

CREATE  EVENT `loan_dataset_async3` ON SCHEDULE EVERY 10 MINUTE STARTS '2021-02-12 19:35:00' ON COMPLETION NOT PRESERVE ENABLE DO BEGIN
select @free:=get_lock('loan_dataset_async3',-1);
IF @free=1
	THEN
		set @max =(select max(id) from data_changes.sublan_data_changes );
		WHILE @max  is not NULL DO
			set @sublanid=NULL;
			select get_lock('update_loan_dataset_select',-1);
			select  @sublanid:=object_id from data_changes.sublan_data_changes where ID<=@max limit 1;
			delete from data_changes.sublan_data_changes where object_id=@sublanid;
			select release_lock('update_loan_dataset_select');		
			IF @sublanid is not null
			THEN
				call sp_loan_dataset(@sublanid);
			END IF;
		END WHILE;
		select release_lock('loan_dataset_async3');
	END IF;
END

CREATE  EVENT `loan_dataset_async2` ON SCHEDULE EVERY 10 MINUTE STARTS '2021-02-12 19:35:00' ON COMPLETION NOT PRESERVE ENABLE DO BEGIN
select @free:=get_lock('loan_dataset_async2',-1);
IF @free=1
	THEN
		set @max =(select max(id) from data_changes.sublan_data_changes );
		WHILE @max  is not NULL DO
			set @sublanid=NULL;
			select get_lock('update_loan_dataset_select',-1);
			select  @sublanid:=object_id from data_changes.sublan_data_changes where ID<=@max limit 1;
			delete from data_changes.sublan_data_changes where object_id=@sublanid;
			select release_lock('update_loan_dataset_select');		
			IF @sublanid is not null
			THEN
				call sp_loan_dataset(@sublanid);
			END IF;
		END WHILE;
		select release_lock('loan_dataset_async2');
	END IF;
END

CREATE  EVENT `loan_dataset_async4` ON SCHEDULE EVERY 10 MINUTE STARTS '2021-02-12 19:35:00' ON COMPLETION NOT PRESERVE ENABLE DO BEGIN
select @free:=get_lock('loan_dataset_async4',-1);
IF @free=1
	THEN
		set @max =(select max(id) from data_changes.sublan_data_changes );
		WHILE @max  is not NULL DO
			set @sublanid=NULL;
			select get_lock('update_loan_dataset_select',-1);
			select  @sublanid:=object_id from data_changes.sublan_data_changes where ID<=@max limit 1;
			delete from data_changes.sublan_data_changes where object_id=@sublanid;
			select release_lock('update_loan_dataset_select');		
			IF @sublanid is not null
			THEN
				call sp_loan_dataset(@sublanid);
			END IF;
		END WHILE;
		select release_lock('loan_dataset_async4');
	END IF;
END

CREATE  EVENT `loan_dataset_async1` ON SCHEDULE EVERY 10 MINUTE STARTS '2021-02-12 19:35:00' ON COMPLETION NOT PRESERVE ENABLE DO BEGIN
select @free:=get_lock('loan_dataset_async1',-1);
IF @free=1
	THEN
		set @max =(select max(id) from data_changes.sublan_data_changes );
		WHILE @max  is not NULL DO
			set @sublanid=NULL;
			select get_lock('update_loan_dataset_select',-1);
			select  @sublanid:=object_id from data_changes.sublan_data_changes where ID<=@max limit 1;
			delete from data_changes.sublan_data_changes where object_id=@sublanid;
			select release_lock('update_loan_dataset_select');		
			IF @sublanid is not null
			THEN
				call sp_loan_dataset(@sublanid);
			END IF;
		END WHILE;
		select release_lock('loan_dataset_async1');
	END IF;
END
