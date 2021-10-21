CREATE DEFINER=`root`@`localhost` EVENT `all_apps_dataset` ON SCHEDULE EVERY 1 MINUTE STARTS '2021-10-19 14:52:00' ON COMPLETION PRESERVE ENABLE DO BEGIN
    SET SESSION TRANSACTION ISOLATION LEVEL READ COMMITTED;
	select @free:=get_lock('all_apps_dataset',5);
	IF @free=1
	THEN
			select @max:=max(auto_id) from data_changes.lapp_data_changes;
            select @cnt:=count(1) from data_changes.lapp_data_changes where auto_id<@max;
			set @limit=1;
			#DELIMITER //
			while @cnt>0 and @limit<=1000
			do
                call loantap_in.sp_process_dataset();
				call loantap_in.sp_all_apps_dataset();
				select @cnt:=count(1) from data_changes.lapp_data_changes where auto_id<@max;
				set @limit=@limit + 1;
			end while;
		select release_lock('all_apps_dataset');
	END IF;
END

CREATE DEFINER=`root`@`10.102.50.174` EVENT `archive_lead_meta` ON SCHEDULE EVERY 1 DAY STARTS '2021-01-26 04:30:00' ON COMPLETION PRESERVE ENABLE DO BEGIN
SET @p0=DATE_SUB(curdate(), INTERVAL 2 MONTH);
CALL archive_lead_meta(@p0);
END

CREATE DEFINER=`root`@`10.102.50.174` EVENT `archive_incomplete_lapp_meta` ON SCHEDULE EVERY 1 DAY STARTS '2021-05-11 07:15:00' ON COMPLETION NOT PRESERVE ENABLE DO BEGIN
SET @p0='incomplete-application';
SET @p1='your-needs';
SET @p2=DATE_SUB(curdate(), INTERVAL 15 Day);
CALL archive_incomplete_lapp_meta(@p0, @p1,@p2);

SET @p0='incomplete-application';
SET @p1='no-offers';
SET @p2=DATE_SUB(curdate(), INTERVAL 15 Day);
CALL archive_incomplete_lapp_meta(@p0, @p1,@p2);


END

CREATE DEFINER=`root`@`10.102.50.174` EVENT `delete_junk_lapp` ON SCHEDULE EVERY 1 HOUR STARTS '2021-05-11 02:32:00' ON COMPLETION PRESERVE ENABLE DO BEGIN
SET @p3='junk';
SET @p4=DATE_SUB(curdate(), INTERVAL 2 Day);  
CALL `delete_junk_app`(@p3, @p4);  
END

CREATE DEFINER=`root`@`10.102.50.174` EVENT `delete_marketing_db` ON SCHEDULE EVERY 30 MINUTE STARTS '2021-03-01 20:13:00' ON COMPLETION PRESERVE ENABLE DO BEGIN
select @free:=get_lock('delete_marketing_tables',5);
IF @free=1
THEN
start transaction;
SELECT @count_tbl:=count(1) FROM delete_marketing_tables WHERE expiry_date < now();

#DELIMITER //
WHILE @count_tbl >0
DO
call loantap_in.sp_clear_marketing_db();
#decrease count
SELECT @count_tbl:=count(1) FROM delete_marketing_tables WHERE expiry_date < now();
END WHILE;
commit;
select release_lock('delete_marketing_tables');
END IF;
END

CREATE DEFINER=`root`@`10.102.50.174` EVENT `seed_all_apps_dataset` ON SCHEDULE EVERY 1 DAY STARTS '2021-01-19 05:00:00' ON COMPLETION PRESERVE ENABLE DO BEGIN
    SET TRANSACTION ISOLATION LEVEL READ COMMITTED;
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
    select '00000000000' as stamp,object_id,'changed','lapp' from t_objects; 

	delete  data_changes.lapp_data_changes from  data_changes.lapp_data_changes join data_store.lapp_archive on lapp_archive .lapp_id=lapp_data_changes.object_id;

END

CREATE DEFINER=`root`@`10.102.50.174` EVENT `seed_loan_dataset` ON SCHEDULE EVERY 1 DAY STARTS '2021-02-19 03:00:00' ON COMPLETION PRESERVE ENABLE DO BEGIN 
  set session transaction isolation level read committed;
  drop  TEMPORARY table IF EXISTS t_objects;

  CREATE TEMPORARY TABLE t_objects (
  object_id varchar(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL
  ) ENGINE=InnoDB;

  insert into t_objects
  select distinct sublan_id from loan_entries where loan_entries.sublan_id !='lan' ;

  insert ignore into data_changes.sublan_data_changes(stamp,object_id,activity,object_type)
  select RIGHT(CONCAT('00000000000', row_number() over()),12) as stamp,object_id,'changed','sublan' from t_objects;

END

CREATE DEFINER=`root`@`10.102.50.174` EVENT `archive_rejected_lapp_meta` ON SCHEDULE EVERY 1 DAY STARTS '2021-05-11 01:32:00' ON COMPLETION PRESERVE ENABLE DO BEGIN
SET @p0='rejected';
SET @p1=DATE_SUB(curdate(), INTERVAL 4 MONTH);
SET @p2='incomplete-application';
CALL archive_rejected_lapp_meta(@p0, @p1,@p2);
END

CREATE DEFINER=`root`@`10.102.50.174` EVENT `loan_dataset_async2` ON SCHEDULE EVERY 10 MINUTE STARTS '2021-02-12 19:35:00' ON COMPLETION NOT PRESERVE ENABLE DO BEGIN
select @free:=get_lock('loan_dataset_async2',1);
IF @free=1
	THEN
		set @max =(select max(id) from data_changes.sublan_data_changes );
        set @sublanid=1;
		WHILE @sublanid  is not NULL DO
			set @sublanid=NULL;
			select @update:=get_lock('update_loan_dataset_select', 10);
			
			if @update=1 THEN

				select  @sublanid:=object_id from data_changes.sublan_data_changes where ID<=@max order by id limit 1;

				delete from data_changes.sublan_data_changes where object_id=@sublanid;

				insert into data_changes.loan_dataset_log(last_run,is_lock,aysnc_id,sublan_id) values(now(),@free,'loan_dataset_async2',@sublanid);

				select release_lock('update_loan_dataset_select');		

			   IF @sublanid!='LAN' then
				call sp_loan_dataset_v4(@sublanid,0);
				call sp_drawdown_data(@sublanid,0);
               END IF;

			END IF;

		END WHILE;

	select release_lock('loan_dataset_async2');
END IF;	
END

CREATE DEFINER=`root`@`10.102.50.174` EVENT `loan_dataset_async4` ON SCHEDULE EVERY 10 MINUTE STARTS '2021-02-12 19:35:00' ON COMPLETION NOT PRESERVE ENABLE DO BEGIN
select @free:=get_lock('loan_dataset_async4',1);
IF @free=1
	THEN
		set @max =(select max(id) from data_changes.sublan_data_changes );
        set @sublanid=1;
		WHILE @sublanid  is not NULL DO
			set @sublanid=NULL;
			select @update:=get_lock('update_loan_dataset_select', 10);
			
			if @update=1 THEN

				select  @sublanid:=object_id from data_changes.sublan_data_changes where ID<=@max order by id limit 1;

				delete from data_changes.sublan_data_changes where object_id=@sublanid;

				insert into data_changes.loan_dataset_log(last_run,is_lock,aysnc_id,sublan_id) values(now(),@free,'loan_dataset_async4',@sublanid);

				select release_lock('update_loan_dataset_select');		

			   IF @sublanid!='LAN' then
				call sp_loan_dataset_v4(@sublanid,0);
				call sp_drawdown_data(@sublanid,0);
               END IF;

			END IF;

		END WHILE;

	select release_lock('loan_dataset_async4');
END IF;	
END

CREATE DEFINER=`root`@`10.102.50.174` EVENT `revision_backup` ON SCHEDULE EVERY 1 DAY STARTS '2021-03-11 02:00:00' ON COMPLETION NOT PRESERVE ENABLE DO BEGIN
Drop table if exists temp_test.revision_backup;
CREATE  TABLE temp_test.revision_backup like wp_posts;
	
	
Drop Table temp_test.revisions_to_be_deleted;
CREATE TABLE temp_test.revisions_to_be_deleted (
	ID bigint(20) unsigned NOT NULL AUTO_INCREMENT,
	row_num bigint(20) NULL,
	post_id bigint(20),
	post_parent bigint(20),
	post_date date NOT NULL,
	status tinyint(1) Default 0,
	PRIMARY KEY ID (ID),
	KEY post_parent (post_parent)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
insert into temp_test.revisions_to_be_deleted(post_id,row_num,post_parent,post_date)
select ID as post_id,ROW_NUMBER() OVER (ORDER BY ID ASC) AS row_num,post_parent,date(post_date) as post_date FROM wp_posts WHERE post_type = 'revision' and date(post_date)   < DATE_SUB(NOW(), INTERVAL 1 MONTH);
select @count:=count(1) from temp_test.revisions_to_be_deleted;
#delimiter //
FOR i IN 1..@count
DO
	select @post_id:=post_id, @post_parent:=post_parent,@post_date:=post_date from temp_test.revisions_to_be_deleted where row_num=i;
	
	select @rev_count:=count(1) from wp_posts where  post_parent=@post_parent and post_date>@post_date;	
	
	insert into backups.revision_backup
	select * from wp_posts where ID=@post_id and post_type = 'revision'  and date(post_date)   < DATE_SUB(NOW(), INTERVAL 1 MONTH) and @rev_count>3 ;
	
    delete from wp_posts where ID=@post_id and post_type = 'revision'  and date(post_date)   < DATE_SUB(NOW(), INTERVAL 1 MONTH) and @rev_count>3 ;
    
END FOR;#//
#delimiter ;
END

CREATE DEFINER=`root`@`10.102.50.174` EVENT `loan_dataset_async3` ON SCHEDULE EVERY 10 MINUTE STARTS '2021-02-12 19:35:00' ON COMPLETION NOT PRESERVE ENABLE DO BEGIN
select @free:=get_lock('loan_dataset_async3',1);
IF @free=1
	THEN
		set @max =(select max(id) from data_changes.sublan_data_changes );
        set @sublanid=1;
		WHILE @sublanid  is not NULL DO
			set @sublanid=NULL;
			select @update:=get_lock('update_loan_dataset_select', 10);
			
			if @update=1 THEN

				select  @sublanid:=object_id from data_changes.sublan_data_changes where ID<=@max order by id limit 1;

				delete from data_changes.sublan_data_changes where object_id=@sublanid;

				insert into data_changes.loan_dataset_log(last_run,is_lock,aysnc_id,sublan_id) values(now(),@free,'loan_dataset_async3',@sublanid);

				select release_lock('update_loan_dataset_select');		

			   IF @sublanid!='LAN' then
				call sp_loan_dataset_v4(@sublanid,0);
				call sp_drawdown_data(@sublanid,0);
               END IF;

			END IF;

		END WHILE;

	select release_lock('loan_dataset_async3');
END IF;	
END

CREATE DEFINER=`root`@`10.102.50.174` EVENT `loan_dataset_async1` ON SCHEDULE EVERY 10 MINUTE STARTS '2021-03-08 22:40:00' ON COMPLETION PRESERVE ENABLE DO BEGIN
select @free:=get_lock('loan_dataset_async1',1);

IF @free=1
	THEN
		set @max =(select max(id) from data_changes.sublan_data_changes );
        set @sublanid=1;
		WHILE @sublanid  is not NULL DO
			set @sublanid=NULL;
			select @update:=get_lock('update_loan_dataset_select', 10);
			
			if @update=1 THEN

				select  @sublanid:=object_id from data_changes.sublan_data_changes where ID<=@max order by id limit 1;

				delete from data_changes.sublan_data_changes where object_id=@sublanid;

				insert into data_changes.loan_dataset_log(last_run,is_lock,aysnc_id,sublan_id) values(now(),@free,'loan_dataset_async1',@sublanid);

				select release_lock('update_loan_dataset_select');		
               
               IF @sublanid!='LAN' then
				call sp_loan_dataset_v4(@sublanid,0);
				call sp_drawdown_data(@sublanid,0);
               END IF;
			END IF;

		END WHILE;

	select release_lock('loan_dataset_async1');
END IF;	
END

CREATE DEFINER=`root`@`10.102.50.174` EVENT `populate_call_center_queue` ON SCHEDULE EVERY 30 MINUTE STARTS '2021-03-02 18:53:00' ON COMPLETION NOT PRESERVE ENABLE DO BEGIN
	select @free:=get_lock('sp_populate_call_center_queue',5);
	IF @free=1
	THEN
        call loantap_in.sp_populate_call_center_queue();
		select release_lock('sp_populate_call_center_queue');
	END IF;
END

CREATE DEFINER=`root`@`10.102.50.174` EVENT `loan_dataset_async5` ON SCHEDULE EVERY 10 MINUTE STARTS '2021-02-12 19:35:00' ON COMPLETION NOT PRESERVE ENABLE DO BEGIN
select @free:=get_lock('loan_dataset_async5',1);
IF @free=1
	THEN
		set @max =(select max(id) from data_changes.sublan_data_changes );
        set @sublanid=1;
		WHILE @sublanid  is not NULL DO
			set @sublanid=NULL;
			select @update:=get_lock('update_loan_dataset_select', 10);
			
			if @update=1 THEN

				select  @sublanid:=object_id from data_changes.sublan_data_changes where ID<=@max order by id limit 1;

				delete from data_changes.sublan_data_changes where object_id=@sublanid;

				insert into data_changes.loan_dataset_log(last_run,is_lock,aysnc_id,sublan_id) values(now(),@free,'loan_dataset_async5',@sublanid);

				select release_lock('update_loan_dataset_select');		

			   IF @sublanid!='LAN' then
				call sp_loan_dataset_v4(@sublanid,0);
				call sp_drawdown_data(@sublanid,0);
               END IF;

			END IF;

		END WHILE;

	select release_lock('loan_dataset_async5');
END IF;	
END
