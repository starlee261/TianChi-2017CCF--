-- 训练集  kk_traindata     8.1-8.31                  特征提取区间
-- 线下训练   kk_offline_train   18-24             kk_feature_offline_train 1-18
-- 线下测试   kk_offline_test    25-31             kk_feature_offline_test  1-25
-- 线上测试   kk_online_test     1-14             kk_feature_online_test   1-31
------------------------------------------------------------------------------------------------------
-- 划分样本集 kk_split_day
drop table if  exists kk_traindata;
drop table if  exists kk_offline_train;
drop table if  exists kk_offline_test;
drop table if  exists kk_online_test;

create table if not  exists kk_traindata as
select *
from user_shop_behavior_median;

create table if not exists kk_offline_train as
select *
from kk_traindata
where time_stamp>'2017-08-18' and time_stamp<'2017-08-25';

create table if not exists kk_offline_test as
select *
from kk_traindata
where time_stamp>'2017-08-25';

create table if not exists kk_online_test as
select *
from evaluation_public_median ;

------------------------------------------------------------------------------------------------------
-- 划分特征提取区间   kk_split_feature_day
drop table if  exists kk_feature_offline_train;
drop table if  exists kk_feature_offline_test;
drop table if  exists kk_feature_online_test;

create table if not exists kk_feature_offline_train as
select *
from kk_traindata
where  time_stamp<'2017-08-18';


create table if not exists kk_feature_offline_test as
select *
from kk_traindata
where  time_stamp<'2017-08-25';


create table if not exists kk_feature_online_test as
select *
from kk_traindata;

------------------------------------------------------------------------------------------------------
-- 建立候选集(采用李强的候选集) kk_sample
drop table if  exists kk_sample_offline_train;
drop table if  exists kk_sample_offline_test;
drop table if  exists kk_sample_online_test;

create table if not exists kk_sample_offline_train as
select *
from lq_sample_offline_train;

create table if not exists kk_sample_offline_test as
select *
from lq_sample_offline_test;

create table if not exists kk_sample_online_test as
select *
from lq_sample_online_test;

------------------------------------------------------------------------------------------------------
-- 分wifi get_jar_table  得运行jar包
drop table if  exists kk_feature_offline_train_shop_expand_wifi;
drop table if  exists kk_feature_offline_test_shop_expand_wifi;
drop table if  exists kk_feature_online_test_shop_expand_wifi;
drop table if  exists kk_offline_train_expand_row_wifi;
drop table if  exists kk_offline_test_expand_row_wifi;
drop table if  exists kk_online_test_expand_row_wifi;


create table if not exists kk_feature_offline_train_shop_expand_wifi
(
     shop_id String,
	 wifi String,
	 wifi_sign bigint 
);

create table if not exists kk_feature_offline_test_shop_expand_wifi
(
     shop_id String,
	 wifi String,
	 wifi_sign bigint 
);

create table if not exists kk_feature_online_test_shop_expand_wifi
(
     shop_id String,
	 wifi String,
	 wifi_sign bigint 
);

create table if not exists kk_offline_train_expand_row_wifi
(
     row_id bigint,
	 wifi String,
	 wifi_sign bigint 
);

create table if not exists kk_offline_test_expand_row_wifi
(
     row_id bigint,
	 wifi String,
	 wifi_sign bigint 
);
create table if not exists kk_online_test_expand_row_wifi
(
     row_id bigint,
	 wifi String,
	 wifi_sign bigint 
);

---分wifi 带concect
drop table if  exists kk_feature_offline_train_shop_expand_wifi_flag;
drop table if  exists kk_feature_offline_test_shop_expand_wifi_flag;
drop table if  exists kk_feature_online_test_shop_expand_wifi_flag;
drop table if  exists kk_offline_train_expand_row_wifi_flag;
drop table if  exists kk_offline_test_expand_row_wifi_flag;
drop table if  exists kk_online_test_expand_row_wifi_flag;

create table if not exists kk_feature_offline_train_shop_expand_wifi_flag
(
     shop_id String,
	 wifi String,
	 wifi_sign bigint,
	 connect String
);

create table if not exists kk_feature_offline_test_shop_expand_wifi_flag
(
     shop_id String,
	 wifi String,
	 wifi_sign bigint,
	 connect String
);

create table if not exists kk_feature_online_test_shop_expand_wifi_flag
(
     shop_id String,
	 wifi String,
	 wifi_sign bigint,
	 connect String
);

create table if not exists kk_offline_train_expand_row_wifi_flag
(
     row_id bigint,
	 wifi String,
	 wifi_sign bigint,
	 connect String
);

create table if not exists kk_offline_test_expand_row_wifi_flag
(
     row_id bigint,
	 wifi String,
	 wifi_sign bigint,
	 connect String
);
create table if not exists kk_online_test_expand_row_wifi_flag
(
     row_id bigint,
	 wifi String,
	 wifi_sign bigint,
	 connect String
);

--wifi按排序展开
drop table if exists kk_offline_train_rank_row_wifi;
create table if not exists kk_offline_train_rank_row_wifi
(
    row_id bigint ,
	pre_shop_id String,
	row_wifi1 String,row_wifi1_sign bigint ,
	row_wifi2 String,row_wifi2_sign bigint ,
    row_wifi3 String,row_wifi3_sign bigint ,
    row_wifi4 String,row_wifi4_sign bigint ,
    row_wifi5 String,row_wifi5_sign bigint ,
    row_wifi6 String,row_wifi6_sign bigint ,
    row_wifi7 String,row_wifi7_sign bigint ,
    row_wifi8 String,row_wifi8_sign bigint ,
    row_wifi9 String,row_wifi9_sign bigint ,
    row_wifi10 String,row_wifi10_sign bigint 
);

drop table if exists kk_offline_test_rank_row_wifi;
create table if not exists kk_offline_test_rank_row_wifi
(
    row_id bigint ,
	pre_shop_id String,
	row_wifi1 String,row_wifi1_sign bigint ,
	row_wifi2 String,row_wifi2_sign bigint ,
    row_wifi3 String,row_wifi3_sign bigint ,
    row_wifi4 String,row_wifi4_sign bigint ,
    row_wifi5 String,row_wifi5_sign bigint ,
    row_wifi6 String,row_wifi6_sign bigint ,
    row_wifi7 String,row_wifi7_sign bigint ,
    row_wifi8 String,row_wifi8_sign bigint ,
    row_wifi9 String,row_wifi9_sign bigint ,
    row_wifi10 String,row_wifi10_sign bigint 
);

drop table if exists kk_online_test_rank_row_wifi;
create table if not exists kk_online_test_rank_row_wifi
(
    row_id bigint ,
	pre_shop_id String,
	row_wifi1 String,row_wifi1_sign bigint ,
	row_wifi2 String,row_wifi2_sign bigint ,
    row_wifi3 String,row_wifi3_sign bigint ,
    row_wifi4 String,row_wifi4_sign bigint ,
    row_wifi5 String,row_wifi5_sign bigint ,
    row_wifi6 String,row_wifi6_sign bigint ,
    row_wifi7 String,row_wifi7_sign bigint ,
    row_wifi8 String,row_wifi8_sign bigint ,
    row_wifi9 String,row_wifi9_sign bigint ,
    row_wifi10 String,row_wifi10_sign bigint 
);




-- 特征提取
------------------------------------------------------------------------------------------------------
-- 基础特征 'user_id','pre_shop_id','hour','pre_s_longitude','pre_s_latitude','pre_category_id','pre_price'(join on [row_id,pre_shop_id])
--kk_get_basic_feature
-- 线下训练
drop table if  exists kk_offline_train_get_basic_feature;
drop table if  exists kk_offline_test_get_basic_feature;
drop table if  exists kk_online_test_get_basic_feature;

create table if not exists kk_offline_train_get_basic_feature as
select t.row_id,t.user_id,t.pre_shop_id,cast(substr(t.time_stamp,12,2) as bigint ) as hour,b.pre_s_longitude,b.pre_s_latitude,b.pre_category_id,b.pre_price,b.mall_id,
cast(split(t.user_id, '_')[1] as bigint) as user_id_int,
cast(split(t.pre_shop_id, '_')[1] as bigint) as pre_shop_id_int,
cast(split(b.pre_category_id, '_')[1] as bigint) as pre_category_id_int,
cast(split(b.mall_id, '_')[1] as bigint) as mall_id_int
from kk_sample_offline_train t left outer join 
(
	select a.shop_id as pre_shop_id,a.longitude as pre_s_longitude,a.latitude as pre_s_latitude,a.price as pre_price,
	a.category_id as pre_category_id, a.mall_id
	from shop_info a
)b
on t.pre_shop_id = b.pre_shop_id;

-- 线下测试
create table if not exists kk_offline_test_get_basic_feature as
select t.row_id,t.user_id,t.pre_shop_id,cast(substr(t.time_stamp,12,2) as bigint ) as hour,b.pre_s_longitude,b.pre_s_latitude,b.pre_category_id,b.pre_price,b.mall_id,
cast(split(t.user_id, '_')[1] as bigint) as user_id_int,
cast(split(t.pre_shop_id, '_')[1] as bigint) as pre_shop_id_int,
cast(split(b.pre_category_id, '_')[1] as bigint) as pre_category_id_int,
cast(split(b.mall_id, '_')[1] as bigint) as mall_id_int
from kk_sample_offline_test t left outer join 
(
	select a.shop_id as pre_shop_id,a.longitude as pre_s_longitude,a.latitude as pre_s_latitude,a.price as pre_price,
	a.category_id as pre_category_id, a.mall_id
	from shop_info a
)b
on t.pre_shop_id = b.pre_shop_id;

-- 线上测试
create table if not exists kk_online_test_get_basic_feature as
select t.row_id,t.user_id,t.pre_shop_id,cast(substr(t.time_stamp,12,2) as bigint ) as hour,b.pre_s_longitude,b.pre_s_latitude,b.pre_category_id,b.pre_price,b.mall_id,
cast(split(t.user_id, '_')[1] as bigint) as user_id_int,
cast(split(t.pre_shop_id, '_')[1] as bigint) as pre_shop_id_int,
cast(split(b.pre_category_id, '_')[1] as bigint) as pre_category_id_int,
cast(split(b.mall_id, '_')[1] as bigint) as mall_id_int
from kk_sample_online_test t left outer join 
(
	select a.shop_id as pre_shop_id,a.longitude as pre_s_longitude,a.latitude as pre_s_latitude,a.price as pre_price,
	a.category_id as pre_category_id, a.mall_id
	from shop_info a
)b
on t.pre_shop_id = b.pre_shop_id;

drop table if  exists kk_offline_train_feature1;
drop table if  exists kk_offline_test_feature1;
drop table if  exists kk_online_test_feature1;
create table if not exists kk_offline_train_feature1 as
select * from kk_offline_train_get_basic_feature;
create table if not exists kk_offline_test_feature1 as
select * from kk_offline_test_get_basic_feature;
create table if not exists kk_online_test_feature1 as
select * from kk_online_test_get_basic_feature;
------------------------------------------------------------------------------------------------------

-- user与shop距离(join on [row_id,pre_shop_id])
--kk_get_user_shop_dis
-- 线下训练
drop table if  exists kk_offline_train_get_user_shop_dis;
drop table if  exists kk_offline_test_get_user_shop_dis;
drop table if  exists kk_online_test_get_user_shop_dis;

create table if not exists kk_offline_train_get_user_shop_dis as
select t.row_id,t.pre_shop_id,abs(t.longitude-b.longitude) as user_shop_lo_gap,abs(t.latitude-b.latitude) as user_shop_la_gap,
abs(t.longitude-b.longitude)+abs(t.latitude-b.latitude) as user_shop_dis
from kk_sample_offline_train t left outer join 
(
	select a.shop_id as pre_shop_id,a.longitude,a.latitude
	from shop_info a
)b
on t.pre_shop_id = b.pre_shop_id;

-- 线下测试
create table if not exists kk_offline_test_get_user_shop_dis as
select t.row_id,t.pre_shop_id,abs(t.longitude-b.longitude) as user_shop_lo_gap,abs(t.latitude-b.latitude) as user_shop_la_gap,
abs(t.longitude-b.longitude)+abs(t.latitude-b.latitude) as user_shop_dis
from kk_sample_offline_test t left outer join 
(
	select a.shop_id as pre_shop_id,a.longitude,a.latitude
	from shop_info a
)b
on t.pre_shop_id = b.pre_shop_id;

-- 线上测试
create table if not exists kk_online_test_get_user_shop_dis as
select t.row_id,t.pre_shop_id,abs(t.longitude-b.longitude) as user_shop_lo_gap,abs(t.latitude-b.latitude) as user_shop_la_gap,
abs(t.longitude-b.longitude)+abs(t.latitude-b.latitude) as user_shop_dis
from kk_sample_online_test t left outer join 
(
	select a.shop_id as pre_shop_id,a.longitude,a.latitude
	from shop_info a
)b
on t.pre_shop_id = b.pre_shop_id;

drop table if  exists kk_offline_train_feature2;
drop table if  exists kk_offline_test_feature2;
drop table if  exists kk_online_test_feature2;
create table if not exists kk_offline_train_feature2 as
select a.*,b.user_shop_lo_gap,b.user_shop_la_gap,b.user_shop_dis
from kk_offline_train_feature1 a left outer join kk_offline_train_get_user_shop_dis b
on a.row_id=b.row_id and a.pre_shop_id=b.pre_shop_id;
create table if not exists kk_offline_test_feature2 as
select a.*,b.user_shop_lo_gap,b.user_shop_la_gap,b.user_shop_dis
from kk_offline_test_feature1 a left outer join kk_offline_test_get_user_shop_dis b
on a.row_id=b.row_id and a.pre_shop_id=b.pre_shop_id;
create table if not exists kk_online_test_feature2 as
select a.*,b.user_shop_lo_gap,b.user_shop_la_gap,b.user_shop_dis
from kk_online_test_feature1 a left outer join kk_online_test_get_user_shop_dis b
on a.row_id=b.row_id and a.pre_shop_id=b.pre_shop_id;
------------------------------------------------------------------------------------------------------

-- user与shop历史中位数位置距离(join on [row_id,pre_shop_id])
--kk_get_user_shop_median_dis
-- 线下训练
drop table if  exists kk_offline_train_get_user_shop_median_dis;
drop table if  exists kk_offline_test_get_user_shop_median_dis;
drop table if  exists kk_online_test_get_user_shop_median_dis;

create table if not exists kk_offline_train_get_user_shop_median_dis as
select t.row_id,t.pre_shop_id,abs(t.longitude-t2.median_longitude) as user_shop_median_lo_gap,
abs(t.latitude-t2.median_latitude) as user_shop_median_la_gap,
abs(t.longitude-t2.median_longitude)+abs(t.latitude-t2.median_latitude) as user_shop_median_dis
from kk_sample_offline_train t left outer join
(
	select median(t1.longitude) as median_longitude,median(t1.latitude) as median_latitude,t1.shop_id as pre_shop_id
	from
	(
		select a.*
		from kk_feature_offline_train a left outer join shop_info b on a.shop_id=b.shop_id
		where abs(a.longitude-b.longitude)<0.01 and abs(a.latitude-b.latitude)<0.01
	)t1
	group by shop_id
)t2
on t.pre_shop_id = t2.pre_shop_id;

-- 线下测试
create table if not exists kk_offline_test_get_user_shop_median_dis as
select t.row_id,t.pre_shop_id,abs(t.longitude-t2.median_longitude) as user_shop_median_lo_gap,
abs(t.latitude-t2.median_latitude) as user_shop_median_la_gap,
abs(t.longitude-t2.median_longitude)+abs(t.latitude-t2.median_latitude) as user_shop_median_dis
from kk_sample_offline_test t left outer join
(
	select median(t1.longitude) as median_longitude,median(t1.latitude) as median_latitude,t1.shop_id as pre_shop_id
	from
	(
		select a.*
		from kk_feature_offline_test a left outer join shop_info b on a.shop_id=b.shop_id
		where abs(a.longitude-b.longitude)<0.01 and abs(a.latitude-b.latitude)<0.01
	)t1
	group by shop_id
)t2
on t.pre_shop_id = t2.pre_shop_id;

-- 线上测试
create table if not exists kk_online_test_get_user_shop_median_dis as
select t.row_id,t.pre_shop_id,abs(t.longitude-t2.median_longitude) as user_shop_median_lo_gap,
abs(t.latitude-t2.median_latitude) as user_shop_median_la_gap,
abs(t.longitude-t2.median_longitude)+abs(t.latitude-t2.median_latitude) as user_shop_median_dis
from kk_sample_online_test t left outer join
(
	select median(t1.longitude) as median_longitude,median(t1.latitude) as median_latitude,t1.shop_id as pre_shop_id
	from
	(
		select a.*
		from kk_feature_online_test a left outer join shop_info b on a.shop_id=b.shop_id
		where abs(a.longitude-b.longitude)<0.01 and abs(a.latitude-b.latitude)<0.01
	)t1
	group by shop_id
)t2
on t.pre_shop_id = t2.pre_shop_id;

drop table if  exists kk_offline_train_feature3;
drop table if  exists kk_offline_test_feature3;
drop table if  exists kk_online_test_feature3;
create table if not exists kk_offline_train_feature3 as
select a.*,b.user_shop_median_lo_gap,b.user_shop_median_la_gap,b.user_shop_median_dis
from kk_offline_train_feature2 a left outer join kk_offline_train_get_user_shop_median_dis b
on a.row_id=b.row_id and a.pre_shop_id=b.pre_shop_id;
create table if not exists kk_offline_test_feature3 as
select a.*,b.user_shop_median_lo_gap,b.user_shop_median_la_gap,b.user_shop_median_dis
from kk_offline_test_feature2 a left outer join kk_offline_test_get_user_shop_median_dis b
on a.row_id=b.row_id and a.pre_shop_id=b.pre_shop_id;
create table if not exists kk_online_test_feature3 as
select a.*,b.user_shop_median_lo_gap,b.user_shop_median_la_gap,b.user_shop_median_dis
from kk_online_test_feature2 a left outer join kk_online_test_get_user_shop_median_dis b
on a.row_id=b.row_id and a.pre_shop_id=b.pre_shop_id;

------------------------------------------------------------------------------------------------------

-- user与shop历史平均位置距离(join on [row_id,pre_shop_id])
--kk_get_user_shop_mean_dis
-- 线下训练
drop table if  exists kk_offline_train_get_user_shop_mean_dis;
drop table if  exists kk_offline_test_get_user_shop_mean_dis;
drop table if  exists kk_online_test_get_user_shop_mean_dis;

create table if not exists kk_offline_train_get_user_shop_mean_dis as
select t.row_id,t.pre_shop_id,abs(t.longitude-t2.mean_longitude) as user_shop_mean_lo_gap,
abs(t.latitude-t2.mean_latitude) as user_shop_mean_la_gap,
abs(t.longitude-t2.mean_longitude)+abs(t.latitude-t2.mean_latitude) as user_shop_mean_dis
from kk_sample_offline_train t left outer join
(
	select avg(t1.longitude) as mean_longitude,avg(t1.latitude) as mean_latitude,t1.shop_id as pre_shop_id
	from
	(
		select a.*
		from kk_feature_offline_train a left outer join shop_info b on a.shop_id=b.shop_id
		where abs(a.longitude-b.longitude)<0.01 and abs(a.latitude-b.latitude)<0.01
	)t1
	group by shop_id
)t2
on t.pre_shop_id = t2.pre_shop_id;

-- 线下测试
create table if not exists kk_offline_test_get_user_shop_mean_dis as
select t.row_id,t.pre_shop_id,abs(t.longitude-t2.mean_longitude) as user_shop_mean_lo_gap,
abs(t.latitude-t2.mean_latitude) as user_shop_mean_la_gap,
abs(t.longitude-t2.mean_longitude)+abs(t.latitude-t2.mean_latitude) as user_shop_mean_dis
from kk_sample_offline_test t left outer join
(
	select avg(t1.longitude) as mean_longitude,avg(t1.latitude) as mean_latitude,t1.shop_id as pre_shop_id
	from
	(
		select a.*
		from kk_feature_offline_test a left outer join shop_info b on a.shop_id=b.shop_id
		where abs(a.longitude-b.longitude)<0.01 and abs(a.latitude-b.latitude)<0.01
	)t1
	group by shop_id
)t2
on t.pre_shop_id = t2.pre_shop_id;

-- 线上测试
create table if not exists kk_online_test_get_user_shop_mean_dis as
select t.row_id,t.pre_shop_id,abs(t.longitude-t2.mean_longitude) as user_shop_mean_lo_gap,
abs(t.latitude-t2.mean_latitude) as user_shop_mean_la_gap,
abs(t.longitude-t2.mean_longitude)+abs(t.latitude-t2.mean_latitude) as user_shop_mean_dis
from kk_sample_online_test t left outer join
(
	select avg(t1.longitude) as mean_longitude,avg(t1.latitude) as mean_latitude,t1.shop_id as pre_shop_id
	from
	(
		select a.*
		from kk_feature_online_test a left outer join shop_info b on a.shop_id=b.shop_id
		where abs(a.longitude-b.longitude)<0.01 and abs(a.latitude-b.latitude)<0.01
	)t1
	group by shop_id
)t2
on t.pre_shop_id = t2.pre_shop_id;

drop table if  exists kk_offline_train_feature4;
drop table if  exists kk_offline_test_feature4;
drop table if  exists kk_online_test_feature4;
create table if not exists kk_offline_train_feature4 as
select a.*,b.user_shop_mean_lo_gap,b.user_shop_mean_la_gap,b.user_shop_mean_dis
from kk_offline_train_feature3 a left outer join kk_offline_train_get_user_shop_mean_dis b
on a.row_id=b.row_id and a.pre_shop_id=b.pre_shop_id;
create table if not exists kk_offline_test_feature4 as
select a.*,b.user_shop_mean_lo_gap,b.user_shop_mean_la_gap,b.user_shop_mean_dis
from kk_offline_test_feature3 a left outer join kk_offline_test_get_user_shop_mean_dis b
on a.row_id=b.row_id and a.pre_shop_id=b.pre_shop_id;
create table if not exists kk_online_test_feature4 as
select a.*,b.user_shop_mean_lo_gap,b.user_shop_mean_la_gap,b.user_shop_mean_dis
from kk_online_test_feature3 a left outer join kk_online_test_get_user_shop_mean_dis b
on a.row_id=b.row_id and a.pre_shop_id=b.pre_shop_id;

------------------------------------------------------------------------------------------------------

-- user到shop历史次数(join on [user_id,pre_shop_id])
-- kk_get_user_shop_count
-- 线下训练
drop table if  exists kk_offline_train_get_user_shop_count;
drop table if  exists kk_offline_test_get_user_shop_count;
drop table if  exists kk_online_test_get_user_shop_count;

create table if not exists kk_offline_train_get_user_shop_count as
select user_id,pre_shop_id,user_shop_count,(user_shop_count/user_count) as user_preshop_rate
from
(
	select b.user_id,b.pre_shop_id,b.user_shop_count,c.user_count
	from
	(
		select user_id,pre_shop_id,count(*) as user_shop_count 
		from
		(
			select user_id,shop_id as pre_shop_id
			from kk_feature_offline_train
		)a
		group by user_id,pre_shop_id
	)b left outer join 
	(
		select user_id,count(*) as user_count 
		from
		(
			select user_id,shop_id as pre_shop_id
			from kk_feature_offline_train
		)a
		group by user_id
	)c
	on b.user_id=c.user_id
)d;


-- 线下测试
create table if not exists kk_offline_test_get_user_shop_count as
select user_id,pre_shop_id,user_shop_count,(user_shop_count/user_count) as user_preshop_rate
from
(
	select b.user_id,b.pre_shop_id,b.user_shop_count,c.user_count
	from
	(
		select user_id,pre_shop_id,count(*) as user_shop_count 
		from
		(
			select user_id,shop_id as pre_shop_id
			from kk_feature_offline_test
		)a
		group by user_id,pre_shop_id
	)b left outer join 
	(
		select user_id,count(*) as user_count 
		from
		(
			select user_id,shop_id as pre_shop_id
			from kk_feature_offline_test
		)a
		group by user_id
	)c
	on b.user_id=c.user_id
)d;

-- 线上测试
create table if not exists kk_online_test_get_user_shop_count as
select user_id,pre_shop_id,user_shop_count,(user_shop_count/user_count) as user_preshop_rate
from
(
	select b.user_id,b.pre_shop_id,b.user_shop_count,c.user_count
	from
	(
		select user_id,pre_shop_id,count(*) as user_shop_count 
		from
		(
			select user_id,shop_id as pre_shop_id
			from kk_feature_online_test
		)a
		group by user_id,pre_shop_id
	)b left outer join 
	(
		select user_id,count(*) as user_count 
		from
		(
			select user_id,shop_id as pre_shop_id
			from kk_feature_online_test
		)a
		group by user_id
	)c
	on b.user_id=c.user_id
)d;

drop table if  exists kk_offline_train_feature5;
drop table if  exists kk_offline_test_feature5;
drop table if  exists kk_online_test_feature5;
create table if not exists kk_offline_train_feature5 as
select a.*,b.user_shop_count,b.user_preshop_rate
from kk_offline_train_feature4 a left outer join kk_offline_train_get_user_shop_count b
on a.user_id=b.user_id and a.pre_shop_id=b.pre_shop_id;
create table if not exists kk_offline_test_feature5 as
select a.*,b.user_shop_count,b.user_preshop_rate
from kk_offline_test_feature4 a left outer join kk_offline_test_get_user_shop_count b
on a.user_id=b.user_id and a.pre_shop_id=b.pre_shop_id;
create table if not exists kk_online_test_feature5 as
select a.*,b.user_shop_count,b.user_preshop_rate
from kk_online_test_feature4 a left outer join kk_online_test_get_user_shop_count b
on a.user_id=b.user_id and a.pre_shop_id=b.pre_shop_id;

------------------------------------------------------------------------------------------------------

-- row跟历史商店wifi，sign相交的个数(join on [row_id,pre_shop_id])
--kk_get_row_shop_same_wifi_sign_count

-- kk_feature_offline_train_shop_expand_wifi
-- kk_feature_offline_test_shop_expand_wifi
-- kk_feature_online_test_shop_expand_wifi
-- kk_offline_train_expand_row_wifi
-- kk_offline_test_expand_row_wifi
-- kk_online_test_expand_row_wifi
-- 线下训练
drop table if  exists kk_offline_train_row_shop_same_wifi_sign_count;
drop table if  exists kk_offline_test_row_shop_same_wifi_sign_count;
drop table if  exists kk_online_test_row_shop_same_wifi_sign_count;

create table if not exists kk_offline_train_row_shop_same_wifi_sign_count as
select t2.row_id,t2.shop_id as pre_shop_id,count(*) as row_shop_same_wifi_sign_count
from
(
	select a.*,t1.shop_id
	from kk_offline_train_expand_row_wifi a inner join
	(
		select distinct shop_id,wifi,wifi_sign from kk_feature_offline_train_shop_expand_wifi
	)t1
	on a.wifi = t1.wifi and a.wifi_sign = t1.wifi_sign
)t2
group by t2.row_id, t2.shop_id;

-- 线下测试
create table if not exists kk_offline_test_row_shop_same_wifi_sign_count as
select t2.row_id,t2.shop_id as pre_shop_id,count(*) as row_shop_same_wifi_sign_count
from
(
	select a.*,t1.shop_id
	from kk_offline_test_expand_row_wifi a inner join
	(
		select distinct shop_id,wifi,wifi_sign from kk_feature_offline_test_shop_expand_wifi
	)t1
	on a.wifi = t1.wifi and a.wifi_sign = t1.wifi_sign
)t2
group by t2.row_id, t2.shop_id;

-- 线上测试
create table if not exists kk_online_test_row_shop_same_wifi_sign_count as
select t2.row_id,t2.shop_id as pre_shop_id,count(*) as row_shop_same_wifi_sign_count
from
(
	select a.*,t1.shop_id
	from kk_online_test_expand_row_wifi a inner join
	(
		select distinct shop_id,wifi,wifi_sign from kk_feature_online_test_shop_expand_wifi
	)t1
	on a.wifi = t1.wifi and a.wifi_sign = t1.wifi_sign
)t2
group by t2.row_id, t2.shop_id;

drop table if  exists kk_offline_train_feature6;
drop table if  exists kk_offline_test_feature6;
drop table if  exists kk_online_test_feature6;
create table if not exists kk_offline_train_feature6 as
select a.*,b.row_shop_same_wifi_sign_count
from kk_offline_train_feature5 a left outer join kk_offline_train_row_shop_same_wifi_sign_count b
on a.row_id=b.row_id and a.pre_shop_id=b.pre_shop_id;
create table if not exists kk_offline_test_feature6 as
select a.*,b.row_shop_same_wifi_sign_count
from kk_offline_test_feature5 a left outer join kk_offline_test_row_shop_same_wifi_sign_count b
on a.row_id=b.row_id and a.pre_shop_id=b.pre_shop_id;
create table if not exists kk_online_test_feature6 as
select a.*,b.row_shop_same_wifi_sign_count
from kk_online_test_feature5 a left outer join kk_online_test_row_shop_same_wifi_sign_count b
on a.row_id=b.row_id and a.pre_shop_id=b.pre_shop_id;


------------------------------------------------------------------------------------------------------

-- row跟历史商店wifi相交的个数(join on [row_id,pre_shop_id])
--kk_get_row_shop_same_wifi_count

-- kk_feature_offline_train_shop_expand_wifi
-- kk_feature_offline_test_shop_expand_wifi
-- kk_feature_online_test_shop_expand_wifi
-- kk_offline_train_expand_row_wifi
-- kk_offline_test_expand_row_wifi
-- kk_online_test_expand_row_wifi
-- 线下训练
drop table if  exists kk_offline_train_row_shop_same_wifi_count;
drop table if  exists kk_offline_test_row_shop_same_wifi_count;
drop table if  exists kk_online_test_row_shop_same_wifi_count;

create table if not exists kk_offline_train_row_shop_same_wifi_count as
select t2.row_id,t2.shop_id as pre_shop_id,count(*) as row_shop_same_wifi_count
from
(
	select a.*,t1.shop_id
	from kk_offline_train_expand_row_wifi a inner join
	(
		select distinct shop_id,wifi from kk_feature_offline_train_shop_expand_wifi
	)t1
	on a.wifi = t1.wifi
)t2
group by t2.row_id, t2.shop_id;

-- 线下测试
create table if not exists kk_offline_test_row_shop_same_wifi_count as
select t2.row_id,t2.shop_id as pre_shop_id,count(*) as row_shop_same_wifi_count
from
(
	select a.*,t1.shop_id
	from kk_offline_test_expand_row_wifi a inner join
	(
		select distinct shop_id,wifi from kk_feature_offline_test_shop_expand_wifi
	)t1
	on a.wifi = t1.wifi
)t2
group by t2.row_id, t2.shop_id;

-- 线上测试
create table if not exists kk_online_test_row_shop_same_wifi_count as
select t2.row_id,t2.shop_id as pre_shop_id,count(*) as row_shop_same_wifi_count
from
(
	select a.*,t1.shop_id
	from kk_online_test_expand_row_wifi a inner join
	(
		select distinct shop_id,wifi from kk_feature_online_test_shop_expand_wifi
	)t1
	on a.wifi = t1.wifi
)t2
group by t2.row_id, t2.shop_id;

drop table if  exists kk_offline_train_feature7;
drop table if  exists kk_offline_test_feature7;
drop table if  exists kk_online_test_feature7;
create table if not exists kk_offline_train_feature7 as
select a.*,b.row_shop_same_wifi_count
from kk_offline_train_feature6 a left outer join kk_offline_train_row_shop_same_wifi_count b
on a.row_id=b.row_id and a.pre_shop_id=b.pre_shop_id;
create table if not exists kk_offline_test_feature7 as
select a.*,b.row_shop_same_wifi_count
from kk_offline_test_feature6 a left outer join kk_offline_test_row_shop_same_wifi_count b
on a.row_id=b.row_id and a.pre_shop_id=b.pre_shop_id;
create table if not exists kk_online_test_feature7 as
select a.*,b.row_shop_same_wifi_count
from kk_online_test_feature6 a left outer join kk_online_test_row_shop_same_wifi_count b
on a.row_id=b.row_id and a.pre_shop_id=b.pre_shop_id;

------------------------------------------------------------------------------------------------------
-- shop最强wifi的id，第二强，第三强，最弱(join on [pre_shop_id])
--kk_get_shop_strongest_wifi

-- kk_feature_offline_train_shop_expand_wifi
-- kk_feature_offline_test_shop_expand_wifi
-- kk_feature_online_test_shop_expand_wifi
-- kk_offline_train_expand_row_wifi
-- kk_offline_test_expand_row_wifi
-- kk_online_test_expand_row_wifi
-- 线下训练
drop table if  exists kk_offline_train_shop_strongest_wifi;
drop table if  exists kk_offline_test_shop_strongest_wifi;
drop table if  exists kk_online_test_shop_strongest_wifi;

create table if not exists kk_offline_train_shop_strongest_wifi as
select e.*,f.weakest_wifi_id
from
(
	select c.*,d.strongest3_wifi_id
	from
	(
		select a.*,b.strongest2_wifi_id
		from
		(	
			select pre_shop_id, cast(substr(wifi, 3) as bigint) as strongest_wifi_id
			from
			(
				select shop_id as pre_shop_id, wifi, row_number() over (partition by shop_id order by wifi_sign desc) as wifi_id_rank
				from 
				(
					select distinct shop_id,wifi,wifi_sign from kk_feature_offline_train_shop_expand_wifi
				)t1
			)t2
			where wifi_id_rank=1
		)a left outer join
		(	
			select pre_shop_id, cast(substr(wifi, 3) as bigint) as strongest2_wifi_id
			from
			(
				select shop_id as pre_shop_id, wifi, row_number() over (partition by shop_id order by wifi_sign desc) as wifi_id_rank
				from 
				(
					select distinct shop_id,wifi,wifi_sign from kk_feature_offline_train_shop_expand_wifi
				)t1
			)t2
			where wifi_id_rank=2
		)b
		on a.pre_shop_id = b.pre_shop_id
	)c
	left outer join
	(	
		select pre_shop_id, cast(substr(wifi, 3) as bigint) as strongest3_wifi_id
		from
		(
			select shop_id as pre_shop_id, wifi, row_number() over (partition by shop_id order by wifi_sign desc) as wifi_id_rank
			from 
			(
				select distinct shop_id,wifi,wifi_sign from kk_feature_offline_train_shop_expand_wifi
			)t1
		)t2
		where wifi_id_rank=3
	)d
	on c.pre_shop_id = d.pre_shop_id
)e
left outer join
(	
	select pre_shop_id, cast(substr(wifi, 3) as bigint) as weakest_wifi_id
	from
	(
		select shop_id as pre_shop_id, wifi, row_number() over (partition by shop_id order by wifi_sign asc) as wifi_id_rank
		from 
		(
			select distinct shop_id,wifi,wifi_sign from kk_feature_offline_train_shop_expand_wifi
		)t1
	)t2
	where wifi_id_rank=1
)f
on e.pre_shop_id = f.pre_shop_id;


-- 线下测试
create table if not exists kk_offline_test_shop_strongest_wifi as
select e.*,f.weakest_wifi_id
from
(
	select c.*,d.strongest3_wifi_id
	from
	(
		select a.*,b.strongest2_wifi_id
		from
		(	
			select pre_shop_id, cast(substr(wifi, 3) as bigint) as strongest_wifi_id
			from
			(
				select shop_id as pre_shop_id, wifi, row_number() over (partition by shop_id order by wifi_sign desc) as wifi_id_rank
				from 
				(
					select distinct shop_id,wifi,wifi_sign from kk_feature_offline_test_shop_expand_wifi
				)t1
			)t2
			where wifi_id_rank=1
		)a left outer join
		(	
			select pre_shop_id, cast(substr(wifi, 3) as bigint) as strongest2_wifi_id
			from
			(
				select shop_id as pre_shop_id, wifi, row_number() over (partition by shop_id order by wifi_sign desc) as wifi_id_rank
				from 
				(
					select distinct shop_id,wifi,wifi_sign from kk_feature_offline_test_shop_expand_wifi
				)t1
			)t2
			where wifi_id_rank=2
		)b
		on a.pre_shop_id = b.pre_shop_id
	)c
	left outer join
	(	
		select pre_shop_id, cast(substr(wifi, 3) as bigint) as strongest3_wifi_id
		from
		(
			select shop_id as pre_shop_id, wifi, row_number() over (partition by shop_id order by wifi_sign desc) as wifi_id_rank
			from 
			(
				select distinct shop_id,wifi,wifi_sign from kk_feature_offline_test_shop_expand_wifi
			)t1
		)t2
		where wifi_id_rank=3
	)d
	on c.pre_shop_id = d.pre_shop_id
)e
left outer join
(	
	select pre_shop_id, cast(substr(wifi, 3) as bigint) as weakest_wifi_id
	from
	(
		select shop_id as pre_shop_id, wifi, row_number() over (partition by shop_id order by wifi_sign asc) as wifi_id_rank
		from 
		(
			select distinct shop_id,wifi,wifi_sign from kk_feature_offline_test_shop_expand_wifi
		)t1
	)t2
	where wifi_id_rank=1
)f
on e.pre_shop_id = f.pre_shop_id;

-- 线上测试
create table if not exists kk_online_test_shop_strongest_wifi as
select e.*,f.weakest_wifi_id
from
(
	select c.*,d.strongest3_wifi_id
	from
	(
		select a.*,b.strongest2_wifi_id
		from
		(	
			select pre_shop_id, cast(substr(wifi, 3) as bigint) as strongest_wifi_id
			from
			(
				select shop_id as pre_shop_id, wifi, row_number() over (partition by shop_id order by wifi_sign desc) as wifi_id_rank
				from 
				(
					select distinct shop_id,wifi,wifi_sign from kk_feature_online_test_shop_expand_wifi
				)t1
			)t2
			where wifi_id_rank=1
		)a left outer join
		(	
			select pre_shop_id, cast(substr(wifi, 3) as bigint) as strongest2_wifi_id
			from
			(
				select shop_id as pre_shop_id, wifi, row_number() over (partition by shop_id order by wifi_sign desc) as wifi_id_rank
				from 
				(
					select distinct shop_id,wifi,wifi_sign from kk_feature_online_test_shop_expand_wifi
				)t1
			)t2
			where wifi_id_rank=2
		)b
		on a.pre_shop_id = b.pre_shop_id
	)c
	left outer join
	(	
		select pre_shop_id, cast(substr(wifi, 3) as bigint) as strongest3_wifi_id
		from
		(
			select shop_id as pre_shop_id, wifi, row_number() over (partition by shop_id order by wifi_sign desc) as wifi_id_rank
			from 
			(
				select distinct shop_id,wifi,wifi_sign from kk_feature_online_test_shop_expand_wifi
			)t1
		)t2
		where wifi_id_rank=3
	)d
	on c.pre_shop_id = d.pre_shop_id
)e
left outer join
(	
	select pre_shop_id, cast(substr(wifi, 3) as bigint) as weakest_wifi_id
	from
	(
		select shop_id as pre_shop_id, wifi, row_number() over (partition by shop_id order by wifi_sign asc) as wifi_id_rank
		from 
		(
			select distinct shop_id,wifi,wifi_sign from kk_feature_online_test_shop_expand_wifi
		)t1
	)t2
	where wifi_id_rank=1
)f
on e.pre_shop_id = f.pre_shop_id;


drop table if  exists kk_offline_train_feature8;
drop table if  exists kk_offline_test_feature8;
drop table if  exists kk_online_test_feature8;
create table if not exists kk_offline_train_feature8 as
select a.*,b.strongest_wifi_id,b.strongest2_wifi_id,b.strongest3_wifi_id,b.weakest_wifi_id
from kk_offline_train_feature7 a left outer join kk_offline_train_shop_strongest_wifi b
on a.pre_shop_id=b.pre_shop_id;
create table if not exists kk_offline_test_feature8 as
select a.*,b.strongest_wifi_id,b.strongest2_wifi_id,b.strongest3_wifi_id,b.weakest_wifi_id
from kk_offline_test_feature7 a left outer join kk_offline_test_shop_strongest_wifi b
on a.pre_shop_id=b.pre_shop_id;
create table if not exists kk_online_test_feature8 as
select a.*,b.strongest_wifi_id,b.strongest2_wifi_id,b.strongest3_wifi_id,b.weakest_wifi_id
from kk_online_test_feature7 a left outer join kk_online_test_shop_strongest_wifi b
on a.pre_shop_id=b.pre_shop_id;
------------------------------------------------------------------------------------------------------
-- wifi_repare_count(join on [row_id,pre_shop_id])  wifi出现在商店的概率（shop,wifi出现次数/wifi出现总次数）
--kk_get_wifi_repare_count

-- kk_feature_offline_train_shop_expand_wifi
-- kk_feature_offline_test_shop_expand_wifi
-- kk_feature_online_test_shop_expand_wifi
-- kk_offline_train_expand_row_wifi
-- kk_offline_test_expand_row_wifi
-- kk_online_test_expand_row_wifi
-- 线下训练
drop table if  exists kk_offline_train_wifi_repare_count;
drop table if  exists kk_offline_test_wifi_repare_count;
drop table if  exists kk_online_test_wifi_repare_count;

create table if not exists kk_offline_train_wifi_repare_count as
select row_id,shop_id as pre_shop_id, avg(repare_shop_count) as repare_shop_count, avg(repare_all_count) as repare_all_count,
avg(wifi_repare_rate) as wifi_repare_rate
from 
(
	select c.row_id,c.wifi,t.shop_id,t.repare_shop_count,t.repare_all_count,t.wifi_repare_rate
	from
	kk_offline_train_expand_row_wifi c left outer join
	(
		select a.shop_id,a.wifi,a.repare_shop_count,b.repare_all_count,(a.repare_shop_count/b.repare_all_count) as wifi_repare_rate
		from
		(
			select shop_id,wifi, count(*) as repare_shop_count
			from 
			(
				select distinct shop_id,wifi,wifi_sign from kk_feature_offline_train_shop_expand_wifi
			)t1
			group by shop_id,wifi

		)a
		left outer join
		(
			select wifi, count(*) as repare_all_count
			from 
			(
				select distinct shop_id,wifi,wifi_sign from kk_feature_offline_train_shop_expand_wifi
			)t2
			group by wifi

		)b
		on a.wifi=b.wifi
	)t
	on c.wifi=t.wifi
)d
group by row_id,shop_id;

-- 线下测试
create table if not exists kk_offline_test_wifi_repare_count as
select row_id,shop_id as pre_shop_id, avg(repare_shop_count) as repare_shop_count, avg(repare_all_count) as repare_all_count,
avg(wifi_repare_rate) as wifi_repare_rate
from 
(
	select c.row_id,c.wifi,t.shop_id,t.repare_shop_count,t.repare_all_count,t.wifi_repare_rate
	from
	kk_offline_test_expand_row_wifi c left outer join
	(
		select a.shop_id,a.wifi,a.repare_shop_count,b.repare_all_count,(a.repare_shop_count/b.repare_all_count) as wifi_repare_rate
		from
		(
			select shop_id,wifi, count(*) as repare_shop_count
			from 
			(
				select distinct shop_id,wifi,wifi_sign from kk_feature_offline_test_shop_expand_wifi
			)t1
			group by shop_id,wifi

		)a
		left outer join
		(
			select wifi, count(*) as repare_all_count
			from 
			(
				select distinct shop_id,wifi,wifi_sign from kk_feature_offline_test_shop_expand_wifi
			)t2
			group by wifi

		)b
		on a.wifi=b.wifi
	)t
	on c.wifi=t.wifi
)d
group by row_id,shop_id;

-- 线上测试
create table if not exists kk_online_test_wifi_repare_count as
select row_id,shop_id as pre_shop_id, avg(repare_shop_count) as repare_shop_count, avg(repare_all_count) as repare_all_count,
avg(wifi_repare_rate) as wifi_repare_rate
from 
(
	select c.row_id,c.wifi,t.shop_id,t.repare_shop_count,t.repare_all_count,t.wifi_repare_rate
	from
	kk_online_test_expand_row_wifi c left outer join
	(
		select a.shop_id,a.wifi,a.repare_shop_count,b.repare_all_count,(a.repare_shop_count/b.repare_all_count) as wifi_repare_rate
		from
		(
			select shop_id,wifi, count(*) as repare_shop_count
			from 
			(
				select distinct shop_id,wifi,wifi_sign from kk_feature_online_test_shop_expand_wifi
			)t1
			group by shop_id,wifi

		)a
		left outer join
		(
			select wifi, count(*) as repare_all_count
			from 
			(
				select distinct shop_id,wifi,wifi_sign from kk_feature_online_test_shop_expand_wifi
			)t2
			group by wifi

		)b
		on a.wifi=b.wifi
	)t
	on c.wifi=t.wifi
)d
group by row_id,shop_id;

drop table if  exists kk_offline_train_feature9;
drop table if  exists kk_offline_test_feature9;
drop table if  exists kk_online_test_feature9;
create table if not exists kk_offline_train_feature9 as
select a.*,b.repare_shop_count,b.repare_all_count,b.wifi_repare_rate
from kk_offline_train_feature8 a left outer join kk_offline_train_wifi_repare_count b
on a.row_id=b.row_id and a.pre_shop_id=b.pre_shop_id;
create table if not exists kk_offline_test_feature9 as
select a.*,b.repare_shop_count,b.repare_all_count,b.wifi_repare_rate
from kk_offline_test_feature8 a left outer join kk_offline_test_wifi_repare_count b
on a.row_id=b.row_id and a.pre_shop_id=b.pre_shop_id;
create table if not exists kk_online_test_feature9 as
select a.*,b.repare_shop_count,b.repare_all_count,b.wifi_repare_rate
from kk_online_test_feature8 a left outer join kk_online_test_wifi_repare_count b
on a.row_id=b.row_id and a.pre_shop_id=b.pre_shop_id;

------------------------------------------------------------------------------------------------------
-- wifi_shop_prob(join on [row_id,pre_shop_id])  出现在某个商店的概率（shop,wifi出现次数/商店出现总次数）
--kk_get_wifi_shop_prob

-- kk_feature_offline_train_shop_expand_wifi
-- kk_feature_offline_test_shop_expand_wifi
-- kk_feature_online_test_shop_expand_wifi
-- kk_offline_train_expand_row_wifi
-- kk_offline_test_expand_row_wifi
-- kk_online_test_expand_row_wifi
-- 线下训练
drop table if  exists kk_offline_train_get_wifi_shop_prob;
drop table if  exists kk_offline_test_get_wifi_shop_prob;
drop table if  exists kk_online_test_get_wifi_shop_prob;

create table if not exists kk_offline_train_get_wifi_shop_prob as
select row_id,shop_id as pre_shop_id, sum(wifi_shop_prob) as wifi_shop_prob
from 
(
	select c.row_id,c.wifi,t.shop_id,t.wifi_shop_prob
	from
	kk_offline_train_expand_row_wifi c left outer join
	(
		select a.shop_id,a.wifi,a.repare_shop_count,b.shop_all_count,(a.repare_shop_count/b.shop_all_count) as wifi_shop_prob
		from
		(
			select shop_id,wifi, count(*) as repare_shop_count
			from 
			(
				select  shop_id,wifi,wifi_sign from kk_feature_offline_train_shop_expand_wifi
			)t1
			group by shop_id,wifi

		)a
		left outer join
		(
			select shop_id, count(*) as shop_all_count
			from 
			(
				select  shop_id,wifi,wifi_sign from kk_feature_offline_train_shop_expand_wifi
			)t2
			group by shop_id

		)b
		on a.shop_id=b.shop_id
	)t
	on c.wifi=t.wifi
)d
group by row_id,shop_id;

-- 线下测试
create table if not exists kk_offline_test_get_wifi_shop_prob as
select row_id,shop_id as pre_shop_id, sum(wifi_shop_prob) as wifi_shop_prob
from 
(
	select c.row_id,c.wifi,t.shop_id,t.wifi_shop_prob
	from
	kk_offline_test_expand_row_wifi c left outer join
	(
		select a.shop_id,a.wifi,a.repare_shop_count,b.shop_all_count,(a.repare_shop_count/b.shop_all_count) as wifi_shop_prob
		from
		(
			select shop_id,wifi, count(*) as repare_shop_count
			from 
			(
				select  shop_id,wifi,wifi_sign from kk_feature_offline_test_shop_expand_wifi
			)t1
			group by shop_id,wifi

		)a
		left outer join
		(
			select shop_id, count(*) as shop_all_count
			from 
			(
				select  shop_id,wifi,wifi_sign from kk_feature_offline_test_shop_expand_wifi
			)t2
			group by shop_id

		)b
		on a.shop_id=b.shop_id
	)t
	on c.wifi=t.wifi
)d
group by row_id,shop_id;

-- 线上测试
create table if not exists kk_online_test_get_wifi_shop_prob as
select row_id,shop_id as pre_shop_id, sum(wifi_shop_prob) as wifi_shop_prob
from 
(
	select c.row_id,c.wifi,t.shop_id,t.wifi_shop_prob
	from
	kk_online_test_expand_row_wifi c left outer join
	(
		select a.shop_id,a.wifi,a.repare_shop_count,b.shop_all_count,(a.repare_shop_count/b.shop_all_count) as wifi_shop_prob
		from
		(
			select shop_id,wifi, count(*) as repare_shop_count
			from 
			(
				select  shop_id,wifi,wifi_sign from kk_feature_online_test_shop_expand_wifi
			)t1
			group by shop_id,wifi

		)a
		left outer join
		(
			select shop_id, count(*) as shop_all_count
			from 
			(
				select  shop_id,wifi,wifi_sign from kk_feature_online_test_shop_expand_wifi
			)t2
			group by shop_id

		)b
		on a.shop_id=b.shop_id
	)t
	on c.wifi=t.wifi
)d
group by row_id,shop_id;

drop table if  exists kk_offline_train_feature10;
drop table if  exists kk_offline_test_feature10;
drop table if  exists kk_online_test_feature10;
create table if not exists kk_offline_train_feature10 as
select a.*,b.wifi_shop_prob
from kk_offline_train_feature9 a left outer join kk_offline_train_get_wifi_shop_prob b
on a.row_id=b.row_id and a.pre_shop_id=b.pre_shop_id;
create table if not exists kk_offline_test_feature10 as
select a.*,b.wifi_shop_prob
from kk_offline_test_feature9 a left outer join kk_offline_test_get_wifi_shop_prob b
on a.row_id=b.row_id and a.pre_shop_id=b.pre_shop_id;
create table if not exists kk_online_test_feature10 as
select a.*,b.wifi_shop_prob
from kk_online_test_feature9 a left outer join kk_online_test_get_wifi_shop_prob b
on a.row_id=b.row_id and a.pre_shop_id=b.pre_shop_id;
------------------------------------------------------------------------------------------------------
-- wifi_shop_prob2222(join on [row_id,pre_shop_id])  出现在某个商店的概率（shop,wifi出现次数/商店出现总次数）
--kk_get_wifi_shop_prob2222

-- kk_feature_offline_train_shop_expand_wifi
-- kk_feature_offline_test_shop_expand_wifi
-- kk_feature_online_test_shop_expand_wifi
-- kk_offline_train_expand_row_wifi
-- kk_offline_test_expand_row_wifi
-- kk_online_test_expand_row_wifi
-- 线下训练
drop table if  exists kk_offline_train_get_wifi_shop_prob2222;
drop table if  exists kk_offline_test_get_wifi_shop_prob2222;
drop table if  exists kk_online_test_get_wifi_shop_prob2222;

create table if not exists kk_offline_train_get_wifi_shop_prob2222 as
select row_id,shop_id as pre_shop_id, sum(wifi_shop_prob2) as wifi_shop_prob2,min(wifi_shop_prob2) as wifi_shop_min_prob
from 
(
	select c.row_id,c.wifi,t.shop_id,t.wifi_shop_prob2
	from
	kk_offline_train_expand_row_wifi c left outer join
	(
		select a.shop_id,a.wifi,a.repare_shop_count,b.shop_all_count,(a.repare_shop_count/b.shop_all_count) as wifi_shop_prob2
		from
		(
			select shop_id,wifi, count(*) as repare_shop_count
			from 
			(
				select  shop_id,wifi,wifi_sign from kk_feature_offline_train_shop_expand_wifi
			)t1
			group by shop_id,wifi

		)a
		left outer join
		(
			select shop_id, count(*) as shop_all_count
			from kk_feature_offline_train
			group by shop_id

		)b
		on a.shop_id=b.shop_id
	)t
	on c.wifi=t.wifi
)d
group by row_id,shop_id;

-- 线下测试
create table if not exists kk_offline_test_get_wifi_shop_prob2222 as
select row_id,shop_id as pre_shop_id, sum(wifi_shop_prob2) as wifi_shop_prob2,min(wifi_shop_prob2) as wifi_shop_min_prob
from 
(
	select c.row_id,c.wifi,t.shop_id,t.wifi_shop_prob2
	from
	kk_offline_test_expand_row_wifi c left outer join
	(
		select a.shop_id,a.wifi,a.repare_shop_count,b.shop_all_count,(a.repare_shop_count/b.shop_all_count) as wifi_shop_prob2
		from
		(
			select shop_id,wifi, count(*) as repare_shop_count
			from 
			(
				select  shop_id,wifi,wifi_sign from kk_feature_offline_test_shop_expand_wifi
			)t1
			group by shop_id,wifi

		)a
		left outer join
		(
			select shop_id, count(*) as shop_all_count
			from kk_feature_offline_test
			group by shop_id

		)b
		on a.shop_id=b.shop_id
	)t
	on c.wifi=t.wifi
)d
group by row_id,shop_id;

-- 线上测试
create table if not exists kk_online_test_get_wifi_shop_prob2222 as
select row_id,shop_id as pre_shop_id, sum(wifi_shop_prob2) as wifi_shop_prob2,min(wifi_shop_prob2) as wifi_shop_min_prob
from 
(
	select c.row_id,c.wifi,t.shop_id,t.wifi_shop_prob2
	from
	kk_online_test_expand_row_wifi c left outer join
	(
		select a.shop_id,a.wifi,a.repare_shop_count,b.shop_all_count,(a.repare_shop_count/b.shop_all_count) as wifi_shop_prob2
		from
		(
			select shop_id,wifi, count(*) as repare_shop_count
			from 
			(
				select  shop_id,wifi,wifi_sign from kk_feature_online_test_shop_expand_wifi
			)t1
			group by shop_id,wifi

		)a
		left outer join
		(
			select shop_id, count(*) as shop_all_count
			from kk_feature_online_test
			group by shop_id

		)b
		on a.shop_id=b.shop_id
	)t
	on c.wifi=t.wifi
)d
group by row_id,shop_id;

drop table if  exists kk_offline_train_feature11;
drop table if  exists kk_offline_test_feature11;
drop table if  exists kk_online_test_feature11;
create table if not exists kk_offline_train_feature11 as
select a.*,b.wifi_shop_prob2,b.wifi_shop_min_prob
from kk_offline_train_feature10 a left outer join kk_offline_train_get_wifi_shop_prob2222 b
on a.row_id=b.row_id and a.pre_shop_id=b.pre_shop_id;
create table if not exists kk_offline_test_feature11 as
select a.*,b.wifi_shop_prob2,b.wifi_shop_min_prob
from kk_offline_test_feature10 a left outer join kk_offline_test_get_wifi_shop_prob2222 b
on a.row_id=b.row_id and a.pre_shop_id=b.pre_shop_id;
create table if not exists kk_online_test_feature11 as
select a.*,b.wifi_shop_prob2,b.wifi_shop_min_prob
from kk_online_test_feature10 a left outer join kk_online_test_get_wifi_shop_prob2222 b
on a.row_id=b.row_id and a.pre_shop_id=b.pre_shop_id;

----------------------------------------------------------------
drop table if exists kk_offline_train_feature12;
drop table if exists kk_offline_test_feature12;
-- drop table if exists kk_online_test_feature12;
create table if not exists kk_offline_train_feature12 as
select a.shop_id,a.label,b.*
from kk_sample_offline_train a left outer join kk_offline_train_feature11 b
on a.row_id=b.row_id and a.pre_shop_id=b.pre_shop_id;
create table if not exists kk_offline_test_feature12 as
select a.shop_id,a.label,b.*
from kk_sample_offline_test a left outer join kk_offline_test_feature11 b
on a.row_id=b.row_id and a.pre_shop_id=b.pre_shop_id;
-- create table if not exists kk_online_test_feature12 as
-- select a.shop_id,a.label,b.*
-- from kk_sample_online_test a left outer join kk_online_test_feature11 b
-- on a.row_id=b.row_id and a.pre_shop_id=b.pre_shop_id;
----------------------------------------------------------------------
------------------------------------------------------------------------------------------------------
-- wifi_shop_prob3333(join on [row_id,pre_shop_id])  出现在某个商店的概率（shop,wifi出现次数/商店出现总次数）
--kk_get_wifi_shop_prob3333

-- kk_feature_offline_train_shop_expand_wifi
-- kk_feature_offline_test_shop_expand_wifi
-- kk_feature_online_test_shop_expand_wifi
-- kk_offline_train_expand_row_wifi
-- kk_offline_test_expand_row_wifi
-- kk_online_test_expand_row_wifi
-- 线下训练
drop table if  exists kk_offline_train_get_wifi_shop_prob3333_1;
drop table if  exists kk_offline_train_get_wifi_shop_prob3333_2;
drop table if  exists kk_offline_train_get_wifi_shop_prob3333_3;
drop table if  exists kk_offline_test_get_wifi_shop_prob3333_1;
drop table if  exists kk_offline_test_get_wifi_shop_prob3333_2;
drop table if  exists kk_offline_test_get_wifi_shop_prob3333_3;
drop table if  exists kk_online_test_get_wifi_shop_prob3333_1;
drop table if  exists kk_online_test_get_wifi_shop_prob3333_2;
drop table if  exists kk_online_test_get_wifi_shop_prob3333_3;

create table if not exists kk_offline_train_get_wifi_shop_prob3333_1 as
select c.row_id,t.shop_id as pre_shop_id,t.wifi_shop_prob2 as strongest_wifi_shop_prob
from
(
	select row_id,wifi
	from
    (
        select row_id,wifi,row_number() over (partition by row_id order by wifi_sign desc) as wifi_id_rank
        from kk_offline_train_expand_row_wifi
    )m
    where wifi_id_rank=1

)c 
inner join
(
	select a.shop_id,a.wifi,a.repare_shop_count,b.shop_all_count,(a.repare_shop_count/b.shop_all_count) as wifi_shop_prob2
	from
	(
		select shop_id,wifi, count(*) as repare_shop_count
		from 
		(
			select  shop_id,wifi,wifi_sign from kk_feature_offline_train_shop_expand_wifi
		)t1
		group by shop_id,wifi

	)a
	left outer join
	(
		select shop_id, count(*) as shop_all_count
		from kk_feature_offline_train
		group by shop_id

	)b
	on a.shop_id=b.shop_id
)t
on c.wifi=t.wifi;

create table if not exists kk_offline_train_get_wifi_shop_prob3333_2 as
select c.row_id,t.shop_id as pre_shop_id,t.wifi_shop_prob2 as strongest2_wifi_shop_prob
from
(
	select row_id,wifi
	from
    (
        select row_id,wifi,row_number() over (partition by row_id order by wifi_sign desc) as wifi_id_rank
        from kk_offline_train_expand_row_wifi
    )m
    where wifi_id_rank=2

)c 
inner join
(
	select a.shop_id,a.wifi,a.repare_shop_count,b.shop_all_count,(a.repare_shop_count/b.shop_all_count) as wifi_shop_prob2
	from
	(
		select shop_id,wifi, count(*) as repare_shop_count
		from 
		(
			select  shop_id,wifi,wifi_sign from kk_feature_offline_train_shop_expand_wifi
		)t1
		group by shop_id,wifi

	)a
	left outer join
	(
		select shop_id, count(*) as shop_all_count
		from kk_feature_offline_train
		group by shop_id

	)b
	on a.shop_id=b.shop_id
)t
on c.wifi=t.wifi;

create table if not exists kk_offline_train_get_wifi_shop_prob3333_3 as
select c.row_id,t.shop_id as pre_shop_id,t.wifi_shop_prob2 as strongest3_wifi_shop_prob
from
(
	select row_id,wifi
	from
    (
        select row_id,wifi,row_number() over (partition by row_id order by wifi_sign desc) as wifi_id_rank
        from kk_offline_train_expand_row_wifi
    )m
    where wifi_id_rank=3

)c 
inner join
(
	select a.shop_id,a.wifi,a.repare_shop_count,b.shop_all_count,(a.repare_shop_count/b.shop_all_count) as wifi_shop_prob2
	from
	(
		select shop_id,wifi, count(*) as repare_shop_count
		from 
		(
			select  shop_id,wifi,wifi_sign from kk_feature_offline_train_shop_expand_wifi
		)t1
		group by shop_id,wifi

	)a
	left outer join
	(
		select shop_id, count(*) as shop_all_count
		from kk_feature_offline_train
		group by shop_id

	)b
	on a.shop_id=b.shop_id
)t
on c.wifi=t.wifi;

-- 线下测试
create table if not exists kk_offline_test_get_wifi_shop_prob3333_1 as
select c.row_id,t.shop_id as pre_shop_id,t.wifi_shop_prob2 as strongest_wifi_shop_prob
from
(
	select row_id,wifi
	from
    (
        select row_id,wifi,row_number() over (partition by row_id order by wifi_sign desc) as wifi_id_rank
        from kk_offline_test_expand_row_wifi
    )m
    where wifi_id_rank=1

)c 
inner join
(
	select a.shop_id,a.wifi,a.repare_shop_count,b.shop_all_count,(a.repare_shop_count/b.shop_all_count) as wifi_shop_prob2
	from
	(
		select shop_id,wifi, count(*) as repare_shop_count
		from 
		(
			select  shop_id,wifi,wifi_sign from kk_feature_offline_test_shop_expand_wifi
		)t1
		group by shop_id,wifi

	)a
	left outer join
	(
		select shop_id, count(*) as shop_all_count
		from kk_feature_offline_test
		group by shop_id

	)b
	on a.shop_id=b.shop_id
)t
on c.wifi=t.wifi;

create table if not exists kk_offline_test_get_wifi_shop_prob3333_2 as
select c.row_id,t.shop_id as pre_shop_id,t.wifi_shop_prob2 as strongest2_wifi_shop_prob
from
(
	select row_id,wifi
	from
    (
        select row_id,wifi,row_number() over (partition by row_id order by wifi_sign desc) as wifi_id_rank
        from kk_offline_test_expand_row_wifi
    )m
    where wifi_id_rank=2

)c 
inner join
(
	select a.shop_id,a.wifi,a.repare_shop_count,b.shop_all_count,(a.repare_shop_count/b.shop_all_count) as wifi_shop_prob2
	from
	(
		select shop_id,wifi, count(*) as repare_shop_count
		from 
		(
			select  shop_id,wifi,wifi_sign from kk_feature_offline_test_shop_expand_wifi
		)t1
		group by shop_id,wifi

	)a
	left outer join
	(
		select shop_id, count(*) as shop_all_count
		from kk_feature_offline_test
		group by shop_id

	)b
	on a.shop_id=b.shop_id
)t
on c.wifi=t.wifi;

create table if not exists kk_offline_test_get_wifi_shop_prob3333_3 as
select c.row_id,t.shop_id as pre_shop_id,t.wifi_shop_prob2 as strongest3_wifi_shop_prob
from
(
	select row_id,wifi
	from
    (
        select row_id,wifi,row_number() over (partition by row_id order by wifi_sign desc) as wifi_id_rank
        from kk_offline_test_expand_row_wifi
    )m
    where wifi_id_rank=3

)c 
inner join
(
	select a.shop_id,a.wifi,a.repare_shop_count,b.shop_all_count,(a.repare_shop_count/b.shop_all_count) as wifi_shop_prob2
	from
	(
		select shop_id,wifi, count(*) as repare_shop_count
		from 
		(
			select  shop_id,wifi,wifi_sign from kk_feature_offline_test_shop_expand_wifi
		)t1
		group by shop_id,wifi

	)a
	left outer join
	(
		select shop_id, count(*) as shop_all_count
		from kk_feature_offline_test
		group by shop_id

	)b
	on a.shop_id=b.shop_id
)t
on c.wifi=t.wifi;

-- 线上测试
create table if not exists kk_online_test_get_wifi_shop_prob3333_1 as
select c.row_id,t.shop_id as pre_shop_id,t.wifi_shop_prob2 as strongest_wifi_shop_prob
from
(
	select row_id,wifi
	from
    (
        select row_id,wifi,row_number() over (partition by row_id order by wifi_sign desc) as wifi_id_rank
        from kk_online_test_expand_row_wifi
    )m
    where wifi_id_rank=1

)c 
inner join
(
	select a.shop_id,a.wifi,a.repare_shop_count,b.shop_all_count,(a.repare_shop_count/b.shop_all_count) as wifi_shop_prob2
	from
	(
		select shop_id,wifi, count(*) as repare_shop_count
		from 
		(
			select  shop_id,wifi,wifi_sign from kk_feature_online_test_shop_expand_wifi
		)t1
		group by shop_id,wifi

	)a
	left outer join
	(
		select shop_id, count(*) as shop_all_count
		from kk_feature_online_test
		group by shop_id

	)b
	on a.shop_id=b.shop_id
)t
on c.wifi=t.wifi;

create table if not exists kk_online_test_get_wifi_shop_prob3333_2 as
select c.row_id,t.shop_id as pre_shop_id,t.wifi_shop_prob2 as strongest2_wifi_shop_prob
from
(
	select row_id,wifi
	from
    (
        select row_id,wifi,row_number() over (partition by row_id order by wifi_sign desc) as wifi_id_rank
        from kk_online_test_expand_row_wifi
    )m
    where wifi_id_rank=2

)c 
inner join
(
	select a.shop_id,a.wifi,a.repare_shop_count,b.shop_all_count,(a.repare_shop_count/b.shop_all_count) as wifi_shop_prob2
	from
	(
		select shop_id,wifi, count(*) as repare_shop_count
		from 
		(
			select  shop_id,wifi,wifi_sign from kk_feature_online_test_shop_expand_wifi
		)t1
		group by shop_id,wifi

	)a
	left outer join
	(
		select shop_id, count(*) as shop_all_count
		from kk_feature_online_test
		group by shop_id

	)b
	on a.shop_id=b.shop_id
)t
on c.wifi=t.wifi;

create table if not exists kk_online_test_get_wifi_shop_prob3333_3 as
select c.row_id,t.shop_id as pre_shop_id,t.wifi_shop_prob2 as strongest3_wifi_shop_prob
from
(
	select row_id,wifi
	from
    (
        select row_id,wifi,row_number() over (partition by row_id order by wifi_sign desc) as wifi_id_rank
        from kk_online_test_expand_row_wifi
    )m
    where wifi_id_rank=3

)c 
inner join
(
	select a.shop_id,a.wifi,a.repare_shop_count,b.shop_all_count,(a.repare_shop_count/b.shop_all_count) as wifi_shop_prob2
	from
	(
		select shop_id,wifi, count(*) as repare_shop_count
		from 
		(
			select  shop_id,wifi,wifi_sign from kk_feature_online_test_shop_expand_wifi
		)t1
		group by shop_id,wifi

	)a
	left outer join
	(
		select shop_id, count(*) as shop_all_count
		from kk_feature_online_test
		group by shop_id

	)b
	on a.shop_id=b.shop_id
)t
on c.wifi=t.wifi;

drop table if  exists kk_offline_train_feature13;
drop table if  exists kk_offline_test_feature13;
drop table if  exists kk_online_test_feature13;
create table if not exists kk_offline_train_feature13 as
select a.*,b.strongest_wifi_shop_prob
from kk_offline_train_feature12 a left outer join kk_offline_train_get_wifi_shop_prob3333_1 b
on a.row_id=b.row_id and a.pre_shop_id=b.pre_shop_id;
create table if not exists kk_offline_test_feature13 as
select a.*,b.strongest_wifi_shop_prob
from kk_offline_test_feature12 a left outer join kk_offline_test_get_wifi_shop_prob3333_1 b
on a.row_id=b.row_id and a.pre_shop_id=b.pre_shop_id;
create table if not exists kk_online_test_feature13 as
select a.*,b.strongest_wifi_shop_prob
from kk_online_test_feature11 a left outer join kk_online_test_get_wifi_shop_prob3333_1 b
on a.row_id=b.row_id and a.pre_shop_id=b.pre_shop_id;

drop table if  exists kk_offline_train_feature14;
drop table if  exists kk_offline_test_feature14;
drop table if  exists kk_online_test_feature14;
create table if not exists kk_offline_train_feature14 as
select a.*,b.strongest2_wifi_shop_prob
from kk_offline_train_feature13 a left outer join kk_offline_train_get_wifi_shop_prob3333_2 b
on a.row_id=b.row_id and a.pre_shop_id=b.pre_shop_id;
create table if not exists kk_offline_test_feature14 as
select a.*,b.strongest2_wifi_shop_prob
from kk_offline_test_feature13 a left outer join kk_offline_test_get_wifi_shop_prob3333_2 b
on a.row_id=b.row_id and a.pre_shop_id=b.pre_shop_id;
create table if not exists kk_online_test_feature14 as
select a.*,b.strongest2_wifi_shop_prob
from kk_online_test_feature13 a left outer join kk_online_test_get_wifi_shop_prob3333_2 b
on a.row_id=b.row_id and a.pre_shop_id=b.pre_shop_id;

drop table if  exists kk_offline_train_feature15;
drop table if  exists kk_offline_test_feature15;
drop table if  exists kk_online_test_feature15;
create table if not exists kk_offline_train_feature15 as
select a.*,b.strongest3_wifi_shop_prob
from kk_offline_train_feature14 a left outer join kk_offline_train_get_wifi_shop_prob3333_3 b
on a.row_id=b.row_id and a.pre_shop_id=b.pre_shop_id;
create table if not exists kk_offline_test_feature15 as
select a.*,b.strongest3_wifi_shop_prob
from kk_offline_test_feature14 a left outer join kk_offline_test_get_wifi_shop_prob3333_3 b
on a.row_id=b.row_id and a.pre_shop_id=b.pre_shop_id;
create table if not exists kk_online_test_feature15 as
select a.*,b.strongest3_wifi_shop_prob
from kk_online_test_feature14 a left outer join kk_online_test_get_wifi_shop_prob3333_3 b
on a.row_id=b.row_id and a.pre_shop_id=b.pre_shop_id;
------------------------------------------------------------------------------------------------------
-- get_row_shop_wifi_count_rate(join on [row_id,pre_shop_id])  
--kk_row_shop_wifi_count_rate

-- kk_feature_offline_train_shop_expand_wifi
-- kk_feature_offline_test_shop_expand_wifi
-- kk_feature_online_test_shop_expand_wifi
-- kk_offline_train_expand_row_wifi
-- kk_offline_test_expand_row_wifi
-- kk_online_test_expand_row_wifi
-- 线下训练
drop table if  exists kk_offline_train_get_row_shop_wifi_count_rate_1;
drop table if  exists kk_offline_train_get_row_shop_wifi_count_rate_2;
drop table if  exists kk_offline_test_get_row_shop_wifi_count_rate_1;
drop table if  exists kk_offline_test_get_row_shop_wifi_count_rate_2;;
drop table if  exists kk_online_test_get_row_shop_wifi_count_rate_1;
drop table if  exists kk_online_test_get_row_shop_wifi_count_rate_2;

create table if not exists kk_offline_train_get_row_shop_wifi_count_rate_1 as
select d.row_id,d.shop_id as pre_shop_id,(d.row_shop_count/e.shop_wifi_count) as shop_wifi_count_rate
from
(
	select row_id,shop_id,count(*) as row_shop_count
	from
	(
		select a.*,b.shop_id
		from kk_offline_train_expand_row_wifi a inner join
		(
			select distinct shop_id,wifi from kk_feature_offline_train_shop_expand_wifi
		)b
		on a.wifi=b.wifi
	)c
	group by row_id,shop_id
)d left outer join
(
	select shop_id,count(*) as shop_wifi_count
	from
	(
		select distinct shop_id,wifi from kk_feature_offline_train_shop_expand_wifi
	)a
	group by shop_id
)e
on d.shop_id=e.shop_id;

create table if not exists kk_offline_train_get_row_shop_wifi_count_rate_2 as
select d.row_id,d.shop_id as pre_shop_id,e.row_wifi_count,(d.row_shop_count/e.row_wifi_count) as row_wifi_count_rate
from
(
	select row_id,shop_id,count(*) as row_shop_count
	from
	(
		select a.*,b.shop_id
		from kk_offline_train_expand_row_wifi a inner join
		(
			select distinct shop_id,wifi from kk_feature_offline_train_shop_expand_wifi
		)b
		on a.wifi=b.wifi
	)c
	group by row_id,shop_id
)d left outer join
(
	select row_id,count(*) as row_wifi_count
	from kk_offline_train_expand_row_wifi
	group by row_id
)e
on d.row_id=e.row_id;

-- 线下测试
create table if not exists kk_offline_test_get_row_shop_wifi_count_rate_1 as
select d.row_id,d.shop_id as pre_shop_id,(d.row_shop_count/e.shop_wifi_count) as shop_wifi_count_rate
from
(
	select row_id,shop_id,count(*) as row_shop_count
	from
	(
		select a.*,b.shop_id
		from kk_offline_test_expand_row_wifi a inner join
		(
			select distinct shop_id,wifi from kk_feature_offline_test_shop_expand_wifi
		)b
		on a.wifi=b.wifi
	)c
	group by row_id,shop_id
)d left outer join
(
	select shop_id,count(*) as shop_wifi_count
	from
	(
		select distinct shop_id,wifi from kk_feature_offline_test_shop_expand_wifi
	)a
	group by shop_id
)e
on d.shop_id=e.shop_id;

create table if not exists kk_offline_test_get_row_shop_wifi_count_rate_2 as
select d.row_id,d.shop_id as pre_shop_id,e.row_wifi_count,(d.row_shop_count/e.row_wifi_count) as row_wifi_count_rate
from
(
	select row_id,shop_id,count(*) as row_shop_count
	from
	(
		select a.*,b.shop_id
		from kk_offline_test_expand_row_wifi a inner join
		(
			select distinct shop_id,wifi from kk_feature_offline_test_shop_expand_wifi
		)b
		on a.wifi=b.wifi
	)c
	group by row_id,shop_id
)d left outer join
(
	select row_id,count(*) as row_wifi_count
	from kk_offline_test_expand_row_wifi
	group by row_id
)e
on d.row_id=e.row_id;

--线上测试
create table if not exists kk_online_test_get_row_shop_wifi_count_rate_1 as
select d.row_id,d.shop_id as pre_shop_id,(d.row_shop_count/e.shop_wifi_count) as shop_wifi_count_rate
from
(
	select row_id,shop_id,count(*) as row_shop_count
	from
	(
		select a.*,b.shop_id
		from kk_online_test_expand_row_wifi a inner join
		(
			select distinct shop_id,wifi from kk_feature_online_test_shop_expand_wifi
		)b
		on a.wifi=b.wifi
	)c
	group by row_id,shop_id
)d left outer join
(
	select shop_id,count(*) as shop_wifi_count
	from
	(
		select distinct shop_id,wifi from kk_feature_online_test_shop_expand_wifi
	)a
	group by shop_id
)e
on d.shop_id=e.shop_id;

create table if not exists kk_online_test_get_row_shop_wifi_count_rate_2 as
select d.row_id,d.shop_id as pre_shop_id,e.row_wifi_count,(d.row_shop_count/e.row_wifi_count) as row_wifi_count_rate
from
(
	select row_id,shop_id,count(*) as row_shop_count
	from
	(
		select a.*,b.shop_id
		from kk_online_test_expand_row_wifi a inner join
		(
			select distinct shop_id,wifi from kk_feature_online_test_shop_expand_wifi
		)b
		on a.wifi=b.wifi
	)c
	group by row_id,shop_id
)d left outer join
(
	select row_id,count(*) as row_wifi_count
	from kk_online_test_expand_row_wifi
	group by row_id
)e
on d.row_id=e.row_id;

drop table if  exists kk_offline_train_feature16;
drop table if  exists kk_offline_test_feature16;
drop table if  exists kk_online_test_feature16;
create table if not exists kk_offline_train_feature16 as
select a.*,b.shop_wifi_count_rate
from kk_offline_train_feature15 a left outer join kk_offline_train_get_row_shop_wifi_count_rate_1 b
on a.row_id=b.row_id and a.pre_shop_id=b.pre_shop_id;
create table if not exists kk_offline_test_feature16 as
select a.*,b.shop_wifi_count_rate
from kk_offline_test_feature15 a left outer join kk_offline_test_get_row_shop_wifi_count_rate_1 b
on a.row_id=b.row_id and a.pre_shop_id=b.pre_shop_id;
create table if not exists kk_online_test_feature16 as
select a.*,b.shop_wifi_count_rate
from kk_online_test_feature15 a left outer join kk_online_test_get_row_shop_wifi_count_rate_1 b
on a.row_id=b.row_id and a.pre_shop_id=b.pre_shop_id;

drop table if  exists kk_offline_train_feature17;
drop table if  exists kk_offline_test_feature17;
drop table if  exists kk_online_test_feature17;
create table if not exists kk_offline_train_feature17 as
select a.*,b.row_wifi_count,b.row_wifi_count_rate
from kk_offline_train_feature16 a left outer join kk_offline_train_get_row_shop_wifi_count_rate_2 b
on a.row_id=b.row_id and a.pre_shop_id=b.pre_shop_id;
create table if not exists kk_offline_test_feature17 as
select a.*,b.row_wifi_count,b.row_wifi_count_rate
from kk_offline_test_feature16 a left outer join kk_offline_test_get_row_shop_wifi_count_rate_2 b
on a.row_id=b.row_id and a.pre_shop_id=b.pre_shop_id;
create table if not exists kk_online_test_feature17 as
select a.*,b.row_wifi_count,b.row_wifi_count_rate
from kk_online_test_feature16 a left outer join kk_online_test_get_row_shop_wifi_count_rate_2 b
on a.row_id=b.row_id and a.pre_shop_id=b.pre_shop_id;
------------------------------------------------------------------------------------------------------
-- get_shop_sign_gap1(join on [row_id,pre_shop_id])  
--kk_shop_sign_gap1

-- kk_feature_offline_train_shop_expand_wifi
-- kk_feature_offline_test_shop_expand_wifi
-- kk_feature_online_test_shop_expand_wifi
-- kk_offline_train_expand_row_wifi
-- kk_offline_test_expand_row_wifi
-- kk_online_test_expand_row_wifi
-- 线下训练   kk_offline_train   18-24             kk_feature_offline_train 1-18
-- 线下测试   kk_offline_test    25-31             kk_feature_offline_test  1-25
-- 线上测试   kk_online_test     18-24             kk_feature_online_test   1-31
-- 线下训练
drop table if  exists kk_offline_train_get_shop_sign_gap_1;
drop table if  exists kk_offline_test_get_shop_sign_gap_1;
drop table if  exists kk_online_test_get_shop_sign_gap_1;

create table if not exists kk_offline_train_get_shop_sign_gap_1 as
select row_id,
shop_id as pre_shop_id,
avg(shop_wifi_sign_mean) as shop_wifi_sign_mean,
max(shop_wifi_sign_mean) as shop_wifi_sign_max,
stddev(shop_wifi_sign_mean) as shop_wifi_sign_std,

avg(wifi_sign) as row_wifi_sign_mean,
max(wifi_sign) as row_wifi_sign_max,
stddev(wifi_sign) as row_wifi_sign_std,

avg(sign_gap_mean_abs) as sign_mean_gap,
sum(sign_gap_mean_abs) as sign_sum_gap,
median(sign_gap_mean_abs) as sign_median_gap,
max(sign_gap_mean_abs) as sign_max_gap,
min(sign_gap_mean_abs) as sign_min_gap,

avg(sign_gap_max_abs) as sign_mean_gap2,
sum(sign_gap_max_abs) as sign_sum_gap2,
median(sign_gap_max_abs) as sign_median_gap2,
max(sign_gap_max_abs) as sign_max_gap2,
min(sign_gap_max_abs) as sign_min_gap2,

avg(sign_gap_mean_sq) as sign_mean_gap3,
sum(sign_gap_mean_sq) as sign_sum_gap3,
median(sign_gap_mean_sq) as sign_median_gap3,
max(sign_gap_mean_sq) as sign_max_gap3,
min(sign_gap_mean_sq) as sign_min_gap3,

avg(sign_gap_median_sq) as sign_mean_gap4,
sum(sign_gap_median_sq) as sign_sum_gap4,
median(sign_gap_median_sq) as sign_median_gap4,
max(sign_gap_median_sq) as sign_max_gap4,
min(sign_gap_median_sq) as sign_min_gap4,

avg(sign_gap_mean_rt) as sign_rt_mean,
max(sign_gap_mean_rt) as sign_rt_max,
min(sign_gap_mean_rt) as sign_rt_min
from 
(
	select b.*,a.shop_id,a.shop_wifi_sign_mean,a.shop_wifi_sign_max,a.shop_wifi_sign_median,
	abs(b.wifi_sign-a.shop_wifi_sign_mean) as sign_gap_mean_abs,(b.wifi_sign-a.shop_wifi_sign_mean)*(b.wifi_sign-a.shop_wifi_sign_mean) as sign_gap_mean_sq,
	if(a.shop_wifi_sign_mean=0.0,0,abs(b.wifi_sign/a.shop_wifi_sign_mean)) as sign_gap_mean_rt,
	abs(b.wifi_sign-a.shop_wifi_sign_max) as sign_gap_max_abs,
	(b.wifi_sign-a.shop_wifi_sign_median)*(b.wifi_sign-a.shop_wifi_sign_median) as sign_gap_median_sq
	from kk_offline_train_expand_row_wifi b inner join
	(
		select shop_id,wifi,avg(wifi_sign) as shop_wifi_sign_mean,max(wifi_sign) as shop_wifi_sign_max,median(wifi_sign) as shop_wifi_sign_median
		from kk_feature_offline_train_shop_expand_wifi
		group by shop_id,wifi
	)a
	on b.wifi=a.wifi
)c
group by row_id,shop_id;

-- 线下测试
create table if not exists kk_offline_test_get_shop_sign_gap_1 as
select row_id,
shop_id as pre_shop_id,
avg(shop_wifi_sign_mean) as shop_wifi_sign_mean,
max(shop_wifi_sign_mean) as shop_wifi_sign_max,
stddev(shop_wifi_sign_mean) as shop_wifi_sign_std,

avg(wifi_sign) as row_wifi_sign_mean,
max(wifi_sign) as row_wifi_sign_max,
stddev(wifi_sign) as row_wifi_sign_std,

avg(sign_gap_mean_abs) as sign_mean_gap,
sum(sign_gap_mean_abs) as sign_sum_gap,
median(sign_gap_mean_abs) as sign_median_gap,
max(sign_gap_mean_abs) as sign_max_gap,
min(sign_gap_mean_abs) as sign_min_gap,

avg(sign_gap_max_abs) as sign_mean_gap2,
sum(sign_gap_max_abs) as sign_sum_gap2,
median(sign_gap_max_abs) as sign_median_gap2,
max(sign_gap_max_abs) as sign_max_gap2,
min(sign_gap_max_abs) as sign_min_gap2,

avg(sign_gap_mean_sq) as sign_mean_gap3,
sum(sign_gap_mean_sq) as sign_sum_gap3,
median(sign_gap_mean_sq) as sign_median_gap3,
max(sign_gap_mean_sq) as sign_max_gap3,
min(sign_gap_mean_sq) as sign_min_gap3,

avg(sign_gap_median_sq) as sign_mean_gap4,
sum(sign_gap_median_sq) as sign_sum_gap4,
median(sign_gap_median_sq) as sign_median_gap4,
max(sign_gap_median_sq) as sign_max_gap4,
min(sign_gap_median_sq) as sign_min_gap4,

avg(sign_gap_mean_rt) as sign_rt_mean,
max(sign_gap_mean_rt) as sign_rt_max,
min(sign_gap_mean_rt) as sign_rt_min
from 
(
	select b.*,a.shop_id,a.shop_wifi_sign_mean,a.shop_wifi_sign_max,a.shop_wifi_sign_median,
	abs(b.wifi_sign-a.shop_wifi_sign_mean) as sign_gap_mean_abs,(b.wifi_sign-a.shop_wifi_sign_mean)*(b.wifi_sign-a.shop_wifi_sign_mean) as sign_gap_mean_sq,
	if(a.shop_wifi_sign_mean=0.0,0,abs(b.wifi_sign/a.shop_wifi_sign_mean)) as sign_gap_mean_rt,
	abs(b.wifi_sign-a.shop_wifi_sign_max) as sign_gap_max_abs,
	(b.wifi_sign-a.shop_wifi_sign_median)*(b.wifi_sign-a.shop_wifi_sign_median) as sign_gap_median_sq
	from kk_offline_test_expand_row_wifi b inner join
	(
		select shop_id,wifi,avg(wifi_sign) as shop_wifi_sign_mean,max(wifi_sign) as shop_wifi_sign_max,median(wifi_sign) as shop_wifi_sign_median
		from kk_feature_offline_test_shop_expand_wifi
		group by shop_id,wifi
	)a
	on b.wifi=a.wifi
)c
group by row_id,shop_id;

-- 线上测试

create table if not exists kk_online_test_get_shop_sign_gap_1 as
select row_id,
shop_id as pre_shop_id,
avg(shop_wifi_sign_mean) as shop_wifi_sign_mean,
max(shop_wifi_sign_mean) as shop_wifi_sign_max,
stddev(shop_wifi_sign_mean) as shop_wifi_sign_std,

avg(wifi_sign) as row_wifi_sign_mean,
max(wifi_sign) as row_wifi_sign_max,
stddev(wifi_sign) as row_wifi_sign_std,

avg(sign_gap_mean_abs) as sign_mean_gap,
sum(sign_gap_mean_abs) as sign_sum_gap,
median(sign_gap_mean_abs) as sign_median_gap,
max(sign_gap_mean_abs) as sign_max_gap,
min(sign_gap_mean_abs) as sign_min_gap,

avg(sign_gap_max_abs) as sign_mean_gap2,
sum(sign_gap_max_abs) as sign_sum_gap2,
median(sign_gap_max_abs) as sign_median_gap2,
max(sign_gap_max_abs) as sign_max_gap2,
min(sign_gap_max_abs) as sign_min_gap2,

avg(sign_gap_mean_sq) as sign_mean_gap3,
sum(sign_gap_mean_sq) as sign_sum_gap3,
median(sign_gap_mean_sq) as sign_median_gap3,
max(sign_gap_mean_sq) as sign_max_gap3,
min(sign_gap_mean_sq) as sign_min_gap3,

avg(sign_gap_median_sq) as sign_mean_gap4,
sum(sign_gap_median_sq) as sign_sum_gap4,
median(sign_gap_median_sq) as sign_median_gap4,
max(sign_gap_median_sq) as sign_max_gap4,
min(sign_gap_median_sq) as sign_min_gap4,

avg(sign_gap_mean_rt) as sign_rt_mean,
max(sign_gap_mean_rt) as sign_rt_max,
min(sign_gap_mean_rt) as sign_rt_min
from 
(
	select b.*,a.shop_id,a.shop_wifi_sign_mean,a.shop_wifi_sign_max,a.shop_wifi_sign_median,
	abs(b.wifi_sign-a.shop_wifi_sign_mean) as sign_gap_mean_abs,(b.wifi_sign-a.shop_wifi_sign_mean)*(b.wifi_sign-a.shop_wifi_sign_mean) as sign_gap_mean_sq,
	if(a.shop_wifi_sign_mean=0.0,0,abs(b.wifi_sign/a.shop_wifi_sign_mean)) as sign_gap_mean_rt,
	abs(b.wifi_sign-a.shop_wifi_sign_max) as sign_gap_max_abs,
	(b.wifi_sign-a.shop_wifi_sign_median)*(b.wifi_sign-a.shop_wifi_sign_median) as sign_gap_median_sq
	from kk_online_test_expand_row_wifi b inner join
	(
		select shop_id,wifi,avg(wifi_sign) as shop_wifi_sign_mean,max(wifi_sign) as shop_wifi_sign_max,median(wifi_sign) as shop_wifi_sign_median
		from kk_feature_online_test_shop_expand_wifi
		group by shop_id,wifi
	)a
	on b.wifi=a.wifi
)c
group by row_id,shop_id;

drop table if  exists kk_offline_train_feature18;
drop table if  exists kk_offline_test_feature18;
drop table if  exists kk_online_test_feature18;
create table if not exists kk_offline_train_feature18 as
select a.*,b.shop_wifi_sign_mean,b.shop_wifi_sign_max,b.shop_wifi_sign_std,
b.row_wifi_sign_mean,b.row_wifi_sign_max,b.row_wifi_sign_std,
b.sign_mean_gap,b.sign_sum_gap,b.sign_median_gap,b.sign_max_gap,b.sign_min_gap,
b.sign_mean_gap2,b.sign_sum_gap2,b.sign_median_gap2,b.sign_max_gap2,b.sign_min_gap2,
b.sign_mean_gap3,b.sign_sum_gap3,b.sign_median_gap3,b.sign_max_gap3,b.sign_min_gap3,
b.sign_mean_gap4,b.sign_sum_gap4,b.sign_median_gap4,b.sign_max_gap4,b.sign_min_gap4,
b.sign_rt_mean,b.sign_rt_max,b.sign_rt_min
from kk_offline_train_feature17 a left outer join kk_offline_train_get_shop_sign_gap_1 b
on a.row_id=b.row_id and a.pre_shop_id=b.pre_shop_id;
create table if not exists kk_offline_test_feature18 as
select a.*,b.shop_wifi_sign_mean,b.shop_wifi_sign_max,b.shop_wifi_sign_std,
b.row_wifi_sign_mean,b.row_wifi_sign_max,b.row_wifi_sign_std,
b.sign_mean_gap,b.sign_sum_gap,b.sign_median_gap,b.sign_max_gap,b.sign_min_gap,
b.sign_mean_gap2,b.sign_sum_gap2,b.sign_median_gap2,b.sign_max_gap2,b.sign_min_gap2,
b.sign_mean_gap3,b.sign_sum_gap3,b.sign_median_gap3,b.sign_max_gap3,b.sign_min_gap3,
b.sign_mean_gap4,b.sign_sum_gap4,b.sign_median_gap4,b.sign_max_gap4,b.sign_min_gap4,
b.sign_rt_mean,b.sign_rt_max,b.sign_rt_min
from kk_offline_test_feature17 a left outer join kk_offline_test_get_shop_sign_gap_1 b
on a.row_id=b.row_id and a.pre_shop_id=b.pre_shop_id;
create table if not exists kk_online_test_feature18 as
select a.*,b.shop_wifi_sign_mean,b.shop_wifi_sign_max,b.shop_wifi_sign_std,
b.row_wifi_sign_mean,b.row_wifi_sign_max,b.row_wifi_sign_std,
b.sign_mean_gap,b.sign_sum_gap,b.sign_median_gap,b.sign_max_gap,b.sign_min_gap,
b.sign_mean_gap2,b.sign_sum_gap2,b.sign_median_gap2,b.sign_max_gap2,b.sign_min_gap2,
b.sign_mean_gap3,b.sign_sum_gap3,b.sign_median_gap3,b.sign_max_gap3,b.sign_min_gap3,
b.sign_mean_gap4,b.sign_sum_gap4,b.sign_median_gap4,b.sign_max_gap4,b.sign_min_gap4,
b.sign_rt_mean,b.sign_rt_max,b.sign_rt_min
from kk_online_test_feature17 a left outer join kk_online_test_get_shop_sign_gap_1 b
on a.row_id=b.row_id and a.pre_shop_id=b.pre_shop_id;

------------------------------------------------------------------------------------------------------
-- get_shop_sign_gap2(join on [row_id,pre_shop_id])  
--kk_shop_sign_gap2

-- kk_feature_offline_train_shop_expand_wifi
-- kk_feature_offline_test_shop_expand_wifi
-- kk_feature_online_test_shop_expand_wifi
-- kk_offline_train_expand_row_wifi
-- kk_offline_test_expand_row_wifi
-- kk_online_test_expand_row_wifi
-- 线下训练   kk_offline_train   18-24             kk_feature_offline_train 1-18
-- 线下测试   kk_offline_test    25-31             kk_feature_offline_test  1-25
-- 线上测试   kk_online_test     18-24             kk_feature_online_test   1-31
-- 线下训练
drop table if  exists kk_offline_train_get_shop_sign_gap_2;
drop table if  exists kk_offline_test_get_shop_sign_gap_2;
drop table if  exists kk_online_test_get_shop_sign_gap_2;

create table if not exists kk_offline_train_get_shop_sign_gap_2 as
select b.row_id,a.shop_id as pre_shop_id,
abs(b.wifi_sign-a.shop_wifi_sign_mean) as strongest_sign_gap1,
(b.wifi_sign-a.shop_wifi_sign_mean)*(b.wifi_sign-a.shop_wifi_sign_mean) as strongest_sign_gap2,
if(a.shop_wifi_sign_mean=0.0,0,abs(b.wifi_sign/a.shop_wifi_sign_mean)) as strongest_sign_gap3,
abs(b.wifi_sign-a.shop_wifi_sign_max) as strongest_sign_gap4,
(b.wifi_sign-a.shop_wifi_sign_median)*(b.wifi_sign-a.shop_wifi_sign_median) as strongest_sign_gap5
from 
(
	select row_id,wifi,wifi_sign
	from
	(
		select row_id,wifi,wifi_sign,row_number() over (partition by row_id order by wifi_sign desc) as wifi_sign_rank
		from kk_offline_train_expand_row_wifi
	)t
	where t.wifi_sign_rank=1
) b inner join
(
	select shop_id,wifi,avg(wifi_sign) as shop_wifi_sign_mean,max(wifi_sign) as shop_wifi_sign_max,median(wifi_sign) as shop_wifi_sign_median
	from kk_feature_offline_train_shop_expand_wifi
	group by shop_id,wifi
)a
on b.wifi=a.wifi;

-- 线下测试 
create table if not exists kk_offline_test_get_shop_sign_gap_2 as
select b.row_id,a.shop_id as pre_shop_id,
abs(b.wifi_sign-a.shop_wifi_sign_mean) as strongest_sign_gap1,
(b.wifi_sign-a.shop_wifi_sign_mean)*(b.wifi_sign-a.shop_wifi_sign_mean) as strongest_sign_gap2,
if(a.shop_wifi_sign_mean=0.0,0,abs(b.wifi_sign/a.shop_wifi_sign_mean)) as strongest_sign_gap3,
abs(b.wifi_sign-a.shop_wifi_sign_max) as strongest_sign_gap4,
(b.wifi_sign-a.shop_wifi_sign_median)*(b.wifi_sign-a.shop_wifi_sign_median) as strongest_sign_gap5
from 
(
	select row_id,wifi,wifi_sign
	from
	(
		select row_id,wifi,wifi_sign,row_number() over (partition by row_id order by wifi_sign desc) as wifi_sign_rank
		from kk_offline_test_expand_row_wifi
	)t
	where t.wifi_sign_rank=1
) b inner join
(
	select shop_id,wifi,avg(wifi_sign) as shop_wifi_sign_mean,max(wifi_sign) as shop_wifi_sign_max,median(wifi_sign) as shop_wifi_sign_median
	from kk_feature_offline_test_shop_expand_wifi
	group by shop_id,wifi
)a
on b.wifi=a.wifi;

-- 线上测试
create table if not exists kk_online_test_get_shop_sign_gap_2 as
select b.row_id,a.shop_id as pre_shop_id,
abs(b.wifi_sign-a.shop_wifi_sign_mean) as strongest_sign_gap1,
(b.wifi_sign-a.shop_wifi_sign_mean)*(b.wifi_sign-a.shop_wifi_sign_mean) as strongest_sign_gap2,
if(a.shop_wifi_sign_mean=0.0,0,abs(b.wifi_sign/a.shop_wifi_sign_mean)) as strongest_sign_gap3,
abs(b.wifi_sign-a.shop_wifi_sign_max) as strongest_sign_gap4,
(b.wifi_sign-a.shop_wifi_sign_median)*(b.wifi_sign-a.shop_wifi_sign_median) as strongest_sign_gap5
from 
(
	select row_id,wifi,wifi_sign
	from
	(
		select row_id,wifi,wifi_sign,row_number() over (partition by row_id order by wifi_sign desc) as wifi_sign_rank
		from kk_online_test_expand_row_wifi
	)t
	where t.wifi_sign_rank=1
) b inner join
(
	select shop_id,wifi,avg(wifi_sign) as shop_wifi_sign_mean,max(wifi_sign) as shop_wifi_sign_max,median(wifi_sign) as shop_wifi_sign_median
	from kk_feature_online_test_shop_expand_wifi
	group by shop_id,wifi
)a
on b.wifi=a.wifi;

drop table if  exists kk_offline_train_feature19;
drop table if  exists kk_offline_test_feature19;
drop table if  exists kk_online_test_feature19;
create table if not exists kk_offline_train_feature19 as
select a.*,b.strongest_sign_gap1,b.strongest_sign_gap2,b.strongest_sign_gap3,
b.strongest_sign_gap4,b.strongest_sign_gap5
from kk_offline_train_feature18 a left outer join kk_offline_train_get_shop_sign_gap_2 b
on a.row_id=b.row_id and a.pre_shop_id=b.pre_shop_id;
create table if not exists kk_offline_test_feature19 as
select a.*,b.strongest_sign_gap1,b.strongest_sign_gap2,b.strongest_sign_gap3,
b.strongest_sign_gap4,b.strongest_sign_gap5
from kk_offline_test_feature18 a left outer join kk_offline_test_get_shop_sign_gap_2 b
on a.row_id=b.row_id and a.pre_shop_id=b.pre_shop_id;
create table if not exists kk_online_test_feature19 as
select a.*,b.strongest_sign_gap1,b.strongest_sign_gap2,b.strongest_sign_gap3,
b.strongest_sign_gap4,b.strongest_sign_gap5
from kk_online_test_feature18 a left outer join kk_online_test_get_shop_sign_gap_2 b
on a.row_id=b.row_id and a.pre_shop_id=b.pre_shop_id;

------------------------------------------------------------------------------------------------------
-- get_row_strongest_wifi_id(join on [row_id])  
--kk_get_row_strongest_wifi_id

-- kk_feature_offline_train_shop_expand_wifi
-- kk_feature_offline_test_shop_expand_wifi
-- kk_feature_online_test_shop_expand_wifi
-- kk_offline_train_expand_row_wifi
-- kk_offline_test_expand_row_wifi
-- kk_online_test_expand_row_wifi
-- 线下训练   kk_offline_train   18-24             kk_feature_offline_train 1-18
-- 线下测试   kk_offline_test    25-31             kk_feature_offline_test  1-25
-- 线上测试   kk_online_test     18-24             kk_feature_online_test   1-31
-- 线下训练
create table if not exists kk_offline_train_get_r_st_w as
select c.*,d.row_3strongest_wifi
from
(
	select a.*,b.row_2strongest_wifi
	from
	(
		select row_id,cast(substr(wifi, 3) as bigint) as row_strongest_wifi
		from
		(
			select row_id,wifi,wifi_sign,row_number() over (partition by row_id order by wifi_sign desc) as wifi_sign_rank
			from kk_offline_train_expand_row_wifi
		)t
		where t.wifi_sign_rank=1
	)a left outer join
	(
		select row_id,cast(substr(wifi, 3) as bigint) as row_2strongest_wifi
		from
		(
			select row_id,wifi,wifi_sign,row_number() over (partition by row_id order by wifi_sign desc) as wifi_sign_rank
			from kk_offline_train_expand_row_wifi
		)t
		where t.wifi_sign_rank=2
	)b
	on a.row_id=b.row_id
)c left outer join
(
	select row_id,cast(substr(wifi, 3) as bigint) as row_3strongest_wifi
	from
	(
		select row_id,wifi,wifi_sign,row_number() over (partition by row_id order by wifi_sign desc) as wifi_sign_rank
		from kk_offline_train_expand_row_wifi
	)t
	where t.wifi_sign_rank=3
)d
on c.row_id=d.row_id;

-- 线下测试
create table if not exists kk_offline_test_get_r_st_w as
select c.*,d.row_3strongest_wifi
from
(
	select a.*,b.row_2strongest_wifi
	from
	(
		select row_id,cast(substr(wifi, 3) as bigint) as row_strongest_wifi
		from
		(
			select row_id,wifi,wifi_sign,row_number() over (partition by row_id order by wifi_sign desc) as wifi_sign_rank
			from kk_offline_test_expand_row_wifi
		)t
		where t.wifi_sign_rank=1
	)a left outer join
	(
		select row_id,cast(substr(wifi, 3) as bigint) as row_2strongest_wifi
		from
		(
			select row_id,wifi,wifi_sign,row_number() over (partition by row_id order by wifi_sign desc) as wifi_sign_rank
			from kk_offline_test_expand_row_wifi
		)t
		where t.wifi_sign_rank=2
	)b
	on a.row_id=b.row_id
)c left outer join
(
	select row_id,cast(substr(wifi, 3) as bigint) as row_3strongest_wifi
	from
	(
		select row_id,wifi,wifi_sign,row_number() over (partition by row_id order by wifi_sign desc) as wifi_sign_rank
		from kk_offline_test_expand_row_wifi
	)t
	where t.wifi_sign_rank=3
)d
on c.row_id=d.row_id;

-- 线上测试
create table if not exists kk_online_test_get_r_st_w as
select c.*,d.row_3strongest_wifi
from
(
	select a.*,b.row_2strongest_wifi
	from
	(
		select row_id,cast(substr(wifi, 3) as bigint) as row_strongest_wifi
		from
		(
			select row_id,wifi,wifi_sign,row_number() over (partition by row_id order by wifi_sign desc) as wifi_sign_rank
			from kk_online_test_expand_row_wifi
		)t
		where t.wifi_sign_rank=1
	)a left outer join
	(
		select row_id,cast(substr(wifi, 3) as bigint) as row_2strongest_wifi
		from
		(
			select row_id,wifi,wifi_sign,row_number() over (partition by row_id order by wifi_sign desc) as wifi_sign_rank
			from kk_online_test_expand_row_wifi
		)t
		where t.wifi_sign_rank=2
	)b
	on a.row_id=b.row_id
)c left outer join
(
	select row_id,cast(substr(wifi, 3) as bigint) as row_3strongest_wifi
	from
	(
		select row_id,wifi,wifi_sign,row_number() over (partition by row_id order by wifi_sign desc) as wifi_sign_rank
		from kk_online_test_expand_row_wifi
	)t
	where t.wifi_sign_rank=3
)d
on c.row_id=d.row_id;

drop table if  exists kk_offline_train_feature20;
drop table if  exists kk_offline_test_feature20;
drop table if  exists kk_online_test_feature20;
create table if not exists kk_offline_train_feature20 as
select a.*,b.row_strongest_wifi,b.row_2strongest_wifi,b.row_3strongest_wifi
from kk_offline_train_feature19 a left outer join kk_offline_train_get_r_st_w b
on a.row_id=b.row_id;
create table if not exists kk_offline_test_feature20 as
select a.*,b.row_strongest_wifi,b.row_2strongest_wifi,b.row_3strongest_wifi
from kk_offline_test_feature19 a left outer join kk_offline_test_get_r_st_w b
on a.row_id=b.row_id;
create table if not exists kk_online_test_feature20 as
select a.*,b.row_strongest_wifi,b.row_2strongest_wifi,b.row_3strongest_wifi
from kk_online_test_feature19 a left outer join kk_online_test_get_r_st_w b
on a.row_id=b.row_id;

------------------------------------------------------------------------------------------------------
-- get_wifi_true_rate(join on [row_id,pre_shop_id])  
--kk_get_wifi_true_rate

-- kk_feature_offline_train_shop_expand_wifi_flag
-- kk_feature_offline_test_shop_expand_wifi_flag
-- kk_feature_online_test_shop_expand_wifi_flag
-- kk_offline_train_expand_row_wifi_flag
-- kk_offline_test_expand_row_wifi_flag
-- kk_online_test_expand_row_wifi_flag
-- 线下训练   kk_offline_train   18-24             kk_feature_offline_train 1-18
-- 线下测试   kk_offline_test    25-31             kk_feature_offline_test  1-25
-- 线上测试   kk_online_test     18-24             kk_feature_online_test   1-31
-- 线下训练
drop table if  exists kk_offline_train_get_wifi_t_rt;
drop table if  exists kk_offline_test_get_wifi_t_rt;
drop table if  exists kk_online_test_get_wifi_t_rt;

create table if not exists kk_offline_train_get_wifi_t_rt as
select e.row_id,d.shop_id as pre_shop_id,d.flag_rate
from
(
	select distinct row_id,wifi from kk_offline_train_expand_row_wifi_flag
	where connect='true'
)e inner join
(
	select b.wifi,b.shop_id,(b.wifi_shop_count/c.wifi_count) as flag_rate
	from
	(
		select wifi,shop_id,count(*) as wifi_shop_count
		from
		(
			select * from kk_feature_offline_train_shop_expand_wifi_flag
			where connect='true'
		)a
		group by wifi,shop_id
	)b left outer join
	(
		select wifi,count(*) as wifi_count
		from
		(
			select * from kk_feature_offline_train_shop_expand_wifi_flag
			where connect='true'
		)a
		group by wifi
	)c
	on b.wifi=c.wifi
)d
on e.wifi=d.wifi;

-- 线下测试
create table if not exists kk_offline_test_get_wifi_t_rt as
select e.row_id,d.shop_id as pre_shop_id,d.flag_rate
from
(
	select distinct row_id,wifi from kk_offline_test_expand_row_wifi_flag
	where connect='true'
)e inner join
(
	select b.wifi,b.shop_id,(b.wifi_shop_count/c.wifi_count) as flag_rate
	from
	(
		select wifi,shop_id,count(*) as wifi_shop_count
		from
		(
			select * from kk_feature_offline_test_shop_expand_wifi_flag
			where connect='true'
		)a
		group by wifi,shop_id
	)b left outer join
	(
		select wifi,count(*) as wifi_count
		from
		(
			select * from kk_feature_offline_test_shop_expand_wifi_flag
			where connect='true'
		)a
		group by wifi
	)c
	on b.wifi=c.wifi
)d
on e.wifi=d.wifi;

-- 线上测试
create table if not exists kk_online_test_get_wifi_t_rt as
select e.row_id,d.shop_id as pre_shop_id,d.flag_rate
from
(
	select distinct row_id,wifi from kk_online_test_expand_row_wifi_flag
	where connect='true'
)e inner join
(
	select b.wifi,b.shop_id,(b.wifi_shop_count/c.wifi_count) as flag_rate
	from
	(
		select wifi,shop_id,count(*) as wifi_shop_count
		from
		(
			select * from kk_feature_online_test_shop_expand_wifi_flag
			where connect='true'
		)a
		group by wifi,shop_id
	)b left outer join
	(
		select wifi,count(*) as wifi_count
		from
		(
			select * from kk_feature_online_test_shop_expand_wifi_flag
			where connect='true'
		)a
		group by wifi
	)c
	on b.wifi=c.wifi
)d
on e.wifi=d.wifi;

drop table if  exists kk_offline_train_feature21;
drop table if  exists kk_offline_test_feature21;
drop table if  exists kk_online_test_feature21;
create table if not exists kk_offline_train_feature21 as
select a.*,b.flag_rate
from kk_offline_train_feature20 a left outer join kk_offline_train_get_wifi_t_rt b
on a.row_id=b.row_id and a.pre_shop_id=b.pre_shop_id;
create table if not exists kk_offline_test_feature21 as
select a.*,b.flag_rate
from kk_offline_test_feature20 a left outer join kk_offline_test_get_wifi_t_rt b
on a.row_id=b.row_id and a.pre_shop_id=b.pre_shop_id;
create table if not exists kk_online_test_feature21 as
select a.*,b.flag_rate
from kk_online_test_feature20 a left outer join kk_online_test_get_wifi_t_rt b
on a.row_id=b.row_id and a.pre_shop_id=b.pre_shop_id;

------------------------------------------------------------------------------------------------------
-- get_user_feature(join on [rpre_shop_id],[mall_id])  
--kk_get_user_feature

-- kk_feature_offline_train_shop_expand_wifi
-- kk_feature_offline_test_shop_expand_wifi
-- kk_feature_online_test_shop_expand_wifi
-- kk_offline_train_expand_row_wifi
-- kk_offline_test_expand_row_wifi
-- kk_online_test_expand_row_wifi
-- 线下训练   kk_offline_train   18-24             kk_feature_offline_train 1-18
-- 线下测试   kk_offline_test    25-31             kk_feature_offline_test  1-25
-- 线上测试   kk_online_test     18-24             kk_feature_online_test   1-31
-- 线下训练
drop table if  exists kk_offline_train_get_user_feature_1;
drop table if  exists kk_offline_train_get_user_feature_2;
drop table if  exists kk_offline_test_get_user_feature_1;
drop table if  exists kk_offline_test_get_user_feature_2;
drop table if  exists kk_online_test_get_user_feature_1;
drop table if  exists kk_online_test_get_user_feature_2;

create table if not exists kk_offline_train_get_user_feature_1 as
select shop_id as pre_shop_id,count(*) as shop_his_count
from kk_feature_offline_train
group by shop_id;

create table if not exists kk_offline_train_get_user_feature_2 as
select mall_id,count(*) as mall_his_count
from kk_feature_offline_train
group by mall_id;

-- 线下测试
create table if not exists kk_offline_test_get_user_feature_1 as
select shop_id as pre_shop_id,count(*) as shop_his_count
from kk_feature_offline_test
group by shop_id;

create table if not exists kk_offline_test_get_user_feature_2 as
select mall_id,count(*) as mall_his_count
from kk_feature_offline_test
group by mall_id;

-- 线上测试
create table if not exists kk_online_test_get_user_feature_1 as
select shop_id as pre_shop_id,count(*) as shop_his_count
from kk_feature_online_test
group by shop_id;

create table if not exists kk_online_test_get_user_feature_2 as
select mall_id,count(*) as mall_his_count
from kk_feature_online_test
group by mall_id;


drop table if  exists kk_offline_train_feature22;
drop table if  exists kk_offline_test_feature22;
drop table if  exists kk_online_test_feature22;
create table if not exists kk_offline_train_feature22 as
select c.*,d.mall_his_count
from
(
	select a.*,b.shop_his_count
	from kk_offline_train_feature21 a left outer join kk_offline_train_get_user_feature_1 b
	on a.pre_shop_id=b.pre_shop_id
)c left outer join kk_offline_train_get_user_feature_2 d
on c.mall_id=d.mall_id;

create table if not exists kk_offline_test_feature22 as
select c.*,d.mall_his_count
from
(
	select a.*,b.shop_his_count
	from kk_offline_test_feature21 a left outer join kk_offline_test_get_user_feature_1 b
	on a.pre_shop_id=b.pre_shop_id
)c left outer join kk_offline_test_get_user_feature_2 d
on c.mall_id=d.mall_id;

create table if not exists kk_online_test_feature22 as
select c.*,d.mall_his_count
from
(
	select a.*,b.shop_his_count
	from kk_online_test_feature21 a left outer join kk_online_test_get_user_feature_1 b
	on a.pre_shop_id=b.pre_shop_id
)c left outer join kk_online_test_get_user_feature_2 d
on c.mall_id=d.mall_id;


------------------------------------------------------------------------------------------------------

--row的10个wifi信号排序展开
drop table if  exists kk_offline_train_feature23;
drop table if  exists kk_offline_test_feature23;
drop table if  exists kk_online_test_feature23;
create table if not exists kk_offline_train_feature23 as
select a.*,b.row_wifi1_sign,b.row_wifi2_sign,b.row_wifi3_sign,b.row_wifi4_sign,b.row_wifi5_sign,
b.row_wifi6_sign,b.row_wifi7_sign,b.row_wifi8_sign,b.row_wifi9_sign,b.row_wifi10_sign
from kk_offline_train_feature22 a left outer join kk_offline_train_rank_row_wifi b
on a.row_id=b.row_id and a.pre_shop_id=b.pre_shop_id;
create table if not exists kk_offline_test_feature23 as
select a.*,b.row_wifi1_sign,b.row_wifi2_sign,b.row_wifi3_sign,b.row_wifi4_sign,b.row_wifi5_sign,
b.row_wifi6_sign,b.row_wifi7_sign,b.row_wifi8_sign,b.row_wifi9_sign,b.row_wifi10_sign
from kk_offline_test_feature22 a left outer join kk_offline_test_rank_row_wifi b
on a.row_id=b.row_id and a.pre_shop_id=b.pre_shop_id;
create table if not exists kk_online_test_feature23 as
select a.*,b.row_wifi1_sign,b.row_wifi2_sign,b.row_wifi3_sign,b.row_wifi4_sign,b.row_wifi5_sign,
b.row_wifi6_sign,b.row_wifi7_sign,b.row_wifi8_sign,b.row_wifi9_sign,b.row_wifi10_sign
from kk_online_test_feature22 a left outer join kk_online_test_rank_row_wifi b
on a.row_id=b.row_id and a.pre_shop_id=b.pre_shop_id;



------------------------------------------------------------------------------------------------------
--knn特征
-- 线下训练
drop table if exists kk_offline_train_feature24;
create table if not exists kk_offline_train_feature24 as
select a.*,b1.nearest1,b2.nearest2,b3.nearest3,b5.nearest5,b7.nearest7,b10.nearest10,b15.nearest15,b20.nearest20
from kk_offline_train_feature23 a left outer join lq_feature_offline_train_nearest1 b1 
on a.row_id = b1.row_id and a.pre_shop_id = b1.pre_shop_id
left outer join lq_feature_offline_train_nearest2 b2 
on a.row_id = b2.row_id and a.pre_shop_id = b2.pre_shop_id
left outer join lq_feature_offline_train_nearest3 b3 
on a.row_id = b3.row_id and a.pre_shop_id = b3.pre_shop_id
left outer join lq_feature_offline_train_nearest5 b5 
on a.row_id = b5.row_id and a.pre_shop_id = b5.pre_shop_id
left outer join lq_feature_offline_train_nearest7 b7 
on a.row_id = b7.row_id and a.pre_shop_id = b7.pre_shop_id
left outer join lq_feature_offline_train_nearest10 b10
on a.row_id = b10.row_id and a.pre_shop_id = b10.pre_shop_id
left outer join lq_feature_offline_train_nearest15 b15
on a.row_id = b15.row_id and a.pre_shop_id = b15.pre_shop_id
left outer join lq_feature_offline_train_nearest20 b20
on a.row_id = b20.row_id and a.pre_shop_id = b20.pre_shop_id;

-- 线下测试
drop table if exists kk_offline_test_feature24;
create table if not exists kk_offline_test_feature24 as
select a.*,b1.nearest1,b2.nearest2,b3.nearest3,b5.nearest5,b7.nearest7,b10.nearest10,b15.nearest15,b20.nearest20
from kk_offline_test_feature23 a left outer join lq_feature_offline_test_nearest1 b1 
on a.row_id = b1.row_id and a.pre_shop_id = b1.pre_shop_id
left outer join lq_feature_offline_test_nearest2 b2 
on a.row_id = b2.row_id and a.pre_shop_id = b2.pre_shop_id
left outer join lq_feature_offline_test_nearest3 b3 
on a.row_id = b3.row_id and a.pre_shop_id = b3.pre_shop_id
left outer join lq_feature_offline_test_nearest5 b5 
on a.row_id = b5.row_id and a.pre_shop_id = b5.pre_shop_id
left outer join lq_feature_offline_test_nearest7 b7 
on a.row_id = b7.row_id and a.pre_shop_id = b7.pre_shop_id
left outer join lq_feature_offline_test_nearest10 b10
on a.row_id = b10.row_id and a.pre_shop_id = b10.pre_shop_id
left outer join lq_feature_offline_test_nearest15 b15
on a.row_id = b15.row_id and a.pre_shop_id = b15.pre_shop_id
left outer join lq_feature_offline_test_nearest20 b20
on a.row_id = b20.row_id and a.pre_shop_id = b20.pre_shop_id;

-- 线上测试
drop table if exists kk_online_test_feature24;
create table if not exists kk_online_test_feature24 as
select a.*,b1.nearest1,b2.nearest2,b3.nearest3,b5.nearest5,b7.nearest7,b10.nearest10,b15.nearest15,b20.nearest20
from kk_online_test_feature23 a left outer join lq_feature_online_test_nearest1 b1 
on a.row_id = b1.row_id and a.pre_shop_id = b1.pre_shop_id
left outer join lq_feature_online_test_nearest2 b2 
on a.row_id = b2.row_id and a.pre_shop_id = b2.pre_shop_id
left outer join lq_feature_online_test_nearest3 b3 
on a.row_id = b3.row_id and a.pre_shop_id = b3.pre_shop_id
left outer join lq_feature_online_test_nearest5 b5 
on a.row_id = b5.row_id and a.pre_shop_id = b5.pre_shop_id
left outer join lq_feature_online_test_nearest7 b7 
on a.row_id = b7.row_id and a.pre_shop_id = b7.pre_shop_id
left outer join lq_feature_online_test_nearest10 b10
on a.row_id = b10.row_id and a.pre_shop_id = b10.pre_shop_id
left outer join lq_feature_online_test_nearest15 b15
on a.row_id = b15.row_id and a.pre_shop_id = b15.pre_shop_id
left outer join lq_feature_online_test_nearest20 b20
on a.row_id = b20.row_id and a.pre_shop_id = b20.pre_shop_id;

------------------------------------------------------------------------------------------------------
-- get_shop_hour_count_rate(join on [row_id,pre_shop_id])  
--kk_shop_hour_count_rate

-- kk_feature_offline_train_shop_expand_wifi
-- kk_feature_offline_test_shop_expand_wifi
-- kk_feature_online_test_shop_expand_wifi
-- kk_offline_train_expand_row_wifi
-- kk_offline_test_expand_row_wifi
-- kk_online_test_expand_row_wifi
-- 线下训练   kk_offline_train   18-24             kk_feature_offline_train 1-18
-- 线下测试   kk_offline_test    25-31             kk_feature_offline_test  1-25
-- 线上测试   kk_online_test     18-24             kk_feature_online_test   1-31
-- 线下训练
drop table if  exists kk_offline_train_get_shop_hour_count_rate;
drop table if  exists kk_offline_test_get_shop_hour_count_rate;
drop table if  exists kk_online_test_get_shop_hour_count_rate;

create table if not exists kk_offline_train_get_shop_hour_count_rate as
select row_id,pre_shop_id,shop_hour_count,shop_hour_count_rate,mall_hour_count,mall_hour_count_rate,
pre_category_id_hour_count,pre_category_id_hour_count_rate,(shop_hour_count/mall_hour_count_rate) as shop_hour_mall_rate
from
(
	select t.row_id,t.pre_shop_id,f.shop_hour_count,f.shop_hour_count_rate,g.mall_hour_count,g.mall_hour_count_rate,
	h.pre_category_id_hour_count,h.pre_category_id_hour_count_rate
	from kk_offline_train_get_basic_feature t left outer join
	(
		select d.shop_id as pre_shop_id,d.hour,d.shop_hour_count,(d.shop_hour_count/e.shop_count) as shop_hour_count_rate
		from
		(
			select shop_id,hour,count(*) as shop_hour_count
			from
			(
				select a.row_id,a.shop_id,a.mall_id,cast(substr(a.time_stamp,12,2) as bigint) as hour,b.category_id
				from kk_feature_offline_train a left outer join shop_info b on a.shop_id=b.shop_id
			)c
			group by shop_id,hour
		)d left outer join 
		(
			select shop_id,count(*) as shop_count 
			from kk_feature_offline_train
			group by shop_id
		)e
		on d.shop_id=e.shop_id
	)f
	on t.pre_shop_id=f.pre_shop_id and t.hour=f.hour
	left outer join
	(
		select d.mall_id,d.hour,d.mall_hour_count,(d.mall_hour_count/e.mall_count) as mall_hour_count_rate
		from
		(
			select mall_id,hour,count(*) as mall_hour_count
			from
			(
				select mall_id,cast(substr(time_stamp,12,2) as bigint) as hour
				from kk_feature_offline_train
			)a
			group by mall_id,hour
		)d left outer join 
		(
			select mall_id,count(*) as mall_count 
			from kk_feature_offline_train
			group by mall_id
		)e
		on d.mall_id=e.mall_id
	)g
	on t.mall_id=g.mall_id and t.hour=g.hour
	left outer join
	(
		select d.pre_category_id,d.hour,d.pre_category_id_hour_count,
		(d.pre_category_id_hour_count/e.pre_category_id_count) as pre_category_id_hour_count_rate
		from
		(
			select pre_category_id,hour,count(*) as pre_category_id_hour_count
			from
			(
				select cast(substr(a.time_stamp,12,2) as bigint) as hour,b.category_id as pre_category_id
				from kk_feature_offline_train a left outer join shop_info b on a.shop_id=b.shop_id
			)c
			group by pre_category_id,hour
		)d left outer join 
		(
			select pre_category_id,count(*) as pre_category_id_count
			from
			(
				select b.category_id as pre_category_id
				from kk_feature_offline_train a left outer join shop_info b on a.shop_id=b.shop_id
			)c
			group by pre_category_id
		)e
		on d.pre_category_id=e.pre_category_id
	)h
	on t.pre_category_id=h.pre_category_id and t.hour=h.hour
)i;

-- 线下测试
create table if not exists kk_offline_test_get_shop_hour_count_rate as
select row_id,pre_shop_id,shop_hour_count,shop_hour_count_rate,mall_hour_count,mall_hour_count_rate,
pre_category_id_hour_count,pre_category_id_hour_count_rate,(shop_hour_count/mall_hour_count_rate) as shop_hour_mall_rate
from
(
	select t.row_id,t.pre_shop_id,f.shop_hour_count,f.shop_hour_count_rate,g.mall_hour_count,g.mall_hour_count_rate,
	h.pre_category_id_hour_count,h.pre_category_id_hour_count_rate
	from kk_offline_test_get_basic_feature t left outer join
	(
		select d.shop_id as pre_shop_id,d.hour,d.shop_hour_count,(d.shop_hour_count/e.shop_count) as shop_hour_count_rate
		from
		(
			select shop_id,hour,count(*) as shop_hour_count
			from
			(
				select a.row_id,a.shop_id,a.mall_id,cast(substr(a.time_stamp,12,2) as bigint) as hour,b.category_id
				from kk_feature_offline_test a left outer join shop_info b on a.shop_id=b.shop_id
			)c
			group by shop_id,hour
		)d left outer join 
		(
			select shop_id,count(*) as shop_count 
			from kk_feature_offline_test
			group by shop_id
		)e
		on d.shop_id=e.shop_id
	)f
	on t.pre_shop_id=f.pre_shop_id and t.hour=f.hour
	left outer join
	(
		select d.mall_id,d.hour,d.mall_hour_count,(d.mall_hour_count/e.mall_count) as mall_hour_count_rate
		from
		(
			select mall_id,hour,count(*) as mall_hour_count
			from
			(
				select mall_id,cast(substr(time_stamp,12,2) as bigint) as hour
				from kk_feature_offline_test
			)a
			group by mall_id,hour
		)d left outer join 
		(
			select mall_id,count(*) as mall_count 
			from kk_feature_offline_test
			group by mall_id
		)e
		on d.mall_id=e.mall_id
	)g
	on t.mall_id=g.mall_id and t.hour=g.hour
	left outer join
	(
		select d.pre_category_id,d.hour,d.pre_category_id_hour_count,
		(d.pre_category_id_hour_count/e.pre_category_id_count) as pre_category_id_hour_count_rate
		from
		(
			select pre_category_id,hour,count(*) as pre_category_id_hour_count
			from
			(
				select cast(substr(a.time_stamp,12,2) as bigint) as hour,b.category_id as pre_category_id
				from kk_feature_offline_test a left outer join shop_info b on a.shop_id=b.shop_id
			)c
			group by pre_category_id,hour
		)d left outer join 
		(
			select pre_category_id,count(*) as pre_category_id_count
			from
			(
				select b.category_id as pre_category_id
				from kk_feature_offline_test a left outer join shop_info b on a.shop_id=b.shop_id
			)c
			group by pre_category_id
		)e
		on d.pre_category_id=e.pre_category_id
	)h
	on t.pre_category_id=h.pre_category_id and t.hour=h.hour
)i;

-- 线上测试
create table if not exists kk_online_test_get_shop_hour_count_rate as
select row_id,pre_shop_id,shop_hour_count,shop_hour_count_rate,mall_hour_count,mall_hour_count_rate,
pre_category_id_hour_count,pre_category_id_hour_count_rate,(shop_hour_count/mall_hour_count_rate) as shop_hour_mall_rate
from
(
	select t.row_id,t.pre_shop_id,f.shop_hour_count,f.shop_hour_count_rate,g.mall_hour_count,g.mall_hour_count_rate,
	h.pre_category_id_hour_count,h.pre_category_id_hour_count_rate
	from kk_online_test_get_basic_feature t left outer join
	(
		select d.shop_id as pre_shop_id,d.hour,d.shop_hour_count,(d.shop_hour_count/e.shop_count) as shop_hour_count_rate
		from
		(
			select shop_id,hour,count(*) as shop_hour_count
			from
			(
				select a.row_id,a.shop_id,a.mall_id,cast(substr(a.time_stamp,12,2) as bigint) as hour,b.category_id
				from kk_feature_online_test a left outer join shop_info b on a.shop_id=b.shop_id
			)c
			group by shop_id,hour
		)d left outer join 
		(
			select shop_id,count(*) as shop_count 
			from kk_feature_online_test
			group by shop_id
		)e
		on d.shop_id=e.shop_id
	)f
	on t.pre_shop_id=f.pre_shop_id and t.hour=f.hour
	left outer join
	(
		select d.mall_id,d.hour,d.mall_hour_count,(d.mall_hour_count/e.mall_count) as mall_hour_count_rate
		from
		(
			select mall_id,hour,count(*) as mall_hour_count
			from
			(
				select mall_id,cast(substr(time_stamp,12,2) as bigint) as hour
				from kk_feature_online_test
			)a
			group by mall_id,hour
		)d left outer join 
		(
			select mall_id,count(*) as mall_count 
			from kk_feature_online_test
			group by mall_id
		)e
		on d.mall_id=e.mall_id
	)g
	on t.mall_id=g.mall_id and t.hour=g.hour
	left outer join
	(
		select d.pre_category_id,d.hour,d.pre_category_id_hour_count,
		(d.pre_category_id_hour_count/e.pre_category_id_count) as pre_category_id_hour_count_rate
		from
		(
			select pre_category_id,hour,count(*) as pre_category_id_hour_count
			from
			(
				select cast(substr(a.time_stamp,12,2) as bigint) as hour,b.category_id as pre_category_id
				from kk_feature_online_test a left outer join shop_info b on a.shop_id=b.shop_id
			)c
			group by pre_category_id,hour
		)d left outer join 
		(
			select pre_category_id,count(*) as pre_category_id_count
			from
			(
				select b.category_id as pre_category_id
				from kk_feature_online_test a left outer join shop_info b on a.shop_id=b.shop_id
			)c
			group by pre_category_id
		)e
		on d.pre_category_id=e.pre_category_id
	)h
	on t.pre_category_id=h.pre_category_id and t.hour=h.hour
)i;

drop table if  exists kk_offline_train_feature25;
drop table if  exists kk_offline_test_feature25;
drop table if  exists kk_online_test_feature25;
create table if not exists kk_offline_train_feature25 as
select a.*,b.shop_hour_count,b.shop_hour_count_rate,b.mall_hour_count,b.mall_hour_count_rate,
b.pre_category_id_hour_count,b.pre_category_id_hour_count_rate,b.shop_hour_mall_rate
from kk_offline_train_feature24 a left outer join kk_offline_train_get_shop_hour_count_rate b
on a.row_id=b.row_id and a.pre_shop_id=b.pre_shop_id;
create table if not exists kk_offline_test_feature25 as
select a.*,b.shop_hour_count,b.shop_hour_count_rate,b.mall_hour_count,b.mall_hour_count_rate,
b.pre_category_id_hour_count,b.pre_category_id_hour_count_rate,b.shop_hour_mall_rate
from kk_offline_test_feature24 a left outer join kk_offline_test_get_shop_hour_count_rate b
on a.row_id=b.row_id and a.pre_shop_id=b.pre_shop_id;
create table if not exists kk_online_test_feature25 as
select a.*,b.shop_hour_count,b.shop_hour_count_rate,b.mall_hour_count,b.mall_hour_count_rate,
b.pre_category_id_hour_count,b.pre_category_id_hour_count_rate,b.shop_hour_mall_rate
from kk_online_test_feature24 a left outer join kk_online_test_get_shop_hour_count_rate b
on a.row_id=b.row_id and a.pre_shop_id=b.pre_shop_id;

------------------------------------------------------------------------------------------------------
-- get_shop_hot10_wifi_count(join on [row_id,pre_shop_id])  
--kk_get_shop_hot10_wifi_count

-- kk_feature_offline_train_shop_expand_wifi
-- kk_feature_offline_test_shop_expand_wifi
-- kk_feature_online_test_shop_expand_wifi
-- kk_offline_train_expand_row_wifi
-- kk_offline_test_expand_row_wifi
-- kk_online_test_expand_row_wifi
-- 线下训练   kk_offline_train   18-24             kk_feature_offline_train 1-18
-- 线下测试   kk_offline_test    25-31             kk_feature_offline_test  1-25
-- 线上测试   kk_online_test     18-24             kk_feature_online_test   1-31
-- 线下训练
drop table if  exists kk_offline_train_get_shop_hot10_wifi_count_1;
drop table if  exists kk_offline_train_get_shop_hot10_wifi_count_2;
drop table if  exists kk_offline_test_get_shop_hot10_wifi_count_1;
drop table if  exists kk_offline_test_get_shop_hot10_wifi_count_2;
drop table if  exists kk_online_test_get_shop_hot10_wifi_count_1;
drop table if  exists kk_online_test_get_shop_hot10_wifi_count_2;

create table if not exists kk_offline_train_get_shop_hot10_wifi_count_1 as
select row_id,shop_id as pre_shop_id,count(*) as shop_hot10_wifi_count
from
(
	select t.row_id,c.shop_id
	from kk_offline_train_expand_row_wifi t inner join
	(
		select shop_id,wifi
		from
		(
			select shop_id,wifi,row_number() over (partition by shop_id order by shop_wifi_count desc) as shop_wifi_count_rank
			from
			(
				select shop_id,wifi,count(*) as shop_wifi_count
				from kk_feature_offline_train_shop_expand_wifi
				group by shop_id,wifi
			)a
		)b
		where shop_wifi_count_rank<=10
	)c
	on t.wifi=c.wifi
)d
group by row_id,shop_id;

create table if not exists kk_offline_train_get_shop_hot10_wifi_count_2 as
select row_id,shop_id as pre_shop_id,count(*) as shop_sign20_wifi_count
from
(
	select t.row_id,c.shop_id
	from kk_offline_train_expand_row_wifi t inner join
	(
		select shop_id,wifi
		from
		(
			select shop_id,wifi,row_number() over (partition by shop_id order by max_wifi_sign desc) as  shop_wifi_sign_rank
			from
			(
				select shop_id,wifi,max(wifi_sign) as max_wifi_sign from kk_feature_offline_train_shop_expand_wifi
				group by shop_id,wifi
			)a
		)b
		where shop_wifi_sign_rank<=20
	)c
	on t.wifi=c.wifi
)d
group by row_id,shop_id;

-- 线下测试
create table if not exists kk_offline_test_get_shop_hot10_wifi_count_1 as
select row_id,shop_id as pre_shop_id,count(*) as shop_hot10_wifi_count
from
(
	select t.row_id,c.shop_id
	from kk_offline_test_expand_row_wifi t inner join
	(
		select shop_id,wifi
		from
		(
			select shop_id,wifi,row_number() over (partition by shop_id order by shop_wifi_count desc) as shop_wifi_count_rank
			from
			(
				select shop_id,wifi,count(*) as shop_wifi_count
				from kk_feature_offline_test_shop_expand_wifi
				group by shop_id,wifi
			)a
		)b
		where shop_wifi_count_rank<=10
	)c
	on t.wifi=c.wifi
)d
group by row_id,shop_id;

create table if not exists kk_offline_test_get_shop_hot10_wifi_count_2 as
select row_id,shop_id as pre_shop_id,count(*) as shop_sign20_wifi_count
from
(
	select t.row_id,c.shop_id
	from kk_offline_test_expand_row_wifi t inner join
	(
		select shop_id,wifi
		from
		(
			select shop_id,wifi,row_number() over (partition by shop_id order by max_wifi_sign desc) as  shop_wifi_sign_rank
			from
			(
				select shop_id,wifi,max(wifi_sign) as max_wifi_sign from kk_feature_offline_test_shop_expand_wifi
				group by shop_id,wifi
			)a
		)b
		where shop_wifi_sign_rank<=20
	)c
	on t.wifi=c.wifi
)d
group by row_id,shop_id;

-- 线上测试
create table if not exists kk_online_test_get_shop_hot10_wifi_count_1 as
select row_id,shop_id as pre_shop_id,count(*) as shop_hot10_wifi_count
from
(
	select t.row_id,c.shop_id
	from kk_online_test_expand_row_wifi t inner join
	(
		select shop_id,wifi
		from
		(
			select shop_id,wifi,row_number() over (partition by shop_id order by shop_wifi_count desc) as shop_wifi_count_rank
			from
			(
				select shop_id,wifi,count(*) as shop_wifi_count
				from kk_feature_online_test_shop_expand_wifi
				group by shop_id,wifi
			)a
		)b
		where shop_wifi_count_rank<=10
	)c
	on t.wifi=c.wifi
)d
group by row_id,shop_id;

create table if not exists kk_online_test_get_shop_hot10_wifi_count_2 as
select row_id,shop_id as pre_shop_id,count(*) as shop_sign20_wifi_count
from
(
	select t.row_id,c.shop_id
	from kk_online_test_expand_row_wifi t inner join
	(
		select shop_id,wifi
		from
		(
			select shop_id,wifi,row_number() over (partition by shop_id order by max_wifi_sign desc) as  shop_wifi_sign_rank
			from
			(
				select shop_id,wifi,max(wifi_sign) as max_wifi_sign from kk_feature_online_test_shop_expand_wifi
				group by shop_id,wifi
			)a
		)b
		where shop_wifi_sign_rank<=20
	)c
	on t.wifi=c.wifi
)d
group by row_id,shop_id;

drop table if  exists kk_offline_train_feature26;
drop table if  exists kk_offline_test_feature26;
drop table if  exists kk_online_test_feature26;
create table if not exists kk_offline_train_feature26 as
select a.*,b.shop_hot10_wifi_count,c.shop_sign20_wifi_count
from kk_offline_train_feature25 a 
left outer join kk_offline_train_get_shop_hot10_wifi_count_1 b
on a.row_id=b.row_id and a.pre_shop_id=b.pre_shop_id
left outer join kk_offline_train_get_shop_hot10_wifi_count_2 c
on a.row_id=c.row_id and a.pre_shop_id=c.pre_shop_id;

create table if not exists kk_offline_test_feature26 as
select a.*,b.shop_hot10_wifi_count,c.shop_sign20_wifi_count
from kk_offline_test_feature25 a 
left outer join kk_offline_test_get_shop_hot10_wifi_count_1 b
on a.row_id=b.row_id and a.pre_shop_id=b.pre_shop_id
left outer join kk_offline_test_get_shop_hot10_wifi_count_2 c
on a.row_id=c.row_id and a.pre_shop_id=c.pre_shop_id;

create table if not exists kk_online_test_feature26 as
select a.*,b.shop_hot10_wifi_count,c.shop_sign20_wifi_count
from kk_online_test_feature25 a 
left outer join kk_online_test_get_shop_hot10_wifi_count_1 b
on a.row_id=b.row_id and a.pre_shop_id=b.pre_shop_id
left outer join kk_online_test_get_shop_hot10_wifi_count_2 c
on a.row_id=c.row_id and a.pre_shop_id=c.pre_shop_id;



------------------------------------------------------------------------------------------------------
-- get_row_dis(join on [row_id,pre_shop_id])  
--kk_get_row_dis

-- kk_feature_offline_train_shop_expand_wifi
-- kk_feature_offline_test_shop_expand_wifi
-- kk_feature_online_test_shop_expand_wifi
-- kk_offline_train_expand_row_wifi
-- kk_offline_test_expand_row_wifi
-- kk_online_test_expand_row_wifi
-- 线下训练   kk_offline_train   18-24             kk_feature_offline_train 1-18
-- 线下测试   kk_offline_test    25-31             kk_feature_offline_test  1-25
-- 线上测试   kk_online_test     18-24             kk_feature_online_test   1-31

drop table if  exists kk_offline_train_get_row_dis;
drop table if  exists kk_offline_test_get_row_dis;
drop table if  exists kk_online_test_get_row_dis;

create table if not exists kk_offline_train_get_row_dis as
select row_id,sqrt((longitude-median_lon)*(longitude-median_lon)+(latitude-median_lat)*(latitude-median_lat)) as dis_1,
sqrt((longitude-up_lon)*(longitude-up_lon)+(latitude-up_lat)*(latitude-up_lat)) as dis_2,
sqrt((longitude-down_lon)*(longitude-down_lon)+(latitude-down_lat)*(latitude-down_lat)) as dis_3,
sqrt((longitude-left_lon)*(longitude-left_lon)+(latitude-left_lat)*(latitude-left_lat)) as dis_4,
sqrt((longitude-right_lon)*(longitude-right_lon)+(latitude-right_lat)*(latitude-right_lat)) as dis_5,
(longitude-median_lon) as angel_1_lon,(latitude-median_lat) as angel_1_la,
(longitude-up_lon) as angel_2_lon,(latitude-up_lat) as angel_2_la,
(longitude-down_lon) as angel_3_lon,(latitude-down_lat) as angel_3_la,
(longitude-left_lon) as angel_4_lon,(latitude-left_lat) as angel_4_la,
(longitude-right_lon) as angel_5_lon,(latitude-right_lat) as angel_5_la
from
(
	select m.row_id,m.mall_id,m.longitude,m.latitude,a.median_lon,a.median_lat,d.up_lon,d.up_lat,e.down_lon,e.down_lat,
	f.left_lon,f.left_lat,g.right_lon,g.right_lat
	from kk_offline_train m left outer join
	(
		select mall_id,median(longitude) as median_lon,median(latitude) as median_lat
		from kk_feature_offline_train
		group by mall_id
	)a
	on m.mall_id=a.mall_id
	left outer join
	(
		select mall_id,median(longitude) as up_lon,median(latitude) as up_lat
		from
		(
			select mall_id,longitude,latitude
			from
			(
				select t.mall_id,t.longitude,t.latitude,a.median_lon,a.median_lat
				from
				kk_feature_offline_train t left outer join
				(
					select mall_id,median(longitude) as median_lon,median(latitude) as median_lat
					from kk_feature_offline_train
					group by mall_id
				)a
				on t.mall_id=a.mall_id
			)b
			where longitude>median_lon and latitude>median_lat
		)c
		group by mall_id
	)d
	on m.mall_id=d.mall_id
	left outer join
	(
		select mall_id,median(longitude) as down_lon,median(latitude) as down_lat
		from
		(
			select mall_id,longitude,latitude
			from
			(
				select t.mall_id,t.longitude,t.latitude,a.median_lon,a.median_lat
				from
				kk_feature_offline_train t left outer join
				(
					select mall_id,median(longitude) as median_lon,median(latitude) as median_lat
					from kk_feature_offline_train
					group by mall_id
				)a
				on t.mall_id=a.mall_id
			)b
			where longitude>median_lon and latitude<median_lat
		)c
		group by mall_id
	)e
	on m.mall_id=e.mall_id
	left outer join
	(
		select mall_id,median(longitude) as left_lon,median(latitude) as left_lat
		from
		(
			select mall_id,longitude,latitude
			from
			(
				select t.mall_id,t.longitude,t.latitude,a.median_lon,a.median_lat
				from
				kk_feature_offline_train t left outer join
				(
					select mall_id,median(longitude) as median_lon,median(latitude) as median_lat
					from kk_feature_offline_train
					group by mall_id
				)a
				on t.mall_id=a.mall_id
			)b
			where longitude<median_lon and latitude<median_lat
		)c
		group by mall_id
	)f
	on m.mall_id=f.mall_id
	left outer join
	(
		select mall_id,median(longitude) as right_lon,median(latitude) as right_lat
		from
		(
			select mall_id,longitude,latitude
			from
			(
				select t.mall_id,t.longitude,t.latitude,a.median_lon,a.median_lat
				from
				kk_feature_offline_train t left outer join
				(
					select mall_id,median(longitude) as median_lon,median(latitude) as median_lat
					from kk_feature_offline_train
					group by mall_id
				)a
				on t.mall_id=a.mall_id
			)b
			where longitude<median_lon and latitude>median_lat
		)c
		group by mall_id
	)g
	on m.mall_id=g.mall_id
)zz;

-- 线下测试
create table if not exists kk_offline_test_get_row_dis as
select row_id,sqrt((longitude-median_lon)*(longitude-median_lon)+(latitude-median_lat)*(latitude-median_lat)) as dis_1,
sqrt((longitude-up_lon)*(longitude-up_lon)+(latitude-up_lat)*(latitude-up_lat)) as dis_2,
sqrt((longitude-down_lon)*(longitude-down_lon)+(latitude-down_lat)*(latitude-down_lat)) as dis_3,
sqrt((longitude-left_lon)*(longitude-left_lon)+(latitude-left_lat)*(latitude-left_lat)) as dis_4,
sqrt((longitude-right_lon)*(longitude-right_lon)+(latitude-right_lat)*(latitude-right_lat)) as dis_5,
(longitude-median_lon) as angel_1_lon,(latitude-median_lat) as angel_1_la,
(longitude-up_lon) as angel_2_lon,(latitude-up_lat) as angel_2_la,
(longitude-down_lon) as angel_3_lon,(latitude-down_lat) as angel_3_la,
(longitude-left_lon) as angel_4_lon,(latitude-left_lat) as angel_4_la,
(longitude-right_lon) as angel_5_lon,(latitude-right_lat) as angel_5_la
from
(
	select m.row_id,m.mall_id,m.longitude,m.latitude,a.median_lon,a.median_lat,d.up_lon,d.up_lat,e.down_lon,e.down_lat,
	f.left_lon,f.left_lat,g.right_lon,g.right_lat
	from kk_offline_test m left outer join
	(
		select mall_id,median(longitude) as median_lon,median(latitude) as median_lat
		from kk_feature_offline_test
		group by mall_id
	)a
	on m.mall_id=a.mall_id
	left outer join
	(
		select mall_id,median(longitude) as up_lon,median(latitude) as up_lat
		from
		(
			select mall_id,longitude,latitude
			from
			(
				select t.mall_id,t.longitude,t.latitude,a.median_lon,a.median_lat
				from
				kk_feature_offline_test t left outer join
				(
					select mall_id,median(longitude) as median_lon,median(latitude) as median_lat
					from kk_feature_offline_test
					group by mall_id
				)a
				on t.mall_id=a.mall_id
			)b
			where longitude>median_lon and latitude>median_lat
		)c
		group by mall_id
	)d
	on m.mall_id=d.mall_id
	left outer join
	(
		select mall_id,median(longitude) as down_lon,median(latitude) as down_lat
		from
		(
			select mall_id,longitude,latitude
			from
			(
				select t.mall_id,t.longitude,t.latitude,a.median_lon,a.median_lat
				from
				kk_feature_offline_test t left outer join
				(
					select mall_id,median(longitude) as median_lon,median(latitude) as median_lat
					from kk_feature_offline_test
					group by mall_id
				)a
				on t.mall_id=a.mall_id
			)b
			where longitude>median_lon and latitude<median_lat
		)c
		group by mall_id
	)e
	on m.mall_id=e.mall_id
	left outer join
	(
		select mall_id,median(longitude) as left_lon,median(latitude) as left_lat
		from
		(
			select mall_id,longitude,latitude
			from
			(
				select t.mall_id,t.longitude,t.latitude,a.median_lon,a.median_lat
				from
				kk_feature_offline_test t left outer join
				(
					select mall_id,median(longitude) as median_lon,median(latitude) as median_lat
					from kk_feature_offline_test
					group by mall_id
				)a
				on t.mall_id=a.mall_id
			)b
			where longitude<median_lon and latitude<median_lat
		)c
		group by mall_id
	)f
	on m.mall_id=f.mall_id
	left outer join
	(
		select mall_id,median(longitude) as right_lon,median(latitude) as right_lat
		from
		(
			select mall_id,longitude,latitude
			from
			(
				select t.mall_id,t.longitude,t.latitude,a.median_lon,a.median_lat
				from
				kk_feature_offline_test t left outer join
				(
					select mall_id,median(longitude) as median_lon,median(latitude) as median_lat
					from kk_feature_offline_test
					group by mall_id
				)a
				on t.mall_id=a.mall_id
			)b
			where longitude<median_lon and latitude>median_lat
		)c
		group by mall_id
	)g
	on m.mall_id=g.mall_id
)zz;

-- 线上测试
create table if not exists kk_online_test_get_row_dis as
select row_id,sqrt((longitude-median_lon)*(longitude-median_lon)+(latitude-median_lat)*(latitude-median_lat)) as dis_1,
sqrt((longitude-up_lon)*(longitude-up_lon)+(latitude-up_lat)*(latitude-up_lat)) as dis_2,
sqrt((longitude-down_lon)*(longitude-down_lon)+(latitude-down_lat)*(latitude-down_lat)) as dis_3,
sqrt((longitude-left_lon)*(longitude-left_lon)+(latitude-left_lat)*(latitude-left_lat)) as dis_4,
sqrt((longitude-right_lon)*(longitude-right_lon)+(latitude-right_lat)*(latitude-right_lat)) as dis_5,
(longitude-median_lon) as angel_1_lon,(latitude-median_lat) as angel_1_la,
(longitude-up_lon) as angel_2_lon,(latitude-up_lat) as angel_2_la,
(longitude-down_lon) as angel_3_lon,(latitude-down_lat) as angel_3_la,
(longitude-left_lon) as angel_4_lon,(latitude-left_lat) as angel_4_la,
(longitude-right_lon) as angel_5_lon,(latitude-right_lat) as angel_5_la
from
(
	select m.row_id,m.mall_id,m.longitude,m.latitude,a.median_lon,a.median_lat,d.up_lon,d.up_lat,e.down_lon,e.down_lat,
	f.left_lon,f.left_lat,g.right_lon,g.right_lat
	from kk_online_test m left outer join
	(
		select mall_id,median(longitude) as median_lon,median(latitude) as median_lat
		from kk_feature_online_test
		group by mall_id
	)a
	on m.mall_id=a.mall_id
	left outer join
	(
		select mall_id,median(longitude) as up_lon,median(latitude) as up_lat
		from
		(
			select mall_id,longitude,latitude
			from
			(
				select t.mall_id,t.longitude,t.latitude,a.median_lon,a.median_lat
				from
				kk_feature_online_test t left outer join
				(
					select mall_id,median(longitude) as median_lon,median(latitude) as median_lat
					from kk_feature_online_test
					group by mall_id
				)a
				on t.mall_id=a.mall_id
			)b
			where longitude>median_lon and latitude>median_lat
		)c
		group by mall_id
	)d
	on m.mall_id=d.mall_id
	left outer join
	(
		select mall_id,median(longitude) as down_lon,median(latitude) as down_lat
		from
		(
			select mall_id,longitude,latitude
			from
			(
				select t.mall_id,t.longitude,t.latitude,a.median_lon,a.median_lat
				from
				kk_feature_online_test t left outer join
				(
					select mall_id,median(longitude) as median_lon,median(latitude) as median_lat
					from kk_feature_online_test
					group by mall_id
				)a
				on t.mall_id=a.mall_id
			)b
			where longitude>median_lon and latitude<median_lat
		)c
		group by mall_id
	)e
	on m.mall_id=e.mall_id
	left outer join
	(
		select mall_id,median(longitude) as left_lon,median(latitude) as left_lat
		from
		(
			select mall_id,longitude,latitude
			from
			(
				select t.mall_id,t.longitude,t.latitude,a.median_lon,a.median_lat
				from
				kk_feature_online_test t left outer join
				(
					select mall_id,median(longitude) as median_lon,median(latitude) as median_lat
					from kk_feature_online_test
					group by mall_id
				)a
				on t.mall_id=a.mall_id
			)b
			where longitude<median_lon and latitude<median_lat
		)c
		group by mall_id
	)f
	on m.mall_id=f.mall_id
	left outer join
	(
		select mall_id,median(longitude) as right_lon,median(latitude) as right_lat
		from
		(
			select mall_id,longitude,latitude
			from
			(
				select t.mall_id,t.longitude,t.latitude,a.median_lon,a.median_lat
				from
				kk_feature_online_test t left outer join
				(
					select mall_id,median(longitude) as median_lon,median(latitude) as median_lat
					from kk_feature_online_test
					group by mall_id
				)a
				on t.mall_id=a.mall_id
			)b
			where longitude<median_lon and latitude>median_lat
		)c
		group by mall_id
	)g
	on m.mall_id=g.mall_id
)zz;

drop table if  exists kk_offline_train_feature27;
drop table if  exists kk_offline_test_feature27;
drop table if  exists kk_online_test_feature27;
create table if not exists kk_offline_train_feature27 as
select a.*,b.dis_1,b.dis_2,b.dis_3,b.dis_4,b.dis_5
from kk_offline_train_feature26 a left outer join kk_offline_train_get_row_dis b
on a.row_id=b.row_id;
create table if not exists kk_offline_test_feature27 as
select a.*,b.dis_1,b.dis_2,b.dis_3,b.dis_4,b.dis_5
from kk_offline_test_feature26 a left outer join kk_offline_test_get_row_dis b
on a.row_id=b.row_id;
create table if not exists kk_online_test_feature27 as
select a.*,b.dis_1,b.dis_2,b.dis_3,b.dis_4,b.dis_5
from kk_online_test_feature26 a left outer join kk_online_test_get_row_dis b
on a.row_id=b.row_id;

------------------------------------------------------------------------------------------------------

