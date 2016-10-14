#!/bin/bash

startDate=''
endDate=''

until [ $# -eq 0 ]
do
	if [ $1'x' = '-sdx' ]; then
		shift
		startDate=$1
	elif [ $1'x' = '-edx' ]; then
		shift
		endDate=$1
	fi
	shift
done

if [ -n "$startDate" ] && [ -n "$endDate" ]; then
	echo "use the arguments of the date"
else
	echo "use the default date"
	startDate=$(date -d last-day +%Y-%m-%d)
	endDate=$(date +%Y-%m-%d)
fi
echo "run of arguments. start date is:$startDate, end date is:$endDate"
echo "start run of order job "

echo "process order info"
hive --database bigdater -e "from event_logs insert overwrite table order_info select oid,pl,s_time,cut,pt,cua where en='e_crt' and pl is not null and s_time >= unix_timestamp('$startDate','yyyy-MM-dd')*1000 and s_time < unix_timestamp('$endDate','yyyy-MM-dd')*1000"
sqoop export --connect jdbc:mysql://hh:3306/report --username hive --password hive --table order_info --export-dir /hive/bigdater.db/order_info/* --input-fields-terminated-by "\\01" --update-mode allowinsert --update-key order_id


echo "process orders"
hive --database bigdater -e "from (select pl,from_unixtime(cast(s_time/1000 as bigint),'yyyy-MM-dd') as date,cut,pt,count(distinct oid) as orders from event_logs where en='e_crt' and pl is not null and s_time >= unix_timestamp('$startDate','yyyy-MM-dd')*1000 and s_time < unix_timestamp('$endDate','yyyy-MM-dd')*1000  group by pl,from_unixtime(cast(s_time/1000 as bigint),'yyyy-MM-dd'),cut,pt) as tmp insert overwrite table stats_order_tmp1 select pl,date,cut,pt,orders insert overwrite table stats_order_tmp2 select platform_convert(pl),date_convert(date),currency_type_convert(cut),payment_type_convert(pt),orders,date"
hive --database bigdater -e "from stats_order_tmp1 insert into table stats_order_tmp2 select platform_convert('all'),date_convert(dt),currency_type_convert(cut),payment_type_convert(pt),sum(values) as orders,dt group by dt,cut,pt"
hive --database bigdater -e "from stats_order_tmp1 insert into table stats_order_tmp2 select platform_convert('all'),date_convert(dt),currency_type_convert('all'),payment_type_convert(pt),sum(values) as orders,dt group by dt,pt"
hive --database bigdater -e "from stats_order_tmp1 insert into table stats_order_tmp2 select platform_convert('all'),date_convert(dt),currency_type_convert(cut),payment_type_convert('all'),sum(values) as orders,dt group by dt,cut"
hive --database bigdater -e "from stats_order_tmp1 insert into table stats_order_tmp2 select platform_convert('all'),date_convert(dt),currency_type_convert('all'),payment_type_convert('all'),sum(values) as orders,dt group by dt"
hive --database bigdater -e "from stats_order_tmp1 insert into table stats_order_tmp2 select platform_convert(pl),date_convert(dt),currency_type_convert('all'),payment_type_convert(pt),sum(values) as orders,dt group by pl,dt,pt"
hive --database bigdater -e "from stats_order_tmp1 insert into table stats_order_tmp2 select platform_convert(pl),date_convert(dt),currency_type_convert('all'),payment_type_convert('all'),sum(values) as orders,dt group by pl,dt"
hive --database bigdater -e "from stats_order_tmp1 insert into table stats_order_tmp2 select platform_convert(pl),date_convert(dt),currency_type_convert(cut),payment_type_convert('all'),sum(values) as orders,dt group by pl,dt,cut"
sqoop export --connect jdbc:mysql://hh:3306/report --username hive --password hive --table stats_order --export-dir /hive/bigdater.db/stats_order_tmp2/* --input-fields-terminated-by "\\01" --update-mode allowinsert --update-key platform_dimension_id,date_dimension_id,currency_type_dimension_id,payment_type_dimension_id --columns platform_dimension_id,date_dimension_id,currency_type_dimension_id,payment_type_dimension_id,orders,created


echo "process order amount"
hive --database bigdater -e "from (from (select pl,from_unixtime(cast(s_time/1000 as bigint),'yyyy-MM-dd') as date,cut,pt,oid,max(cua) as amount from event_logs where en='e_crt' and pl is not null and s_time >= unix_timestamp('$startDate','yyyy-MM-dd')*1000 and s_time < unix_timestamp('$endDate','yyyy-MM-dd')*1000  group by pl,from_unixtime(cast(s_time/1000 as bigint),'yyyy-MM-dd'),cut,pt,oid) as tmp select pl,date,cut,pt,sum(amount) as amount group by pl,date,cut,pt ) as tmp2 insert overwrite table stats_order_tmp1 select pl,date,cut,pt,amount insert overwrite table stats_order_tmp2 select platform_convert(pl),date_convert(date),currency_type_convert(cut),payment_type_convert(pt),amount,date"
hive --database bigdater -e "from stats_order_tmp1 insert into table stats_order_tmp2 select platform_convert('all'),date_convert(dt),currency_type_convert(cut),payment_type_convert(pt),sum(values) as amount,dt group by dt,cut,pt"
hive --database bigdater -e "from stats_order_tmp1 insert into table stats_order_tmp2 select platform_convert('all'),date_convert(dt),currency_type_convert('all'),payment_type_convert(pt),sum(values) as amount,dt group by dt,pt"
hive --database bigdater -e "from stats_order_tmp1 insert into table stats_order_tmp2 select platform_convert('all'),date_convert(dt),currency_type_convert(cut),payment_type_convert('all'),sum(values) as amount,dt group by dt,cut"
hive --database bigdater -e "from stats_order_tmp1 insert into table stats_order_tmp2 select platform_convert('all'),date_convert(dt),currency_type_convert('all'),payment_type_convert('all'),sum(values) as amount,dt group by dt"
hive --database bigdater -e "from stats_order_tmp1 insert into table stats_order_tmp2 select platform_convert(pl),date_convert(dt),currency_type_convert('all'),payment_type_convert(pt),sum(values) as amount,dt group by pl,dt,pt"
hive --database bigdater -e "from stats_order_tmp1 insert into table stats_order_tmp2 select platform_convert(pl),date_convert(dt),currency_type_convert('all'),payment_type_convert('all'),sum(values) as amount,dt group by pl,dt"
hive --database bigdater -e "from stats_order_tmp1 insert into table stats_order_tmp2 select platform_convert(pl),date_convert(dt),currency_type_convert(cut),payment_type_convert('all'),sum(values) as amount,dt group by pl,dt,cut"
sqoop export --connect jdbc:mysql://hh:3306/report --username hive --password hive --table stats_order --export-dir /hive/bigdater.db/stats_order_tmp2/* --input-fields-terminated-by "\\01" --update-mode allowinsert --update-key platform_dimension_id,date_dimension_id,currency_type_dimension_id,payment_type_dimension_id --columns platform_dimension_id,date_dimension_id,currency_type_dimension_id,payment_type_dimension_id,order_amount,created


echo "process success orders"
hive --database bigdater -e "from (select order_info(oid,'pl') as pl,from_unixtime(cast(s_time/1000 as bigint),'yyyy-MM-dd') as date,order_info(oid,'cut') as cut,order_info(oid,'pt') as pt,count(distinct oid) as orders from event_logs where en='e_cs' and pl is not null and s_time >= unix_timestamp('$startDate','yyyy-MM-dd')*1000 and s_time < unix_timestamp('$endDate','yyyy-MM-dd')*1000  group by order_info(oid,'pl'),from_unixtime(cast(s_time/1000 as bigint),'yyyy-MM-dd'),order_info(oid,'cut'),order_info(oid,'pt') ) as tmp insert overwrite table stats_order_tmp1 select pl,date,cut,pt,orders insert overwrite table stats_order_tmp2 select platform_convert(pl),date_convert(date),currency_type_convert(cut),payment_type_convert(pt),orders,date"
hive --database bigdater -e "from stats_order_tmp1 insert into table stats_order_tmp2 select platform_convert('all'),date_convert(dt),currency_type_convert(cut),payment_type_convert(pt),sum(values) as orders,dt group by dt,cut,pt"
hive --database bigdater -e "from stats_order_tmp1 insert into table stats_order_tmp2 select platform_convert('all'),date_convert(dt),currency_type_convert('all'),payment_type_convert(pt),sum(values) as orders,dt group by dt,pt"
hive --database bigdater -e "from stats_order_tmp1 insert into table stats_order_tmp2 select platform_convert('all'),date_convert(dt),currency_type_convert(cut),payment_type_convert('all'),sum(values) as orders,dt group by dt,cut"
hive --database bigdater -e "from stats_order_tmp1 insert into table stats_order_tmp2 select platform_convert('all'),date_convert(dt),currency_type_convert('all'),payment_type_convert('all'),sum(values) as orders,dt group by dt"
hive --database bigdater -e "from stats_order_tmp1 insert into table stats_order_tmp2 select platform_convert(pl),date_convert(dt),currency_type_convert('all'),payment_type_convert(pt),sum(values) as orders,dt group by pl,dt,pt"
hive --database bigdater -e "from stats_order_tmp1 insert into table stats_order_tmp2 select platform_convert(pl),date_convert(dt),currency_type_convert('all'),payment_type_convert('all'),sum(values) as orders,dt group by pl,dt"
hive --database bigdater -e "from stats_order_tmp1 insert into table stats_order_tmp2 select platform_convert(pl),date_convert(dt),currency_type_convert(cut),payment_type_convert('all'),sum(values) as orders,dt group by pl,dt,cut"
sqoop export --connect jdbc:mysql://hh:3306/report --username hive --password hive --table stats_order --export-dir /hive/bigdater.db/stats_order_tmp2/* --input-fields-terminated-by "\\01" --update-mode allowinsert --update-key platform_dimension_id,date_dimension_id,currency_type_dimension_id,payment_type_dimension_id --columns platform_dimension_id,date_dimension_id,currency_type_dimension_id,payment_type_dimension_id,success_orders,created


echo "process revenue amount"
hive --database bigdater -e "from (from (select order_info(oid,'pl') as pl,from_unixtime(cast(s_time/1000 as bigint),'yyyy-MM-dd') as date,order_info(oid,'cut') as cut,order_info(oid,'pt') as pt,oid,max(order_info(oid)) as amount from event_logs where en='e_cs' and pl is not null and s_time >= unix_timestamp('$startDate','yyyy-MM-dd')*1000 and s_time < unix_timestamp('$endDate','yyyy-MM-dd')*1000 group by order_info(oid,'pl'),from_unixtime(cast(s_time/1000 as bigint),'yyyy-MM-dd'),order_info(oid,'cut'),order_info(oid,'pt'),oid) as tmp select pl,date,cut,pt,sum(amount) as amount group by pl,date,cut,pt) as tmp2 insert overwrite table stats_order_tmp1 select pl,date,cut,pt,amount insert overwrite table stats_order_tmp2 select platform_convert(pl),date_convert(date),currency_type_convert(cut),payment_type_convert(pt),amount,date"
hive --database bigdater -e "from stats_order_tmp1 insert into table stats_order_tmp2 select platform_convert('all'),date_convert(dt),currency_type_convert(cut),payment_type_convert(pt),sum(values) as amount,dt group by dt,cut,pt"
hive --database bigdater -e "from stats_order_tmp1 insert into table stats_order_tmp2 select platform_convert('all'),date_convert(dt),currency_type_convert('all'),payment_type_convert(pt),sum(values) as amount,dt group by dt,pt"
hive --database bigdater -e "from stats_order_tmp1 insert into table stats_order_tmp2 select platform_convert('all'),date_convert(dt),currency_type_convert(cut),payment_type_convert('all'),sum(values) as amount,dt group by dt,cut"
hive --database bigdater -e "from stats_order_tmp1 insert into table stats_order_tmp2 select platform_convert('all'),date_convert(dt),currency_type_convert('all'),payment_type_convert('all'),sum(values) as amount,dt group by dt"
hive --database bigdater -e "from stats_order_tmp1 insert into table stats_order_tmp2 select platform_convert(pl),date_convert(dt),currency_type_convert('all'),payment_type_convert(pt),sum(values) as amount,dt group by pl,dt,pt"
hive --database bigdater -e "from stats_order_tmp1 insert into table stats_order_tmp2 select platform_convert(pl),date_convert(dt),currency_type_convert('all'),payment_type_convert('all'),sum(values) as amount,dt group by pl,dt"
hive --database bigdater -e "from stats_order_tmp1 insert into table stats_order_tmp2 select platform_convert(pl),date_convert(dt),currency_type_convert(cut),payment_type_convert('all'),sum(values) as amount,dt group by pl,dt,cut"
sqoop export --connect jdbc:mysql://hh:3306/report --username hive --password hive --table stats_order --export-dir /hive/bigdater.db/stats_order_tmp2/* --input-fields-terminated-by "\\01" --update-mode allowinsert --update-key platform_dimension_id,date_dimension_id,currency_type_dimension_id,payment_type_dimension_id --columns platform_dimension_id,date_dimension_id,currency_type_dimension_id,payment_type_dimension_id,revenue_amount,created


echo "process total revenue amount"
hive --database bigdater -e "from (from (select order_info(oid,'pl') as pl,from_unixtime(cast(s_time/1000 as bigint),'yyyy-MM-dd') as date,order_info(oid,'cut') as cut,order_info(oid,'pt') as pt,oid,max(order_info(oid)) as amount from event_logs where en='e_cs' and pl is not null and s_time >= unix_timestamp('$startDate','yyyy-MM-dd')*1000 and s_time < unix_timestamp('$endDate','yyyy-MM-dd')*1000  group by order_info(oid,'pl'),from_unixtime(cast(s_time/1000 as bigint),'yyyy-MM-dd'),order_info(oid,'cut'),order_info(oid,'pt'),oid) as tmp select pl,date,cut,pt,sum(amount) as amount group by pl,date,cut,pt) as tmp2 insert overwrite table stats_order_tmp1 select pl,date,cut,pt,amount insert overwrite table stats_order_tmp2 select platform_convert(pl),date_convert(date),currency_type_convert(cut),payment_type_convert(pt),order_total_amount(platform_convert(pl),date_convert(date),currency_type_convert(cut),payment_type_convert(pt),cast(amount as int),'revenue') as amount,date"
hive --database bigdater -e "from stats_order_tmp1 insert into table stats_order_tmp2 select platform_convert('all'),date_convert(dt),currency_type_convert(cut),payment_type_convert(pt),order_total_amount(platform_convert('all'),date_convert(dt),currency_type_convert(cut),payment_type_convert(pt),cast(sum(values) as int),"revenue") as amount,dt group by dt,cut,pt"
hive --database bigdater -e "from stats_order_tmp1 insert into table stats_order_tmp2 select platform_convert('all'),date_convert(dt),currency_type_convert('all'),payment_type_convert(pt),order_total_amount(platform_convert('all'),date_convert(dt),currency_type_convert('all'),payment_type_convert(pt),cast(sum(values) as int),"revenue") as amount,dt group by dt,pt"
hive --database bigdater -e "from stats_order_tmp1 insert into table stats_order_tmp2 select platform_convert('all'),date_convert(dt),currency_type_convert(cut),payment_type_convert('all'),order_total_amount(platform_convert('all'),date_convert(dt),currency_type_convert(cut),payment_type_convert('all'),cast(sum(values) as int),"revenue") as amount,dt group by dt,cut"
hive --database bigdater -e "from stats_order_tmp1 insert into table stats_order_tmp2 select platform_convert('all'),date_convert(dt),currency_type_convert('all'),payment_type_convert('all'),order_total_amount(platform_convert('all'),date_convert(dt),currency_type_convert('all'),payment_type_convert('all'),cast(sum(values) as int),"revenue") as amount,dt group by dt"
hive --database bigdater -e "from stats_order_tmp1 insert into table stats_order_tmp2 select platform_convert(pl),date_convert(dt),currency_type_convert('all'),payment_type_convert(pt),order_total_amount(platform_convert(pl),date_convert(dt),currency_type_convert('all'),payment_type_convert(pt),cast(sum(values) as int),"revenue") as amount,dt group by pl,dt,pt"
hive --database bigdater -e "from stats_order_tmp1 insert into table stats_order_tmp2 select platform_convert(pl),date_convert(dt),currency_type_convert('all'),payment_type_convert('all'),order_total_amount(platform_convert(pl),date_convert(dt),currency_type_convert('all'),payment_type_convert('all'),cast(sum(values) as int),"revenue") as amount,dt group by pl,dt"
hive --database bigdater -e "from stats_order_tmp1 insert into table stats_order_tmp2 select platform_convert(pl),date_convert(dt),currency_type_convert(cut),payment_type_convert('all'),order_total_amount(platform_convert(pl),date_convert(dt),currency_type_convert(cut),payment_type_convert('all'),cast(sum(values) as int),"revenue") as amount,dt group by pl,dt,cut"
sqoop export --connect jdbc:mysql://hh:3306/report --username hive --password hive --table stats_order --export-dir /hive/bigdater.db/stats_order_tmp2/* --input-fields-terminated-by "\\01" --update-mode allowinsert --update-key platform_dimension_id,date_dimension_id,currency_type_dimension_id,payment_type_dimension_id --columns platform_dimension_id,date_dimension_id,currency_type_dimension_id,payment_type_dimension_id,total_revenue_amount,created


echo "process refund orders"
hive --database bigdater -e "from (select order_info(oid,'pl') as pl,from_unixtime(cast(s_time/1000 as bigint),'yyyy-MM-dd') as date,order_info(oid,'cut') as cut,order_info(oid,'pt') as pt,count(distinct oid) as orders from event_logs where en='e_cr' and pl is not null and s_time >= unix_timestamp('$startDate','yyyy-MM-dd')*1000 and s_time < unix_timestamp('$endDate','yyyy-MM-dd')*1000  group by order_info(oid,'pl'),from_unixtime(cast(s_time/1000 as bigint),'yyyy-MM-dd'),order_info(oid,'cut'),order_info(oid,'pt') ) as tmp insert overwrite table stats_order_tmp1 select pl,date,cut,pt,orders insert overwrite table stats_order_tmp2 select platform_convert(pl),date_convert(date),currency_type_convert(cut),payment_type_convert(pt),orders,date"
hive --database bigdater -e "from stats_order_tmp1 insert into table stats_order_tmp2 select platform_convert('all'),date_convert(dt),currency_type_convert(cut),payment_type_convert(pt),sum(values) as orders,dt group by dt,cut,pt"
hive --database bigdater -e "from stats_order_tmp1 insert into table stats_order_tmp2 select platform_convert('all'),date_convert(dt),currency_type_convert('all'),payment_type_convert(pt),sum(values) as orders,dt group by dt,pt"
hive --database bigdater -e "from stats_order_tmp1 insert into table stats_order_tmp2 select platform_convert('all'),date_convert(dt),currency_type_convert(cut),payment_type_convert('all'),sum(values) as orders,dt group by dt,cut"
hive --database bigdater -e "from stats_order_tmp1 insert into table stats_order_tmp2 select platform_convert('all'),date_convert(dt),currency_type_convert('all'),payment_type_convert('all'),sum(values) as orders,dt group by dt"
hive --database bigdater -e "from stats_order_tmp1 insert into table stats_order_tmp2 select platform_convert(pl),date_convert(dt),currency_type_convert('all'),payment_type_convert(pt),sum(values) as orders,dt group by pl,dt,pt"
hive --database bigdater -e "from stats_order_tmp1 insert into table stats_order_tmp2 select platform_convert(pl),date_convert(dt),currency_type_convert('all'),payment_type_convert('all'),sum(values) as orders,dt group by pl,dt"
hive --database bigdater -e "from stats_order_tmp1 insert into table stats_order_tmp2 select platform_convert(pl),date_convert(dt),currency_type_convert(cut),payment_type_convert('all'),sum(values) as orders,dt group by pl,dt,cut"
sqoop export --connect jdbc:mysql://hh:3306/report --username hive --password hive --table stats_order --export-dir /hive/bigdater.db/stats_order_tmp2/* --input-fields-terminated-by "\\01" --update-mode allowinsert --update-key platform_dimension_id,date_dimension_id,currency_type_dimension_id,payment_type_dimension_id --columns platform_dimension_id,date_dimension_id,currency_type_dimension_id,payment_type_dimension_id,refund_orders,created


echo "process refund amount"
hive --database bigdater -e "from (from (select order_info(oid,'pl') as pl,from_unixtime(cast(s_time/1000 as bigint),'yyyy-MM-dd') as date,order_info(oid,'cut') as cut,order_info(oid,'pt') as pt,oid,max(order_info(oid)) as amount from event_logs where en='e_cr' and pl is not null and s_time >= unix_timestamp('$startDate','yyyy-MM-dd')*1000 and s_time < unix_timestamp('$endDate','yyyy-MM-dd')*1000  group by order_info(oid,'pl'),from_unixtime(cast(s_time/1000 as bigint),'yyyy-MM-dd'),order_info(oid,'cut'),order_info(oid,'pt'),oid) as tmp select pl,date,cut,pt,sum(amount) as amount group by pl,date,cut,pt) as tmp2 insert overwrite table stats_order_tmp1 select pl,date,cut,pt,amount insert overwrite table stats_order_tmp2 select platform_convert(pl),date_convert(date),currency_type_convert(cut),payment_type_convert(pt),amount,date"
hive --database bigdater -e "from stats_order_tmp1 insert into table stats_order_tmp2 select platform_convert('all'),date_convert(dt),currency_type_convert(cut),payment_type_convert(pt),sum(values) as amount,dt group by dt,cut,pt"
hive --database bigdater -e "from stats_order_tmp1 insert into table stats_order_tmp2 select platform_convert('all'),date_convert(dt),currency_type_convert('all'),payment_type_convert(pt),sum(values) as amount,dt group by dt,pt"
hive --database bigdater -e "from stats_order_tmp1 insert into table stats_order_tmp2 select platform_convert('all'),date_convert(dt),currency_type_convert(cut),payment_type_convert('all'),sum(values) as amount,dt group by dt,cut"
hive --database bigdater -e "from stats_order_tmp1 insert into table stats_order_tmp2 select platform_convert('all'),date_convert(dt),currency_type_convert('all'),payment_type_convert('all'),sum(values) as amount,dt group by dt"
hive --database bigdater -e "from stats_order_tmp1 insert into table stats_order_tmp2 select platform_convert(pl),date_convert(dt),currency_type_convert('all'),payment_type_convert(pt),sum(values) as amount,dt group by pl,dt,pt"
hive --database bigdater -e "from stats_order_tmp1 insert into table stats_order_tmp2 select platform_convert(pl),date_convert(dt),currency_type_convert('all'),payment_type_convert('all'),sum(values) as amount,dt group by pl,dt"
hive --database bigdater -e "from stats_order_tmp1 insert into table stats_order_tmp2 select platform_convert(pl),date_convert(dt),currency_type_convert(cut),payment_type_convert('all'),sum(values) as amount,dt group by pl,dt,cut"
sqoop export --connect jdbc:mysql://hh:3306/report --username hive --password hive --table stats_order --export-dir /hive/bigdater.db/stats_order_tmp2/* --input-fields-terminated-by "\\01" --update-mode allowinsert --update-key platform_dimension_id,date_dimension_id,currency_type_dimension_id,payment_type_dimension_id --columns platform_dimension_id,date_dimension_id,currency_type_dimension_id,payment_type_dimension_id,refund_amount,created


echo "process refund total amount"
hive --database bigdater -e "from (from (select order_info(oid,'pl') as pl,from_unixtime(cast(s_time/1000 as bigint),'yyyy-MM-dd') as date,order_info(oid,'cut') as cut,order_info(oid,'pt') as pt,oid,max(order_info(oid)) as amount from event_logs where en='e_cr' and pl is not null and s_time >= unix_timestamp('$startDate','yyyy-MM-dd')*1000 and s_time < unix_timestamp('$endDate','yyyy-MM-dd')*1000 group by order_info(oid,'pl'),from_unixtime(cast(s_time/1000 as bigint),'yyyy-MM-dd'),order_info(oid,'cut'),order_info(oid,'pt'),oid) as tmp select pl,date,cut,pt,sum(amount) as amount group by pl,date,cut,pt) as tmp2 insert overwrite table stats_order_tmp1 select pl,date,cut,pt,amount insert overwrite table stats_order_tmp2 select platform_convert(pl),date_convert(date),currency_type_convert(cut),payment_type_convert(pt),order_total_amount(platform_convert(pl),date_convert(date),currency_type_convert(cut),payment_type_convert(pt),cast(amount as int),'refund') as amount,date "
hive --database bigdater -e "from stats_order_tmp1 insert into table stats_order_tmp2 select platform_convert('all'),date_convert(dt),currency_type_convert(cut),payment_type_convert(pt),order_total_amount(platform_convert('all'),date_convert(dt),currency_type_convert(cut),payment_type_convert(pt),cast(sum(values) as int),'refund') as amount,dt group by dt,cut,pt"
hive --database bigdater -e "from stats_order_tmp1 insert into table stats_order_tmp2 select platform_convert('all'),date_convert(dt),currency_type_convert('all'),payment_type_convert(pt),order_total_amount(platform_convert('all'),date_convert(dt),currency_type_convert('all'),payment_type_convert(pt),cast(sum(values) as int),'refund') as amount,dt group by dt,pt"
hive --database bigdater -e "from stats_order_tmp1 insert into table stats_order_tmp2 select platform_convert('all'),date_convert(dt),currency_type_convert(cut),payment_type_convert('all'),order_total_amount(platform_convert('all'),date_convert(dt),currency_type_convert(cut),payment_type_convert('all'),cast(sum(values) as int),'refund') as amount,dt group by dt,cut"
hive --database bigdater -e "from stats_order_tmp1 insert into table stats_order_tmp2 select platform_convert('all'),date_convert(dt),currency_type_convert('all'),payment_type_convert('all'),order_total_amount(platform_convert('all'),date_convert(dt),currency_type_convert('all'),payment_type_convert('all'),cast(sum(values) as int),'refund') as amount,dt group by dt"
hive --database bigdater -e "from stats_order_tmp1 insert into table stats_order_tmp2 select platform_convert(pl),date_convert(dt),currency_type_convert('all'),payment_type_convert(pt),order_total_amount(platform_convert(pl),date_convert(dt),currency_type_convert('all'),payment_type_convert(pt),cast(sum(values) as int),'refund') as amount,dt group by pl,dt,pt"
hive --database bigdater -e "from stats_order_tmp1 insert into table stats_order_tmp2 select platform_convert(pl),date_convert(dt),currency_type_convert('all'),payment_type_convert('all'),order_total_amount(platform_convert(pl),date_convert(dt),currency_type_convert('all'),payment_type_convert('all'),cast(sum(values) as int),'refund') as amount,dt group by pl,dt"
hive --database bigdater -e "from stats_order_tmp1 insert into table stats_order_tmp2 select platform_convert(pl),date_convert(dt),currency_type_convert(cut),payment_type_convert('all'),order_total_amount(platform_convert(pl),date_convert(dt),currency_type_convert(cut),payment_type_convert('all'),cast(sum(values) as int),'refund') as amount,dt group by pl,dt,cut"
sqoop export --connect jdbc:mysql://hh:3306/report --username hive --password hive --table stats_order --export-dir /hive/bigdater.db/stats_order_tmp2/* --input-fields-terminated-by "\\01" --update-mode allowinsert --update-key platform_dimension_id,date_dimension_id,currency_type_dimension_id,payment_type_dimension_id --columns platform_dimension_id,date_dimension_id,currency_type_dimension_id,payment_type_dimension_id,total_refund_amount,created


echo "complete run the event job"