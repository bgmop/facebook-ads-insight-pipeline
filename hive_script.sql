-- raw
drop table if exists facebook_raw;

create external table if not exists facebook_raw(
account_id string, 
account_name string, 
action_values ARRAY<STRUCT<28d_click:double, 28d_view:double, 7d_view:double, value:double, 1d_click:double, 1d_view:double, action_type:string, 7d_click:double>>, 
actions ARRAY<STRUCT<28d_click:int, 28d_view:int, 7d_view:int, 1d_view:int, value:int, 1d_click:int, action_type:string, 7d_click:int>>, 
ad_id string, 
ad_name string, 
adset_id string, 
adset_name string, 
app_store_clicks string, 
buying_type string, 
call_to_action_clicks int, 
campaign_id string, 
campaign_name string, 
clicks string, 
date_start string,
date_stop string, 
deeplink_clicks string, 
frequency double, 
impressions string, 
impression_device string, 
inline_link_clicks string, 
inline_post_engagement string, 
newsfeed_avg_position double, 
newsfeed_clicks string, 
newsfeed_impressions string, 
objective string, 
placement string, 
place_page_name string, 
reach string, 
relevance_score string, 
social_clicks string, 
social_impressions string, 
social_reach string, 
social_spend double, 
spend double, 
total_action_value double, 
total_actions string, 
total_unique_actions string, 
unique_actions ARRAY<STRUCT<28d_click:int, 28d_view:int, 7d_view:int, 1d_view:int, value:int, 1d_click:int, action_type:string, 7d_click:int>>, 
unique_clicks string, 
unique_ctr double, 
unique_impressions string, 
unique_inline_link_clicks string, 
unique_inline_link_clicks_ctr double, 
unique_link_clicks_ctr double, 
unique_social_clicks string, 
unique_social_impressions string, 
website_clicks string
)
PARTITIONED BY (snapshot string, dt string)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
LOCATION 'your_s3_raw_path';

MSCK REPAIR TABLE facebook_raw;


-- history
drop table if exists facebook_history;

create external table if not exists facebook_history(
account_id string, 
account_name string, 
action_values ARRAY<STRUCT<28d_click:double, 28d_view:double, 7d_view:double, value:double, 1d_click:double, 1d_view:double, action_type:string, 7d_click:double>>, 
actions ARRAY<STRUCT<28d_click:int, 28d_view:int, 7d_view:int, 1d_view:int, value:int, 1d_click:int, action_type:string, 7d_click:int>>, 
ad_id string, 
ad_name string, 
adset_id string, 
adset_name string, 
app_store_clicks string, 
buying_type string, 
call_to_action_clicks int, 
campaign_id string, 
campaign_name string, 
clicks string, 
date_start string, 
date_stop string, 
deeplink_clicks string, 
frequency double, 
impressions string, 
impression_device string, 
inline_link_clicks string, 
inline_post_engagement string, 
newsfeed_avg_position double, 
newsfeed_clicks string, 
newsfeed_impressions string, 
objective string, 
placement string, 
place_page_name string, 
reach string, 
relevance_score string, 


set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
set mapred.output.compress=true;
set hive.exec.compress.output=true;
set mapred.output.compression.codec=org.apache.hadoop.io.compress.GzipCodec;
set io.compression.codecs=org.apache.hadoop.io.compress.GzipCodec;


-- current
drop table if exists facebook_current;

create external table if not exists facebook_current(
account_id string, 
account_name string, 
action_values ARRAY<STRUCT<28d_click:double, 28d_view:double, 7d_view:double, value:double, 1d_click:double, 1d_view:double, action_type:string, 7d_click:double>>, 
actions ARRAY<STRUCT<28d_click:int, 28d_view:int, 7d_view:int, 1d_view:int, value:int, 1d_click:int, action_type:string, 7d_click:int>>, 
ad_id string, 
ad_name string, 
adset_id string, 
adset_name string, 
app_store_clicks string, 
buying_type string, 
call_to_action_clicks int, 
campaign_id string, 
campaign_name string, 
clicks string, 
date_start string,
date_stop string, 
deeplink_clicks string, 
frequency double, 
impressions string, 
impression_device string, 
inline_link_clicks string, 
inline_post_engagement string, 
newsfeed_avg_position double, 
newsfeed_clicks string, 
newsfeed_impressions string, 
objective string, 
placement string, 
place_page_name string, 
reach string, 
relevance_score string, 
social_clicks string, 
social_impressions string, 
social_reach string, 
social_spend double, 
spend double, 
total_action_value double, 
total_actions string, 
total_unique_actions string, 
unique_actions ARRAY<STRUCT<28d_click:int, 28d_view:int, 7d_view:int, 1d_view:int, value:int, 1d_click:int, action_type:string, 7d_click:int>>, 
unique_clicks string, 
unique_ctr double, 
unique_impressions string, 
unique_inline_link_clicks string, 
unique_inline_link_clicks_ctr double, 
unique_link_clicks_ctr double, 
unique_social_clicks string, 
unique_social_impressions string, 
website_clicks string
)
PARTITIONED BY (dt string)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
LOCATION 'your_s3_final_path';


set hive.support.quoted.identifiers=none;

insert overwrite table facebook_current
partition (dt)
select `(snapshot)?+.+` from facebook_raw where snapshot='${snapshot}';
