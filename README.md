# mysql-syncer-clickhouse

监听binlog，将源mysql数据库的表实时同步到目标clickhouse库中

合理配置bulk_size、thread 详细配置见etc/river.toml

```
 CREATE TABLE adv (
  adv_id Int64,
  site_id Int64,
  ap_id Int64,
  adv_title String,
  adv_url String,
  adv_image String,
  slide_sort Int64,
  price Int64,
  background String,
  state Int64
) ENGINE = MergeTree() ORDER BY (adv_id);



# MySQL master address, user and password
source_addr = "127.0.0.1:3306"
source_user = "root"
source_pass = "ef08ef776ce21a44"
source_charset = "utf8"

# clickhouse slave address, user and password
sink_addr = "127.0.0.1:9000"
sink_user = "root"
sink_pass = "ef08ef776ce21a44"
sink_charset = "utf8"

# The client connection pool
thread      = 10

# Max Connections
max_connect = 20

# Maximum number of open
max_open    = 30

# Path to store data, like master.info, if not set or empty,
# we must use this to support breakpoint resume syncing. 
# TODO: support other storage, like etcd. 
data_dir = "./log"

# Inner Http status address    /stat    /debug/pprof/
stat_addr = "0.0.0.0:12800"

# pseudo server id like a slave 
server_id = 10041

# mysql or mariadb
flavor = "mysql"

# mysqldump execution path
# if not set or empty, ignore mysqldump.
mysqldump = "mysqldump"

# if we have no privilege to use mysqldump with --master-data,
# we must skip it.
#skip_master_data = false

# minimal items to be inserted in one bulk
bulk_size = 128

# force flush the pending requests if we don't have enough items >= bulk_size
flush_bulk_time = "1ms"

# Ignore table without primary key
skip_no_pk_table = false

# MySQL data source
[[source]]
schema = "niushop"
tables = ["notice", "addon", "adv"]

[[rule]]
source_schema = "niushop"
source_table = "notice"
sink_schema = "mysqlss"
sink_table = "notice"

# Field synchronous set, the default synchronization target table field already
filter = ["id", "site_id","title"]

[[rule]]
source_schema = "niushop"
source_table = "addon"
sink_schema = "mysqlss"
sink_table = "addon"

[[rule]]
source_schema = "niushop"
source_table = "adv"
sink_schema = "mysqlss"
sink_table = "adv"

```