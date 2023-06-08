# mysql-syncer-clickhouse

监听binlog，将源mysql数据库的表实时同步到目标clickhouse库中

合理配置bulk_size、thread 详细配置见etc/river.toml

```
Usage of /tmp/go-build3489104203/b001/exe/main:
  -config string
    	mysqlsync config file (default "./etc/river.toml")
  -delete
    	Ignore the delete operation (default true)
  -exec string
    	mysqldump execution path
  -flavor string
    	flavor: mysql or mariadb (default "mysql")
  -log_level string
    	log level (default "info")
  -thread int
    	The client connect thread num (default 1)
  -update
    	Ignore the update operation (default true)



 CREATE TABLE adv (
  adv_id Int64,
  site_id Int64,
  ap_id Int64,
  adv_title String,
  adv_url String,
  adv_image String,
  slide_sort Int64,
  price Nullable(Decimal(10, 2)),
  background String,
  state Int64,
  add_time DateTime
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




sql = "CREATE TABLE IF NOT EXISTS `" + This.GetSchemaName(data.SchemaName) + "`.`" + This.GetFieldName(data.TableName) + "` ("
ckField = make([]fieldStruct, 0)
var val = ""
//组装
var addCkField = func(ckFieldName,ckType string) {
	if val == "" {
		val = "`"+ckFieldName +"` " +ckType
	} else {
		val += ",`" + ckFieldName +"` "+ ckType
	}
	return
}
priArr := make([]string, 0)
var toCkType string
//数据字段循环
for _, priK := range data.Pri {
	fileName0 := This.GetFieldName(priK)
	toCkType = TransferToCkTypeByColumnType(data.Rows[0][priK], true)
	addCkField(fileName0,toCkType)
}
sql += val + ") ENGINE = ReplacingMergeTree ORDER BY (" + strings.Join(priArr, ",") + ")"



func  TransferToCkTypeByColumnType(columnType string, nullable bool) (toType string) {
	toType = "String"
	switch columnType {
	case "uint64", "Nullable(uint64)":
		toType = "UInt64"
	case "int64", "Nullable(int64)":
		toType = "Int64"
	case "uint32", "Nullable(uint32)", "uint24", "Nullable(uint24)":
		toType = "UInt32"
	case "int32", "Nullable(int32)", "int24", "Nullable(int24)":
		toType = "Int32"
	case "uint16", "Nullable(uint16)":
		toType = "UInt16"
	case "int16", "Nullable(int16)", "year(4)", "Nullable(year(4))", "year(2)", "Nullable(year(2))":
		toType = "Int16"
	case "uint8", "Nullable(uint8)":
		toType = "UInt8"
	case "int8", "Nullable(int8)", "bool", "Nullable(bool)":
		toType = "Int8"
	case "float", "Nullable(float)":
		toType = "Float32"
	case "double", "Nullable(double)":
		toType = "Float64"
	case "date", "Nullable(date)":
		toType = "Date"
	default:
		if strings.Index(columnType, "double") >= 0 {
			toType = "Float64"
			break
		}
		if strings.Index(columnType, "float") >= 0 {
			toType = "Float32"
			break
		}
		if strings.Index(columnType, "bit") >= 0 {
			toType = "Int64"
			break
		}
		if strings.Index(columnType, "timestamp") >= 0 {
			i := strings.Index(columnType, "timestamp(")
			if i >= 0 {
				// 0000-00-00 00:00:00.000000
				// 由于 ck DateTime64 在19.19 某个小版本开始支持，考滤分支过细的问题，我们统一以20版本开始支持 DateTime64 转换
				if This.ckVersion >= 2000000000 || This.ckVersion == 0 {
					nsecNum := strings.Split(columnType[i+10:], ")")[0]
					toType = "DateTime64(" + nsecNum + ")"
				} else {
					toType = "String"
				}
				break
			}
			toType = "DateTime"
			break
		}
		if strings.Index(columnType, "datetime") >= 0 {
			i := strings.Index(columnType, "datetime(")
			if i >= 0 {
				if This.ckVersion >= 2000000000 || This.ckVersion == 0 {
					nsecNum := strings.Split(columnType[i+9:], ")")[0]
					toType = "DateTime64(" + nsecNum + ")"
				} else {
					toType = "String"
				}
				break
			}
			toType = "DateTime"
			break
		}
		if strings.Index(columnType, "decimal") >= 0 {
			i := strings.Index(columnType, "decimal(")
			if i < 0 {
				toType = "Decimal(18,2)"
				break
			}
			dataTypeParam := strings.Split(columnType[i+8:], ")")[0]
			dataTypeParam = strings.Trim(dataTypeParam, " ")
			if dataTypeParam == "" {
				toType = "Decimal(18,2)"
				break
			}
			p := strings.Split(dataTypeParam, ",")
			M, _ := strconv.Atoi(strings.Trim(p[0], " "))
			var D int
			if len(p) == 2 {
				D, _ = strconv.Atoi(strings.Trim(p[1], " "))
			}
			// M,D.   M > 18 就属于 Decimal128 , M > 39 就属于 Decimal256  ，但是当前你 go ck 驱动只支持 Decimal64
			if M > 18 {
				toType = "String"
			} else {
				toType = fmt.Sprintf("Decimal(%d,%d)", M, D)
			}
			break
		}
	}
	if nullable {
		if strings.Index(columnType, "Nullable") >= 0 {
			toType = "Nullable(" + toType + ")"
		}
	}
	return
}
```