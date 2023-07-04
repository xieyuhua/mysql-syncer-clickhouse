package mysql

import (
	"fmt"
	"flag"
	"time"
	"sort"
	"sync"
	"strconv"
	"github.com/siddontang/go-log/log"
    "github.com/pingcap/errors"
    "database/sql"
    "github.com/ClickHouse/clickhouse-go"
)

type Client struct {
	conn *sql.DB
	thread   int
}

// ClientConfig is the configuration for the client.
type ClientConfig struct {
	Addr     string
	User     string
	Password string
	Schema   string
	Table    string
	Thread   int
	MaxConnect  int
	MaxOpen     int
}

// NewClient creates the Cient with configuration.
func NewClient(conf *ClientConfig) *Client {
	c := new(Client)
	connString := "tcp://"+ conf.Addr+"?debug=false&username="+conf.User+"&password="+conf.Password
	c.conn, _ = sql.Open("clickhouse", connString)
	//最大打开的连接数
	c.conn.SetMaxOpenConns(conf.MaxOpen)
	//最大连接数
	c.conn.SetMaxIdleConns(conf.MaxConnect)
	
    if err := c.conn.Ping(); err != nil {
        if exception, ok := err.(*clickhouse.Exception); ok {
            fmt.Printf("[%d] %s \n%s\n", exception.Code, exception.Message, exception.StackTrace)
        } else {
            fmt.Println(err)
        }
    }
	
	c.thread = conf.Thread
	return c
}

const (
	ActionInsert = "insert"
	ActionUpdate = "update"
	ActionDelete = "delete"
)

// BulkRequest is used to send multi request in batch.
type BulkRequest struct {
	Action string
	Schema string
	Table  string
	Data   map[string]interface{}

	PkName  string
	PkValue interface{}
}


//保证前段走完
var m = make(chan bool, 1)

// Bulk sends the bulk request.
func (c *Client) Bulk(reqs []*BulkRequest)  (interface{} ,error) {
    
    // 阻塞等待     全量和增量  判断   一个channel 里面多个客户端处理
    m <- true
    
    //线程池
    var chans = make(chan bool, c.thread)
    
    //任务执行等待完
    var wg sync.WaitGroup 
    
    //是否执行错误
    var ddi error
    ddi = nil
    
    insertdata := make(map[string][]*BulkRequest)
    for _, req := range reqs {
    	switch req.Action {
        	case ActionInsert:
        	    insertdata[req.Table] = append(insertdata[req.Table], req)
        	    break;
        	default:  
        	    chans <- true
    	    //占位
            go func(req *BulkRequest,c *Client, wg *sync.WaitGroup) {
                wg.Add(1)
                txs, _   := c.conn.Begin()
        		if err := req.bulk(c, txs); err != nil {
        		    <-chans
        		    wg.Done()
        		    ddi = errors.Trace(err)
        		}
                if err := txs.Commit(); err != nil {
                    wg.Done()
                    <-chans
                    ddi =  errors.Trace(err)
                }
        		wg.Done()
        		<-chans
            }(req,c,&wg)
    	}
    }
    
    // 等待所有的任务完成
    wg.Wait()
    if ddi != nil {
        return nil, ddi
    }
    //放入
    <- m
    
    //批量插入数据
    if len(insertdata)>0 {
        for table, _ := range insertdata {
            
            if len(insertdata[table])>0 {
                //预处理   单表
                vreq := insertdata[table][0]
                // 将Map的键放入切片中
                keys := make([]string, 0, len(vreq.Data))
                for kv := range vreq.Data {
                    keys = append(keys, kv)
                }
                // 对切片进行排序
                sort.Strings(keys)
        		fields  := ""
        		values  := ""
                for _, kk := range keys {
        			if fields == ""{
        				fields = kk
        				values =  "?"
        			}else{
        				fields += ","+kk
        				values += ",?"
        			}
                }
        		sql := "INSERT INTO "+vreq.Schema + "."+vreq.Table+" ("+fields+") VALUES ("+values+")"
            	log.Infof(sql)
                tx, _   := c.conn.Begin()
                stmtIns, err := tx.Prepare(sql)
            	if err != nil {
                    log.Errorf("Execute Prepare! --> %v", err)
                    return  nil,errors.Trace(err)
            	}
            	defer stmtIns.Close()

                //新增一组数据
                for _, vv := range insertdata[table] {
                    value := make([]interface{}, 0, len(vv.Data))
                    // 将Map的键放入切片中
                    keysvv := make([]string, 0, len(vv.Data))
                    for k := range vv.Data {
                        keysvv = append(keysvv, k)
                    }
                    // 对切片进行排序
                    sort.Strings(keysvv)
                    
                    
                    //新增一条数据
                    for _, k := range keysvv {
                        v := vv.Data[k]
            		    switch v.(type) {
                    	    case string:
                    	        v = v.(string)
                				if v.(string) == "0000-00-00" {
                					fmt.Println("Date",k,  v.(string))
                					break
                				}
                				_, err := time.Parse("2006-01-02", v.(string))
                				if err == nil {
                					fmt.Println("Date",k,  v.(string))
                				} else {
                					fmt.Println("string",k,  v.(string))
                				}
                            	   
                        		break
                        	case int,uint,int8,uint8,int16,uint16,int32,uint32,int64,uint64:
                        	    fmt.Println("int",k, v)
                        	    
                                vs := fmt.Sprintf("%d", v)
                        	    v = StrToInt64(vs)
                        	   // value = append(value, v.(int)) 
                        	break
                        	case float64,float32:
                                fmt.Println("float",k, v)
                        	    v = v.(float64)
                        	   // value = append(value, v.(float64)) 
                        	break
                        	default:
                                v = fmt.Sprintf("%v",k, v)
                    	}
                    	// int,uint,int8,uint8,int16,uint16,int32,uint32,int64,uint64,float32,float64
                    	value = append(value, v) 
                    }
                	if len(value) == 0 {
                		break;
                	}
                	log.Errorf("Execute --> %v", value)
                	
                	_, err = stmtIns.Exec(value...)
                	if err != nil {
                        log.Errorf("Execute Error! --> %v", err)
                        return  nil, errors.Trace(err)
                	}
                }
                //提交
                if err := tx.Commit(); err != nil {
                    return nil, errors.Trace(err)
                }
            }
           
            
        }
    }
    
    return nil,nil
}
func StrToInt64(tmpStr string) int{
    tmpInt,_ := strconv.Atoi(tmpStr)
    return tmpInt
}

//是否过滤删除操作
var FilterDelete = flag.Bool("delete", true, "Ignore the delete operation")

//是否过滤更新操作
var FilterUpdate = flag.Bool("update", true, "Ignore the update operation")

//执行更新
func (r *BulkRequest) bulk(c *Client,tx *sql.Tx) error {
	
	switch r.Action {
	case ActionDelete:
		// for delete
		
	    if *FilterDelete {
	        break;
	    }
		
		sql := "ALTER TABLE " + r.Schema + "." + r.Table+ " DELETE WHERE " + r.PkName + " = ?"
		log.Infof("Execute success --> %v", sql)
        stmtIns, err := tx.Prepare(sql)
    	if err != nil {
            log.Errorf("Prepare Error! --> %v", err)
            return  errors.Trace(err)
    	}
    	defer stmtIns.Close()
    // 	fmt.Println(r.PkValue)
    	_, err = stmtIns.Exec(r.PkValue)
    	if err != nil {
            log.Errorf("Execute Error! --> %v", err)
            return  errors.Trace(err)
    	}
    	
	case ActionUpdate:
	
	    if *FilterUpdate {
	        break;
	    }
	
// 	INSERT INTO test_unique_key ( `id`, `name`, `term_id`, `class_id`, `course_id` ) VALUES( '17012', 'cate', '172012', '170', '1711' ) ON DUPLICATE KEY UPDATE name = '张三1',course_id=32
		keys := make([]string, 0, len(r.Data))
		values := make([]interface{}, 0, len(r.Data))
		for k, v := range r.Data {
		    switch v.(type) {
        	    case string:
        	        v = v.(string)
    				if v.(string) == "0000-00-00" {
    					fmt.Println("UPDATE Date",k,  v.(string))
    					break
    				}
    				_, err := time.Parse("2006-01-02", v.(string))
    				if err == nil {
    					fmt.Println("UPDATE Date",k,  v.(string))
    				} else {
    					fmt.Println("UPDATE string",k,  v.(string))
    				}
            		break
            	case int,uint,int8,uint8,int16,uint16,int32,uint32,int64,uint64:
            	    fmt.Println("UPDATE int",k, v)
            	    
                    vs := fmt.Sprintf("%d", v)
            	    v = StrToInt64(vs)
            	   // value = append(value, v.(int)) 
            	break
            	case float64,float32:
                    fmt.Println("UPDATE float", k,v)
            	    v = v.(float64)
            	   // value = append(value, v.(float64)) 
            	break
            	default:
                    v = fmt.Sprintf("UPDATE %v",k, v)
        	}
			keys   = append(keys, k)
			values = append(values, v)
		}
    	if len(keys) == 0 {
    		break;
    	}
    	sql := " ALTER TABLE " + r.Schema + "." + r.Table + " UPDATE " + keys[0] + " = ? "
		for _, v := range keys[1:] {
			sql += ", " + v + " = ?"
		}
		sql += " WHERE " + r.PkName + " = ?"
		values = append(values, r.PkValue)
		log.Infof("Execute success --> %v", sql)
// 		log.Infof("Execute success --> %v", values)
        stmtIns, err := tx.Prepare(sql)
    	if err != nil {
    	    log.Infof("Execute err")
            // log.Errorf("Execute Error! --> %v", err)
            return  errors.Trace(err)
    	}
    	defer stmtIns.Close()
    	_, err = stmtIns.Exec(values...)
    	if err != nil {
            log.Errorf("Execute Error! --> %v", err)
            return  errors.Trace(err)
    	}
		
	    default:
	    //已经批量处理过了
	
	}
	
	return nil
}


func trans(v interface{}) string {
	if v == nil {
		return "null"
	}
	switch v.(type) {
	case string:
        return fmt.Sprintf("\"%v\"", v)
	default:
		return fmt.Sprintf("%v", v)
	}
}