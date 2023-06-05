package mysql

import (
	"fmt"
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
    
	//限制多线程执行
	for _, req := range reqs {
	    chans <- true
	    //占位
        go func(req *BulkRequest,c *Client, wg *sync.WaitGroup) {
            wg.Add(1)
            // log.Infof("************ success --> %v", i)
            // time.Sleep(4 * time.Second) 
            tx, _   := c.conn.Begin()
    		if err := req.bulk(c, tx); err != nil {
    		    <-chans
    		    wg.Done()
    		    ddi = errors.Trace(err)
    		}
            if err := tx.Commit(); err != nil {
                ddi =  errors.Trace(err)
            }
    		wg.Done()
    		<-chans
        }(req,c,&wg)
	}
    // 等待所有的任务完成
    wg.Wait()
    if ddi != nil {
        return nil, ddi
    }
    
    //放新的一组进入
    <-m
    
	return nil, nil
}

func StrToInt64(tmpStr string) int{
    tmpInt,_ := strconv.Atoi(tmpStr)
    return tmpInt
}

//执行更新
func (r *BulkRequest) bulk(c *Client,tx *sql.Tx) error {
	
	switch r.Action {
	case ActionDelete:
		// for delete
		sql := "ALTER TABLE " + r.Schema + "." + r.Table+ " DELETE WHERE " + r.PkName + " = ?"
		log.Infof("Execute success --> %v", sql)
		
        stmtIns, err := tx.Prepare(sql)
    	if err != nil {
    	    log.Infof("Prepare err")
            // log.Errorf("Execute Error! --> %v", err)
            return  errors.Trace(err)
    	}
    	defer stmtIns.Close()
    // 	fmt.Println(r.PkValue)
    	_, err = stmtIns.Exec(r.PkValue)
    	if err != nil {
    	    log.Infof("Execute err")
            // log.Errorf("Execute Error! --> %v", err)
            return  errors.Trace(err)
    	}
    	
    	
	case ActionUpdate:
// 	INSERT INTO test_unique_key ( `id`, `name`, `term_id`, `class_id`, `course_id` ) VALUES( '17012', 'cate', '172012', '170', '1711' ) ON DUPLICATE KEY UPDATE name = '张三1',course_id=32
		keys := make([]string, 0, len(r.Data))
		values := make([]interface{}, 0, len(r.Data))
		for k, v := range r.Data {
		    
		    switch v.(type) {
        	    case string:
        	        fmt.Println("string",k,  v.(string))
                    // tmpDecimal, _ := decimal.NewFromString(v.(string))
                    // v = tmpDecimal.String()
            	   // value = append(value, v) 
            		break
            	case int:
            	    fmt.Println("int", v.(int))
            	    
                    vs := fmt.Sprintf("%d", v)
            	    v = StrToInt64(vs)
            	   // value = append(value, v.(int)) 
            	break
            	case uint:
            	    fmt.Println("uint", v.(uint))
            	    
                    vs := fmt.Sprintf("%d", v)
            	    v = StrToInt64(vs)
            	   // value = append(value, v.(uint)) 
            	break
            	case int8:
            	    fmt.Println("int8", v.(int8))
                    vs := fmt.Sprintf("%d", v)
            	    v = StrToInt64(vs)
            	   // value = append(value, v.(int8)) 
            	break
            	case uint8:
            	    fmt.Println("uint8", v.(uint8))
            	    
                    vs := fmt.Sprintf("%d", v)
            	    v = StrToInt64(vs)
            	   // value = append(value, v.(uint8)) 
            	break
            	case int16:
            	    fmt.Println("int16", v.(int16))
                    vs := fmt.Sprintf("%d", v)
            	    v = StrToInt64(vs)
            	   // value = append(value, v.(int16)) 
            	break
            	case uint16:
            	    fmt.Println("uint16", v.(uint16))
            	    
                    vs := fmt.Sprintf("%d", v)
            	    v = StrToInt64(vs)
            	   // value = append(value, v.(uint16)) 
            	break
            	case int32:
            	    fmt.Println("int32", v.(int32))
            	    
                    vs := fmt.Sprintf("%d", v)
            	    v = StrToInt64(vs)
            	   // value = append(value, v.(int32)) 
            	break
            	case uint32:
            	    fmt.Println("uint32", v.(uint32))
            	    
                    vs := fmt.Sprintf("%d", v)
            	    v = StrToInt64(vs)
            	   // value = append(value, v.(uint32)) 
            	break
            	case int64:
            	    fmt.Println("int64", v.(int64))
            	    
                    vs := fmt.Sprintf("%d", v)
            	    v = StrToInt64(vs)
            	   // value = append(value, v.(int64)) 
            	break
            	case uint64:
            	    fmt.Println("uint64", v.(uint64))
            	    
                    vs := fmt.Sprintf("%d", v)
            	    v = StrToInt64(vs)
            	    
            	   // value = append(value, v.(uint64)) 
            	break
            	case float64:
                    fmt.Println("float64 to int", v.(float64))
                    
                    vs := fmt.Sprintf("%v", v)
            	    v = StrToInt64(vs)
            	    
            	   // value = append(value, v.(float64)) 
            	break
            	case float32:
                    fmt.Println("float32 to int", v.(float32))
                    
                    vs := fmt.Sprintf("%d", v)
                    v = StrToInt64(vs)
            	   // value = append(value, v.(float32)) 
            	break
            	default:
                    v = fmt.Sprintf("%v", v)
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
    	    log.Infof("Execute err")
            // log.Errorf("Execute Error! --> %v", err)
            return  errors.Trace(err)
    	}
		
	default:
		// for insert
		value := make([]interface{}, 0, len(r.Data))
		
		fields  := ""
		values  := ""
		
		for k, v := range r.Data {
		    switch v.(type) {
        	    case string:
        	        fmt.Println("string",k,  v.(string))
                    // tmpDecimal, _ := decimal.NewFromString(v.(string))
                    // v = tmpDecimal.String()
            	   // value = append(value, v) 
            		break
            	case int:
            	    fmt.Println("int", v.(int))
            	    
                    vs := fmt.Sprintf("%d", v)
            	    v = StrToInt64(vs)
            	   // value = append(value, v.(int)) 
            	break
            	case uint:
            	    fmt.Println("uint", v.(uint))
            	    
                    vs := fmt.Sprintf("%d", v)
            	    v = StrToInt64(vs)
            	   // value = append(value, v.(uint)) 
            	break
            	case int8:
            	    fmt.Println("int8", v.(int8))
                    vs := fmt.Sprintf("%d", v)
            	    v = StrToInt64(vs)
            	   // value = append(value, v.(int8)) 
            	break
            	case uint8:
            	    fmt.Println("uint8", v.(uint8))
            	    
                    vs := fmt.Sprintf("%d", v)
            	    v = StrToInt64(vs)
            	   // value = append(value, v.(uint8)) 
            	break
            	case int16:
            	    fmt.Println("int16", v.(int16))
                    vs := fmt.Sprintf("%d", v)
            	    v = StrToInt64(vs)
            	   // value = append(value, v.(int16)) 
            	break
            	case uint16:
            	    fmt.Println("uint16", v.(uint16))
            	    
                    vs := fmt.Sprintf("%d", v)
            	    v = StrToInt64(vs)
            	   // value = append(value, v.(uint16)) 
            	break
            	case int32:
            	    fmt.Println("int32", v.(int32))
            	    
                    vs := fmt.Sprintf("%d", v)
            	    v = StrToInt64(vs)
            	   // value = append(value, v.(int32)) 
            	break
            	case uint32:
            	    fmt.Println("uint32", v.(uint32))
            	    
                    vs := fmt.Sprintf("%d", v)
            	    v = StrToInt64(vs)
            	   // value = append(value, v.(uint32)) 
            	break
            	case int64:
            	    fmt.Println("int64", v.(int64))
            	    
                    vs := fmt.Sprintf("%d", v)
            	    v = StrToInt64(vs)
            	   // value = append(value, v.(int64)) 
            	break
            	case uint64:
            	    fmt.Println("uint64", v.(uint64))
            	    
                    vs := fmt.Sprintf("%d", v)
            	    v = StrToInt64(vs)
            	    
            	   // value = append(value, v.(uint64)) 
            	break
            	case float64:
                    fmt.Println("float64 to int", v.(float64))
                    
                    vs := fmt.Sprintf("%v", v)
            	    v = StrToInt64(vs)
            	    
            	   // value = append(value, v.(float64)) 
            	break
            	case float32:
                    fmt.Println("float32 to int", v.(float32))
                    
                    vs := fmt.Sprintf("%d", v)
                    v = StrToInt64(vs)
            	   // value = append(value, v.(float32)) 
            	break
            	default:
                    v = fmt.Sprintf("%v", v)
        	}
        	
        	value = append(value, v) 
        	
			if fields == ""{
				fields = k
				values =  "?"
			}else{
				fields += ","+k
				values += ",?"
			}
		}
    	if len(value) == 0 {
    		break;
    	}
    	
		sql := "INSERT INTO "+r.Schema + "."+r.Table+" ("+fields+") VALUES ("+values+")"
        //执行
		log.Infof("Execute success  --> %v", sql)
        // log.Infof("Execute value  --> %v", value)
		

        stmtIns, err := tx.Prepare(sql)
    	if err != nil {
    	    log.Infof("Execute err")
            // log.Errorf("Execute Error! --> %v", err)
            return  errors.Trace(err)
    	}
    	defer stmtIns.Close()
    	_, err = stmtIns.Exec(value...)
    	if err != nil {
    	    log.Infof("Execute err")
            // log.Errorf("Execute Error! --> %v", err)
            return  errors.Trace(err)
    	}
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
