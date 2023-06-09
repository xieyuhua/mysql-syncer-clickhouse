package river

import (
	"context"
	"fmt"
	"regexp"
	"strings"
	"sync"

	"github.com/juju/errors"
	"github.com/siddontang/go-log/log"
	"github.com/zhaochuanyun/go-mysql-ck/clickhouse"
	"github.com/zhaochuanyun/go-mysql/canal"
	"database/sql"
)

// ErrRuleNotExist is the error if rule is not defined.
var ErrRuleNotExist = errors.New("rule is not exist")

var Db *sql.DB

// River is a pluggable service within Elasticsearch pulling data then indexing it into Elasticsearch.
// We use this definition here too, although it may not run within Elasticsearch.
// Maybe later I can implement a acutal river in Elasticsearch, but I must learn java. :-)
type River struct {
	c *Config

	canal *canal.Canal

	rules map[string]*Rule

	ctx    context.Context
	cancel context.CancelFunc

	wg sync.WaitGroup

	mysql *mysql.Client

	st *stat

	master *masterInfo

	syncCh chan interface{}
}

// NewRiver creates the River from config
func NewRiver(c *Config) (*River, error) {
	r := new(River)

	r.c = c
	r.rules = make(map[string]*Rule)
	r.syncCh = make(chan interface{}, 4096)
	r.ctx, r.cancel = context.WithCancel(context.Background())


	cfg := new(mysql.ClientConfig)
	cfg.Addr = r.c.SinkAddr
	cfg.User = r.c.SinkUser
	cfg.Password = r.c.SinkPassword
	cfg.Thread = r.c.Thread
	cfg.MaxConnect = r.c.MaxConnect
	cfg.MaxOpen = r.c.MaxOpen
	r.mysql = mysql.NewClient(cfg)
	
    //clickhouse  表结构
// 	connString := cfg.User + ":" + cfg.Password + "@tcp(" + cfg.Addr + ")/"
	connString := "tcp://"+ cfg.Addr+"?debug=false&username="+cfg.User+"&password="+cfg.Password
    var err error
	Db, err  = sql.Open("clickhouse", connString)
	if err != nil {
		return nil,errors.Trace(err)
	}
	
	if r.master, err = loadMasterInfo(c.DataDir); err != nil {
		return nil, errors.Trace(err)
	}

	if err = r.newCanal(); err != nil {
		return nil, errors.Trace(err)
	}

	if err = r.prepareRule(); err != nil {
		return nil, errors.Trace(err)
	}

	if err = r.prepareCanal(); err != nil {
		return nil, errors.Trace(err)
	}

	// We must use binlog full row image
	if err = r.canal.CheckBinlogRowImage("FULL"); err != nil {
		return nil, errors.Trace(err)
	}


	r.st = &stat{r: r}
	go r.st.Run(r.c.StatAddr)

	return r, nil
}

func (r *River) newCanal() error {
	cfg := canal.NewDefaultConfig()
	cfg.Addr = r.c.SourceAddr
	cfg.User = r.c.SourceUser
	cfg.Password = r.c.SourcePassword
	cfg.Charset = r.c.SourceCharset
	cfg.Flavor = r.c.Flavor

	cfg.ServerID = r.c.ServerID
	cfg.Dump.ExecutionPath = r.c.DumpExec
	cfg.Dump.DiscardErr = false

	for _, s := range r.c.Sources {
		for _, t := range s.Tables {
			cfg.IncludeTableRegex = append(cfg.IncludeTableRegex, s.Schema+"\\."+t)
		}
	}

	var err error
	r.canal, err = canal.NewCanal(cfg)
	return errors.Trace(err)
}

func (r *River) prepareCanal() error {
	var db string
	dbs := map[string]struct{}{}
	tables := make([]string, 0, len(r.rules))
	for _, rule := range r.rules {
		db = rule.SourceSchema
		dbs[rule.SourceSchema] = struct{}{}
		tables = append(tables, rule.SourceTable)
	}

	if len(dbs) == 1 {
		// one db, we can shrink using table
		r.canal.AddDumpTables(db, tables...)
	} else {
		// many dbs, can only assign databases to dump
		keys := make([]string, 0, len(dbs))
		for key := range dbs {
			keys = append(keys, key)
		}

		r.canal.AddDumpDatabases(keys...)
	}

	r.canal.SetEventHandler(&eventHandler{r})

	return nil
}


    
func (r *River) newRule(schema, table string) error {
	key := ruleKey(schema, table)

	if _, ok := r.rules[key]; ok {
		return errors.Errorf("duplicate source %s, %s defined in config", schema, table)
	}
	
	r.rules[key] = newDefaultRule(schema, table)
	return nil
}

func (r *River) updateRule(schema, table string) error {
	rule, ok := r.rules[ruleKey(schema, table)]
	if !ok {
		return ErrRuleNotExist
	}

	tableInfo, err := r.canal.GetTable(schema, table)
	if err != nil {
		return errors.Trace(err)
	}

	rule.TableInfo = tableInfo

	return nil
}

func (r *River) parseSource() (map[string][]string, error) {
	wildTables := make(map[string][]string, len(r.c.Sources))

	// first, check sources
	for _, s := range r.c.Sources {
		if !isValidTables(s.Tables) {
			return nil, errors.Errorf("wildcard * is not allowed for multiple tables")
		}

		for _, table := range s.Tables {
			if len(s.Schema) == 0 {
				return nil, errors.Errorf("empty schema not allowed for source")
			}

			if regexp.QuoteMeta(table) != table {
				if _, ok := wildTables[ruleKey(s.Schema, table)]; ok {
					return nil, errors.Errorf("duplicate wildcard table defined for %s.%s", s.Schema, table)
				}

				tables := []string{}

				sql := fmt.Sprintf(`SELECT table_name FROM information_schema.tables WHERE
					table_name RLIKE "%s" AND table_schema = "%s";`, buildTable(table), s.Schema)

				res, err := r.canal.Execute(sql)
				if err != nil {
					return nil, errors.Trace(err)
				}
//select COLUMN_NAME from information_schema.COLUMNS where table_name = 'szy_banner' and table_schema = 'azmbk_com_db';
				for i := 0; i < res.Resultset.RowNumber(); i++ {
					f, _ := res.GetString(i, 0)
					err := r.newRule(s.Schema, f)
					if err != nil {
						return nil, errors.Trace(err)
					}

					tables = append(tables, f)
				}

				wildTables[ruleKey(s.Schema, table)] = tables
			} else {
				err := r.newRule(s.Schema, table)
				if err != nil {
					return nil, errors.Trace(err)
				}
			}
		}
	}

	if len(r.rules) == 0 {
		return nil, errors.Errorf("no source data defined")
	}

	return wildTables, nil
}

func (r *River) prepareRule() error {
	wildtables, err := r.parseSource()
	if err != nil {
		return errors.Trace(err)
	}

	if r.c.Rules != nil {
	    
		// then, set custom mapping rule
		for _, rule := range r.c.Rules {
			if len(rule.SourceSchema) == 0 {
				return errors.Errorf("empty schema not allowed for rule")
			}

			if regexp.QuoteMeta(rule.SourceTable) != rule.SourceTable {
				//wildcard table
				tables, ok := wildtables[ruleKey(rule.SourceSchema, rule.SourceTable)]
				if !ok {
					return errors.Errorf("wildcard table for %s.%s is not defined in source", rule.SourceSchema, rule.SourceTable)
				}

				rule.prepare()

				for _, table := range tables {
					rr := r.rules[ruleKey(rule.SourceSchema, table)]
					rr.ID = rule.ID
					rr.FieldMapping = rule.FieldMapping
				}
			} else {
				key := ruleKey(rule.SourceSchema, rule.SourceTable)
				if _, ok := r.rules[key]; !ok {
					return errors.Errorf("rule %s, %s not defined in source", rule.SourceSchema, rule.SourceTable)
				}
				rule.prepare()
				r.rules[key] = rule
			}
		}
	}

	rules := make(map[string]*Rule)
	for key, rule := range r.rules {
		if rule.TableInfo, err = r.canal.GetTable(rule.SourceSchema, rule.SourceTable); err != nil {
			return errors.Trace(err)
		}

		if len(rule.TableInfo.PKColumns) == 0 {
			if !r.c.SkipNoPkTable {
				return errors.Errorf("%s.%s must have a PK for a column", rule.SourceSchema, rule.SourceTable)
			}

			log.Errorf("ignored table without a primary key: %s\n", rule.TableInfo.Name)
		} else {
			rules[key] = rule
		}
		
		//取存在的全部字段  filter 不为空  SELECT name columnName,type FROM  system.columns 
		if len(rules[key].Filter) == 0 {
        	sql := fmt.Sprintf(`SELECT name columnName FROM  system.columns  WHERE table = '%s' AND database = '%s';`, buildTable(rule.SinkTable), rule.SinkSchema)
        	rows, err := Db.Query(sql)
        	defer rows.Close()
        	if err != nil {
        		return errors.Trace(err)
        	}
            rulefilter := []string{}
            for rows.Next() {
                var f string
                err = rows.Scan(&f)
            	if err != nil {
            		return errors.Trace(err)
            	}
            	rulefilter = append(rulefilter, f)
            }
            rule.Filter = rulefilter
            rules[key] = rule
		}
	}
	r.rules = rules

	return nil
}

func ruleKey(schema string, table string) string {
	return strings.ToLower(fmt.Sprintf("%s:%s", schema, table))
}

// Run syncs the data from MySQL and inserts to ES.
func (r *River) Run() error {
	r.wg.Add(1)
	go r.syncLoop()

	pos := r.master.Position()
	if err := r.canal.RunFrom(pos); err != nil {
		log.Errorf("start canal err %v", err)
		return errors.Trace(err)
	}

	return nil
}

// Ctx returns the internal context for outside use.
func (r *River) Ctx() context.Context {
	return r.ctx
}

// Close closes the River
func (r *River) Close() {
	log.Infof("closing river")

	r.cancel()

	r.canal.Close()

	r.master.Close()

	r.wg.Wait()
}

func isValidTables(tables []string) bool {
	if len(tables) > 1 {
		for _, table := range tables {
			if table == "*" {
				return false
			}
		}
	}
	return true
}

func buildTable(table string) string {
	if table == "*" {
		return "." + table
	}
	return table
}
