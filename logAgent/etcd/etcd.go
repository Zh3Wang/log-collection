package etcd

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/coreos/etcd/clientv3"
	"log"
	"time"
)

var Cli *clientv3.Client

type LogEntry struct {
	FilePath string `json:"filepath"`
	FileName string `json:"filename"`
	Topic    string `json:"topic"`
}

func InitEtcd(addr []interface{}) {
	//把 []interface{} 转成 []string
	var addrString []string
	for _, v := range addr {
		addrString = append(addrString, v.(string))
	}
	//连接
	var err error
	Cli, err = clientv3.New(clientv3.Config{
		Endpoints:   addrString,
		DialTimeout: time.Second * 3,
	})
	if err != nil {
		log.Fatal("连接etcd失败：", err)
	}
	log.Println("连接etcd成功~~")
}

func GetLogConf(key string) (LogEntry []*LogEntry) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	resp, err := Cli.Get(ctx, key)
	cancel()
	if err != nil {
		log.Fatal(fmt.Sprintf("etcd get 操作失败：%v \n", err))
	}
	for _, v := range resp.Kvs {
		log.Println(string(v.Value))
		_ = json.Unmarshal(v.Value, &LogEntry)
	}
	//now := time.Now()
	//for k,_ := range LogEntry{
	//	date := now.Format("2006-01-02")
	//	dates := strings.Split(date,"-")
	//	year, month, day := dates[0], dates[1], dates[2]
	//	LogEntry[k].FileName = fmt.Sprintf(LogEntry[k].FileName,day)
	//	LogEntry[k].FilePath = fmt.Sprintf(LogEntry[k].FilePath,year,month)
	//}
	return
}

//监测新配置，并写入到指定channel中
func Watcher(key string, logConfChan chan<- []*LogEntry) {
	//派一个哨兵，检测某个key是否有变化
	ch := Cli.Watch(context.TODO(), key)
	for v := range ch {
		for _, vv := range v.Events {
			fmt.Println(string(vv.Kv.Key), string(vv.Kv.Value), vv.Type)
			var LogEntry []*LogEntry
			_ = json.Unmarshal(vv.Kv.Value, &LogEntry)
			logConfChan <- LogEntry
		}
	}
}
