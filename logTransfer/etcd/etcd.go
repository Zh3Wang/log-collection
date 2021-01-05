package etcd

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/coreos/etcd/clientv3"
	"log"
	"time"
)

type LogEntry struct {
	Topic    string `json:"topic"`
	FileName string `json:"filename"`
}

var Cli *clientv3.Client

//初始化，连接etcd
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
}

//获取配置
func GetLogConfTopic(key string) []string {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	resp, err := Cli.Get(ctx, key)
	cancel()
	if err != nil {
		log.Fatal("获取配置失败：", err)
	}
	var logEntry []*LogEntry
	for _, v := range resp.Kvs {
		_ = json.Unmarshal(v.Value, &logEntry)
	}

	var topics []string
	for _, v := range logEntry {
		topics = append(topics, v.Topic)
	}
	return topics
}

//watcher
func Watcher(key string, newConfChan chan<- []*LogEntry) {
	ch := Cli.Watch(context.TODO(), key)
	for {
		select {
		case cfg := <-ch:
			//配置更新
			for _, v := range cfg.Events {
				fmt.Println("新配置来啦~~ : ", string(v.Kv.Key), string(v.Kv.Value), v.Type)
				var LogEntry []*LogEntry
				_ = json.Unmarshal(v.Kv.Value, &LogEntry)
				newConfChan <- LogEntry
			}
		default:
			time.Sleep(time.Millisecond * 500)
		}
	}
}
