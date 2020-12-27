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
}

func GetLogConf(key string) (LogEntry []*LogEntry) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	resp, err := Cli.Get(ctx, key)
	cancel()
	if err != nil {
		log.Fatal(fmt.Sprintf("etcd get 操作失败：%v \n", err))
	}

	for _, v := range resp.Kvs {
		_ = json.Unmarshal(v.Value, &LogEntry)
	}

	return
}
