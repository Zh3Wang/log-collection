package main

import (
	"github.com/spf13/viper"
	"log-collection/logAgent/conf"
	"log-collection/logAgent/etcd"
	"log-collection/logAgent/kafka"
	"log-collection/logAgent/taillog"
	"sync"
)

var (
	wg sync.WaitGroup
)

//入口
func main() {
	//初始化
	InitObj()
}

func InitObj() {
	//加载配置文件
	conf.Load()

	//实例化etcd
	etcd.InitEtcd(viper.Get("etcd.addr").([]interface{}))

	//连接kafka
	addr := viper.Get("kafka.addr").([]interface{})
	maxSize := viper.Get("kafka.maxSize").(int64)
	kafka.InitKafka(addr, int(maxSize))

	//从etcd中获取日志收集项的配置
	logConf := etcd.GetLogConf(viper.Get("etcd.logKey").(string))
	//实例化tail模块，读取日志
	taillog.Init(logConf)

	//logchan用来存放新配置
	logConfChan := taillog.LogConfChan()

	//开启一个watcher监测etcd中配置key是否有更新，有更新的话写入到指定channel中
	wg.Add(1)
	go etcd.Watcher(viper.Get("etcd.logKey").(string), logConfChan)
	wg.Wait()
}
