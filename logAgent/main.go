package main

import (
	"github.com/spf13/viper"
	"log-collection/logAgent/conf"
	"log-collection/logAgent/etcd"
	"log-collection/logAgent/kafka"
	"log-collection/logAgent/taillog"
)

//入口
func main() {
	//初始化
	InitObj()
}

func InitObj() {
	//加载配置文件
	conf.Load()
	//连接kafka
	addr := viper.Get("kafka.addr").([]interface{})
	maxSize := viper.Get("kafka.maxSize").(int64)
	kafka.InitKafka(addr, int(maxSize))

	//实例化etcd
	etcd.InitEtcd(viper.Get("etcd.addr").([]interface{}))
	//从etcd中获取日志收集项的配置
	logConf := etcd.GetLogConf(viper.Get("etcd.logKey").(string))
	//实例化tail模块，监测日志
	taillog.Init(logConf)
	select {}
}
