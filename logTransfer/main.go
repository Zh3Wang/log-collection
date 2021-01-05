package main

import (
	"github.com/spf13/viper"
	"log-collection/logTransfer/conf"
	"log-collection/logTransfer/etcd"
	"log-collection/logTransfer/kafka"
)

//从etcd中读取配置，获得有哪些topic日志
//从Kafka中根据topic读取日志数据
//将日志数据发往ES

func main() {
	Init()
}

func Init() {
	//加载配置
	conf.Load()

	//etcd
	addr := viper.Get("etcd.addr").([]interface{})
	etcd.InitEtcd(addr)

	//获取配置topic,决定要从kafka哪个topic中读取数据
	key := viper.GetString("etcd.logKey")
	topics := etcd.GetLogConfTopic(key)

	//创建kafka消费者，处理topic
	addr = viper.Get("kafka.addr").([]interface{})
	kafka.InitConsumer(addr, topics)

	//阻塞进程
	select {}
}
