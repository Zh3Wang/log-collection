package main

import (
	"fmt"
	"github.com/spf13/viper"
	"log"
	"log-collection/logAgent/conf"
	"log-collection/logAgent/kafka"
	"log-collection/logAgent/taillog"
	"time"
)

//入口
func main() {
	//初始化
	InitObj()
	//运行
	Run()
}

func Run() {
	for {
		select {
		case line := <-taillog.ReadChan():
			fmt.Println(line.Text)
			err := kafka.SendTo("web_log", line.Text)
			if err != nil {
				log.Println("发送kafka失败,", err)
			}
		default:
			time.Sleep(time.Millisecond * 500)
		}

	}
}

func InitObj() {
	//加载配置文件
	conf.Load()

	//实例化tail模块，监测日志
	err := taillog.InitTail(viper.Get("log.file").(string))
	if err != nil {
		panic(fmt.Sprintf("实例化tail失败：%s", err.Error()))
	}

	//连接kafka
	err = kafka.InitKafka(viper.Get("kafka.addr").([]interface{}))
	if err != nil {
		panic(fmt.Sprintf("Kafka连接失败：%s", err.Error()))
	}
}
