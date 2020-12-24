package main

import (
	"fmt"
	"log"
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
	//实例化tail模块，监测日志
	err := taillog.InitTail("./my.log")
	if err != nil {
		panic(fmt.Sprintf("实例化tail失败：%s", err.Error()))
	}

	//连接kafka
	err = kafka.InitKafka([]string{"127.0.0.1:9092"})
	if err != nil {
		panic(fmt.Sprintf("Kafka连接失败：%s", err.Error()))
	}
}
