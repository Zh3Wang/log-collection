package taillog

import (
	"fmt"
	"log-collection/logAgent/etcd"
)

var (
	taskMgr *tailManager
)

//tail任务管理对象，存储当前收集中的tail对象配置信息
type tailManager struct {
	LogEntry    []*etcd.LogEntry
	LogConfChan chan []*etcd.LogEntry
}

func Init(logConf []*etcd.LogEntry) {
	//将日志收集的配置信息存起来，方便后期进行热更新
	taskMgr = &tailManager{
		LogEntry:    logConf,
		LogConfChan: make(chan []*etcd.LogEntry), //接受新配置的通道
	}
	for _, v := range logConf {
		newTailTask(v.Topic, v.FilePath+v.FileName)
	}

	//监听新配置channel
	go watchNewConf()
}

//返回tailManager对象的一个只写channel，用于etcd监测新配置写入到其中
func LogConfChan() chan<- []*etcd.LogEntry {
	return taskMgr.LogConfChan
}

func watchNewConf() {
	for {
		select {
		case newConf := <-taskMgr.LogConfChan:
			fmt.Println("新配置：", newConf)
		}
	}

}
