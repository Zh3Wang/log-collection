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
	LogTailMap  map[string]*TailTask //记录每个topic日志配置的tail对象
}

func Init(logConf []*etcd.LogEntry) {
	//将日志收集的配置信息存起来，方便后期进行热更新
	taskMgr = &tailManager{
		LogEntry:    logConf,
		LogConfChan: make(chan []*etcd.LogEntry), //接受新配置的通道
		LogTailMap:  make(map[string]*TailTask, 16),
	}
	for _, v := range logConf {
		tailTask := newTailTask(v.Topic, v.FilePath+v.FileName)
		mapKey := fmt.Sprintf("%s_%s", v.Topic, v.FilePath+v.FileName)
		taskMgr.LogTailMap[mapKey] = tailTask
	}

	//监听新配置channel
	go taskMgr.watchNewConf()
}

//返回tailManager对象的一个只写channel，用于etcd监测新配置写入到其中
func LogConfChan() chan<- []*etcd.LogEntry {
	return taskMgr.LogConfChan
}

func (t *tailManager) watchNewConf() {
	for {
		select {
		case newConf := <-taskMgr.LogConfChan:
			fmt.Println("新配置：", newConf)
			//获取到新配置
			//新增配置
			for _, v := range newConf {
				mapKey := fmt.Sprintf("%s_%s", v.Topic, v.FilePath+v.FileName)
				_, ok := t.LogTailMap[mapKey]
				if !ok {
					//实例化新的tailtask对象
					tailTask := newTailTask(v.Topic, v.FilePath+v.FileName)
					t.LogTailMap[mapKey] = tailTask
					//t.LogEntry = append(t.LogEntry, v)
				}
			}
			//删除配置
			for _, old := range t.LogEntry {
				//先遍历旧的配置，再到新的中去找，如果没找到，说明要把旧的配置删掉
				isDel := true
				for _, n := range newConf {
					if old.Topic == n.Topic {
						isDel = false
					}
				}
				if isDel {
					mapKey := fmt.Sprintf("%s_%s", old.Topic, old.FilePath+old.FileName)
					t.LogTailMap[mapKey].cancelFunc()
					delete(t.LogTailMap, mapKey)
				}
			}
			//新配置直接覆盖旧的
			t.LogEntry = newConf
			curTopic := []string{}
			for _, v := range t.LogEntry {
				curTopic = append(curTopic, v.Topic)
			}
			fmt.Println("当前配置项：", curTopic, "Map：", t.LogTailMap)
		}
	}

}
