package taillog

import (
	"fmt"
	"log"
	"log-collection/logAgent/etcd"
	"strings"
	"time"
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
		v = ParseLogEntry(*v)
		tailTask := newTailTask(v.Topic, v.FilePath+v.FileName)
		mapKey := fmt.Sprintf("%s_%s", v.Topic, v.FilePath+v.FileName)
		taskMgr.LogTailMap[mapKey] = tailTask
	}
	//监听新配置channel
	go taskMgr.watchNewConf()
	//监听日期变化
	go taskMgr.watchTime()
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
				v = ParseLogEntry(*v)
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
					old = ParseLogEntry(*old)
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

func (t *tailManager) watchTime() {
	for {
		now := time.Now()
		//计算下一个零点
		next := now.Add(time.Hour * 24)
		next = time.Date(next.Year(), next.Month(), next.Day(), 0, 0, 0, 0, next.Location())
		ticker := time.NewTimer(next.Sub(now))
		//ticker := time.NewTimer(time.Second*2)
		//无缓冲通道，阻塞在这里，直到目标时间，通道可以取出值以后
		<-ticker.C
		//删掉所有旧goroutine，重新对新的日志文件收集
		for k, task := range t.LogTailMap {
			log.Println("删除旧的tail对象:", k)
			task.cancelFunc()
			delete(t.LogTailMap, k)
		}
		//重新生成tail文件读取路径
		for _, v := range t.LogEntry {
			v = ParseLogEntry(*v)
			tailTask := newTailTask(v.Topic, v.FilePath+v.FileName)
			mapKey := fmt.Sprintf("%s_%s", v.Topic, v.FilePath+v.FileName)
			taskMgr.LogTailMap[mapKey] = tailTask
			t.LogTailMap[mapKey] = tailTask
		}

	}
}

func ParseLogEntry(LogEntry etcd.LogEntry) *etcd.LogEntry {
	now := time.Now()
	date := now.Format("2006-01-02")
	dates := strings.Split(date, "-")
	year, month, day := dates[0], dates[1], dates[2]
	LogEntry.FileName = fmt.Sprintf(LogEntry.FileName, day)
	LogEntry.FilePath = fmt.Sprintf(LogEntry.FilePath, year, month)
	return &LogEntry
}
