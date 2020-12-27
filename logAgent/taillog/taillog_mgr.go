package taillog

import "log-collection/logAgent/etcd"

var (
	taskMgr *tailManager
)

//tail任务管理对象，存储当前收集中的tail对象配置信息
type tailManager struct {
	LogEntry []*etcd.LogEntry
}

func Init(logConf []*etcd.LogEntry) {
	//将日志收集的配置信息存起来，方便后期进行热更新
	taskMgr = &tailManager{
		LogEntry: logConf,
	}
	for _, v := range logConf {
		newTailTask(v.Topic, v.FilePath+v.FileName)
	}

}
