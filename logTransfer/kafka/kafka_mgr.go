package kafka

import (
	"github.com/Shopify/sarama"
	"log"
	"log-collection/logTransfer/etcd"
	"time"
)

/*
	kafka消费者管理
	1. InitConsumer函数传入kafka地址和topic数组，为每一个topic下每一个分区创建一个消费者
	2. 创建一个ComsumerManager结构体，用来保存当前的消费者map、新配置的接受channel
	3. 对外暴露一个新配置通道————NewConfChann，etcd通过watcher监测到新配置更新时，将新配置写入到此channel中
	4. 开启一个goroutine监听新配置通道————NewConfChann，根据配置的新增或修改，进行消费者任务的创建和删除
*/

type ComsumerManager struct {
	ComsumersMap map[string][]*ConsumeTask
	NewConfChann chan []*etcd.LogEntry
}

var (
	Consumer sarama.Consumer
	Manager  = &ComsumerManager{
		ComsumersMap: make(map[string][]*ConsumeTask, 16),
		NewConfChann: make(chan []*etcd.LogEntry, 16),
	}
)

func InitConsumer(addr []interface{}, topics []string) {
	var newAddr []string
	for _, v := range addr {
		newAddr = append(newAddr, v.(string))
	}

	var err error
	Consumer, err = sarama.NewConsumer(newAddr, nil)
	if err != nil {
		log.Fatal("kafka消费者创建失败: ", err)
	}

	for _, topic := range topics {
		comsumerTasks := NewConsume(topic)
		//将每一个消费者任务保存起来
		Manager.ComsumersMap[topic] = comsumerTasks
	}

	//监听新配置通道
	go Manager.newConfMonitor()
}

func GetNewConfChan() chan<- []*etcd.LogEntry {
	return Manager.NewConfChann
}

//监控是否有新的topic配置
func (mgr *ComsumerManager) newConfMonitor() {
	for {
		select {
		case newConf := <-mgr.NewConfChann:
			//新增配置
			for _, v := range newConf {
				_, ok := mgr.ComsumersMap[v.Topic]
				if !ok {
					//重新创建一个topic的消费者
					comsumerTask := NewConsume(v.Topic)
					//将每一个消费者任务保存起来
					mgr.ComsumersMap[v.Topic] = comsumerTask
				}
			}
			//删除配置
			for topic, tasks := range mgr.ComsumersMap {
				isDel := true
				for _, v := range newConf {
					if topic == v.Topic {
						//如果新配置中没有旧配置的topic，需要把旧的topic任务删除掉
						isDel = false
					}
				}
				if isDel {
					//将该topic下所有分区的消费者goroutine停掉
					for _, v := range tasks {
						v.cancelFunc()
					}
					delete(mgr.ComsumersMap, topic)
				}
			}

		default:
			time.Sleep(time.Millisecond * 500)
		}
	}
}
