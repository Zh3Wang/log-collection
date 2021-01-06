package kafka

import (
	"context"
	"fmt"
	"github.com/Shopify/sarama"
	"log"
	"log-collection/logTransfer/es"
	"log-collection/logTransfer/lib"
	"time"
)

type ConsumeTask struct {
	Topic             string
	PartitionConsumer sarama.PartitionConsumer
	//为了能够退出协程，用context控制
	ctx        context.Context
	cancelFunc context.CancelFunc
}

//消费队列数据，发送到ES中
func NewConsume(topic string) (tasks []*ConsumeTask) {
	//获取指定topic的分区列表
	partitionList, err := Consumer.Partitions(topic)
	if err != nil {
		log.Fatal(fmt.Sprintf("kafka消费者拉取分区失败 , err: %v", err))
	}
	fmt.Println(topic, partitionList)

	// 遍历所有的分区
	for partition := range partitionList {
		// 针对每个分区创建一个对应的分区消费者
		pc, err := Consumer.ConsumePartition(topic, int32(partition), sarama.OffsetNewest)
		if err != nil {
			log.Fatal(fmt.Sprintf("failed to start consumer for partition %d,err:%v\n", partition, err))
		}

		//为每一个topic下的每一个分区新建一个消费任务
		var ct = ConsumeTask{
			Topic: topic,
		}
		ct.ctx, ct.cancelFunc = context.WithCancel(context.Background())
		//收集一个topic下的所有分区消费者任务
		tasks = append(tasks, &ct)
		// 创建goroutine，异步从每个topic下的每个分区中消费信息
		go ct.run(pc)
	}

	return
}

//监听队列，发送到es通道中异步处理
func (ct *ConsumeTask) run(pc sarama.PartitionConsumer) {
	defer pc.AsyncClose()
	for {
		select {
		case msg := <-pc.Messages():
			fmt.Printf("Partition:%d Offset:%d Value:%s\n", msg.Partition, msg.Offset, msg.Value)
			//构造发送的数据结构体
			ip, _ := lib.GetInternetIp()
			var b = &es.BodyData{
				Topic:   ct.Topic,
				Content: string(msg.Value),
				Ip:      ip,
			}
			var data = &es.MainData{
				Index:    ct.Topic,
				BodyData: b,
			}
			es.SendEs(data)
		case <-ct.ctx.Done():
			fmt.Println("退出一个topic的kafka消费者：", ct.Topic)
			return
		default:
			time.Sleep(time.Millisecond * 100)
		}
	}

}
