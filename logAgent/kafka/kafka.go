package kafka

import (
	"fmt"
	"github.com/Shopify/sarama"
)

var Kclient sarama.SyncProducer

//连接kafka
func InitKafka(addr []string) error {
	config := sarama.NewConfig()
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Partitioner = sarama.NewRandomPartitioner
	config.Producer.Return.Successes = true

	var err error
	Kclient, err = sarama.NewSyncProducer(addr, config)
	if err != nil {
		return err
	}
	return nil
}

//发送消息
func SendTo(topic string, content string) error {
	msg := &sarama.ProducerMessage{}
	msg.Topic = topic
	msg.Value = sarama.StringEncoder(content)

	pid, offset, err := Kclient.SendMessage(msg)
	if err != nil {
		return err
	}
	fmt.Printf("pid: %v offset:%v\n", pid, offset)
	return nil
}
