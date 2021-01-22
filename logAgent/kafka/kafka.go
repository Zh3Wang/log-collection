package kafka

import (
	"fmt"
	"github.com/Shopify/sarama"
	"log"
	"time"
)

var (
	Kclient sarama.SyncProducer
	LogChan chan *LogData
)

type LogData struct {
	Topic   string `json:"topic"`
	Content string `json:"content"`
}

//连接kafka
func InitKafka(addr []interface{}, maxSize int) {
	config := sarama.NewConfig()
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Partitioner = sarama.NewRandomPartitioner
	config.Producer.Return.Successes = true

	var err error
	var addrString []string
	for _, v := range addr {
		addrString = append(addrString, v.(string))
	}
	Kclient, err = sarama.NewSyncProducer(addrString, config)
	if err != nil {
		log.Fatal(fmt.Sprintf("Kafka连接失败：%s", err.Error()))
	}
	log.Println("连接kafka成功~~")
	//创建一个协程
	LogChan = make(chan *LogData, maxSize)
	go send()
}

//监听channel，发送到Kafka
func send() {
	for {
		select {
		case ld := <-LogChan:
			SendTo(ld)
		default:
			time.Sleep(time.Second)
		}
	}
}

func SendToChann(topic, content string) {
	LogChan <- &LogData{
		Topic:   topic,
		Content: content,
	}
}

//发送消息
func SendTo(ld *LogData) {
	msg := &sarama.ProducerMessage{}
	msg.Topic = ld.Topic
	msg.Value = sarama.StringEncoder(ld.Content)

	//pid, offset, _ := Kclient.SendMessage(msg)
	_, _, _ = Kclient.SendMessage(msg)
	//if err != nil {
	//	return err
	//}
	//fmt.Printf("pid: %v offset:%v\n", pid, offset)
	//return nil
}
