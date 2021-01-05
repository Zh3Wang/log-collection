package kafka

import (
	"github.com/Shopify/sarama"
	"log"
)

var Consumer sarama.Consumer

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
		NewConsume(topic)
	}

}
