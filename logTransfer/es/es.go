package es

import (
	"context"
	"fmt"
	"github.com/olivere/elastic/v7"
	"log"
	"time"
)

type MainData struct {
	Index    string    `json:"index"`
	BodyData *BodyData `json:"bodydata"`
}

type BodyData struct {
	Topic   string `json:"topic"`
	Content string `json:"content"`
	Ip      string `json:"ip"`
}

var (
	Cli      *elastic.Client
	DataChan = make(chan *MainData, 10000)
)

func Init(addr string) {
	var err error
	Cli, err = elastic.NewClient(elastic.SetURL(addr))
	if err != nil {
		log.Fatal(fmt.Sprintf("ES 连接失败：%v", err))
	}
	fmt.Println("ES 连接成功")
	go Insert()
}

func SendEs(d *MainData) {
	DataChan <- d
}

func Insert() {
	for {
		select {
		case md := <-DataChan:
			put, err := Cli.Index().Index(md.Index).BodyJson(md.BodyData).Do(context.TODO())
			if err != nil {
				log.Fatal(fmt.Sprintf("ES 插入失败：%v", err))
			}
			fmt.Printf("Indexed user %s to index %s, type %s\n", put.Id, put.Index, put.Type)
		default:
			time.Sleep(time.Millisecond * 100)
		}
	}

}
