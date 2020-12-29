package taillog

import (
	"fmt"
	"github.com/hpcloud/tail"
	"log-collection/logAgent/kafka"
	"time"
)

type TailTask struct {
	topic    string
	file     string
	instance *tail.Tail
}

//实例化一个tail任务
func newTailTask(topic, file string) {
	var t = TailTask{
		topic: topic,
		file:  file,
	}
	t.initTail(file)
}

func (t *TailTask) initTail(file string) {
	config := tail.Config{
		ReOpen: true,
		Follow: true,
		Location: &tail.SeekInfo{
			Offset: 0,
			Whence: 2,
		},
		MustExist: false,
		Poll:      true,
	}
	t.instance, _ = tail.TailFile(file, config)
	go t.run()

}

func (t *TailTask) run() {
	fmt.Println(t.topic, t.file)
	for {
		select {
		case line := <-t.instance.Lines:
			fmt.Println(t.topic, t.file, " ----- ", line.Text)
			kafka.SendToChann(t.topic, line.Text)
		default:
			time.Sleep(time.Millisecond * 500)
		}

	}
}
