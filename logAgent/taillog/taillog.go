package taillog

import (
	"context"
	"fmt"
	"github.com/hpcloud/tail"
	"log-collection/logAgent/kafka"
	"time"
)

type TailTask struct {
	topic    string
	file     string
	instance *tail.Tail
	//为了能够退出协程，用context控制
	ctx        context.Context
	cancelFunc context.CancelFunc
}

//实例化一个tail任务
func newTailTask(topic, file string) *TailTask {
	var t = TailTask{
		topic: topic,
		file:  file,
	}
	t.ctx, t.cancelFunc = context.WithCancel(context.Background())
	t.initTail(file)
	return &t
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
	fmt.Println("实例化新配置：", t.topic, t.file)
	for {
		select {
		case line := <-t.instance.Lines:
			fmt.Println(t.topic, t.file, " ----- ", line.Text)
			kafka.SendToChann(t.topic, line.Text)
		case <-t.ctx.Done():
			fmt.Println("退出一个tail任务：", t.topic, t.file)
			return
		default:
			time.Sleep(time.Millisecond * 500)
		}

	}
}
