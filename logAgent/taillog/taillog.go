package taillog

import (
	"github.com/hpcloud/tail"
)

var tailObj *tail.Tail

func InitTail(filename string) error {
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
	var err error
	tailObj, err = tail.TailFile(filename, config)
	return err
}

func ReadChan() chan *tail.Line {
	return tailObj.Lines
}
