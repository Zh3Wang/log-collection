package lib

import (
	"fmt"
	"net"
)

//外网IP
func GetInternetIp() (string, error) {
	conn, err := net.Dial("udp", "google.com:80")
	if err != nil {
		fmt.Println(err.Error())
		return "", err
	}
	defer conn.Close()
	return conn.LocalAddr().String(), nil
}
