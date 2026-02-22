package main

import (
	"fmt"
	"net"
	"os"
)

func main() {
	itfs, _ := net.Interfaces()

	for _, itf := range itfs {
		if itf.Name == "lo" {
			continue
		}

		ips, _ := itf.Addrs()
		fmt.Println(itf.Name, ips[0].String())
	}

	host, _ := os.Hostname()
	fmt.Printf("Hello from %s \n", host)
}
