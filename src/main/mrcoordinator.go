package main

//
// start the coordinator process, which is implemented
// in ../mr/coordinator.go
//
// go run mrcoordinator.go pg*.txt
//
// Please do not change this file.
//

import (
	"6.5840/mr"
	"fmt"
	"os"
	"time"
)

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintf(os.Stderr, "Usage: mrcoordinator inputfiles...\n")
		os.Exit(1)
	}

	m := mr.MakeCoordinator(os.Args[1:], 10)
	for m.Done() == false {
		time.Sleep(time.Second)
	}

	time.Sleep(time.Second)

	// local test
	//var filename []string
	//filename = append(filename, "pg-being_ernest.txt")
	//filename = append(filename, "pg-dorian_gray.txt")
	//filename = append(filename, "pg-frankenstein.txt")
	//filename = append(filename, "pg-grimm.txt")
	//filename = append(filename, "pg-huckleberry_finn.txt")
	//filename = append(filename, "pg-metamorphosis.txt")
	//filename = append(filename, "pg-sherlock_holmes.txt")
	//filename = append(filename, "pg-tom_sawyer.txt")
	//m := mr.MakeCoordinator(filename, 10)
	//for m.Done() == false {
	//	time.Sleep(time.Second)
	//}
	//
	//time.Sleep(time.Second)
}
