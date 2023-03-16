package main

import (
	"fmt"
	"time"
)

func main() {
	timer := time.NewTimer(time.Duration(500) * time.Millisecond)
	ch := make(chan struct{})
outer:
	for {

		timer.Reset(time.Duration(300) * time.Millisecond)
		fmt.Println("set ok")
		for {
			select {
			case <-timer.C:
				fmt.Println("======")

				continue outer
			case <-ch:
			}
		}
	}
}
