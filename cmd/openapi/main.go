package main

import (
	"fmt"
	"gpu-telemetry-pipeline/internal/gateway"
	"os"
)

func main() {
	b, err := gateway.Schema()
	if err == nil {
		_, err = os.Stdout.Write(append(b, '\n'))
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
