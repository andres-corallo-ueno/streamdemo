package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"
)

func main() {
	// Create a channel to receive OS signals
	sigs := make(chan os.Signal, 1)

	// Notify the channel on specific signals
	ctx := signal.NotifyContext(context.Background(), sigs, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT)

	// Block until a signal is received
	sig := <-sigs
	fmt.Println("Received signal:", sig)
}
