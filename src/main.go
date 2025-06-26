package main

import (
	"bufio"
	"context"
	"encoding/csv"
	"fmt"
	"log"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

type StepFunc func(done <-chan any, streamIn <-chan string, workers int) <-chan string

// Custom resolver para LocalStack
func localstackResolver(endpoint string) aws.EndpointResolverWithOptions {
	return aws.EndpointResolverWithOptionsFunc(func(service, region string, options ...interface{}) (aws.Endpoint, error) {
		return aws.Endpoint{
			PartitionID:       "aws",
			URL:               endpoint,
			SigningRegion:     "us-east-1",
			HostnameImmutable: true,
		}, nil
	})
}

func main() {
	endpoint := "http://localhost:4566"
	bucket := "mi-bucket"
	key := "details-1a99313b-677c-4c00-addf-2f117715258a.csv"

	// Configuración personalizada para LocalStack
	cfg, err := config.LoadDefaultConfig(context.TODO(),
		config.WithRegion("us-east-1"),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("test", "test", "")),
		config.WithEndpointResolverWithOptions(localstackResolver(endpoint)),
		config.WithHTTPClient(&http.Client{}),
	)
	if err != nil {
		log.Fatalf("failed to load AWS config: %v", err)
	}

	client := s3.NewFromConfig(cfg)

	// Asegurarse que el archivo existe
	_, err = client.HeadObject(context.TODO(), &s3.HeadObjectInput{
		Bucket: &bucket,
		Key:    &key,
	})
	if err != nil {
		log.Fatalf("failed to find object: %v", err)
	}

	done := make(chan any)
	timeStart := time.Now()

	run := runner(done, ReadFileAsyncStream(done, client, bucket, key),
		50, // Number of workers
		showStream,
		StepTransformTextAsynStream,
		StepStoreDataAsynStream,
	)
	run()

	timeSince := time.Since(timeStart)
	log.Printf("Total time taken: %s", timeSince)
}

func showStream(stream <-chan string) {
	count := 0
	for s := range stream {
		count++
		log.Println(s)
	}
	fmt.Printf("Total records processed: %d\n", count)
}

func ReadFileAsyncStream(done <-chan any, client *s3.Client, bucket, key string) <-chan string {
	stream := make(chan string)
	resp, err := client.GetObject(context.TODO(), &s3.GetObjectInput{
		Bucket: &bucket,
		Key:    &key,
	})
	if err != nil {
		log.Fatalf("failed to get object: %v", err)
	}
	reader := csv.NewReader(bufio.NewReader(resp.Body))

	go func() {
		defer func() {
			close(stream)
			resp.Body.Close()
		}()
		for {
			select {
			case <-done:
				return
			default:
				record, err := reader.Read()
				if err != nil {
					if err.Error() == "EOF" {
						return // Fin del archivo
					}
					log.Printf("Error reading CSV: %v", err)
					return
				}
				stream <- strings.Join(record, ", ")
			}
		}
	}()
	return stream
}

func runner(done <-chan any, streamIn <-chan string, workers int, oper func(<-chan string), steps ...StepFunc) func() {
	return func() {
		if len(steps) == 0 {
			return
		}
		// Initialize the pipeline slices.
		var currentStream <-chan string = streamIn

		// Chain each step together.
		for _, step := range steps {
			currentStream = step(done, currentStream, workers)
		}

		// Perform the final operation on the last step's output.
		oper(currentStream)
	}
}

func StepStoreDataAsynStream(done <-chan any, streamIn <-chan string, workers int) <-chan string {
	streamOut := make(chan string)
	wg := sync.WaitGroup{}

	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-done:
					return
				case s, ok := <-streamIn:
					if !ok {
						return
					}
					// Store in database or any other storage
					time.Sleep(50 * time.Millisecond) // Simulate some processing delay
					streamOut <- s
				}
			}
		}()
	}

	go func() {
		wg.Wait()
		close(streamOut) // Close the channel after all workers are done
	}()
	return streamOut
}

func StepTransformTextAsynStream(done <-chan any, streamIn <-chan string, workers int) <-chan string {
	streamOut := make(chan string)
	wg := sync.WaitGroup{}

	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-done:
					return
				case s, ok := <-streamIn:
					if !ok {
						return
					}
					streamOut <- TransformText(s,
						TransformToUpper,
						TransformToLower,
						TransformGetPhone,
					)
				}
			}
		}()
	}

	go func() {
		wg.Wait()
		close(streamOut) // Close the channel after all workers are done
	}()

	return streamOut
}

func TransformText(s string, fn ...func(string) string) string {
	// Transformación de ejemplo: convertir a mayúsculas
	result := s
	for _, f := range fn {
		result = f(result)
	}
	return result
}

// Tranformation functions
func TransformToUpper(a string) string {
	// Transformación de ejemplo: convertir a mayúsculas
	return strings.ToUpper(a)
}

func TransformToLower(b string) string {
	// Transformación de ejemplo: convertir a minúsculas
	return strings.ToLower(b)
}

func TransformGetPhone(c string) string {
	// obtener el número de teléfono
	parts := strings.Split(c, ",")
	return strings.TrimSpace(parts[1]) // Asumiendo que el número de teléfono está en la segunda columna
}
