package main

import (
	"context"
	"errors"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/aws"
	awshttp "github.com/aws/aws-sdk-go-v2/aws/transport/http"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

type File struct {
	SourceDir string
	DestPath  string
}

func main() {
	start := time.Now()
	concurrent := true
	if len(os.Args) != 5 {
		log.Fatalf("Usage: %s <bucket> <source> <destination> <region>", os.Args[0])
	}

	bucket := os.Args[1]
	source := os.Args[2]
	destination := os.Args[3]
	region := os.Args[4]

	cfg, err := config.LoadDefaultConfig(context.TODO(), config.WithRegion(region))
	if err != nil {
		log.Fatalf("Failed to load configuration: %v", err)
	}

	client := s3.NewFromConfig(cfg)

	if concurrent {
		err = UploadConcurrently(client, bucket, source, destination)
	} else {
		err = UploadDirectory(client, bucket, source, destination)
	}
	if err != nil {
		log.Fatalf("Failed to upload directory: %v", err)
	}

	fmt.Println("All files uploaded successfully!")
	fmt.Println("Elapsed Time", time.Since(start))
}

func FileExists(ctx context.Context, client *s3.Client, bucket string, key string) (bool, error) {
	_, err := client.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		var responseError *awshttp.ResponseError
		if errors.As(err, &responseError) && responseError.ResponseError.HTTPStatusCode() == http.StatusNotFound {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

func UploadFile(client *s3.Client, bucket string, sourcePath string, key string) error {
	exists, err := FileExists(context.Background(), client, bucket, key)
	if err != nil {
		return err
	}

	if !exists {
		file, err := os.Open(sourcePath)
		if err != nil {
			return fmt.Errorf("failed to open source file %q, %v", sourcePath, err)
		}
		defer file.Close()

		_, err = client.PutObject(context.TODO(), &s3.PutObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(key),
			Body:   file,
		})
		if err != nil {
			return fmt.Errorf("failed to upload file %q to bucket %q, %v", key, bucket, err)
		}

		fmt.Printf("Successfully uploaded %q to %q\n", sourcePath, key)
	} else {
		fmt.Printf("File exists, skipping %q\n", key)
	}

	return nil
}

func UploadDirectory(client *s3.Client, bucket string, sourcePath string, destPath string) error {
	return filepath.Walk(sourcePath, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		if !info.IsDir() {
			err = UploadFile(client, bucket, path, destPath)
			if err != nil {
				return err
			}
		}
		return nil
	})
}

func UploadConcurrently(client *s3.Client, bucket string, sourcePath string, destPath string) error {
	var wg sync.WaitGroup
	maxConcurrency := 6
	fileQueue := make(chan File, maxConcurrency)

	files, err := BuildFileList(sourcePath, destPath)
	if err != nil {
		return fmt.Errorf("BuildFileList - failed to build files list, %v", err)
	}

	wg.Add(len(files))
	for range maxConcurrency {
		go func() {
			for file := range fileQueue {
				err = UploadFile(client, bucket, file.SourceDir, file.DestPath)
				if err != nil {
					fmt.Fprintf(os.Stderr, "Failed to upload file %q to bucket %q, %v", file.SourceDir, bucket, err)
				}
				wg.Done()
			}
		}()
	}

	for i := range files {
		fmt.Printf("Write file to queue: %s \n", files[i].SourceDir)
		fileQueue <- files[i]
	}

	wg.Wait()
	close(fileQueue)
	return nil
}

func BuildFileList(sourcePath string, destPath string) ([]File, error) {
	var files []File
	err := filepath.Walk(sourcePath, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		if !info.IsDir() {
			relativePath, err := filepath.Rel(sourcePath, path)
			if err != nil {
				return err
			}

			files = append(files, File{
				SourceDir: path,
				DestPath:  strings.Join([]string{destPath, relativePath}, "/"),
			})
		}
		return nil
	})
	return files, err
}

func listBuckets(ctx context.Context, client *s3.Client) {
	// Get the first page of results for ListObjectsV2 for a bucket
	count := 10
	result, err := client.ListBuckets(ctx, &s3.ListBucketsInput{})
	if err != nil {
		log.Fatal(err)
	}

	if len(result.Buckets) == 0 {
		fmt.Println("You don't have any buckets!")
	} else {
		if count > len(result.Buckets) {
			count = len(result.Buckets)
		}
		for _, bucket := range result.Buckets[:count] {
			fmt.Printf("\t%v\n", *bucket.Name)
		}
	}
}

func list(ctx context.Context, client *s3.Client, bucket string) {
	// Get the first page of results for ListObjectsV2 for a bucket
	output, err := client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
		Bucket: aws.String(bucket),
	})
	if err != nil {
		log.Fatal(err)
	}

	log.Println("first page results:")
	for _, object := range output.Contents {
		log.Printf("key=%s size=%d", aws.ToString(object.Key), object.Size)
	}
}
