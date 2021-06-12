package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"image/png"
	"io/ioutil"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
)

type Contents struct {
	Contents []Content `json:"Contents"`
}

const BucketName = "<BUCKET_NAME>"
const AWSRegion = "<AWS_REGION>"

type Content struct {
	Key          string    `json:"Key"`
	LastModified time.Time `json:"LastModified"`
	ETag         string    `json:"ETag"`
	Size         int       `json:"Size"`
	StorageClass string    `json:"StorageClass"`
	Owner        Owner     `json:"Owner"`
}

type Owner struct {
	DisplayName string `json:"DisplayName"`
	ID          string `json:"ID"`
}

func main() {
	var dryRunFlag = flag.Bool("dryrun", true, "just list files")
	flag.Parse()
	jsonFile, err := os.Open("results.json")
	if err != nil {
		panic(err)
	}
	fmt.Println("Successfully opened results.json")
	defer jsonFile.Close()

	dec := json.NewDecoder(jsonFile)
	var contents Contents
	err = dec.Decode(&contents)
	if err != nil {
		panic(err)
	}

	// Limit 20 goroutines
	tokens := make(chan struct{}, 20)
	ch := make(chan *s3.ObjectIdentifier)
	sess := loadAWSConfig()
	go func() {
		var wg sync.WaitGroup
		wg.Add(len(contents.Contents))
		for _, content := range contents.Contents {
			go func(content Content) {
				defer wg.Done()
				findPNGFiles(tokens, content, ch, *dryRunFlag, sess)
			}(content)
		}
		wg.Wait()
		close(ch)
	}()

	fmt.Println("Waiting for work...")
	time.Sleep(2 * time.Second)
	s3objects := make([]*s3.ObjectIdentifier, 0)
	for {
		select {
		case v, ok := <-ch:
			if ok {
				s3objects = append(s3objects, v)
				if len(s3objects) == 20 {
					if *dryRunFlag {
						fmt.Println(s3objects)
					} else {
						// Delete 20 S3 objects in the current slice
						deleteS3Objects(sess, s3objects, BucketName)
					}
					// Clean and create a new slice
					s3objects = nil
					s3objects = make([]*s3.ObjectIdentifier, 0)
				}
			} else {
				if *dryRunFlag {
					fmt.Println(s3objects)
				} else {
					if len(s3objects) != 0 {
						// Delete remaining S3 objects in the current slide
						deleteS3Objects(sess, s3objects, BucketName)
					}
				}
				ch = nil
			}
		}
		if ch == nil {
			break
		}
	}
	fmt.Println("Done!")
}

func findPNGFiles(tokens chan struct{}, content Content, ch chan *s3.ObjectIdentifier, dryRunFlag bool, sess *session.Session) {
	defer func() {
		<-tokens // release token
	}()
	tokens <- struct{}{}
	if strings.HasSuffix(content.Key, ".pdf") || strings.HasSuffix(content.Key, ".csv") {
		return
	}
	if isOlderThan28Days(content.LastModified) {
		if strings.HasSuffix(content.Key, ".png") {
			ch <- &s3.ObjectIdentifier{Key: aws.String(content.Key)}
		} else {
			if dryRunFlag {
				ch <- &s3.ObjectIdentifier{Key: aws.String(content.Key)}
			} else {
				s3ObjectBytes := getS3Object(sess, content.Key, BucketName)
				if isPNGFile(s3ObjectBytes) {
					ch <- &s3.ObjectIdentifier{Key: aws.String(content.Key)}
					fmt.Println(content.Key, "is a PNG file, added to the channel")
				}
			}
		}
	}
}

func isOlderThan28Days(t time.Time) bool {
	return t.Before(time.Now().AddDate(0, 0, -28))
}

func loadAWSConfig() *session.Session {
	sess, err := session.NewSession(&aws.Config{
		Region: aws.String(AWSRegion)})
	if err != nil {
		panic("Failed to load config, " + err.Error())
	}
	return sess
}

func getS3Object(sess *session.Session, objectKey string, bucketName string) []byte {
	svc := s3.New(sess)
	input := &s3.GetObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(objectKey),
	}

	fmt.Println("Getting object from S3", objectKey)
	resp, err := svc.GetObject(input)
	if err != nil {
		panic(err)
	}

	s3ObjectBytes, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		panic(err)
	}
	return s3ObjectBytes
}

func deleteS3Objects(sess *session.Session, s3objects []*s3.ObjectIdentifier, bucketName string) {
	svc := s3.New(sess)
	input := &s3.DeleteObjectsInput{
		Bucket: aws.String(bucketName),
		Delete: &s3.Delete{
			Objects: s3objects,
		},
	}

	resp, err := svc.DeleteObjects(input)

	if err != nil {
		panic(err)
	}
	fmt.Println(resp)
}

func isPNGFile(b []byte) bool {
	if _, err := png.DecodeConfig(bytes.NewReader(b)); err != nil {
		return false
	}
	return true
}
