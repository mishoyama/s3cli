package main

import (
	"context"
	"flag"
	"io"
	"log"
	"os"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

var (
	endpoint string
	region   string
	ak       string
	sk       string
	bucket   string
	key      string
	debug    bool
)

func init() {
	flag.StringVar(&endpoint, "e", "http://192.168.55.2:9000", "The `endpoint` of the S3 service")
	flag.StringVar(&region, "r", "cn", "region")
	flag.StringVar(&ak, "ak", "object_user1", "access key")
	flag.StringVar(&sk, "sk", "ChangeMeChangeMeChangeMeChangeMeChangeMe", "secret key")
	flag.StringVar(&bucket, "b", "bucket", "The `name` of the S3 bucket")
	flag.StringVar(&key, "k", "test", "The `name` of the Object key")
	flag.BoolVar(&debug, "debug", false, "show debug log")
}

func main() {
	flag.Parse()
	if len(bucket) == 0 {
		flag.PrintDefaults()
		log.Fatalf("invalid parameters, bucket name required")
	}

	//var logMode aws.ClientLogMode
	//if debug {
	//	logMode = aws.LogRetries | aws.LogRequest | aws.LogResponse | aws.LogSigning
	//}

	cfg := aws.Config{
		Region: region,
		Credentials: aws.CredentialsProviderFunc(func(context.Context) (aws.Credentials, error) {
			return aws.Credentials{
				AccessKeyID:     ak,
				SecretAccessKey: sk,
			}, nil
		}),
		EndpointResolver: aws.EndpointResolverFunc(func(service, region string) (aws.Endpoint, error) {
			return aws.Endpoint{
				URL: endpoint,
			}, nil
		}),
		ClientLogMode: aws.LogRetries | aws.LogRequest | aws.LogResponse | aws.LogSigning,
	}

	client := s3.NewFromConfig(cfg, func(o *s3.Options) {
		o.UsePathStyle = true
	})

	// Set the parameters based on the CLI flag inputs.
	params := &s3.GetObjectInput{
		Bucket: &bucket,
		Key:    &key,
	}

	out, err := client.GetObject(context.Background(), params)
	if err != nil {
		log.Fatalf("failed to getObject, %v", err)
	}

	if _, err := io.Copy(os.Stdout, out.Body); err != nil {
		log.Fatalf("failed to download Object, %v", err)
	}

}
