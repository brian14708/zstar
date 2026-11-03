// Command tutorial_s3 creates an archive, uploads it, reads it, and deletes it.
package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"
	zstar "github.com/brian14708/zstar/go"
	zstars3 "github.com/brian14708/zstar/go/s3"
)

func main() {
	if err := run(context.Background()); err != nil {
		log.Fatal(err)
	}
}

func run(ctx context.Context) (result error) {
	ctx, cancel := context.WithTimeout(ctx, 2*time.Minute)
	defer cancel()
	// region configure
	for _, name := range []string{"ZSTAR_S3_BUCKET", "AWS_REGION", "AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY"} {
		if os.Getenv(name) == "" {
			return fmt.Errorf("set %s before running the example", name)
		}
	}
	bucket := os.Getenv("ZSTAR_S3_BUCKET")
	options := awss3.Options{
		Region: os.Getenv("AWS_REGION"),
		Credentials: credentials.NewStaticCredentialsProvider(
			os.Getenv("AWS_ACCESS_KEY_ID"), os.Getenv("AWS_SECRET_ACCESS_KEY"), os.Getenv("AWS_SESSION_TOKEN"),
		),
	}
	if endpoint := os.Getenv("ZSTAR_S3_ENDPOINT"); endpoint != "" {
		options.BaseEndpoint = aws.String(endpoint)
		options.UsePathStyle = true
	}
	client := awss3.New(options)
	// endregion configure

	// region upload
	encoded, err := createArchive(ctx)
	if err != nil {
		return err
	}
	key := fmt.Sprintf("zstar-tutorial/go-%d-%d.zstar", os.Getpid(), time.Now().UnixNano())
	if err := zstars3.Upload(ctx, client, bucket, key, bytes.NewReader(encoded), zstars3.UploadOptions{Immutable: true}); err != nil {
		return err
	}
	// endregion upload
	// region cleanup
	defer func() {
		cleanup, cancel := context.WithTimeout(context.WithoutCancel(ctx), 30*time.Second)
		defer cancel()
		_, err := client.DeleteObject(cleanup, &awss3.DeleteObjectInput{Bucket: aws.String(bucket), Key: aws.String(key)})
		result = errors.Join(result, err)
		if err == nil {
			fmt.Println("Deleted S3 object")
		}
	}()
	// endregion cleanup

	// region read
	source := zstars3.NewSource(client, bucket, key)
	archive, err := zstar.Open(ctx, source)
	if err != nil {
		return err
	}
	defer func() { result = errors.Join(result, archive.Close()) }()
	whole, err := archive.Read(ctx, "example.txt", 0, uint64(len("Hello from Zstar\n")))
	if err != nil {
		return err
	}
	selected, err := archive.Read(ctx, "example.txt", 0, 5)
	if err != nil {
		return err
	}
	if string(whole) != "Hello from Zstar\n" || string(selected) != "Hello" {
		return errors.New("unexpected archive contents")
	}
	fmt.Printf("%s%s\n", whole, selected)
	// endregion read
	return nil
}

func createArchive(ctx context.Context) (encoded []byte, result error) {
	var output bytes.Buffer
	writer, err := zstar.NewWriter(&output, zstar.WriterOptions{})
	if err != nil {
		return nil, err
	}
	defer func() { result = errors.Join(result, writer.Close()) }()
	content := []byte("Hello from Zstar\n")
	if err := writer.Add(ctx, zstar.Header{Path: "example.txt", Mode: 0o644, Size: uint64(len(content))}, bytes.NewReader(content)); err != nil {
		return nil, err
	}
	if err := writer.Close(); err != nil {
		return nil, err
	}
	return output.Bytes(), nil
}
