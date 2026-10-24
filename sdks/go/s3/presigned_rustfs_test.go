//go:build integration

package s3

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"net/url"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"
	zstar "github.com/brian14708/zstar/sdks/go"
)

func testPresignedGETAndPUT(ctx context.Context, t *testing.T, client *awss3.Client, bucket string) {
	presigner := awss3.NewPresignClient(client)
	defer func() {
		if _, err := client.DeleteObject(ctx, &awss3.DeleteObjectInput{Bucket: aws.String(bucket), Key: aws.String("archive.bin")}); err != nil {
			t.Error(err)
		}
	}()
	defer func() {
		if _, err := client.DeleteObject(ctx, &awss3.DeleteObjectInput{Bucket: aws.String(bucket), Key: aws.String("presigned.bin")}); err != nil {
			t.Error(err)
		}
	}()
	defer func() {
		if _, err := client.DeleteObject(ctx, &awss3.DeleteObjectInput{Bucket: aws.String(bucket), Key: aws.String("indexed.zstar")}); err != nil {
			t.Error(err)
		}
	}()
	const key = "archive.bin"
	body := []byte("0123456789abcdef")
	if _, err := client.PutObject(ctx, &awss3.PutObjectInput{Bucket: aws.String(bucket), Key: aws.String(key), Body: bytes.NewReader(body)}); err != nil {
		t.Fatal(err)
	}
	getURL, err := presigner.PresignGetObject(ctx, &awss3.GetObjectInput{Bucket: aws.String(bucket), Key: aws.String(key)})
	if err != nil {
		t.Fatal(err)
	}
	reader := &zstar.HTTPRangeReader{URL: getURL.URL, Headers: getURL.SignedHeader, Client: http.DefaultClient}
	got, err := reader.ReadRange(ctx, 4, 5)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != "45678" {
		t.Fatalf("range = %q", got)
	}
	putCalls := 0
	if err = zstar.PutPresigned(ctx, func(ctx context.Context, force bool) (string, http.Header, error) {
		putCalls++
		if force != (putCalls == 2) {
			t.Fatalf("PUT call %d force=%v", putCalls, force)
		}
		putURL, signErr := presigner.PresignPutObject(ctx, &awss3.PutObjectInput{Bucket: aws.String(bucket), Key: aws.String("presigned.bin")})
		if signErr != nil {
			return "", nil, signErr
		}
		parsed, parseErr := url.Parse(putURL.URL)
		if parseErr != nil {
			return "", nil, parseErr
		}
		if !force {
			query := parsed.Query()
			query.Set("X-Amz-Signature", strings.Repeat("0", 64))
			parsed.RawQuery = query.Encode()
		}
		return parsed.String(), putURL.SignedHeader, nil
	}, http.DefaultClient, []byte("presigned upload")); err != nil {
		t.Fatalf("presigned PUT: %v", err)
	}
	if putCalls != 2 {
		t.Fatalf("PUT provider calls=%d", putCalls)
	}
	obj, err := client.GetObject(ctx, &awss3.GetObjectInput{Bucket: aws.String(bucket), Key: aws.String("presigned.bin")})
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := obj.Body.Close(); err != nil {
			t.Error(err)
		}
	}()
	result, err := io.ReadAll(obj.Body)
	if err != nil {
		t.Fatal(err)
	}
	if string(result) != "presigned upload" {
		t.Fatalf("uploaded bytes: %q", result)
	}
	if err = zstar.PutPresigned(ctx, func(ctx context.Context, _ bool) (string, http.Header, error) {
		putURL, signErr := presigner.PresignPutObject(ctx, &awss3.PutObjectInput{Bucket: aws.String(bucket), Key: aws.String("presigned.bin")})
		if signErr != nil {
			return "", nil, signErr
		}
		return putURL.URL, putURL.SignedHeader, nil
	}, nil, nil); err != nil {
		t.Fatal(err)
	}
	info, err := client.HeadObject(ctx, &awss3.HeadObjectInput{Bucket: aws.String(bucket), Key: aws.String("presigned.bin")})
	if err != nil {
		t.Fatal(err)
	}
	if size := aws.ToInt64(info.ContentLength); size != 0 {
		t.Fatalf("empty PUT: size=%d", size)
	}

	// Exercise a real archive, not just arbitrary object bytes. Upload through
	// presigned PUT, then authenticate its manifest and read one member over signed ranges.
	var archiveBytes bytes.Buffer
	writer, err := zstar.NewWriter(&archiveBytes, zstar.WriterOptions{FrameSize: 1024})
	if err != nil {
		t.Fatal(err)
	}
	if err = writer.Add(ctx, zstar.Header{Path: "data", Mode: 0o644, Size: 8192}, bytes.NewReader(bytes.Repeat([]byte("abcd"), 2048))); err != nil {
		t.Fatal(err)
	}
	if err = writer.Close(); err != nil {
		t.Fatal(err)
	}
	if err = zstar.PutPresigned(ctx, func(ctx context.Context, _ bool) (string, http.Header, error) {
		archivePut, signErr := presigner.PresignPutObject(ctx, &awss3.PutObjectInput{Bucket: aws.String(bucket), Key: aws.String("indexed.zstar")})
		if signErr != nil {
			return "", nil, signErr
		}
		return archivePut.URL, archivePut.SignedHeader, nil
	}, http.DefaultClient, archiveBytes.Bytes()); err != nil {
		t.Fatalf("archive PUT: %v", err)
	}
	archiveGet, err := presigner.PresignGetObject(ctx, &awss3.GetObjectInput{Bucket: aws.String(bucket), Key: aws.String("indexed.zstar")})
	if err != nil {
		t.Fatal(err)
	}
	archive, err := zstar.Open(ctx, &zstar.HTTPRangeReader{URL: archiveGet.URL, Headers: archiveGet.SignedHeader, Client: http.DefaultClient})
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := archive.Close(); err != nil {
			t.Error(err)
		}
	}()
	if _, err := archive.Verify(ctx, false); err != nil {
		t.Fatal(err)
	}
	selected, err := archive.Read(ctx, "data", 4096, 12)
	if err != nil || string(selected) != "abcdabcdabcd" {
		t.Fatalf("indexed read %q: %v", selected, err)
	}
}
