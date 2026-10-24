package s3

import (
	"bytes"
	"context"
	"crypto/sha256"
	"fmt"
	"io"
	"testing"

	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"
	zstar "github.com/brian14708/zstar/sdks/go"
	"github.com/klauspost/compress/zstd"
)

type capturingBlobClient struct {
	readOnlyClient
	encoded []byte
}

func (c *capturingBlobClient) PutObject(_ context.Context, input *awss3.PutObjectInput, _ ...func(*awss3.Options)) (*awss3.PutObjectOutput, error) {
	encoded, err := io.ReadAll(input.Body)
	if err != nil {
		return nil, err
	}
	c.encoded = encoded
	return &awss3.PutObjectOutput{}, nil
}

func TestBlobPutObjectWindows(t *testing.T) {
	decoder, err := zstd.NewReader(nil, zstd.WithDecoderConcurrency(1), zstd.WithDecoderMaxMemory(17<<20))
	if err != nil {
		t.Fatal(err)
	}
	defer decoder.Close()
	for _, size := range []int{1, 1024, 1025, 64<<10 + 1, 1<<20 + 1, 16 << 20} {
		t.Run(fmt.Sprintf("bytes=%d", size), func(t *testing.T) {
			client := &capturingBlobClient{}
			store, err := NewBlobStore(client, "bucket", BlobStoreOptions{})
			if err != nil {
				t.Fatal(err)
			}
			raw := make([]byte, size)
			for i := range raw {
				raw[i] = byte(i ^ (i >> 8))
			}
			extent, err := store.Put(context.Background(), raw)
			if err != nil {
				t.Fatal(err)
			}
			decoded, err := decoder.DecodeAll(client.encoded, nil)
			if err != nil || !bytes.Equal(decoded, raw) {
				t.Fatalf("decoded object: %v", err)
			}
			if extent.Source != zstar.ObjectSource(sha256.Sum256(raw)) || extent.Size != uint64(size) || extent.EncodedSize != uint64(len(client.encoded)) {
				t.Fatal("object extent changed")
			}
			var header zstd.Header
			if err := header.Decode(client.encoded); err != nil {
				t.Fatal(err)
			}
			if !header.HasCheckSum {
				t.Fatal("missing object checksum")
			}
		})
	}
}
