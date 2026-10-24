package zstar

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"os"
	"testing"
)

type chunkRecipe struct {
	Kind           string
	Length         int
	Insert, Delete *int
}

func vectorInput(recipe chunkRecipe) []byte {
	data := make([]byte, 0, recipe.Length+10)
	if recipe.Kind == "zeros" {
		data = make([]byte, recipe.Length)
	} else {
		for i := uint64(0); len(data) < recipe.Length; i++ {
			var counter [8]byte
			binary.LittleEndian.PutUint64(counter[:], i)
			sum := sha256.Sum256(counter[:])
			data = append(data, sum[:]...)
		}
		data = data[:recipe.Length]
	}
	if recipe.Insert != nil {
		at := *recipe.Insert
		data = append(append(append([]byte{}, data[:at]...), []byte("zstar-edit")...), data[at:]...)
	}
	if recipe.Delete != nil {
		at := *recipe.Delete
		data = append(data[:at], data[at+13:]...)
	}
	return data
}

type shortChunkInput struct {
	io.Reader
	step int
}

func (s shortChunkInput) Read(out []byte) (int, error) {
	return s.Reader.Read(out[:min(len(out), s.step)])
}

func TestSharedChunkingVectors(t *testing.T) {
	raw, err := os.ReadFile("../testdata/chunking/vectors.json")
	if err != nil {
		t.Fatal(err)
	}
	var vectors struct {
		Cases []struct {
			Name             string
			Recipe           chunkRecipe
			Min, Target, Max int
			Chunks           [][3]json.RawMessage
		}
	}
	if err := json.Unmarshal(raw, &vectors); err != nil {
		t.Fatal(err)
	}
	for _, vector := range vectors.Cases {
		t.Run(vector.Name, func(t *testing.T) {
			input := vectorInput(vector.Recipe)
			for _, step := range []int{1, 7, 4096, 1 << 20} {
				chunker, err := NewChunker(shortChunkInput{bytes.NewReader(input), step}, ChunkingOptions{FastCDC: true, MinBytes: vector.Min, TargetBytes: vector.Target, MaxBytes: vector.Max})
				if err != nil {
					t.Fatal(err)
				}
				position, index := 0, 0
				for {
					chunk, err := chunker.Next(context.Background())
					if errors.Is(err, io.EOF) {
						break
					}
					if err != nil {
						t.Fatal(err)
					}
					if index >= len(vector.Chunks) {
						t.Fatal("extra chunk")
					}
					var offset, length int
					var hash string
					if err = json.Unmarshal(vector.Chunks[index][0], &offset); err != nil {
						t.Fatal(err)
					}
					if err = json.Unmarshal(vector.Chunks[index][1], &length); err != nil {
						t.Fatal(err)
					}
					if err = json.Unmarshal(vector.Chunks[index][2], &hash); err != nil {
						t.Fatal(err)
					}
					digest := sha256.Sum256(chunk)
					if offset != position || length != len(chunk) || hash != hex.EncodeToString(digest[:]) {
						t.Fatalf("step %d chunk %d mismatch", step, index)
					}
					if !bytes.Equal(chunk, input[position:position+len(chunk)]) {
						t.Fatal("reconstruction")
					}
					position += len(chunk)
					index++
				}
				if index != len(vector.Chunks) || position != len(input) {
					t.Fatal("missing chunks")
				}
			}
		})
	}
}

func TestChunkerInvalidSettingsAndCancellation(t *testing.T) {
	for _, options := range []ChunkingOptions{{TargetBytes: -1}, {TargetBytes: 17 << 20}, {FastCDC: true, MinBytes: 63}, {FastCDC: true, TargetBytes: 250}} {
		if _, err := NewChunker(bytes.NewReader(nil), options); err == nil {
			t.Fatal("invalid settings accepted")
		}
	}
	chunker, err := NewChunker(bytes.NewReader([]byte("data")), ChunkingOptions{})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := chunker.Next(ctx); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
}
