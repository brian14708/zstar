// Command tutorial_file writes an archive to a file, reads it, and verifies it.
package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log"
	"os"

	zstar "github.com/brian14708/zstar/go"
)

func main() {
	if err := run(context.Background()); err != nil {
		log.Fatal(err)
	}
}

func run(ctx context.Context) (result error) {
	if err := writeArchive(ctx); err != nil {
		return err
	}
	// region read
	input, err := os.Open("example.zstar")
	if err != nil {
		return err
	}
	defer func() { result = errors.Join(result, input.Close()) }()
	info, err := input.Stat()
	if err != nil {
		return err
	}
	source := zstar.ReaderAtSource{Reader: input, Length: info.Size()}
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

	// region verify
	if _, err := archive.Verify(ctx, false); err != nil {
		return err
	}
	fmt.Println("Archive verified")
	// endregion verify
	return nil
}

func writeArchive(ctx context.Context) (result error) {
	// region create
	output, err := os.Create("example.zstar")
	if err != nil {
		return err
	}
	defer func() { result = errors.Join(result, output.Close()) }()
	writer, err := zstar.NewWriter(output, zstar.WriterOptions{})
	if err != nil {
		return err
	}
	defer func() { result = errors.Join(result, writer.Close()) }()
	content := []byte("Hello from Zstar\n")
	header := zstar.Header{Path: "example.txt", Mode: 0o644, Size: uint64(len(content))}
	if err := writer.Add(ctx, header, bytes.NewReader(content)); err != nil {
		return err
	}
	if err := writer.Close(); err != nil {
		return err
	}
	// endregion create
	return nil
}
