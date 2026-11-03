package zstar

import (
	"errors"
	"sync"

	"github.com/klauspost/compress/zstd"
)

func newFrameEncoder(target, level int) (*zstd.Encoder, error) {
	return zstd.NewWriter(nil, zstd.WithEncoderConcurrency(1),
		zstd.WithEncoderCRC(true), zstd.WithWindowSize(frameWindow(target)),
		zstd.WithEncoderLevel(manifestCompressionLevel(level)))
}

type (
	encodedFrame struct{ raw, encoded []byte }
	frameWorker  struct {
		codec  *zstd.Encoder
		input  chan []byte
		output chan encodedFrame
	}
)

// Each worker has one outstanding frame. Output buffers are reused only after
// the writer consumes the ordered result and submits another frame to that worker.
type frameWorkers struct {
	workers       []frameWorker
	head, pending int
	group         sync.WaitGroup
}

func newFrameWorkers(count, target, level int) (*frameWorkers, error) {
	p := &frameWorkers{}
	for range count {
		codec, err := newFrameEncoder(target, level)
		if err != nil {
			return nil, errors.Join(err, p.close())
		}
		worker := frameWorker{codec: codec, input: make(chan []byte, 1), output: make(chan encodedFrame, 1)}
		p.workers = append(p.workers, worker)
		p.group.Go(func() {
			var encoded []byte
			for raw := range worker.input {
				encoded = codec.EncodeAll(raw, encoded[:0])
				worker.output <- encodedFrame{raw: raw, encoded: encoded}
			}
		})
	}
	return p, nil
}

func (p *frameWorkers) submit(raw []byte) {
	p.workers[(p.head+p.pending)%len(p.workers)].input <- raw
	p.pending++
}

func (p *frameWorkers) receive() encodedFrame {
	frame := <-p.workers[p.head].output
	p.head = (p.head + 1) % len(p.workers)
	p.pending--
	return frame
}

func (p *frameWorkers) close() (err error) {
	for _, worker := range p.workers {
		close(worker.input)
	}
	p.group.Wait()
	for _, worker := range p.workers {
		err = errors.Join(err, worker.codec.Close())
	}
	p.workers = nil
	return err
}
