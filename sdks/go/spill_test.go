package zstar

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"sync"
	"testing"
)

type failingCloseSpill struct {
	bytes.Buffer
	closed  bool
	failure error
}

func (s *failingCloseSpill) ReadAt(out []byte, at int64) (int, error) {
	if s.closed {
		return 0, io.ErrClosedPipe
	}
	if at < 0 || at >= int64(s.Len()) {
		return 0, io.EOF
	}
	n := copy(out, s.Bytes()[at:])
	if n != len(out) {
		return n, io.EOF
	}
	return n, nil
}
func (s *failingCloseSpill) Close() error { s.closed = true; return s.failure }

func TestCustomSpillOwnershipAndCleanupFailure(t *testing.T) {
	data, err := os.ReadFile("../../testdata/entry-manifest/inline.zstar")
	if err != nil {
		t.Fatal(err)
	}
	cleanupFailure := errors.New("custom spill cleanup failed")
	store := &failingCloseSpill{}
	reader, err := OpenWithOptions(context.Background(), memoryRange(data), ReaderOptions{Manifest: ManifestOptions{
		MemoryBytes: 1, CreateSpillStore: func() (ManifestSpillStore, error) { return store, nil },
	}})
	if err != nil {
		t.Fatal(err)
	}
	if !store.closed {
		t.Fatal("decoded spool retained after entry indexing")
	}
	if _, err = reader.Verify(context.Background(), false); err != nil {
		t.Fatal(err)
	}
	if err = reader.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err = reader.Entry("a/file"); err == nil {
		t.Fatal("closed reader used")
	}
	store = &failingCloseSpill{failure: cleanupFailure}
	_, err = OpenWithOptions(context.Background(), memoryRange(data), ReaderOptions{Manifest: ManifestOptions{
		MemoryBytes: 1, CreateSpillStore: func() (ManifestSpillStore, error) { return store, nil },
	}})
	if !errors.Is(err, cleanupFailure) || !store.closed {
		t.Fatalf("opening cleanup ignored: %v", err)
	}
	// Opening failure must close its transferred store and retain both causes.
	corrupt := bytes.Clone(data)
	corrupt[len(corrupt)-1] ^= 1
	store = &failingCloseSpill{failure: cleanupFailure}
	_, err = OpenWithOptions(context.Background(), memoryRange(corrupt), ReaderOptions{Manifest: ManifestOptions{
		MemoryBytes: 1, CreateSpillStore: func() (ManifestSpillStore, error) { return store, nil },
	}})
	if ErrorCategory(err) != "integrity-failure" || !errors.Is(err, cleanupFailure) || !store.closed {
		t.Fatalf("opening cleanup lost cause: %v", err)
	}
}

func TestSharedSpoolQuotaPrecedesIOAndReleasesAfterCleanupFailure(t *testing.T) {
	budget := NewSpoolBudget(5)
	calls := 0
	cleanupFailure := errors.New("close failed")
	options := (ManifestOptions{
		MemoryBytes: 1, DisableDisk: true, SpoolBudget: budget,
		CreateSpillStore: func() (ManifestSpillStore, error) {
			calls++
			return &failingCloseSpill{failure: cleanupFailure}, nil
		},
	}).defaults()
	first, second, third := manifestSpool{options: options}, manifestSpool{options: options}, manifestSpool{options: options}
	if err := first.append([]byte("abc")); err != nil {
		t.Fatal(err)
	}
	if err := second.append([]byte("de")); err != nil {
		t.Fatal(err)
	}
	if err := third.append([]byte("f")); ErrorCategory(err) != "resource-limit" {
		t.Fatal(err)
	}
	if budget.Used() != 5 || calls != 2 {
		t.Fatalf("usage=%d callbacks=%d", budget.Used(), calls)
	}
	if err := first.close(); !errors.Is(err, cleanupFailure) {
		t.Fatal(err)
	}
	if budget.Used() != 2 {
		t.Fatal(budget.Used())
	}
	if err := second.append([]byte("f")); err != nil {
		t.Fatal(err)
	}
	if err := second.close(); !errors.Is(err, cleanupFailure) {
		t.Fatal(err)
	}
	if err := third.close(); err != nil {
		t.Fatal(err)
	}
	if budget.Used() != 0 {
		t.Fatal(budget.Used())
	}
}

func TestSpoolBudgetSharedByConcurrentOwners(t *testing.T) {
	budget := NewSpoolBudget(4)
	ready, release := make(chan struct{}, 8), make(chan struct{})
	var wait sync.WaitGroup
	results := make(chan bool, 8)
	for range 8 {
		wait.Go(func() {
			spool := manifestSpool{options: (ManifestOptions{SpoolBudget: budget}).defaults()}
			results <- spool.append([]byte("x")) == nil
			ready <- struct{}{}
			<-release
			if err := spool.close(); err != nil {
				t.Error(err)
			}
		})
	}
	for range 8 {
		<-ready
	}
	if budget.Used() != 4 {
		t.Fatal(budget.Used())
	}
	close(release)
	wait.Wait()
	accepted := 0
	for range 8 {
		if <-results {
			accepted++
		}
	}
	if accepted != 4 || budget.Used() != 0 {
		t.Fatalf("accepted=%d retained=%d", accepted, budget.Used())
	}
}

type partialSpill struct{ failingCloseSpill }

func (s *partialSpill) Write(data []byte) (int, error) {
	if len(data) == 0 {
		return 0, nil
	}
	_, _ = s.Buffer.Write(data[:1])
	return 1, io.ErrClosedPipe
}

func TestPartialSpillWriteRetainsQuotaUntilClose(t *testing.T) {
	budget := NewSpoolBudget(4)
	options := (ManifestOptions{
		MemoryBytes: 1, SpoolBudget: budget,
		CreateSpillStore: func() (ManifestSpillStore, error) { return &partialSpill{}, nil },
	}).defaults()
	spool := manifestSpool{options: options}
	if err := spool.append([]byte("abcd")); !errors.Is(err, io.ErrClosedPipe) {
		t.Fatal(err)
	}
	if budget.Used() != 4 || spool.size != 0 {
		t.Fatalf("retained=%d logical=%d", budget.Used(), spool.size)
	}
	if err := spool.append([]byte("e")); !errors.Is(err, io.ErrClosedPipe) {
		t.Fatalf("failed append-only store was reused: %v", err)
	}
	other := manifestSpool{options: options}
	if err := other.append([]byte("e")); ErrorCategory(err) != "resource-limit" {
		t.Fatal(err)
	}
	testClose(t, &spoolCloser{&spool})
	if err := other.append([]byte("e")); err != nil {
		t.Fatal(err)
	}
	testClose(t, &spoolCloser{&other})
	if budget.Used() != 0 {
		t.Fatal(budget.Used())
	}
}

type spoolCloser struct{ *manifestSpool }

func (s *spoolCloser) Close() error { return s.close() }

func TestSpoolQuotaPublicReaderAndWriterFailureCleanup(t *testing.T) {
	data, err := os.ReadFile("../../testdata/entry-manifest/inline.zstar")
	if err != nil {
		t.Fatal(err)
	}
	budget := NewSpoolBudget(0)
	options := ManifestOptions{
		SpoolBudget: budget, MemoryBytes: 1,
		CreateSpillStore: func() (ManifestSpillStore, error) { t.Fatal("quota must precede callback"); return nil, nil },
	}
	if _, err = OpenManifest(context.Background(), memoryRange(data), options); ErrorCategory(err) != "resource-limit" {
		t.Fatal(err)
	}
	writer, err := NewEntryArchiveWriter(io.Discard, 2048, options)
	if err != nil {
		t.Fatal(err)
	}
	if err = writer.Add(context.Background(), Entry{Path: "file", Type: TypeFile, Mode: 420}, 0, nil); err != nil && ErrorCategory(err) != "resource-limit" {
		t.Fatal(err)
	}
	if err = writer.Close(); ErrorCategory(err) != "resource-limit" {
		t.Fatal(err)
	}
	if budget.Used() != 0 {
		t.Fatal(budget.Used())
	}
}

func TestRecordQuotaFailurePreservesLogicalSpool(t *testing.T) {
	budget := NewSpoolBudget(5)
	spool := manifestSpool{options: (ManifestOptions{SpoolBudget: budget}).defaults()}
	defer func() { checkCleanup(t, spool.close()) }()
	if err := spool.append([]byte("a")); err != nil {
		t.Fatal(err)
	}
	if err := spool.appendRecord(uint64(1)); ErrorCategory(err) != "resource-limit" {
		t.Fatal(err)
	}
	if spool.size != 1 || budget.Used() != 1 {
		t.Fatal("length prefix survived quota failure")
	}
}

func TestFactoryFailureRollsBackReservationAndRejectsNonemptyStore(t *testing.T) {
	budget := NewSpoolBudget(3)
	factoryFailure := errors.New("factory failed")
	spool := manifestSpool{options: (ManifestOptions{
		MemoryBytes: 1, SpoolBudget: budget,
		CreateSpillStore: func() (ManifestSpillStore, error) { return nil, factoryFailure },
	}).defaults()}
	defer func() { checkCleanup(t, spool.close()) }()
	if err := spool.append([]byte("a")); err != nil {
		t.Fatal(err)
	}
	if err := spool.append([]byte("bc")); !errors.Is(err, factoryFailure) {
		t.Fatal(err)
	}
	if budget.Used() != 1 || spool.size != 1 {
		t.Fatal("failed factory retained reservation")
	}
	store := &failingCloseSpill{}
	_, _ = store.WriteString("nonempty")
	spool.options.CreateSpillStore = func() (ManifestSpillStore, error) { return store, nil }
	if err := spool.append([]byte("bc")); ErrorCategory(err) != "unsupported-operation" || !store.closed {
		t.Fatal(err)
	}
	if budget.Used() != 1 {
		t.Fatal(budget.Used())
	}
	if data, err := spool.read(0, 1); err != nil || string(data) != "a" {
		t.Fatalf("%q %v", data, err)
	}
}

type shortReadSpill struct{ failingCloseSpill }

func (s *shortReadSpill) ReadAt(out []byte, at int64) (int, error) {
	if s.Len() == 0 {
		return 0, io.EOF
	}
	return 0, nil
}

func TestCustomSpillCannotSilentlyReturnShortMetadata(t *testing.T) {
	spool := manifestSpool{options: (ManifestOptions{
		MemoryBytes:      1,
		CreateSpillStore: func() (ManifestSpillStore, error) { return &shortReadSpill{}, nil },
	}).defaults()}
	defer func() { checkCleanup(t, spool.close()) }()
	if err := spool.append([]byte("abc")); err != nil {
		t.Fatal(err)
	}
	if _, err := spool.read(0, 3); !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatal(err)
	}
}
