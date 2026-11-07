package zstar

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json/v2"
	"os"
	"path/filepath"
	"testing"
)

type manifestCorpusObjects struct {
	root  string
	paths map[string]string
}

func (s manifestCorpusObjects) GetEncoded(_ context.Context, digest [32]byte, _, maxBytes uint64) ([]byte, error) {
	path, ok := s.paths["sha256:"+hex.EncodeToString(digest[:])]
	if !ok {
		return nil, rejected("missing-external-content", "corpus object missing")
	}
	if !filepath.IsLocal(path) {
		return nil, rejected("unsupported-operation", "invalid corpus object path")
	}
	data, err := os.ReadFile(filepath.Join(s.root, path))
	if err == nil && uint64(len(data)) > maxBytes {
		return nil, rejected("resource-limit", "corpus object size")
	}
	return data, err
}

func TestEntryManifestCorpusOperations(t *testing.T) {
	var corpus struct {
		SchemaVersion int `json:"schema_version"`
		WireVersion   int `json:"wire_version"`
		Archives      map[string]struct {
			SHA256 string `json:"sha256"`
			Cases  []struct {
				Name          string            `json:"name"`
				ExpectedError string            `json:"expected_error"`
				Objects       map[string]string `json:"objects"`
				Limits        struct {
					Policy          Limits `json:"limits"`
					MaxDecodedBytes uint64 `json:"max_decoded_bytes"`
					MaxReadBytes    uint64 `json:"max_read_bytes"`
				} `json:"limits"`
				Request *struct {
					Path   string `json:"path"`
					Offset uint64 `json:"offset"`
					Length uint64 `json:"length"`
				} `json:"request"`
				RangeResultHex string `json:"range_result_hex"`
				VerifyExternal bool   `json:"verify_external"`
			} `json:"cases"`
		} `json:"archives"`
	}
	root := "../testdata/entry-manifest"
	artifact := func(t *testing.T, path string) []byte {
		t.Helper()
		if !filepath.IsLocal(path) {
			t.Fatalf("invalid corpus artifact %q", path)
		}
		data, err := os.ReadFile(filepath.Join(root, path))
		if err != nil {
			t.Fatal(err)
		}
		return data
	}
	if err := json.Unmarshal(artifact(t, "manifest.json"), &corpus); err != nil {
		t.Fatal(err)
	}
	if corpus.SchemaVersion != 2 || corpus.WireVersion != 1 {
		t.Fatal("unsupported corpus or wire version")
	}
	for path, archive := range corpus.Archives {
		for _, test := range archive.Cases {
			t.Run(test.Name, func(t *testing.T) {
				ctx := context.Background()
				data := artifact(t, path)
				digest := sha256.Sum256(data)
				if hex.EncodeToString(digest[:]) != archive.SHA256 {
					t.Fatal("corpus artifact hash mismatch")
				}
				resolver := manifestCorpusObjects{root, test.Objects}
				verify := test.VerifyExternal
				m, err := OpenWithOptions(ctx, memoryRange(data), ReaderOptions{Manifest: ManifestReaderOptions{Limits: test.Limits.Policy, MemoryBytes: 1, MaxDecodedBytes: test.Limits.MaxDecodedBytes, MaxReadBytes: test.Limits.MaxReadBytes}, Resolver: resolver, VerifyExternalHash: verify})
				if err == nil {
					defer testClose(t, m)
					if test.Request == nil {
						_, err = m.EntryCount()
					} else {
						var got []byte
						var selective ReadReport
						got, selective, err = m.ReadWithReport(ctx, test.Request.Path, test.Request.Offset, test.Request.Length)
						batch, batchErr := m.ReadMany(ctx, []ReadRequest{ReadRequest(*test.Request)})
						if ErrorCategory(batchErr) != ErrorCategory(err) || err == nil && (len(batch) != 1 || !bytes.Equal(batch[0], got)) {
							t.Fatalf("batch disagrees: %v", batchErr)
						}
						if err == nil {
							expected, decodeErr := hex.DecodeString(test.RangeResultHex)
							if decodeErr != nil {
								t.Fatal(decodeErr)
							}
							if !bytes.Equal(got, expected) {
								t.Fatal("range bytes mismatch")
							}
							if !selective.MetadataValidated {
								t.Fatal("selective verification scope")
							}
							if len(test.Objects) != 0 && verify && len(selective.ExternalObjectsSHA256Checked) != 1 {
								t.Fatal("missing external hash scope")
							}
							if len(test.Objects) != 0 && !verify && len(selective.ExternalObjectsUnchecked) != 1 {
								t.Fatal("unchecked hash scope")
							}
						}
					}
				}
				if ErrorCategory(err) != test.ExpectedError {
					t.Fatalf("want %q got %v", test.ExpectedError, err)
				}
			})
		}
	}
}
