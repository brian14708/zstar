package zstar

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
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
		Cases         []struct {
			Name, Archive, Operation string
			ArchiveSHA256            string         `json:"archive_sha256"`
			ExpectedError            *string        `json:"expected_error"`
			ExpectedVerification     map[string]any `json:"expected_verification"`
			Objects, Members, Links  map[string]string
			Limits                   struct {
				MaxDecodedBytes uint64 `json:"max_decoded_bytes"`
				MaxReadBytes    uint64 `json:"max_read_bytes"`
			}
			Request        ReadRequest
			RangeResult    string `json:"range_result"`
			VerifyExternal *bool  `json:"verify_external"`
		}
	}
	root := "../../testdata/entry-manifest"
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
	if corpus.SchemaVersion != 1 || corpus.WireVersion != 1 {
		t.Fatal("unsupported corpus or wire version")
	}
	for _, test := range corpus.Cases {
		t.Run(test.Name, func(t *testing.T) {
			ctx := context.Background()
			data := artifact(t, test.Archive)
			digest := sha256.Sum256(data)
			if hex.EncodeToString(digest[:]) != test.ArchiveSHA256 {
				t.Fatal("corpus artifact hash mismatch")
			}
			resolver := manifestCorpusObjects{root, test.Objects}
			verify := test.VerifyExternal == nil || *test.VerifyExternal
			m, err := OpenWithOptions(ctx, memoryRange(data), ReaderOptions{Manifest: ManifestOptions{MemoryBytes: 1, MaxDecodedBytes: test.Limits.MaxDecodedBytes, MaxReadBytes: test.Limits.MaxReadBytes}, Resolver: resolver, SkipExternalHash: !verify})
			var report VerificationReport
			if err == nil {
				defer testClose(t, m)
				switch test.Operation {
				case "metadata":
					err = m.ValidateMetadata(ctx)
				case "full-archive", "external-closure":
					report, err = m.Verify(ctx, test.Operation == "external-closure")
				case "inline-range", "external-range":
					var got []byte
					var selective ReadReport
					got, selective, err = m.ReadWithReport(ctx, test.Request.Path, test.Request.Offset, test.Request.Length)
					batch, batchErr := m.ReadMany(ctx, []ReadRequest{test.Request})
					if ErrorCategory(batchErr) != ErrorCategory(err) || err == nil && (len(batch) != 1 || !bytes.Equal(batch[0], got)) {
						t.Fatalf("batch disagrees: %v", batchErr)
					}
					if err == nil {
						if !bytes.Equal(got, artifact(t, test.RangeResult)) {
							t.Fatal("range bytes mismatch")
						}
						if !selective.MetadataAuthenticated || selective.FullArchiveValidated {
							t.Fatal("selective verification scope")
						}
						if test.Operation == "external-range" && verify && len(selective.ExternalObjectsSHA256Checked) != 1 {
							t.Fatal("missing external hash scope")
						}
						if test.Operation == "external-range" && !verify && len(selective.ExternalObjectsUnchecked) != 1 {
							t.Fatal("unchecked hash scope")
						}
					}
				default:
					t.Fatalf("unknown operation %q", test.Operation)
				}
			}
			expected := ""
			if test.ExpectedError != nil {
				expected = *test.ExpectedError
			}
			if ErrorCategory(err) != expected {
				t.Fatalf("want %q got %v", expected, err)
			}
			if err != nil {
				return
			}
			actualScopes := map[string]any{
				"full_archive_verified":           report.FullArchiveValidated,
				"data_checksums_checked":          report.DataFramesChecked != 0,
				"external_closure_validated":      report.ExternalClosureValidated,
				"external_objects_sha256_checked": report.ExternalObjectsSHA256Checked,
			}
			for key, want := range test.ExpectedVerification {
				if !reflect.DeepEqual(actualScopes[key], want) {
					t.Fatalf("scope %s: want %#v got %#v", key, want, actualScopes[key])
				}
			}
			requests := make([]ReadRequest, 0, len(test.Members))
			wants := make([][]byte, 0, len(test.Members))
			for path, artifactPath := range test.Members {
				want := artifact(t, artifactPath)
				requests = append(requests, ReadRequest{path, 0, uint64(len(want))})
				wants = append(wants, want)
				file, err := m.OpenFile(ctx, path)
				if err != nil {
					t.Fatal(err)
				}
				var output bytes.Buffer
				if _, err = file.WriteTo(&output); err != nil || !bytes.Equal(output.Bytes(), want) {
					t.Fatalf("stream %s: %v", path, err)
				}
			}
			if len(requests) != 0 {
				results, err := m.ReadMany(ctx, requests)
				if err != nil {
					t.Fatal(err)
				}
				for i := range results {
					if !bytes.Equal(results[i], wants[i]) {
						t.Fatalf("batch %s", requests[i].Path)
					}
				}
			}
			for path, target := range test.Links {
				entry, err := m.Entry(path)
				if err != nil || entry == nil || entry.Linkname == nil || *entry.Linkname != target {
					t.Fatalf("link %s: %v", path, err)
				}
			}
		})
	}
}
