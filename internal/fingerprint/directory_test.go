package fingerprint

import (
	"bytes"
	"testing"

	"github.com/kgorelov/findex/internal/model"
)

func TestComputeDirectoryFingerprint_Empty(t *testing.T) {
	fp1, err := ComputeDirectoryFingerprint(nil)
	if err != nil {
		t.Fatalf("empty directory fingerprint failed: %v", err)
	}
	if len(fp1) != 32 {
		t.Fatalf("expected 32 bytes, got %d", len(fp1))
	}

	fp2, err := ComputeDirectoryFingerprint([]ChildDescriptor{})
	if err != nil {
		t.Fatalf("empty slice directory fingerprint failed: %v", err)
	}
	if !bytes.Equal(fp1, fp2) {
		t.Errorf("expected nil and empty slice to yield same fingerprint")
	}
}

func TestComputeDirectoryFingerprint_OrderIndependence(t *testing.T) {
	childA := ChildDescriptor{
		Type:        model.EntryTypeRegular,
		SizeBytes:   100,
		Fingerprint: []byte("11111111111111111111111111111111"),
	}
	childB := ChildDescriptor{
		Type:        model.EntryTypeRegular,
		SizeBytes:   200,
		Fingerprint: []byte("22222222222222222222222222222222"),
	}
	childC := ChildDescriptor{
		Type:        model.EntryTypeDirectory,
		SizeBytes:   0,
		Fingerprint: []byte("33333333333333333333333333333333"),
	}

	order1 := []ChildDescriptor{childA, childB, childC}
	order2 := []ChildDescriptor{childC, childA, childB}
	order3 := []ChildDescriptor{childB, childC, childA}

	fp1, err := ComputeDirectoryFingerprint(order1)
	if err != nil {
		t.Fatalf("fp1 failed: %v", err)
	}
	fp2, err := ComputeDirectoryFingerprint(order2)
	if err != nil {
		t.Fatalf("fp2 failed: %v", err)
	}
	fp3, err := ComputeDirectoryFingerprint(order3)
	if err != nil {
		t.Fatalf("fp3 failed: %v", err)
	}

	if !bytes.Equal(fp1, fp2) || !bytes.Equal(fp2, fp3) {
		t.Errorf("expected directory fingerprints to be order-independent")
	}
}

func TestComputeDirectoryFingerprint_MultisetProperty(t *testing.T) {
	child := ChildDescriptor{
		Type:        model.EntryTypeRegular,
		SizeBytes:   500,
		Fingerprint: []byte("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"),
	}

	// Directory with one copy
	oneCopy := []ChildDescriptor{child}
	// Directory with two identical copies
	twoCopies := []ChildDescriptor{child, child}

	fpOne, err := ComputeDirectoryFingerprint(oneCopy)
	if err != nil {
		t.Fatalf("fpOne failed: %v", err)
	}
	fpTwo, err := ComputeDirectoryFingerprint(twoCopies)
	if err != nil {
		t.Fatalf("fpTwo failed: %v", err)
	}

	if bytes.Equal(fpOne, fpTwo) {
		t.Errorf("multiset violation: 1 copy and 2 copies produced identical fingerprints")
	}
}

func TestComputeSymlinkFingerprint(t *testing.T) {
	fp1 := ComputeSymlinkFingerprint("../target/path")
	fp2 := ComputeSymlinkFingerprint("../target/path")
	fp3 := ComputeSymlinkFingerprint("../different/target")

	if len(fp1) != 32 {
		t.Fatalf("expected 32 bytes for symlink fingerprint, got %d", len(fp1))
	}
	if !bytes.Equal(fp1, fp2) {
		t.Errorf("symlink fingerprint is not deterministic")
	}
	if bytes.Equal(fp1, fp3) {
		t.Errorf("different targets produced same symlink fingerprint")
	}
}
