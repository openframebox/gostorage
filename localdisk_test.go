package gostorage

import (
	"context"
	"os"
	"path/filepath"
	"testing"
)

func TestLocalDisk_BasicOperations(t *testing.T) {
	// Create temp directory for testing
	tmpDir := t.TempDir()

	disk, err := NewLocalDisk(&LocalDiskConfig{
		Path: tmpDir,
	})
	if err != nil {
		t.Fatalf("Failed to create LocalDisk: %v", err)
	}

	ctx := context.Background()

	// Test Put
	content := []byte("Hello, World!")
	err = disk.put(ctx, "test.txt", content)
	if err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	// Test Get
	data, err := disk.get(ctx, "test.txt")
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if string(data) != string(content) {
		t.Errorf("Expected %s, got %s", content, data)
	}

	// Test Exists
	exists, err := disk.exists(ctx, "test.txt")
	if err != nil {
		t.Fatalf("Exists failed: %v", err)
	}
	if !exists {
		t.Error("File should exist")
	}

	// Test Size
	size, err := disk.size(ctx, "test.txt")
	if err != nil {
		t.Fatalf("Size failed: %v", err)
	}
	if size != int64(len(content)) {
		t.Errorf("Expected size %d, got %d", len(content), size)
	}

	// Test Delete
	err = disk.delete(ctx, "test.txt")
	if err != nil {
		t.Fatalf("Delete failed: %v", err)
	}

	// Verify deletion
	exists, err = disk.exists(ctx, "test.txt")
	if err != nil {
		t.Fatalf("Exists check after delete failed: %v", err)
	}
	if exists {
		t.Error("File should not exist after deletion")
	}
}

func TestLocalDisk_NestedPaths(t *testing.T) {
	tmpDir := t.TempDir()

	disk, err := NewLocalDisk(&LocalDiskConfig{
		Path: tmpDir,
	})
	if err != nil {
		t.Fatalf("Failed to create LocalDisk: %v", err)
	}

	ctx := context.Background()

	// Test nested path
	content := []byte("Nested content")
	err = disk.put(ctx, "dir1/dir2/nested.txt", content)
	if err != nil {
		t.Fatalf("Put to nested path failed: %v", err)
	}

	// Verify file exists
	data, err := disk.get(ctx, "dir1/dir2/nested.txt")
	if err != nil {
		t.Fatalf("Get from nested path failed: %v", err)
	}
	if string(data) != string(content) {
		t.Errorf("Expected %s, got %s", content, data)
	}
}

func TestLocalDisk_List(t *testing.T) {
	tmpDir := t.TempDir()

	disk, err := NewLocalDisk(&LocalDiskConfig{
		Path: tmpDir,
	})
	if err != nil {
		t.Fatalf("Failed to create LocalDisk: %v", err)
	}

	ctx := context.Background()

	// Create test files
	files := []string{
		"file1.txt",
		"file2.txt",
		"dir1/file3.txt",
		"dir1/file4.txt",
		"dir2/file5.txt",
	}

	for _, file := range files {
		err := disk.put(ctx, file, []byte("content"))
		if err != nil {
			t.Fatalf("Failed to create file %s: %v", file, err)
		}
	}

	// List all files
	allFiles, err := disk.list(ctx, "")
	if err != nil {
		t.Fatalf("List failed: %v", err)
	}

	// Should have at least the files we created (may have directories too)
	if len(allFiles) < len(files) {
		t.Errorf("Expected at least %d files, got %d", len(files), len(allFiles))
	}

	// List files in dir1
	dir1Files, err := disk.list(ctx, "dir1")
	if err != nil {
		t.Fatalf("List dir1 failed: %v", err)
	}

	// Should find the 2 files in dir1
	count := 0
	for _, f := range dir1Files {
		if !f.IsDir {
			count++
		}
	}
	if count < 2 {
		t.Errorf("Expected at least 2 files in dir1, got %d", count)
	}
}

func TestLocalDisk_CopyMove(t *testing.T) {
	tmpDir := t.TempDir()

	disk, err := NewLocalDisk(&LocalDiskConfig{
		Path: tmpDir,
	})
	if err != nil {
		t.Fatalf("Failed to create LocalDisk: %v", err)
	}

	ctx := context.Background()
	content := []byte("Original content")

	// Create source file
	err = disk.put(ctx, "source.txt", content)
	if err != nil {
		t.Fatalf("Failed to create source file: %v", err)
	}

	// Test Copy
	err = disk.copy(ctx, "source.txt", "copy.txt")
	if err != nil {
		t.Fatalf("Copy failed: %v", err)
	}

	// Verify copy
	copyData, err := disk.get(ctx, "copy.txt")
	if err != nil {
		t.Fatalf("Failed to read copy: %v", err)
	}
	if string(copyData) != string(content) {
		t.Errorf("Copy content mismatch")
	}

	// Both files should exist
	sourceExists, _ := disk.exists(ctx, "source.txt")
	copyExists, _ := disk.exists(ctx, "copy.txt")
	if !sourceExists || !copyExists {
		t.Error("Both source and copy should exist after copy")
	}

	// Test Move
	err = disk.move(ctx, "copy.txt", "moved.txt")
	if err != nil {
		t.Fatalf("Move failed: %v", err)
	}

	// Verify move
	movedData, err := disk.get(ctx, "moved.txt")
	if err != nil {
		t.Fatalf("Failed to read moved file: %v", err)
	}
	if string(movedData) != string(content) {
		t.Errorf("Moved content mismatch")
	}

	// Source of move should not exist
	copyExists, _ = disk.exists(ctx, "copy.txt")
	if copyExists {
		t.Error("Source file should not exist after move")
	}
}

func TestLocalDisk_Metadata(t *testing.T) {
	tmpDir := t.TempDir()

	disk, err := NewLocalDisk(&LocalDiskConfig{
		Path: tmpDir,
	})
	if err != nil {
		t.Fatalf("Failed to create LocalDisk: %v", err)
	}

	ctx := context.Background()
	content := []byte("File with metadata")
	meta := &Metadata{
		ContentType: "text/plain",
		CustomHeaders: map[string]string{
			"author":  "Test Author",
			"version": "1.0",
		},
	}

	// Put with metadata
	err = disk.putWithMetadata(ctx, "meta.txt", content, meta)
	if err != nil {
		t.Fatalf("PutWithMetadata failed: %v", err)
	}

	// Get metadata
	retrievedMeta, err := disk.getMetadata(ctx, "meta.txt")
	if err != nil {
		t.Fatalf("GetMetadata failed: %v", err)
	}

	if retrievedMeta == nil {
		t.Fatal("Metadata should not be nil")
	}

	if retrievedMeta.ContentType != meta.ContentType {
		t.Errorf("ContentType mismatch: expected %s, got %s", meta.ContentType, retrievedMeta.ContentType)
	}

	if retrievedMeta.CustomHeaders["author"] != "Test Author" {
		t.Error("Custom header 'author' mismatch")
	}
}

func TestLocalDisk_PathValidation(t *testing.T) {
	tmpDir := t.TempDir()

	disk, err := NewLocalDisk(&LocalDiskConfig{
		Path: tmpDir,
	})
	if err != nil {
		t.Fatalf("Failed to create LocalDisk: %v", err)
	}

	ctx := context.Background()

	// Test directory traversal protection
	err = disk.put(ctx, "../escape.txt", []byte("should fail"))
	if err == nil {
		t.Error("Should reject path with directory traversal")
	}

	err = disk.put(ctx, "../../escape.txt", []byte("should fail"))
	if err == nil {
		t.Error("Should reject path with directory traversal")
	}
}

func TestLocalDisk_CustomPermissions(t *testing.T) {
	tmpDir := t.TempDir()

	disk, err := NewLocalDisk(&LocalDiskConfig{
		Path:            tmpDir,
		DirPermissions:  0750,
		FilePermissions: 0640,
	})
	if err != nil {
		t.Fatalf("Failed to create LocalDisk: %v", err)
	}

	ctx := context.Background()

	// Create a file
	err = disk.put(ctx, "perms.txt", []byte("test"))
	if err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	// Check file permissions
	filePath := filepath.Join(tmpDir, "perms.txt")
	info, err := os.Stat(filePath)
	if err != nil {
		t.Fatalf("Failed to stat file: %v", err)
	}

	mode := info.Mode().Perm()
	if mode != 0640 {
		t.Errorf("Expected file permissions 0640, got %o", mode)
	}
}
