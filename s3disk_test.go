package gostorage

import (
	"context"
	"os"
	"testing"
)

// TestS3Disk tests require environment variables to be set:
// S3_ENDPOINT, S3_REGION, S3_ACCESS_KEY, S3_SECRET_KEY, S3_BUCKET
func getS3TestConfig(t *testing.T) *S3Config {
	endpoint := os.Getenv("S3_ENDPOINT")
	region := os.Getenv("S3_REGION")
	accessKey := os.Getenv("S3_ACCESS_KEY")
	secretKey := os.Getenv("S3_SECRET_KEY")
	bucket := os.Getenv("S3_BUCKET")

	if accessKey == "" || secretKey == "" || bucket == "" {
		t.Skip("S3 credentials not set. Set S3_ACCESS_KEY, S3_SECRET_KEY, and S3_BUCKET environment variables to run S3 tests")
	}

	if region == "" {
		region = "us-east-1"
	}

	return &S3Config{
		Endpoint:     endpoint,
		Region:       region,
		AccessKey:    accessKey,
		SecretKey:    secretKey,
		Bucket:       bucket,
		Prefix:       "test/",      // Use a test prefix
		UsePathStyle: endpoint != "", // Use path style if endpoint is set (for MinIO)
	}
}

func TestS3Disk_BasicOperations(t *testing.T) {
	cfg := getS3TestConfig(t)

	disk, err := NewS3Disk(cfg)
	if err != nil {
		t.Fatalf("Failed to create S3Disk: %v", err)
	}

	ctx := context.Background()

	// Test Put
	content := []byte("Hello, S3!")
	err = disk.put(ctx, "test-basic.txt", content)
	if err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	// Test Get
	data, err := disk.get(ctx, "test-basic.txt")
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if string(data) != string(content) {
		t.Errorf("Expected %s, got %s", content, data)
	}

	// Test Delete
	err = disk.delete(ctx, "test-basic.txt")
	if err != nil {
		t.Fatalf("Delete failed: %v", err)
	}
}

func TestS3Disk_Metadata(t *testing.T) {
	cfg := getS3TestConfig(t)

	disk, err := NewS3Disk(cfg)
	if err != nil {
		t.Fatalf("Failed to create S3Disk: %v", err)
	}

	ctx := context.Background()
	content := []byte("File with metadata")
	meta := &Metadata{
		ContentType: "text/plain",
		CustomHeaders: map[string]string{
			"author": "Test",
		},
	}

	// Put with metadata
	err = disk.putWithMetadata(ctx, "test-meta.txt", content, meta)
	if err != nil {
		t.Fatalf("PutWithMetadata failed: %v", err)
	}

	// Get metadata
	retrievedMeta, err := disk.getMetadata(ctx, "test-meta.txt")
	if err != nil {
		t.Fatalf("GetMetadata failed: %v", err)
	}

	if retrievedMeta == nil {
		t.Fatal("Metadata should not be nil")
	}

	if retrievedMeta.ContentType != meta.ContentType {
		t.Errorf("ContentType mismatch: expected %s, got %s", meta.ContentType, retrievedMeta.ContentType)
	}

	// Cleanup
	_ = disk.delete(ctx, "test-meta.txt")
}

func TestS3Disk_ConfigValidation(t *testing.T) {
	// Test nil config
	_, err := NewS3Disk(nil)
	if err == nil {
		t.Error("Should reject nil config")
	}

	// Test empty bucket
	_, err = NewS3Disk(&S3Config{
		Region:    "us-east-1",
		AccessKey: "key",
		SecretKey: "secret",
	})
	if err == nil {
		t.Error("Should reject empty bucket")
	}
}
