package main

import (
	"bytes"
	"context"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	"github.com/openframebox/gostorage"
)

func main() {
	ctx := context.Background()
	storage := gostorage.NewStorage()

	// Add local disk - Simple configuration!
	localDisk, err := gostorage.NewLocalDisk(&gostorage.LocalDiskConfig{
		Path: "storage",
	})
	if err != nil {
		log.Fatal(err)
	}
	storage.AddDisk("local", localDisk)

	fmt.Println("=== GoStorage Package Demo ===")
	fmt.Println()

	// Optionally add S3 disk if credentials are set
	if os.Getenv("S3_ACCESS_KEY") != "" {
		s3Disk, err := gostorage.NewS3Disk(&gostorage.S3Config{
			Endpoint:     os.Getenv("S3_ENDPOINT"),     // e.g., "https://s3.amazonaws.com" or "https://minio.example.com"
			Region:       getEnvOrDefault("S3_REGION", "us-east-1"),
			AccessKey:    os.Getenv("S3_ACCESS_KEY"),
			SecretKey:    os.Getenv("S3_SECRET_KEY"),
			Bucket:       os.Getenv("S3_BUCKET"),
			UsePathStyle: os.Getenv("S3_ENDPOINT") != "", // Use path style for custom endpoints
		})
		if err != nil {
			log.Printf("Failed to configure S3: %v", err)
		} else {
			storage.AddDisk("s3", s3Disk)
			fmt.Println("✓ S3 disk configured")
		}
	} else {
		fmt.Println("ℹ S3 disk not configured (set S3_ACCESS_KEY, S3_SECRET_KEY, S3_BUCKET to enable)")
	}

	fmt.Println()

	fmt.Println("=== Basic Operations Demo ===")
	basicOperations(ctx, storage)

	fmt.Println("\n=== Streaming Operations Demo ===")
	streamingOperations(ctx, storage)

	fmt.Println("\n=== Metadata Operations Demo ===")
	metadataOperations(ctx, storage)

	fmt.Println("\n=== File Operations Demo ===")
	fileOperations(ctx, storage)

	fmt.Println("\n=== List Operations Demo ===")
	listOperations(ctx, storage)

	// Only run S3 tests if configured
	if storage.HasDisk("s3") {
		fmt.Println("\n=== S3 Operations Demo ===")
		s3Operations(ctx, storage)

		fmt.Println("\n=== Cross-Disk Operations Demo ===")
		crossDiskOperations(ctx, storage)
	}
}

func getEnvOrDefault(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}

func basicOperations(ctx context.Context, storage *gostorage.Storage) {
	// Put a file
	content := []byte("Hello World!")
	err := storage.Put(ctx, "local", "basic/hello.txt", content)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("✓ File created: basic/hello.txt")

	// Get the file
	data, err := storage.Get(ctx, "local", "basic/hello.txt")
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("✓ File content: %s\n", string(data))

	// Check if file exists
	exists, err := storage.Exists(ctx, "local", "basic/hello.txt")
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("✓ File exists: %v\n", exists)

	// Get file size
	size, err := storage.Size(ctx, "local", "basic/hello.txt")
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("✓ File size: %d bytes\n", size)

	// Delete the file
	err = storage.Delete(ctx, "local", "basic/hello.txt")
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("✓ File deleted")
}

func streamingOperations(ctx context.Context, storage *gostorage.Storage) {
	// Put a file using streaming
	content := strings.NewReader("This is a large file content that might be streamed!")
	err := storage.PutStream(ctx, "local", "streaming/large.txt", content, nil)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("✓ File uploaded via stream: streaming/large.txt")

	// Get a file using streaming
	reader, err := storage.GetStream(ctx, "local", "streaming/large.txt")
	if err != nil {
		log.Fatal(err)
	}
	defer reader.Close()

	// Read from stream
	buf := new(bytes.Buffer)
	_, err = buf.ReadFrom(reader)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("✓ File content from stream: %s\n", buf.String())

	// Clean up
	_ = storage.Delete(ctx, "local", "streaming/large.txt")
}

func metadataOperations(ctx context.Context, storage *gostorage.Storage) {
	// Put a file with metadata
	content := []byte("File with metadata")
	metadata := &gostorage.Metadata{
		ContentType: "text/plain",
		CustomHeaders: map[string]string{
			"author":      "John Doe",
			"version":     "1.0",
			"description": "Example file with metadata",
		},
	}

	err := storage.PutWithMetadata(ctx, "local", "metadata/document.txt", content, metadata)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("✓ File created with metadata: metadata/document.txt")

	// Get metadata
	meta, err := storage.GetMetadata(ctx, "local", "metadata/document.txt")
	if err != nil {
		log.Fatal(err)
	}
	if meta != nil {
		fmt.Printf("✓ Content-Type: %s\n", meta.ContentType)
		fmt.Printf("✓ Custom headers: %v\n", meta.CustomHeaders)
	}

	// Update metadata
	newMeta := &gostorage.Metadata{
		ContentType: "text/plain; charset=utf-8",
		CustomHeaders: map[string]string{
			"author":  "John Doe",
			"version": "2.0",
			"updated": time.Now().Format(time.RFC3339),
		},
	}
	err = storage.SetMetadata(ctx, "local", "metadata/document.txt", newMeta)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("✓ Metadata updated")

	// Clean up
	_ = storage.Delete(ctx, "local", "metadata/document.txt")
}

func fileOperations(ctx context.Context, storage *gostorage.Storage) {
	// Create a source file
	err := storage.Put(ctx, "local", "operations/source.txt", []byte("Original content"))
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("✓ Source file created: operations/source.txt")

	// Copy the file
	err = storage.Copy(ctx, "local", "operations/source.txt", "operations/copy.txt")
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("✓ File copied: operations/source.txt → operations/copy.txt")

	// Move the file
	err = storage.Move(ctx, "local", "operations/copy.txt", "operations/moved.txt")
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("✓ File moved: operations/copy.txt → operations/moved.txt")

	// Verify copy no longer exists
	exists, _ := storage.Exists(ctx, "local", "operations/copy.txt")
	fmt.Printf("✓ Copy file exists after move: %v\n", exists)

	// Clean up
	_ = storage.Delete(ctx, "local", "operations/source.txt")
	_ = storage.Delete(ctx, "local", "operations/moved.txt")
}

func listOperations(ctx context.Context, storage *gostorage.Storage) {
	// Create multiple files
	files := []string{
		"list/documents/file1.txt",
		"list/documents/file2.txt",
		"list/images/photo1.jpg",
		"list/images/photo2.jpg",
	}

	for _, file := range files {
		err := storage.Put(ctx, "local", file, []byte("content"))
		if err != nil {
			log.Fatal(err)
		}
	}
	fmt.Println("✓ Created test files")

	// List all files under list/
	allFiles, err := storage.List(ctx, "local", "list")
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("✓ Total files in list/: %d\n", len(allFiles))

	// List only documents
	docs, err := storage.List(ctx, "local", "list/documents")
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("✓ Documents found: %d\n", len(docs))
	for _, doc := range docs {
		if !doc.IsDir {
			fmt.Printf("  - %s (%d bytes)\n", doc.Path, doc.Size)
		}
	}

	// List only images
	images, err := storage.List(ctx, "local", "list/images")
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("✓ Images found: %d\n", len(images))
	for _, img := range images {
		if !img.IsDir {
			fmt.Printf("  - %s (%d bytes)\n", img.Path, img.Size)
		}
	}

	// Clean up
	for _, file := range files {
		_ = storage.Delete(ctx, "local", file)
	}
}

func s3Operations(ctx context.Context, storage *gostorage.Storage) {
	fmt.Println("Testing S3/MinIO backend...")

	// First, try to list to verify connectivity and permissions
	fmt.Println("Attempting to list bucket contents...")
	files, err := storage.List(ctx, "s3", "")
	if err != nil {
		log.Printf("✗ Failed to list S3 bucket: %v", err)
		log.Printf("   Cannot proceed with S3 tests - check credentials and bucket permissions")
		return
	}
	fmt.Printf("✓ Successfully listed bucket. Found %d existing files\n", len(files))
	if len(files) > 0 && len(files) < 10 {
		for _, f := range files {
			fmt.Printf("   - %s (%d bytes)\n", f.Path, f.Size)
		}
	}

	// Test basic put
	content := []byte("Hello from S3/MinIO!")
	fmt.Println("\nAttempting to upload file to S3...")
	err = storage.Put(ctx, "s3", "test/hello-s3.txt", content)
	if err != nil {
		log.Printf("✗ Failed to put file to S3: %v", err)
		return
	}
	fmt.Println("✓ File uploaded to S3: test/hello-s3.txt")

	// Test get
	fmt.Println("Attempting to read file from S3...")
	data, err := storage.Get(ctx, "s3", "test/hello-s3.txt")
	if err != nil {
		log.Printf("✗ Failed to get file from S3: %v", err)
		log.Printf("   Note: Upload succeeded but read failed")
		// Continue with other tests
	} else {
		fmt.Printf("✓ File content from S3: %s\n", string(data))
	}

	// Test exists
	exists, err := storage.Exists(ctx, "s3", "test/hello-s3.txt")
	if err != nil {
		log.Printf("✗ Failed to check file existence: %v", err)
		return
	}
	fmt.Printf("✓ File exists on S3: %v\n", exists)

	// Test size
	size, err := storage.Size(ctx, "s3", "test/hello-s3.txt")
	if err != nil {
		log.Printf("✗ Failed to get file size: %v", err)
		return
	}
	fmt.Printf("✓ File size on S3: %d bytes\n", size)

	// Test streaming
	streamContent := strings.NewReader("Streamed content to S3!")
	err = storage.PutStream(ctx, "s3", "test/stream.txt", streamContent, &gostorage.Metadata{
		ContentType: "text/plain",
		CustomHeaders: map[string]string{
			"uploaded-by": "gostorage-test",
		},
	})
	if err != nil {
		log.Printf("✗ Failed to stream to S3: %v", err)
		return
	}
	fmt.Println("✓ File streamed to S3: test/stream.txt")

	// Test metadata
	meta, err := storage.GetMetadata(ctx, "s3", "test/stream.txt")
	if err != nil {
		log.Printf("✗ Failed to get metadata: %v", err)
	} else if meta != nil {
		fmt.Printf("✓ S3 Content-Type: %s\n", meta.ContentType)
		fmt.Printf("✓ S3 Custom headers: %v\n", meta.CustomHeaders)
	}

	// Test copy
	err = storage.Copy(ctx, "s3", "test/hello-s3.txt", "test/hello-s3-copy.txt")
	if err != nil {
		log.Printf("✗ Failed to copy on S3: %v", err)
		return
	}
	fmt.Println("✓ File copied on S3: test/hello-s3.txt → test/hello-s3-copy.txt")

	// Test list
	testFiles, err := storage.List(ctx, "s3", "test")
	if err != nil {
		log.Printf("✗ Failed to list S3 files: %v", err)
		return
	}
	fmt.Printf("✓ Files found on S3 with prefix 'test': %d\n", len(testFiles))
	for _, file := range testFiles {
		fmt.Printf("  - %s (%d bytes)\n", file.Path, file.Size)
	}

	// Clean up
	fmt.Println("Cleaning up S3 test files...")
	_ = storage.Delete(ctx, "s3", "test/hello-s3.txt")
	_ = storage.Delete(ctx, "s3", "test/hello-s3-copy.txt")
	_ = storage.Delete(ctx, "s3", "test/stream.txt")
	fmt.Println("✓ S3 test files cleaned up")
}

func crossDiskOperations(ctx context.Context, storage *gostorage.Storage) {
	fmt.Println("Testing cross-disk operations between Local and S3...")

	// Create a file on local
	content := []byte("Cross-disk transfer test!")
	err := storage.Put(ctx, "local", "cross-disk/source.txt", content)
	if err != nil {
		log.Printf("✗ Failed to create local file: %v", err)
		return
	}
	fmt.Println("✓ Created file on local disk: cross-disk/source.txt")

	// Copy from local to S3
	err = storage.CopyBetweenDisks(ctx, "local", "s3", "cross-disk/source.txt", "cross-disk/from-local.txt")
	if err != nil {
		log.Printf("✗ Failed to copy from local to S3: %v", err)
		return
	}
	fmt.Println("✓ Copied file from local to S3: cross-disk/from-local.txt")

	// Verify the file exists on S3
	exists, err := storage.Exists(ctx, "s3", "cross-disk/from-local.txt")
	if err != nil {
		log.Printf("✗ Failed to check S3 file: %v", err)
		return
	}
	fmt.Printf("✓ File exists on S3: %v\n", exists)

	// Create a file on S3
	err = storage.Put(ctx, "s3", "cross-disk/s3-source.txt", []byte("From S3 to Local!"))
	if err != nil {
		log.Printf("✗ Failed to create S3 file: %v", err)
		return
	}
	fmt.Println("✓ Created file on S3: cross-disk/s3-source.txt")

	// Move from S3 to local
	err = storage.MoveBetweenDisks(ctx, "s3", "local", "cross-disk/s3-source.txt", "cross-disk/from-s3.txt")
	if err != nil {
		log.Printf("✗ Failed to move from S3 to local: %v", err)
		return
	}
	fmt.Println("✓ Moved file from S3 to local: cross-disk/from-s3.txt")

	// Verify file exists on local
	localExists, _ := storage.Exists(ctx, "local", "cross-disk/from-s3.txt")
	fmt.Printf("✓ File exists on local: %v\n", localExists)

	// Verify file no longer exists on S3
	s3Exists, _ := storage.Exists(ctx, "s3", "cross-disk/s3-source.txt")
	fmt.Printf("✓ File removed from S3 after move: %v\n", !s3Exists)

	// Clean up
	fmt.Println("Cleaning up cross-disk test files...")
	_ = storage.Delete(ctx, "local", "cross-disk/source.txt")
	_ = storage.Delete(ctx, "local", "cross-disk/from-s3.txt")
	_ = storage.Delete(ctx, "s3", "cross-disk/from-local.txt")
	fmt.Println("✓ Cross-disk test files cleaned up")
}

