# GoStorage

A flexible, extensible storage abstraction layer for Go that supports multiple backends including local filesystem and AWS S3.

## Features

- **Multiple Storage Backends**
  - Local filesystem storage
  - AWS S3 storage
  - Easy to extend with custom backends

- **Rich File Operations**
  - Basic operations: Put, Get, Delete
  - Streaming support for large files
  - File management: Copy, Move, List, Exists, Size
  - Metadata handling with custom headers

- **Security**
  - Path validation to prevent directory traversal attacks
  - Sanitization of file paths
  - Protection against malicious path patterns

- **Developer Friendly**
  - Context support for all operations
  - Comprehensive error handling
  - Clean, idiomatic Go API

## Installation

```bash
go get github.com/openframebox/gostorage
```

All dependencies (including AWS SDK v2 for S3/MinIO support) are automatically handled by Go modules.

## Quick Start

### Local Filesystem Storage

```go
package main

import (
    "context"
    "log"
    "github.com/openframebox/gostorage"
)

func main() {
    ctx := context.Background()
    storage := gostorage.NewStorage()

    // Add a local disk - Simple configuration!
    localDisk, err := gostorage.NewLocalDisk(&gostorage.LocalDiskConfig{
        Path: "./storage",
    })
    if err != nil {
        log.Fatal(err)
    }
    storage.AddDisk("local", localDisk)

    // Put a file
    content := []byte("Hello, World!")
    err = storage.Put(ctx, "local", "hello.txt", content)
    if err != nil {
        panic(err)
    }

    // Get a file
    data, err := storage.Get(ctx, "local", "hello.txt")
    if err != nil {
        panic(err)
    }
    println(string(data)) // Output: Hello, World!

    // Delete a file
    err = storage.Delete(ctx, "local", "hello.txt")
    if err != nil {
        panic(err)
    }
}
```

### AWS S3 Storage

```go
package main

import (
    "context"
    "log"
    "github.com/openframebox/gostorage"
)

func main() {
    ctx := context.Background()
    storage := gostorage.NewStorage()

    // Configure AWS S3 - Simple!
    s3Disk, err := gostorage.NewS3Disk(&gostorage.S3Config{
        Region:    "us-west-2",
        AccessKey: "your-access-key",
        SecretKey: "your-secret-key",
        Bucket:    "my-bucket",
        Prefix:    "my-prefix/", // Optional
    })
    if err != nil {
        log.Fatal(err)
    }

    storage.AddDisk("s3", s3Disk)

    // Use it just like local storage
    err = storage.Put(ctx, "s3", "document.txt", []byte("S3 content"))
    if err != nil {
        panic(err)
    }
}
```

### MinIO Storage

```go
// Configure MinIO - Just specify the endpoint!
s3Disk, err := gostorage.NewS3Disk(&gostorage.S3Config{
    Endpoint:     "https://minio.example.com",
    Region:       "us-east-1",
    AccessKey:    "minio-access-key",
    SecretKey:    "minio-secret-key",
    Bucket:       "my-bucket",
    UsePathStyle: true, // Required for MinIO
})
if err != nil {
    log.Fatal(err)
}

storage.AddDisk("minio", s3Disk)
```

## Configuration

### S3Config Reference

```go
type S3Config struct {
    // Endpoint is the S3 endpoint URL
    // For AWS S3: leave empty (uses default)
    // For MinIO: "https://minio.example.com"
    Endpoint string

    // Region is the AWS region (default: "us-east-1")
    Region string

    // AccessKey is the AWS access key ID or MinIO access key
    AccessKey string

    // SecretKey is the AWS secret access key or MinIO secret key
    SecretKey string

    // Bucket is the S3 bucket name (required)
    Bucket string

    // Prefix is an optional prefix for all keys (useful for multi-tenancy)
    // Example: "tenant1/" will prefix all paths
    Prefix string

    // UsePathStyle forces path-style addressing (required for MinIO)
    // true:  https://endpoint/bucket/key
    // false: https://bucket.endpoint/key (default for AWS S3)
    UsePathStyle bool

    // SessionToken is optional for temporary AWS credentials
    SessionToken string
}
```

### LocalDiskConfig Reference

```go
type LocalDiskConfig struct {
    // Path is the base directory for file storage (required)
    Path string

    // CreateIfNotExist will create the directory if it doesn't exist
    // Default: true
    CreateIfNotExist bool

    // Permissions for created directories
    // Default: 0755
    DirPermissions os.FileMode

    // Permissions for created files
    // Default: 0644
    FilePermissions os.FileMode
}
```

**Example with custom permissions:**
```go
localDisk, err := gostorage.NewLocalDisk(&gostorage.LocalDiskConfig{
    Path:            "/var/app/storage",
    DirPermissions:  0750, // rwxr-x---
    FilePermissions: 0640, // rw-r-----
})
```

## API Reference

### Storage Manager

```go
// Create a new storage manager
storage := gostorage.NewStorage()

// Manage disks
storage.AddDisk(name string, disk Disk)
storage.RemoveDisk(name string)
storage.HasDisk(name string) bool
storage.DiskNames() []string
```

### Basic Operations

```go
// Put a file
err := storage.Put(ctx, "disk", "path/to/file.txt", content)

// Get a file
data, err := storage.Get(ctx, "disk", "path/to/file.txt")

// Delete a file
err := storage.Delete(ctx, "disk", "path/to/file.txt")

// Check if file exists
exists, err := storage.Exists(ctx, "disk", "path/to/file.txt")

// Get file size
size, err := storage.Size(ctx, "disk", "path/to/file.txt")
```

### Streaming Operations

For large files, use streaming to avoid loading everything into memory:

```go
// Upload large file using streaming
file, _ := os.Open("large-file.mp4")
defer file.Close()

err := storage.PutStream(ctx, "disk", "videos/large.mp4", file, &gostorage.Metadata{
    ContentType: "video/mp4",
})

// Download large file using streaming
reader, err := storage.GetStream(ctx, "disk", "videos/large.mp4")
if err != nil {
    panic(err)
}
defer reader.Close()

// Process the stream
io.Copy(outputFile, reader)
```

### File Operations

```go
// Copy a file within the same disk
err := storage.Copy(ctx, "disk", "source.txt", "destination.txt")

// Move a file within the same disk
err := storage.Move(ctx, "disk", "old-path.txt", "new-path.txt")

// Copy between different disks
err := storage.CopyBetweenDisks(ctx, "local", "s3", "local-file.txt", "s3-file.txt")

// Move between different disks
err := storage.MoveBetweenDisks(ctx, "local", "s3", "local-file.txt", "s3-file.txt")

// List files with a prefix
files, err := storage.List(ctx, "disk", "documents/")
for _, file := range files {
    fmt.Printf("%s - %d bytes\n", file.Path, file.Size)
}
```

### Metadata Operations

Store custom metadata with your files:

```go
// Put file with metadata
metadata := &gostorage.Metadata{
    ContentType: "application/json",
    CustomHeaders: map[string]string{
        "author":  "John Doe",
        "version": "1.0",
    },
}
err := storage.PutWithMetadata(ctx, "disk", "data.json", content, metadata)

// Get metadata
meta, err := storage.GetMetadata(ctx, "disk", "data.json")
if meta != nil {
    fmt.Println("Content-Type:", meta.ContentType)
    fmt.Println("Author:", meta.CustomHeaders["author"])
}

// Update metadata
newMeta := &gostorage.Metadata{
    ContentType: "application/json",
    CustomHeaders: map[string]string{
        "author":  "Jane Doe",
        "version": "2.0",
    },
}
err = storage.SetMetadata(ctx, "disk", "data.json", newMeta)
```

## File Information

The `List` operation returns detailed file information:

```go
type FileInfo struct {
    Path         string      // File path
    Size         int64       // File size in bytes
    LastModified time.Time   // Last modification time
    IsDir        bool        // Whether it's a directory
    Metadata     *Metadata   // File metadata (if available)
}
```

## Error Handling

The package provides structured errors:

```go
data, err := storage.Get(ctx, "disk", "file.txt")
if err != nil {
    var pathErr *gostorage.PathError
    if errors.As(err, &pathErr) {
        fmt.Printf("Operation: %s, Path: %s, Error: %v\n",
            pathErr.Op, pathErr.Path, pathErr.Err)
    }

    if errors.Is(err, gostorage.ErrFileNotFound) {
        fmt.Println("File not found")
    }
}
```

Available errors:
- `ErrFileNotFound` - File doesn't exist
- `ErrInvalidPath` - Invalid path provided
- `ErrOperationNotSupported` - Operation not supported by disk
- `DiskNotFoundError` - Disk not found

## Security

All paths are automatically validated and sanitized to prevent:
- Directory traversal attacks (e.g., `../../../etc/passwd`)
- Null byte injection
- Invalid path characters

Paths are automatically converted to relative paths and cleaned.

## Creating Custom Disk Backends

Implement the `Disk` interface to create custom backends:

```go
type Disk interface {
    // Basic operations
    put(ctx context.Context, path string, content []byte) error
    get(ctx context.Context, path string) ([]byte, error)
    delete(ctx context.Context, path string) error

    // Streaming operations
    putStream(ctx context.Context, path string, reader io.Reader, metadata *Metadata) error
    getStream(ctx context.Context, path string) (io.ReadCloser, error)

    // File operations
    exists(ctx context.Context, path string) (bool, error)
    size(ctx context.Context, path string) (int64, error)
    list(ctx context.Context, prefix string) ([]FileInfo, error)
    copy(ctx context.Context, sourcePath, destPath string) error
    move(ctx context.Context, sourcePath, destPath string) error

    // Metadata operations
    putWithMetadata(ctx context.Context, path string, content []byte, metadata *Metadata) error
    getMetadata(ctx context.Context, path string) (*Metadata, error)
    setMetadata(ctx context.Context, path string, metadata *Metadata) error
}
```

## Examples

See the [example](./example/main.go) directory for comprehensive usage examples including:
- Basic file operations
- Streaming large files
- Working with metadata
- Copying and moving files
- Listing files with prefixes

Run the example:
```bash
cd example
go run main.go
```

## Use Cases

- **Multi-cloud applications**: Abstract storage to easily switch between local and cloud storage
- **File upload services**: Handle user uploads with consistent API regardless of backend
- **Backup systems**: Copy files between local and cloud storage
- **Content management**: Store and retrieve files with metadata
- **Large file processing**: Stream large files without memory constraints

## License

MIT

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.
