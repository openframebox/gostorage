package gostorage

import (
	"context"
	"io"
	"time"
)

// Metadata represents file metadata
type Metadata struct {
	ContentType   string
	Size          int64
	LastModified  time.Time
	CustomHeaders map[string]string
}

// FileInfo represents file information
type FileInfo struct {
	Path         string
	Size         int64
	LastModified time.Time
	IsDir        bool
	Metadata     *Metadata
}

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
