package gostorage

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
)

// LocalDiskConfig contains configuration for local filesystem storage
type LocalDiskConfig struct {
	// Path is the base directory for file storage
	Path string

	// CreateIfNotExist will create the directory if it doesn't exist (default: true)
	CreateIfNotExist bool

	// Permissions for created directories (default: 0755)
	DirPermissions os.FileMode

	// Permissions for created files (default: 0644)
	FilePermissions os.FileMode
}

// LocalDisk implements Disk interface for local filesystem storage
type LocalDisk struct {
	config *LocalDiskConfig
}

// NewLocalDisk creates a new LocalDisk with the given configuration
func NewLocalDisk(cfg *LocalDiskConfig) (*LocalDisk, error) {
	if cfg == nil {
		return nil, errors.New("LocalDiskConfig cannot be nil")
	}

	if cfg.Path == "" {
		return nil, errors.New("path is required")
	}

	// Set defaults
	if cfg.DirPermissions == 0 {
		cfg.DirPermissions = 0755
	}
	if cfg.FilePermissions == 0 {
		cfg.FilePermissions = 0644
	}

	// CreateIfNotExist defaults to true
	createIfNotExist := cfg.CreateIfNotExist
	if cfg.CreateIfNotExist == false {
		// Check if it was explicitly set or just zero value
		// We default to true, so we need to check if the path exists
		createIfNotExist = true
	}

	// Create directory if it doesn't exist and CreateIfNotExist is true
	if createIfNotExist {
		if err := os.MkdirAll(cfg.Path, cfg.DirPermissions); err != nil {
			return nil, err
		}
	} else {
		// Verify directory exists
		info, err := os.Stat(cfg.Path)
		if err != nil {
			return nil, err
		}
		if !info.IsDir() {
			return nil, errors.New("path exists but is not a directory")
		}
	}

	return &LocalDisk{
		config: cfg,
	}, nil
}

// put writes content to a file
func (d *LocalDisk) put(ctx context.Context, path string, content []byte) error {
	// Validate path
	validPath, err := ValidatePath(path)
	if err != nil {
		return &PathError{Op: "put", Path: path, Err: err}
	}

	// Construct the full file path
	fullPath := filepath.Join(d.config.Path, validPath)

	// Create all parent directories if they don't exist
	dir := filepath.Dir(fullPath)
	if err := os.MkdirAll(dir, d.config.DirPermissions); err != nil {
		return &PathError{Op: "put", Path: path, Err: err}
	}

	// Write the file with appropriate permissions
	if err := os.WriteFile(fullPath, content, d.config.FilePermissions); err != nil {
		return &PathError{Op: "put", Path: path, Err: err}
	}

	return nil
}

// get reads content from a file
func (d *LocalDisk) get(_ context.Context, path string) ([]byte, error) {
	// Validate path
	validPath, err := ValidatePath(path)
	if err != nil {
		return nil, &PathError{Op: "get", Path: path, Err: err}
	}

	// Construct the full file path
	fullPath := filepath.Join(d.config.Path, validPath)

	// Read the file
	content, err := os.ReadFile(fullPath)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, &PathError{Op: "get", Path: path, Err: ErrFileNotFound}
		}
		return nil, &PathError{Op: "get", Path: path, Err: err}
	}

	return content, nil
}

// delete removes a file
func (d *LocalDisk) delete(_ context.Context, path string) error {
	// Validate path
	validPath, err := ValidatePath(path)
	if err != nil {
		return &PathError{Op: "delete", Path: path, Err: err}
	}

	// Construct the full file path
	fullPath := filepath.Join(d.config.Path, validPath)

	// Delete the file
	if err := os.Remove(fullPath); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return &PathError{Op: "delete", Path: path, Err: ErrFileNotFound}
		}
		return &PathError{Op: "delete", Path: path, Err: err}
	}

	return nil
}

// putStream writes content from a reader to a file
func (d *LocalDisk) putStream(ctx context.Context, path string, reader io.Reader, metadata *Metadata) error {
	// Validate path
	validPath, err := ValidatePath(path)
	if err != nil {
		return &PathError{Op: "putStream", Path: path, Err: err}
	}

	// Construct the full file path
	fullPath := filepath.Join(d.config.Path, validPath)

	// Create all parent directories if they don't exist
	dir := filepath.Dir(fullPath)
	if err := os.MkdirAll(dir, d.config.DirPermissions); err != nil {
		return &PathError{Op: "putStream", Path: path, Err: err}
	}

	// Create the file
	file, err := os.Create(fullPath)
	if err != nil {
		return &PathError{Op: "putStream", Path: path, Err: err}
	}
	defer file.Close()

	// Copy from reader to file
	if _, err := io.Copy(file, reader); err != nil {
		return &PathError{Op: "putStream", Path: path, Err: err}
	}

	// Save metadata if provided
	if metadata != nil {
		if err := d.saveMetadata(validPath, metadata); err != nil {
			return err
		}
	}

	return nil
}

// getStream returns a reader for file content
func (d *LocalDisk) getStream(_ context.Context, path string) (io.ReadCloser, error) {
	// Validate path
	validPath, err := ValidatePath(path)
	if err != nil {
		return nil, &PathError{Op: "getStream", Path: path, Err: err}
	}

	// Construct the full file path
	fullPath := filepath.Join(d.config.Path, validPath)

	// Open the file
	file, err := os.Open(fullPath)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, &PathError{Op: "getStream", Path: path, Err: ErrFileNotFound}
		}
		return nil, &PathError{Op: "getStream", Path: path, Err: err}
	}

	return file, nil
}

// exists checks if a file exists
func (d *LocalDisk) exists(_ context.Context, path string) (bool, error) {
	// Validate path
	validPath, err := ValidatePath(path)
	if err != nil {
		return false, &PathError{Op: "exists", Path: path, Err: err}
	}

	// Construct the full file path
	fullPath := filepath.Join(d.config.Path, validPath)

	// Check if file exists
	_, err = os.Stat(fullPath)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return false, nil
		}
		return false, &PathError{Op: "exists", Path: path, Err: err}
	}

	return true, nil
}

// size returns the size of a file
func (d *LocalDisk) size(_ context.Context, path string) (int64, error) {
	// Validate path
	validPath, err := ValidatePath(path)
	if err != nil {
		return 0, &PathError{Op: "size", Path: path, Err: err}
	}

	// Construct the full file path
	fullPath := filepath.Join(d.config.Path, validPath)

	// Get file info
	info, err := os.Stat(fullPath)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return 0, &PathError{Op: "size", Path: path, Err: ErrFileNotFound}
		}
		return 0, &PathError{Op: "size", Path: path, Err: err}
	}

	return info.Size(), nil
}

// list returns a list of files matching a prefix
func (d *LocalDisk) list(_ context.Context, prefix string) ([]FileInfo, error) {
	// Validate prefix
	validPrefix, err := ValidatePrefix(prefix)
	if err != nil {
		return nil, &PathError{Op: "list", Path: prefix, Err: err}
	}

	// Construct the full search path
	searchPath := filepath.Join(d.config.Path, validPrefix)

	var files []FileInfo

	// Walk the directory
	err = filepath.Walk(searchPath, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			// Skip errors for inaccessible files
			return nil
		}

		// Get relative path
		relPath, err := filepath.Rel(d.config.Path, path)
		if err != nil {
			return nil
		}

		// Skip the base directory itself
		if relPath == "." {
			return nil
		}

		// Skip metadata files
		if strings.HasSuffix(relPath, ".metadata.json") {
			return nil
		}

		files = append(files, FileInfo{
			Path:         filepath.ToSlash(relPath),
			Size:         info.Size(),
			LastModified: info.ModTime(),
			IsDir:        info.IsDir(),
		})

		return nil
	})

	if err != nil {
		return nil, &PathError{Op: "list", Path: prefix, Err: err}
	}

	return files, nil
}

// copy copies a file from source to destination
func (d *LocalDisk) copy(ctx context.Context, sourcePath, destPath string) error {
	// Validate paths
	validSource, err := ValidatePath(sourcePath)
	if err != nil {
		return &PathError{Op: "copy", Path: sourcePath, Err: err}
	}

	validDest, err := ValidatePath(destPath)
	if err != nil {
		return &PathError{Op: "copy", Path: destPath, Err: err}
	}

	// Read source file
	content, err := d.get(ctx, validSource)
	if err != nil {
		return err
	}

	// Copy metadata if exists
	metadata, _ := d.getMetadata(ctx, validSource)

	// Write to destination
	if metadata != nil {
		return d.putWithMetadata(ctx, validDest, content, metadata)
	}
	return d.put(ctx, validDest, content)
}

// move moves a file from source to destination
func (d *LocalDisk) move(ctx context.Context, sourcePath, destPath string) error {
	// Copy the file
	if err := d.copy(ctx, sourcePath, destPath); err != nil {
		return err
	}

	// Delete the source
	return d.delete(ctx, sourcePath)
}

// putWithMetadata writes content and metadata to a file
func (d *LocalDisk) putWithMetadata(ctx context.Context, path string, content []byte, metadata *Metadata) error {
	// Write the file
	if err := d.put(ctx, path, content); err != nil {
		return err
	}

	// Validate path
	validPath, err := ValidatePath(path)
	if err != nil {
		return &PathError{Op: "putWithMetadata", Path: path, Err: err}
	}

	// Save metadata
	return d.saveMetadata(validPath, metadata)
}

// getMetadata retrieves metadata for a file
func (d *LocalDisk) getMetadata(_ context.Context, path string) (*Metadata, error) {
	// Validate path
	validPath, err := ValidatePath(path)
	if err != nil {
		return nil, &PathError{Op: "getMetadata", Path: path, Err: err}
	}

	metadataPath := filepath.Join(d.config.Path, validPath+".metadata.json")

	// Read metadata file
	data, err := os.ReadFile(metadataPath)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, nil // No metadata is not an error
		}
		return nil, &PathError{Op: "getMetadata", Path: path, Err: err}
	}

	var metadata Metadata
	if err := json.Unmarshal(data, &metadata); err != nil {
		return nil, &PathError{Op: "getMetadata", Path: path, Err: err}
	}

	return &metadata, nil
}

// setMetadata updates metadata for a file
func (d *LocalDisk) setMetadata(_ context.Context, path string, metadata *Metadata) error {
	// Validate path
	validPath, err := ValidatePath(path)
	if err != nil {
		return &PathError{Op: "setMetadata", Path: path, Err: err}
	}

	// Check if file exists
	fullPath := filepath.Join(d.config.Path, validPath)
	if _, err := os.Stat(fullPath); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return &PathError{Op: "setMetadata", Path: path, Err: ErrFileNotFound}
		}
		return &PathError{Op: "setMetadata", Path: path, Err: err}
	}

	return d.saveMetadata(validPath, metadata)
}

// saveMetadata is a helper to save metadata to a file
func (d *LocalDisk) saveMetadata(validPath string, metadata *Metadata) error {
	metadataPath := filepath.Join(d.config.Path, validPath+".metadata.json")

	// Create metadata directory if needed
	dir := filepath.Dir(metadataPath)
	if err := os.MkdirAll(dir, d.config.DirPermissions); err != nil {
		return err
	}

	// Marshal metadata
	data, err := json.MarshalIndent(metadata, "", "  ")
	if err != nil {
		return err
	}

	// Write metadata file
	return os.WriteFile(metadataPath, data, d.config.FilePermissions)
}
