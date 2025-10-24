package gostorage

import (
	"errors"
	"fmt"
)

var (
	// ErrFileNotFound is returned when a file doesn't exist
	ErrFileNotFound = errors.New("file not found")

	// ErrInvalidPath is returned when a path is invalid
	ErrInvalidPath = errors.New("invalid path")

	// ErrOperationNotSupported is returned when an operation is not supported
	ErrOperationNotSupported = errors.New("operation not supported")
)

// DiskNotFoundError represents a disk not found error
type DiskNotFoundError struct {
	DiskName string
}

func (e *DiskNotFoundError) Error() string {
	return fmt.Sprintf("disk not found: %s", e.DiskName)
}

func ErrDiskNotFound(name string) error {
	return &DiskNotFoundError{DiskName: name}
}

// PathError represents a path-related error
type PathError struct {
	Op   string
	Path string
	Err  error
}

func (e *PathError) Error() string {
	return fmt.Sprintf("%s %s: %v", e.Op, e.Path, e.Err)
}

func (e *PathError) Unwrap() error {
	return e.Err
}
