package gostorage

import (
	"context"
	"io"
)

type Storage struct {
	Disks map[string]Disk
}

func NewStorage() *Storage {
	return &Storage{
		Disks: make(map[string]Disk),
	}
}

func (s *Storage) AddDisk(name string, disk Disk) {
	s.Disks[name] = disk
}

// Basic operations

func (s *Storage) Put(ctx context.Context, disk string, path string, content []byte) error {
	d := s.getDisk(disk)
	if d == nil {
		return ErrDiskNotFound(disk)
	}

	return d.put(ctx, path, content)
}

func (s *Storage) Get(ctx context.Context, disk string, path string) ([]byte, error) {
	d := s.getDisk(disk)
	if d == nil {
		return nil, ErrDiskNotFound(disk)
	}

	return d.get(ctx, path)
}

func (s *Storage) Delete(ctx context.Context, disk string, path string) error {
	d := s.getDisk(disk)
	if d == nil {
		return ErrDiskNotFound(disk)
	}

	return d.delete(ctx, path)
}

// Streaming operations

func (s *Storage) PutStream(ctx context.Context, disk string, path string, reader io.Reader, metadata *Metadata) error {
	d := s.getDisk(disk)
	if d == nil {
		return ErrDiskNotFound(disk)
	}

	return d.putStream(ctx, path, reader, metadata)
}

func (s *Storage) GetStream(ctx context.Context, disk string, path string) (io.ReadCloser, error) {
	d := s.getDisk(disk)
	if d == nil {
		return nil, ErrDiskNotFound(disk)
	}

	return d.getStream(ctx, path)
}

// File operations

func (s *Storage) Exists(ctx context.Context, disk string, path string) (bool, error) {
	d := s.getDisk(disk)
	if d == nil {
		return false, ErrDiskNotFound(disk)
	}

	return d.exists(ctx, path)
}

func (s *Storage) Size(ctx context.Context, disk string, path string) (int64, error) {
	d := s.getDisk(disk)
	if d == nil {
		return 0, ErrDiskNotFound(disk)
	}

	return d.size(ctx, path)
}

func (s *Storage) List(ctx context.Context, disk string, prefix string) ([]FileInfo, error) {
	d := s.getDisk(disk)
	if d == nil {
		return nil, ErrDiskNotFound(disk)
	}

	return d.list(ctx, prefix)
}

func (s *Storage) Copy(ctx context.Context, disk string, sourcePath, destPath string) error {
	d := s.getDisk(disk)
	if d == nil {
		return ErrDiskNotFound(disk)
	}

	return d.copy(ctx, sourcePath, destPath)
}

func (s *Storage) Move(ctx context.Context, disk string, sourcePath, destPath string) error {
	d := s.getDisk(disk)
	if d == nil {
		return ErrDiskNotFound(disk)
	}

	return d.move(ctx, sourcePath, destPath)
}

// Cross-disk operations

func (s *Storage) CopyBetweenDisks(ctx context.Context, sourceDisk, destDisk, sourcePath, destPath string) error {
	src := s.getDisk(sourceDisk)
	if src == nil {
		return ErrDiskNotFound(sourceDisk)
	}

	dst := s.getDisk(destDisk)
	if dst == nil {
		return ErrDiskNotFound(destDisk)
	}

	// Read from source
	content, err := src.get(ctx, sourcePath)
	if err != nil {
		return err
	}

	// Get metadata if available
	metadata, _ := src.getMetadata(ctx, sourcePath)

	// Write to destination
	if metadata != nil {
		return dst.putWithMetadata(ctx, destPath, content, metadata)
	}
	return dst.put(ctx, destPath, content)
}

func (s *Storage) MoveBetweenDisks(ctx context.Context, sourceDisk, destDisk, sourcePath, destPath string) error {
	// Copy between disks
	if err := s.CopyBetweenDisks(ctx, sourceDisk, destDisk, sourcePath, destPath); err != nil {
		return err
	}

	// Delete from source
	return s.Delete(ctx, sourceDisk, sourcePath)
}

// Metadata operations

func (s *Storage) PutWithMetadata(ctx context.Context, disk string, path string, content []byte, metadata *Metadata) error {
	d := s.getDisk(disk)
	if d == nil {
		return ErrDiskNotFound(disk)
	}

	return d.putWithMetadata(ctx, path, content, metadata)
}

func (s *Storage) GetMetadata(ctx context.Context, disk string, path string) (*Metadata, error) {
	d := s.getDisk(disk)
	if d == nil {
		return nil, ErrDiskNotFound(disk)
	}

	return d.getMetadata(ctx, path)
}

func (s *Storage) SetMetadata(ctx context.Context, disk string, path string, metadata *Metadata) error {
	d := s.getDisk(disk)
	if d == nil {
		return ErrDiskNotFound(disk)
	}

	return d.setMetadata(ctx, path, metadata)
}

// Helper methods

func (s *Storage) getDisk(name string) Disk {
	return s.Disks[name]
}

func (s *Storage) HasDisk(name string) bool {
	_, exists := s.Disks[name]
	return exists
}

func (s *Storage) RemoveDisk(name string) {
	delete(s.Disks, name)
}

func (s *Storage) DiskNames() []string {
	names := make([]string, 0, len(s.Disks))
	for name := range s.Disks {
		names = append(names, name)
	}
	return names
}
