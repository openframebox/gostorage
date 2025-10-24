package gostorage

import (
	"fmt"
	"path/filepath"
	"strings"
)

// ValidatePath validates and sanitizes a file path to prevent directory traversal attacks
func ValidatePath(path string) (string, error) {
	if path == "" {
		return "", fmt.Errorf("path cannot be empty")
	}

	// Clean the path to remove any '..' or '.' components
	cleaned := filepath.Clean(path)

	// Ensure the path doesn't try to escape the base directory
	if strings.HasPrefix(cleaned, "..") || strings.Contains(cleaned, "/../") {
		return "", fmt.Errorf("invalid path: directory traversal detected")
	}

	// Remove leading slash to ensure relative paths
	cleaned = strings.TrimPrefix(cleaned, "/")
	cleaned = strings.TrimPrefix(cleaned, "\\")

	// Check for null bytes which could be used for path manipulation
	if strings.Contains(cleaned, "\x00") {
		return "", fmt.Errorf("invalid path: null byte detected")
	}

	return cleaned, nil
}

// ValidatePrefix validates a prefix for listing operations
func ValidatePrefix(prefix string) (string, error) {
	if prefix == "" {
		return "", nil
	}

	// Clean the prefix
	cleaned := filepath.Clean(prefix)

	// Ensure the prefix doesn't try to escape the base directory
	if strings.HasPrefix(cleaned, "..") || strings.Contains(cleaned, "/../") {
		return "", fmt.Errorf("invalid prefix: directory traversal detected")
	}

	// Remove leading slash to ensure relative paths
	cleaned = strings.TrimPrefix(cleaned, "/")
	cleaned = strings.TrimPrefix(cleaned, "\\")

	return cleaned, nil
}
