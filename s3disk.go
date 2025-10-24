package gostorage

import (
	"bytes"
	"context"
	"errors"
	"io"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

// S3Config contains configuration for S3/MinIO storage
type S3Config struct {
	// Endpoint is the S3 endpoint URL (e.g., "https://s3.amazonaws.com" or "https://minio.example.com")
	// Leave empty for AWS S3 default endpoint
	Endpoint string

	// Region is the AWS region (e.g., "us-east-1")
	Region string

	// AccessKey is the AWS access key ID or MinIO access key
	AccessKey string

	// SecretKey is the AWS secret access key or MinIO secret key
	SecretKey string

	// Bucket is the S3 bucket name
	Bucket string

	// Prefix is an optional prefix for all keys (useful for multi-tenancy)
	Prefix string

	// UsePathStyle forces path-style addressing (required for MinIO)
	UsePathStyle bool

	// SessionToken is optional for temporary credentials
	SessionToken string
}

// S3Disk implements Disk interface for AWS S3
type S3Disk struct {
	client *s3.Client
	config *S3Config
}

// NewS3Disk creates a new S3Disk with the given configuration
func NewS3Disk(cfg *S3Config) (*S3Disk, error) {
	if cfg == nil {
		return nil, errors.New("S3Config cannot be nil")
	}

	if cfg.Bucket == "" {
		return nil, errors.New("bucket name is required")
	}

	if cfg.Region == "" {
		cfg.Region = "us-east-1" // Default region
	}

	// Load AWS config
	awsConfig, err := config.LoadDefaultConfig(context.Background(),
		config.WithRegion(cfg.Region),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(
			cfg.AccessKey,
			cfg.SecretKey,
			cfg.SessionToken,
		)),
	)
	if err != nil {
		return nil, err
	}

	// Create S3 client with custom endpoint if provided
	s3Client := s3.NewFromConfig(awsConfig, func(o *s3.Options) {
		if cfg.Endpoint != "" {
			o.BaseEndpoint = aws.String(cfg.Endpoint)
		}
		o.UsePathStyle = cfg.UsePathStyle
		o.Region = cfg.Region
	})

	return &S3Disk{
		client: s3Client,
		config: cfg,
	}, nil
}

// buildKey constructs the full S3 key with prefix
func (d *S3Disk) buildKey(path string) string {
	if d.config.Prefix == "" {
		return path
	}
	return strings.TrimSuffix(d.config.Prefix, "/") + "/" + strings.TrimPrefix(path, "/")
}

// put writes content to S3
func (d *S3Disk) put(ctx context.Context, path string, content []byte) error {
	// Validate path
	validPath, err := ValidatePath(path)
	if err != nil {
		return &PathError{Op: "put", Path: path, Err: err}
	}

	key := d.buildKey(validPath)

	_, err = d.client.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(d.config.Bucket),
		Key:    aws.String(key),
		Body:   bytes.NewReader(content),
	})

	if err != nil {
		return &PathError{Op: "put", Path: path, Err: err}
	}

	return nil
}

// get reads content from S3
func (d *S3Disk) get(ctx context.Context, path string) ([]byte, error) {
	// Validate path
	validPath, err := ValidatePath(path)
	if err != nil {
		return nil, &PathError{Op: "get", Path: path, Err: err}
	}

	key := d.buildKey(validPath)

	result, err := d.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(d.config.Bucket),
		Key:    aws.String(key),
	})

	if err != nil {
		var nsk *types.NoSuchKey
		if errors.As(err, &nsk) {
			return nil, &PathError{Op: "get", Path: path, Err: ErrFileNotFound}
		}
		return nil, &PathError{Op: "get", Path: path, Err: err}
	}
	defer result.Body.Close()

	// Read all content
	content, err := io.ReadAll(result.Body)
	if err != nil {
		return nil, &PathError{Op: "get", Path: path, Err: err}
	}

	return content, nil
}

// delete removes an object from S3
func (d *S3Disk) delete(ctx context.Context, path string) error {
	// Validate path
	validPath, err := ValidatePath(path)
	if err != nil {
		return &PathError{Op: "delete", Path: path, Err: err}
	}

	key := d.buildKey(validPath)

	_, err = d.client.DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket: aws.String(d.config.Bucket),
		Key:    aws.String(key),
	})

	if err != nil {
		return &PathError{Op: "delete", Path: path, Err: err}
	}

	return nil
}

// putStream writes content from a reader to S3
func (d *S3Disk) putStream(ctx context.Context, path string, reader io.Reader, metadata *Metadata) error {
	// Validate path
	validPath, err := ValidatePath(path)
	if err != nil {
		return &PathError{Op: "putStream", Path: path, Err: err}
	}

	key := d.buildKey(validPath)

	input := &s3.PutObjectInput{
		Bucket: aws.String(d.config.Bucket),
		Key:    aws.String(key),
		Body:   reader,
	}

	// Add metadata if provided
	if metadata != nil {
		if metadata.ContentType != "" {
			input.ContentType = aws.String(metadata.ContentType)
		}
		if len(metadata.CustomHeaders) > 0 {
			input.Metadata = metadata.CustomHeaders
		}
	}

	_, err = d.client.PutObject(ctx, input)
	if err != nil {
		return &PathError{Op: "putStream", Path: path, Err: err}
	}

	return nil
}

// getStream returns a reader for S3 object content
func (d *S3Disk) getStream(ctx context.Context, path string) (io.ReadCloser, error) {
	// Validate path
	validPath, err := ValidatePath(path)
	if err != nil {
		return nil, &PathError{Op: "getStream", Path: path, Err: err}
	}

	key := d.buildKey(validPath)

	result, err := d.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(d.config.Bucket),
		Key:    aws.String(key),
	})

	if err != nil {
		var nsk *types.NoSuchKey
		if errors.As(err, &nsk) {
			return nil, &PathError{Op: "getStream", Path: path, Err: ErrFileNotFound}
		}
		return nil, &PathError{Op: "getStream", Path: path, Err: err}
	}

	return result.Body, nil
}

// exists checks if an object exists in S3
func (d *S3Disk) exists(ctx context.Context, path string) (bool, error) {
	// Validate path
	validPath, err := ValidatePath(path)
	if err != nil {
		return false, &PathError{Op: "exists", Path: path, Err: err}
	}

	key := d.buildKey(validPath)

	_, err = d.client.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: aws.String(d.config.Bucket),
		Key:    aws.String(key),
	})

	if err != nil {
		var nsk *types.NoSuchKey
		var notFound *types.NotFound
		if errors.As(err, &nsk) || errors.As(err, &notFound) {
			return false, nil
		}
		return false, &PathError{Op: "exists", Path: path, Err: err}
	}

	return true, nil
}

// size returns the size of an S3 object
func (d *S3Disk) size(ctx context.Context, path string) (int64, error) {
	// Validate path
	validPath, err := ValidatePath(path)
	if err != nil {
		return 0, &PathError{Op: "size", Path: path, Err: err}
	}

	key := d.buildKey(validPath)

	result, err := d.client.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: aws.String(d.config.Bucket),
		Key:    aws.String(key),
	})

	if err != nil {
		var nsk *types.NoSuchKey
		var notFound *types.NotFound
		if errors.As(err, &nsk) || errors.As(err, &notFound) {
			return 0, &PathError{Op: "size", Path: path, Err: ErrFileNotFound}
		}
		return 0, &PathError{Op: "size", Path: path, Err: err}
	}

	return aws.ToInt64(result.ContentLength), nil
}

// list returns a list of objects matching a prefix
func (d *S3Disk) list(ctx context.Context, prefix string) ([]FileInfo, error) {
	// Validate prefix
	validPrefix, err := ValidatePrefix(prefix)
	if err != nil {
		return nil, &PathError{Op: "list", Path: prefix, Err: err}
	}

	key := d.buildKey(validPrefix)

	var files []FileInfo
	paginator := s3.NewListObjectsV2Paginator(d.client, &s3.ListObjectsV2Input{
		Bucket: aws.String(d.config.Bucket),
		Prefix: aws.String(key),
	})

	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			return nil, &PathError{Op: "list", Path: prefix, Err: err}
		}

		for _, obj := range page.Contents {
			objKey := aws.ToString(obj.Key)

			// Remove prefix if present
			if d.config.Prefix != "" {
				objKey = strings.TrimPrefix(objKey, strings.TrimSuffix(d.config.Prefix, "/")+"/")
			}

			files = append(files, FileInfo{
				Path:         objKey,
				Size:         aws.ToInt64(obj.Size),
				LastModified: aws.ToTime(obj.LastModified),
				IsDir:        false,
			})
		}
	}

	return files, nil
}

// copy copies an object within S3
func (d *S3Disk) copy(ctx context.Context, sourcePath, destPath string) error {
	// Validate paths
	validSource, err := ValidatePath(sourcePath)
	if err != nil {
		return &PathError{Op: "copy", Path: sourcePath, Err: err}
	}

	validDest, err := ValidatePath(destPath)
	if err != nil {
		return &PathError{Op: "copy", Path: destPath, Err: err}
	}

	sourceKey := d.buildKey(validSource)
	destKey := d.buildKey(validDest)

	_, err = d.client.CopyObject(ctx, &s3.CopyObjectInput{
		Bucket:     aws.String(d.config.Bucket),
		CopySource: aws.String(d.config.Bucket + "/" + sourceKey),
		Key:        aws.String(destKey),
	})

	if err != nil {
		return &PathError{Op: "copy", Path: sourcePath, Err: err}
	}

	return nil
}

// move moves an object within S3
func (d *S3Disk) move(ctx context.Context, sourcePath, destPath string) error {
	// Copy the object
	if err := d.copy(ctx, sourcePath, destPath); err != nil {
		return err
	}

	// Delete the source
	return d.delete(ctx, sourcePath)
}

// putWithMetadata writes content and metadata to S3
func (d *S3Disk) putWithMetadata(ctx context.Context, path string, content []byte, metadata *Metadata) error {
	// Validate path
	validPath, err := ValidatePath(path)
	if err != nil {
		return &PathError{Op: "putWithMetadata", Path: path, Err: err}
	}

	key := d.buildKey(validPath)

	input := &s3.PutObjectInput{
		Bucket: aws.String(d.config.Bucket),
		Key:    aws.String(key),
		Body:   bytes.NewReader(content),
	}

	// Add metadata
	if metadata != nil {
		if metadata.ContentType != "" {
			input.ContentType = aws.String(metadata.ContentType)
		}
		if len(metadata.CustomHeaders) > 0 {
			input.Metadata = metadata.CustomHeaders
		}
	}

	_, err = d.client.PutObject(ctx, input)
	if err != nil {
		return &PathError{Op: "putWithMetadata", Path: path, Err: err}
	}

	return nil
}

// getMetadata retrieves metadata for an S3 object
func (d *S3Disk) getMetadata(ctx context.Context, path string) (*Metadata, error) {
	// Validate path
	validPath, err := ValidatePath(path)
	if err != nil {
		return nil, &PathError{Op: "getMetadata", Path: path, Err: err}
	}

	key := d.buildKey(validPath)

	result, err := d.client.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: aws.String(d.config.Bucket),
		Key:    aws.String(key),
	})

	if err != nil {
		var nsk *types.NoSuchKey
		var notFound *types.NotFound
		if errors.As(err, &nsk) || errors.As(err, &notFound) {
			return nil, &PathError{Op: "getMetadata", Path: path, Err: ErrFileNotFound}
		}
		return nil, &PathError{Op: "getMetadata", Path: path, Err: err}
	}

	metadata := &Metadata{
		ContentType:   aws.ToString(result.ContentType),
		Size:          aws.ToInt64(result.ContentLength),
		LastModified:  aws.ToTime(result.LastModified),
		CustomHeaders: result.Metadata,
	}

	return metadata, nil
}

// setMetadata updates metadata for an S3 object
func (d *S3Disk) setMetadata(ctx context.Context, path string, metadata *Metadata) error {
	// Validate path
	validPath, err := ValidatePath(path)
	if err != nil {
		return &PathError{Op: "setMetadata", Path: path, Err: err}
	}

	key := d.buildKey(validPath)

	// S3 requires copying the object to update metadata
	input := &s3.CopyObjectInput{
		Bucket:            aws.String(d.config.Bucket),
		CopySource:        aws.String(d.config.Bucket + "/" + key),
		Key:               aws.String(key),
		MetadataDirective: types.MetadataDirectiveReplace,
	}

	if metadata != nil {
		if metadata.ContentType != "" {
			input.ContentType = aws.String(metadata.ContentType)
		}
		if len(metadata.CustomHeaders) > 0 {
			input.Metadata = metadata.CustomHeaders
		}
	}

	_, err = d.client.CopyObject(ctx, input)
	if err != nil {
		return &PathError{Op: "setMetadata", Path: path, Err: err}
	}

	return nil
}
