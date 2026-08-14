package rgw

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

var ErrNotConfigured = errors.New("S3/RGW is not configured")

type Client struct {
	s3 *s3.Client
}

type ObjectInfo struct {
	Key          string
	LastModified *time.Time
	Size         int64
}

type MultipartInfo struct {
	Key       string
	UploadID  string
	Initiated *time.Time
}

func New(endpoint, accessKey, secretKey string) *Client {
	endpoint = strings.TrimRight(strings.TrimSpace(endpoint), "/")
	if endpoint == "" || accessKey == "" || secretKey == "" {
		return nil
	}
	cli := s3.NewFromConfig(aws.Config{
		Region:      "us-east-1",
		Credentials: credentials.NewStaticCredentialsProvider(accessKey, secretKey, ""),
	}, func(o *s3.Options) {
		o.BaseEndpoint = aws.String(endpoint)
		o.UsePathStyle = true
	})
	return &Client{s3: cli}
}

type BucketInfo struct {
	Name         string
	CreationDate *time.Time
}

func (c *Client) ListBuckets(ctx context.Context) ([]BucketInfo, error) {
	if c == nil {
		return nil, ErrNotConfigured
	}
	out, err := c.s3.ListBuckets(ctx, &s3.ListBucketsInput{})
	if err != nil {
		return nil, err
	}
	items := make([]BucketInfo, 0, len(out.Buckets))
	for _, b := range out.Buckets {
		if b.Name == nil {
			continue
		}
		items = append(items, BucketInfo{Name: *b.Name, CreationDate: b.CreationDate})
	}
	return items, nil
}

func (c *Client) GetBucketPolicy(ctx context.Context, bucket string) (map[string]any, error) {
	if c == nil {
		return nil, ErrNotConfigured
	}
	out, err := c.s3.GetBucketPolicy(ctx, &s3.GetBucketPolicyInput{Bucket: aws.String(bucket)})
	if err != nil {
		// ponytail: RGW often returns NoSuchBucketPolicy as a string code, not a typed error.
		if strings.Contains(err.Error(), "NoSuchBucketPolicy") {
			return nil, nil
		}
		return nil, err
	}
	if out.Policy == nil || *out.Policy == "" {
		return nil, nil
	}
	var policy map[string]any
	if err := json.Unmarshal([]byte(*out.Policy), &policy); err != nil {
		return nil, err
	}
	return policy, nil
}

func (c *Client) PutBucketPolicy(ctx context.Context, bucket string, policy map[string]any) error {
	if c == nil {
		return ErrNotConfigured
	}
	raw, err := json.Marshal(policy)
	if err != nil {
		return err
	}
	_, err = c.s3.PutBucketPolicy(ctx, &s3.PutBucketPolicyInput{
		Bucket: aws.String(bucket), Policy: aws.String(string(raw)),
	})
	return err
}

func (c *Client) DeleteBucketPolicy(ctx context.Context, bucket string) error {
	if c == nil {
		return ErrNotConfigured
	}
	_, err := c.s3.DeleteBucketPolicy(ctx, &s3.DeleteBucketPolicyInput{Bucket: aws.String(bucket)})
	return err
}

func (c *Client) ListObjects(ctx context.Context, bucket, prefix, token string, maxKeys int32) ([]ObjectInfo, string, bool, error) {
	if c == nil {
		return nil, "", false, ErrNotConfigured
	}
	if maxKeys <= 0 {
		maxKeys = 1000
	}
	in := &s3.ListObjectsV2Input{
		Bucket:  aws.String(bucket),
		Prefix:  aws.String(prefix),
		MaxKeys: aws.Int32(maxKeys),
	}
	if token != "" {
		in.ContinuationToken = aws.String(token)
	}
	out, err := c.s3.ListObjectsV2(ctx, in)
	if err != nil {
		return nil, "", false, err
	}
	items := make([]ObjectInfo, 0, len(out.Contents))
	for _, o := range out.Contents {
		if o.Key == nil {
			continue
		}
		items = append(items, ObjectInfo{Key: *o.Key, LastModified: o.LastModified, Size: aws.ToInt64(o.Size)})
	}
	next := ""
	if out.NextContinuationToken != nil {
		next = *out.NextContinuationToken
	}
	return items, next, aws.ToBool(out.IsTruncated), nil
}

func (c *Client) DeleteObject(ctx context.Context, bucket, key string) error {
	if c == nil {
		return ErrNotConfigured
	}
	_, err := c.s3.DeleteObject(ctx, &s3.DeleteObjectInput{Bucket: aws.String(bucket), Key: aws.String(key)})
	return err
}

func (c *Client) ListMultipart(ctx context.Context, bucket, prefix, keyMarker, uploadMarker string) ([]MultipartInfo, string, string, bool, error) {
	if c == nil {
		return nil, "", "", false, ErrNotConfigured
	}
	in := &s3.ListMultipartUploadsInput{
		Bucket: aws.String(bucket), Prefix: aws.String(prefix), MaxUploads: aws.Int32(1000),
	}
	if keyMarker != "" {
		in.KeyMarker = aws.String(keyMarker)
	}
	if uploadMarker != "" {
		in.UploadIdMarker = aws.String(uploadMarker)
	}
	out, err := c.s3.ListMultipartUploads(ctx, in)
	if err != nil {
		return nil, "", "", false, err
	}
	items := make([]MultipartInfo, 0, len(out.Uploads))
	for _, u := range out.Uploads {
		if u.Key == nil || u.UploadId == nil {
			continue
		}
		items = append(items, MultipartInfo{Key: *u.Key, UploadID: *u.UploadId, Initiated: u.Initiated})
	}
	nextKey, nextUpload := "", ""
	if out.NextKeyMarker != nil {
		nextKey = *out.NextKeyMarker
	}
	if out.NextUploadIdMarker != nil {
		nextUpload = *out.NextUploadIdMarker
	}
	return items, nextKey, nextUpload, aws.ToBool(out.IsTruncated), nil
}

func (c *Client) AbortMultipart(ctx context.Context, bucket, key, uploadID string) error {
	if c == nil {
		return ErrNotConfigured
	}
	_, err := c.s3.AbortMultipartUpload(ctx, &s3.AbortMultipartUploadInput{
		Bucket: aws.String(bucket), Key: aws.String(key), UploadId: aws.String(uploadID),
	})
	return err
}
