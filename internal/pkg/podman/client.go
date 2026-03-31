package podman

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	pkgclient "github.com/neatflowcv/cephclient/internal/pkg/client"
	"github.com/neatflowcv/cephclient/internal/pkg/domain"
)

var _ pkgclient.Client = (*Client)(nil)

type Client struct {
	runner Runner
}

func NewClient() (*Client, error) {
	runner, err := newExecCommandRunner()
	if err != nil {
		return nil, err
	}

	return &Client{
		runner: runner,
	}, nil
}

func NewClientWithRunner(runner Runner) *Client {
	return &Client{runner: runner}
}

func (c *Client) BIListByShard(
	ctx context.Context,
	containerName, bucketName string,
	shardID int,
) (*domain.BIList, error) {
	commandArgs := []string{
		"exec",
		"-i",
		containerName,
		"radosgw-admin",
		"bi",
		"list",
		"--bucket=" + bucketName,
		fmt.Sprintf("--shard-id=%d", shardID),
	}

	stdout, stderr, err := c.runner.Run(ctx, commandArgs...)
	if err != nil {
		return nil, fmt.Errorf(
			"run podman %s: %w: %s",
			strings.Join(commandArgs, " "),
			err,
			strings.TrimSpace(stderr),
		)
	}

	biList, err := decodeBIList(stdout)
	if err != nil {
		return nil, fmt.Errorf("parse bi list output: %w", err)
	}

	return biList, nil
}

func (c *Client) BIListByObject(
	ctx context.Context,
	containerName, bucketName, objectName string,
	shardID int,
) (*domain.BIList, error) {
	commandArgs := []string{
		"exec",
		"-i",
		containerName,
		"radosgw-admin",
		"bi",
		"list",
		"--bucket=" + bucketName,
		"--object=" + objectName,
		fmt.Sprintf("--shard-id=%d", shardID),
	}

	stdout, stderr, err := c.runner.Run(ctx, commandArgs...)
	if err != nil {
		return nil, fmt.Errorf(
			"run podman %s: %w: %s",
			strings.Join(commandArgs, " "),
			err,
			strings.TrimSpace(stderr),
		)
	}

	biList, err := decodeBIList(stdout)
	if err != nil {
		return nil, fmt.Errorf("parse bi list output: %w", err)
	}

	return biList, nil
}

func (c *Client) BucketStats(ctx context.Context, containerName, bucketName string) (*domain.BucketStats, error) {
	commandArgs := []string{
		"exec",
		"-i",
		containerName,
		"radosgw-admin",
		"bucket",
		"stats",
		"--bucket=" + bucketName,
	}

	stdout, stderr, err := c.runner.Run(ctx, commandArgs...)
	if err != nil {
		return nil, fmt.Errorf(
			"run podman %s: %w: %s",
			strings.Join(commandArgs, " "),
			err,
			strings.TrimSpace(stderr),
		)
	}

	stats, err := decodeBucketStats(stdout)
	if err != nil {
		return nil, fmt.Errorf("parse bucket stats output: %w", err)
	}

	return stats, nil
}

func (c *Client) BucketLayout(ctx context.Context, containerName, bucketName string) (*domain.Layout, error) {
	commandArgs := []string{
		"exec",
		"-i",
		containerName,
		"radosgw-admin",
		"bucket",
		"layout",
		"--bucket=" + bucketName,
	}

	stdout, stderr, err := c.runner.Run(ctx, commandArgs...)
	if err != nil {
		return nil, fmt.Errorf(
			"run podman %s: %w: %s",
			strings.Join(commandArgs, " "),
			err,
			strings.TrimSpace(stderr),
		)
	}

	layout, err := decodeBucketLayout(stdout)
	if err != nil {
		return nil, fmt.Errorf("parse bucket layout output: %w", err)
	}

	return layout, nil
}

func (c *Client) ListBuckets(ctx context.Context, containerName string) ([]string, error) {
	commandArgs := []string{
		"exec",
		"-i",
		containerName,
		"radosgw-admin",
		"bucket",
		"list",
	}

	stdout, stderr, err := c.runner.Run(ctx, commandArgs...)
	if err != nil {
		return nil, fmt.Errorf(
			"run podman %s: %w: %s",
			strings.Join(commandArgs, " "),
			err,
			strings.TrimSpace(stderr),
		)
	}

	buckets, err := decodeBucketList(stdout)
	if err != nil {
		return nil, fmt.Errorf("parse bucket list output: %w", err)
	}

	return buckets, nil
}

func (c *Client) ObjectShard(
	ctx context.Context,
	containerName, objectName string,
	totalShards int,
) (*domain.ObjectShard, error) {
	commandArgs := []string{
		"exec",
		"-i",
		containerName,
		"radosgw-admin",
		"bucket",
		"object",
		"shard",
		"--object=" + objectName,
		fmt.Sprintf("--num-shards=%d", totalShards),
	}

	stdout, stderr, err := c.runner.Run(ctx, commandArgs...)
	if err != nil {
		return nil, fmt.Errorf(
			"run podman %s: %w: %s",
			strings.Join(commandArgs, " "),
			err,
			strings.TrimSpace(stderr),
		)
	}

	shard, err := decodeObjectShard(stdout)
	if err != nil {
		return nil, fmt.Errorf("parse object shard output: %w", err)
	}

	return shard, nil
}

func decodeBucketStats(data []byte) (*domain.BucketStats, error) {
	var raw bucketStatsResponse

	err := json.Unmarshal(data, &raw)
	if err != nil {
		return nil, fmt.Errorf("decode bucket stats response: %w", err)
	}

	return domain.NewBucketStats(raw.ID, raw.NumShards), nil
}

func decodeBucketLayout(data []byte) (*domain.Layout, error) {
	var raw bucketLayoutResponse

	err := json.Unmarshal(data, &raw)
	if err != nil {
		return nil, fmt.Errorf("decode bucket layout response: %w", err)
	}

	return raw.toDomain(), nil
}

func decodeBIList(data []byte) (*domain.BIList, error) {
	var raw biListResponse

	err := json.Unmarshal(data, &raw)
	if err != nil {
		return nil, fmt.Errorf("decode bi list response: %w", err)
	}

	return raw.toDomain()
}

func decodeBucketList(data []byte) ([]string, error) {
	var raw listBucketsResponse

	err := json.Unmarshal(data, &raw)
	if err != nil {
		return nil, fmt.Errorf("decode bucket list response: %w", err)
	}

	return []string(raw), nil
}

func decodeObjectShard(data []byte) (*domain.ObjectShard, error) {
	var raw objectShardResponse

	err := json.Unmarshal(data, &raw)
	if err != nil {
		return nil, fmt.Errorf("decode object shard response: %w", err)
	}

	return domain.NewObjectShard(raw.Shard), nil
}
