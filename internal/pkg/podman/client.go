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

func decodeBucketStats(data []byte) (*domain.BucketStats, error) {
	var raw bucketStatsResponse

	err := json.Unmarshal(data, &raw)
	if err != nil {
		return nil, fmt.Errorf("decode bucket stats response: %w", err)
	}

	return domain.NewBucketStats(raw.ID, raw.NumShards), nil
}

func decodeBucketList(data []byte) ([]string, error) {
	var raw listBucketsResponse

	err := json.Unmarshal(data, &raw)
	if err != nil {
		return nil, fmt.Errorf("decode bucket list response: %w", err)
	}

	return []string(raw), nil
}
