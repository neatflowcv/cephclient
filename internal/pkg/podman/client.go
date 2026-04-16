package podman

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"strings"

	pkgclient "github.com/neatflowcv/cephclient/internal/pkg/client"
	"github.com/neatflowcv/cephclient/internal/pkg/domain"
)

var _ pkgclient.Client = (*Client)(nil)

type Client struct {
	runner Runner
}

func NewClient(debug bool, stderr io.Writer) (*Client, error) {
	runner, err := newExecCommandRunner(debug, stderr)
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

func (c *Client) ListBucketIndexByObject(
	ctx context.Context,
	containerName, bucketName, objectName string,
	shardID int,
) (*domain.EntryGroup, error) {
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

	return toEntryGroup(biList), nil
}

func toEntryGroup(biList *domain.BIList) *domain.EntryGroup {
	if biList == nil {
		return domain.NewEntryGroup(nil, nil, nil)
	}

	var (
		olhs      []*domain.OLHBIEntry
		plains    []*domain.PlainBIEntry
		instances []*domain.InstanceBIEntry
	)

	for _, entry := range biList.Entries() {
		switch typed := entry.(type) {
		case *domain.OLHBIEntry:
			olhs = append(olhs, typed)
		case *domain.PlainBIEntry:
			plains = append(plains, typed)
		case *domain.InstanceBIEntry:
			instances = append(instances, typed)
		}
	}

	return domain.NewEntryGroup(olhs, plains, instances)
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

func (c *Client) GetDefaultZone(ctx context.Context, containerName string) (*domain.Zone, error) {
	commandArgs := []string{
		"exec",
		"-i",
		containerName,
		"radosgw-admin",
		"zone",
		"get",
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

	zone, err := decodeZone(stdout)
	if err != nil {
		return nil, fmt.Errorf("parse zone get output: %w", err)
	}

	return zone, nil
}

func (c *Client) HasRawObject(
	ctx context.Context,
	containerName, pool, rawObject string,
) (bool, error) {
	commandArgs := []string{
		"exec",
		"-i",
		containerName,
		"rados",
		"-p",
		pool,
		"stat",
		rawObject,
	}

	_, stderr, err := c.runner.Run(ctx, commandArgs...)
	if err == nil {
		return true, nil
	}

	if isRadosStatNotFound(err, stderr) {
		return false, nil
	}

	return false, fmt.Errorf(
		"run podman %s: %w: %s",
		strings.Join(commandArgs, " "),
		err,
		strings.TrimSpace(stderr),
	)
}

func (c *Client) ListOmapKeys(
	ctx context.Context,
	containerName, indexPool, marker string,
	shard int,
) ([]*domain.BIIndex, error) {
	commandArgs := []string{
		"exec",
		"-i",
		containerName,
		"rados",
		"-p",
		indexPool,
		"listomapkeys",
		fmt.Sprintf(".dir.%s.%d", marker, shard),
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

	indexes := decodeListOmapKeys(stdout)

	return indexes, nil
}

func (c *Client) RemoveOmapKey(
	ctx context.Context,
	containerName, indexPool, marker string,
	shard int,
	key string,
) error {
	stdout, err := c.runPodmanCommand(ctx, []string{
		"exec",
		"-i",
		containerName,
		"mktemp",
	})
	if err != nil {
		return err
	}

	tmpFile := strings.TrimSpace(string(stdout))
	writeKeyCommand := `printf "` + key + `" > "` + tmpFile + `"`

	err = c.runPodmanNoOutput(ctx, []string{
		"exec",
		"-i",
		containerName,
		"sh",
		"-c",
		writeKeyCommand,
	})
	if err != nil {
		return err
	}

	err = c.runPodmanNoOutput(ctx, []string{
		"exec",
		"-i",
		containerName,
		"rados",
		"-p",
		indexPool,
		"rmomapkey",
		fmt.Sprintf(".dir.%s.%d", marker, shard),
		"--omap-key-file=" + tmpFile,
	})
	if err != nil {
		return err
	}

	return nil
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

func (c *Client) RemoveObject(
	ctx context.Context,
	containerName, bucketName, objectName, version string,
) error {
	return c.runPodmanNoOutput(ctx, []string{
		"exec",
		"-i",
		containerName,
		"radosgw-admin",
		"object",
		"rm",
		"--bucket=" + bucketName,
		"--object=" + objectName,
		"--object-version=" + version,
	})
}

func (c *Client) runPodmanNoOutput(ctx context.Context, commandArgs []string) error {
	_, err := c.runPodmanCommand(ctx, commandArgs)

	return err
}

func (c *Client) runPodmanCommand(ctx context.Context, commandArgs []string) ([]byte, error) {
	stdout, stderr, err := c.runner.Run(ctx, commandArgs...)
	if err != nil {
		return nil, fmt.Errorf(
			"run podman %s: %w: %s",
			strings.Join(commandArgs, " "),
			err,
			strings.TrimSpace(stderr),
		)
	}

	return stdout, nil
}

func decodeBucketStats(data []byte) (*domain.BucketStats, error) {
	var raw bucketStatsResponse

	err := json.Unmarshal(data, &raw)
	if err != nil {
		return nil, fmt.Errorf("decode bucket stats response: %w", err)
	}

	stats, err := domain.NewBucketStats(
		raw.ID,
		raw.Name,
		raw.NumShards,
		raw.Marker,
		raw.Usage.RGWMain.Size,
		raw.Usage.RGWMain.ObjectCount,
		domain.VersioningStatus(raw.Versioning),
	)
	if err != nil {
		return nil, fmt.Errorf("build bucket stats domain: %w", err)
	}

	return stats, nil
}

func decodeListOmapKeys(data []byte) []*domain.BIIndex {
	lines := bytes.Split(data, []byte{'\n'})
	indexes := make([]*domain.BIIndex, 0, len(lines))

	for _, line := range lines {
		if len(line) == 0 {
			continue
		}

		indexes = append(indexes, domain.NewBIIndex(string(line)))
	}

	return indexes
}

func decodeBucketLayout(data []byte) (*domain.Layout, error) {
	var raw bucketLayoutResponse

	err := json.Unmarshal(data, &raw)
	if err != nil {
		return nil, fmt.Errorf("decode bucket layout response: %w", err)
	}

	return raw.toDomain(), nil
}

func decodeZone(data []byte) (*domain.Zone, error) {
	var raw zoneResponse

	err := json.Unmarshal(data, &raw)
	if err != nil {
		return nil, fmt.Errorf("decode zone get response: %w", err)
	}

	return raw.toDomain()
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

func isRadosStatNotFound(err error, stderr string) bool {
	lowerStderr := strings.ToLower(stderr)

	if !strings.Contains(lowerStderr, "error stat-ing ") {
		return false
	}

	if !strings.Contains(lowerStderr, "(2) no such file or directory") {
		return false
	}

	return err != nil
}
