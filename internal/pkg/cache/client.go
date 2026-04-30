package cache

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	pkgclient "github.com/neatflowcv/cephclient/internal/pkg/client"
	"github.com/neatflowcv/cephclient/internal/pkg/domain"
	_ "modernc.org/sqlite"
)

var _ pkgclient.Client = (*Client)(nil)

const cacheDirMode = 0o700

type Client struct {
	next pkgclient.Client
	db   *sql.DB
}

func NewClient(ctx context.Context, next pkgclient.Client, path string) (*Client, error) {
	err := os.MkdirAll(filepath.Dir(path), cacheDirMode)
	if err != nil {
		return nil, fmt.Errorf("create cache dir: %w", err)
	}

	db, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, fmt.Errorf("open sqlite cache: %w", err)
	}

	db.SetMaxOpenConns(1)

	client := NewClientWithDB(next, db)

	err = client.init(ctx)
	if err != nil {
		closeErr := db.Close()
		if closeErr != nil {
			return nil, fmt.Errorf("init sqlite cache: %w: close sqlite cache: %w", err, closeErr)
		}

		return nil, err
	}

	return client, nil
}

func NewClientWithDB(next pkgclient.Client, db *sql.DB) *Client {
	return &Client{
		next: next,
		db:   db,
	}
}

func (c *Client) Close() error {
	err := c.db.Close()
	if err != nil {
		return fmt.Errorf("close sqlite cache: %w", err)
	}

	return nil
}

func (c *Client) BIListByShard(
	ctx context.Context,
	containerName, bucketName string,
	shardID int,
) (*domain.BIList, error) {
	biList, err := c.next.BIListByShard(ctx, containerName, bucketName, shardID)
	if err != nil {
		return nil, fmt.Errorf("get bi list by shard: %w", err)
	}

	return biList, nil
}

func (c *Client) ListBIByObject(
	ctx context.Context,
	containerName, bucketName, objectName string,
	shardID int,
) (*domain.BIList, error) {
	biList, err := c.next.ListBIByObject(ctx, containerName, bucketName, objectName, shardID)
	if err != nil {
		return nil, fmt.Errorf("list bi by object: %w", err)
	}

	return biList, nil
}

func (c *Client) ListBucketIndexByObject(
	ctx context.Context,
	containerName, bucketName, objectName string,
	shardID int,
) (*domain.EntryGroup, error) {
	entryGroup, err := c.next.ListBucketIndexByObject(ctx, containerName, bucketName, objectName, shardID)
	if err != nil {
		return nil, fmt.Errorf("list bucket index by object: %w", err)
	}

	return entryGroup, nil
}

func (c *Client) GetBucketLayout(ctx context.Context, containerName, bucketName string) (*domain.Layout, error) {
	key := cacheKey("GetBucketLayout", containerName, bucketName)

	var cached layoutResponse

	found, err := c.get(ctx, key, &cached)
	if err != nil {
		return nil, err
	}

	if found {
		return cached.toDomain(), nil
	}

	layout, err := c.next.GetBucketLayout(ctx, containerName, bucketName)
	if err != nil {
		return nil, fmt.Errorf("get bucket layout: %w", err)
	}

	err = c.set(ctx, key, newLayoutResponse(layout))
	if err != nil {
		return nil, err
	}

	return layout, nil
}

func (c *Client) GetBucketStats(ctx context.Context, containerName, bucketName string) (*domain.BucketStats, error) {
	key := cacheKey("GetBucketStats", containerName, bucketName)

	var cached bucketStatsResponse

	found, err := c.get(ctx, key, &cached)
	if err != nil {
		return nil, err
	}

	if found {
		return cached.toDomain()
	}

	stats, err := c.next.GetBucketStats(ctx, containerName, bucketName)
	if err != nil {
		return nil, fmt.Errorf("get bucket stats: %w", err)
	}

	err = c.set(ctx, key, newBucketStatsResponse(stats))
	if err != nil {
		return nil, err
	}

	return stats, nil
}

func (c *Client) GetDefaultZone(ctx context.Context, containerName string) (*domain.Zone, error) {
	key := cacheKey("GetDefaultZone", containerName)

	var cached zoneResponse

	found, err := c.get(ctx, key, &cached)
	if err != nil {
		return nil, err
	}

	if found {
		return cached.toDomain(), nil
	}

	zone, err := c.next.GetDefaultZone(ctx, containerName)
	if err != nil {
		return nil, fmt.Errorf("get default zone: %w", err)
	}

	err = c.set(ctx, key, newZoneResponse(zone))
	if err != nil {
		return nil, err
	}

	return zone, nil
}

func (c *Client) HasRawObject(ctx context.Context, containerName, pool, rawObject string) (bool, error) {
	exists, err := c.next.HasRawObject(ctx, containerName, pool, rawObject)
	if err != nil {
		return false, fmt.Errorf("check raw object: %w", err)
	}

	return exists, nil
}

func (c *Client) ListOmapKeys(
	ctx context.Context,
	containerName, indexPool string,
	rawObject string,
) ([]*domain.BIIndex, error) {
	keys, err := c.next.ListOmapKeys(ctx, containerName, indexPool, rawObject)
	if err != nil {
		return nil, fmt.Errorf("list omap keys: %w", err)
	}

	return keys, nil
}

func (c *Client) ListBuckets(ctx context.Context, containerName string) ([]string, error) {
	buckets, err := c.next.ListBuckets(ctx, containerName)
	if err != nil {
		return nil, fmt.Errorf("list buckets: %w", err)
	}

	return buckets, nil
}

func (c *Client) ObjectShard(
	ctx context.Context,
	containerName, objectName string,
	totalShards int,
) (*domain.ObjectShard, error) {
	shard, err := c.next.ObjectShard(ctx, containerName, objectName, totalShards)
	if err != nil {
		return nil, fmt.Errorf("get object shard: %w", err)
	}

	return shard, nil
}

func (c *Client) RemoveObject(ctx context.Context, containerName, bucketName, objectName, version string) error {
	err := c.next.RemoveObject(ctx, containerName, bucketName, objectName, version)
	if err != nil {
		return fmt.Errorf("remove object: %w", err)
	}

	return nil
}

func (c *Client) RemoveRawObject(ctx context.Context, containerName, pool, rawObject string) error {
	err := c.next.RemoveRawObject(ctx, containerName, pool, rawObject)
	if err != nil {
		return fmt.Errorf("remove raw object: %w", err)
	}

	return nil
}

func (c *Client) RemoveOmapKey(
	ctx context.Context,
	containerName, indexPool string,
	rawObject string,
	key string,
) error {
	err := c.next.RemoveOmapKey(ctx, containerName, indexPool, rawObject, key)
	if err != nil {
		return fmt.Errorf("remove omap key: %w", err)
	}

	return nil
}

func (c *Client) init(ctx context.Context) error {
	_, err := c.db.ExecContext(ctx, `
PRAGMA journal_mode = WAL;
PRAGMA busy_timeout = 3000;
CREATE TABLE IF NOT EXISTS cache (
	key TEXT PRIMARY KEY,
	value BLOB NOT NULL
);
`)
	if err != nil {
		return fmt.Errorf("init sqlite cache: %w", err)
	}

	return nil
}

func (c *Client) get(ctx context.Context, key string, value any) (bool, error) {
	var data []byte

	err := c.db.QueryRowContext(ctx, `SELECT value FROM cache WHERE key = ?`, key).Scan(&data)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}

	if err != nil {
		return false, fmt.Errorf("get cache value %s: %w", key, err)
	}

	err = json.Unmarshal(data, value)
	if err != nil {
		return false, fmt.Errorf("decode cache value %s: %w", key, err)
	}

	return true, nil
}

func (c *Client) set(ctx context.Context, key string, value any) error {
	data, err := json.Marshal(value)
	if err != nil {
		return fmt.Errorf("encode cache value %s: %w", key, err)
	}

	_, err = c.db.ExecContext(
		ctx,
		`INSERT INTO cache (key, value) VALUES (?, ?)
ON CONFLICT(key) DO UPDATE SET value = excluded.value`,
		key,
		data,
	)
	if err != nil {
		return fmt.Errorf("set cache value %s: %w", key, err)
	}

	return nil
}

func cacheKey(operation string, parameters ...string) string {
	return operation + "/" + strings.Join(parameters, "/")
}

type layoutResponse struct {
	Generation int `json:"generation"`
}

func newLayoutResponse(layout *domain.Layout) layoutResponse {
	return layoutResponse{
		Generation: layout.Generation(),
	}
}

func (r layoutResponse) toDomain() *domain.Layout {
	return domain.NewLayout(r.Generation)
}

type bucketStatsResponse struct {
	ID          string                  `json:"id"`
	Name        string                  `json:"name"`
	TotalShards int                     `json:"totalShards"`
	Marker      string                  `json:"marker"`
	Size        int64                   `json:"size"`
	ObjectCount int                     `json:"objectCount"`
	Versioning  domain.VersioningStatus `json:"versioning"`
}

func newBucketStatsResponse(stats *domain.BucketStats) bucketStatsResponse {
	return bucketStatsResponse{
		ID:          stats.ID(),
		Name:        stats.Name(),
		TotalShards: stats.TotalShards(),
		Marker:      stats.Marker(),
		Size:        stats.Size(),
		ObjectCount: stats.ObjectCount(),
		Versioning:  stats.Versioning(),
	}
}

func (r bucketStatsResponse) toDomain() (*domain.BucketStats, error) {
	stats, err := domain.NewBucketStats(
		r.ID,
		r.Name,
		r.TotalShards,
		r.Marker,
		r.Size,
		r.ObjectCount,
		r.Versioning,
	)
	if err != nil {
		return nil, fmt.Errorf("build bucket stats from cache: %w", err)
	}

	return stats, nil
}

type zoneResponse struct {
	DataPool  string `json:"dataPool"`
	IndexPool string `json:"indexPool"`
}

func newZoneResponse(zone *domain.Zone) zoneResponse {
	return zoneResponse{
		DataPool:  zone.DataPool(),
		IndexPool: zone.IndexPool(),
	}
}

func (r zoneResponse) toDomain() *domain.Zone {
	return domain.NewZone(r.DataPool, r.IndexPool)
}
