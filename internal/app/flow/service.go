package flow

import (
	"context"
	"fmt"
	"log/slog"
	"slices"
	"time"

	"github.com/neatflowcv/cephclient/internal/pkg/client"
	"github.com/neatflowcv/cephclient/internal/pkg/domain"
)

type Service struct {
	client client.Client
}

type PurgeObjectRequest struct {
	ContainerName string
	BucketName    string
	ObjectName    string
	TotalShards   *int
}

func NewService(client client.Client) *Service {
	return &Service{client: client}
}

func (s *Service) ListBIByShard(
	ctx context.Context,
	containerName, bucketName string,
	shardID int,
) (*domain.BIList, error) {
	biList, err := s.client.BIListByShard(ctx, containerName, bucketName, shardID)
	if err != nil {
		return nil, fmt.Errorf("get bucket index list: %w", err)
	}

	return biList, nil
}

func (s *Service) ListBIByObject(
	ctx context.Context,
	req ListBIByObjectRequest,
) (*ListBIByObjectResponse, error) {
	shardID, err := s.resolveListBIByObjectShard(ctx, req)
	if err != nil {
		return nil, err
	}

	biList, err := s.client.BIListByObject(ctx, req.ContainerName, req.BucketName, req.ObjectName, shardID)
	if err != nil {
		return nil, fmt.Errorf("get bucket index list: %w", err)
	}

	return NewListBIByObjectResponse(biList), nil
}

func (s *Service) GetBucketStats(
	ctx context.Context,
	req GetBucketStatsRequest,
) (*GetBucketStatsResponse, error) {
	stats, err := s.client.BucketStats(ctx, req.ContainerName, req.BucketName)
	if err != nil {
		return nil, fmt.Errorf("get bucket stats: %w", err)
	}

	return newGetBucketStatsResponse(req, stats), nil
}

func (s *Service) GetBucketLayout(ctx context.Context, containerName, bucketName string) (*domain.Layout, error) {
	layout, err := s.client.BucketLayout(ctx, containerName, bucketName)
	if err != nil {
		return nil, fmt.Errorf("get bucket layout: %w", err)
	}

	return layout, nil
}

func (s *Service) GetDefaultZone(ctx context.Context, containerName string) (*domain.Zone, error) {
	zone, err := s.client.GetDefaultZone(ctx, containerName)
	if err != nil {
		return nil, fmt.Errorf("get default zone: %w", err)
	}

	return zone, nil
}

func (s *Service) HasRawObject(
	ctx context.Context,
	containerName, pool, rawObject string,
) (bool, error) {
	exists, err := s.client.HasRawObject(ctx, containerName, pool, rawObject)
	if err != nil {
		return false, fmt.Errorf("check raw object existence: %w", err)
	}

	return exists, nil
}

func (s *Service) ListOmapKeys(
	ctx context.Context,
	req ListOmapKeysRequest,
) (*ListOmapKeysResponse, error) {
	indexObject, err := s.bucketIndexObject(
		ctx,
		req.ContainerName,
		req.BucketName,
		req.Marker,
		req.ShardID,
	)
	if err != nil {
		return nil, err
	}

	indexes, err := s.client.ListOmapKeys(ctx, req.ContainerName, req.IndexPool, indexObject)
	if err != nil {
		return nil, fmt.Errorf("get omap keys: %w", err)
	}

	return newListOmapKeysResponse(indexes), nil
}

func (s *Service) ListBuckets(ctx context.Context, containerName string) ([]string, error) {
	buckets, err := s.client.ListBuckets(ctx, containerName)
	if err != nil {
		return nil, fmt.Errorf("get bucket list: %w", err)
	}

	return buckets, nil
}

func (s *Service) GetObjectShard(
	ctx context.Context,
	containerName, objectName string,
	totalShards int,
) (*domain.ObjectShard, error) {
	shard, err := s.client.ObjectShard(ctx, containerName, objectName, totalShards)
	if err != nil {
		return nil, fmt.Errorf("get object shard: %w", err)
	}

	return shard, nil
}

func (s *Service) InspectObject(
	ctx context.Context,
	req InspectObjectRequest,
) (*InspectObjectResponse, error) {
	zone, err := s.client.GetDefaultZone(ctx, req.ContainerName)
	if err != nil {
		return nil, fmt.Errorf("read default zone: %w", err)
	}

	stats, err := s.client.BucketStats(ctx, req.ContainerName, req.BucketName)
	if err != nil {
		return nil, fmt.Errorf("read bucket stats: %w", err)
	}

	shard, err := s.client.ObjectShard(ctx, req.ContainerName, req.ObjectName, stats.TotalShards())
	if err != nil {
		return nil, fmt.Errorf("read object shard: %w", err)
	}

	shardID := shard.Shard()

	biList, err := s.client.BIListByObject(ctx, req.ContainerName, req.BucketName, req.ObjectName, shardID)
	if err != nil {
		return nil, fmt.Errorf("read bucket index list: %w", err)
	}

	rawObjects, err := s.inspectRawObjects(
		ctx,
		req.ContainerName,
		zone.DataPool(),
		stats.Marker(),
		req.ObjectName,
		biList,
	)
	if err != nil {
		return nil, err
	}

	return NewInspectObjectResponse(
		zone.DataPool(),
		stats.Marker(),
		stats.TotalShards(),
		shard.Shard(),
		biList,
		rawObjects,
	), nil
}

func (s *Service) RemoveObject(
	ctx context.Context,
	containerName, bucketName, objectName, version string,
) error {
	err := s.client.RemoveObject(ctx, containerName, bucketName, objectName, version)
	if err != nil {
		return fmt.Errorf("remove object: %w", err)
	}

	return nil
}

func (s *Service) RemoveRawObject(
	ctx context.Context,
	containerName, pool, rawObject string,
) error {
	err := s.client.RemoveRawObject(ctx, containerName, pool, rawObject)
	if err != nil {
		return fmt.Errorf("remove raw object: %w", err)
	}

	return nil
}

//nolint:funlen,cyclop // Purge flow intentionally keeps remove, verify, and fallback cleanup in one place.
func (s *Service) PurgeObject(
	ctx context.Context,
	req PurgeObjectRequest,
) error {
	shardID, err := s.resolvePurgeObjectShard(ctx, req)
	if err != nil {
		return err
	}

	entryGroup, err := s.client.ListBucketIndexByObject(
		ctx,
		req.ContainerName,
		req.BucketName,
		req.ObjectName,
		shardID,
	)
	if err != nil {
		return fmt.Errorf("read bucket index list: %w", err)
	}

	instances := entryGroup.Instances()
	slices.SortFunc(instances, func(a, b *domain.InstanceBIEntry) int {
		aTime, _ := time.Parse(time.RFC3339Nano, a.Entry().Meta().MTime())
		bTime, _ := time.Parse(time.RFC3339Nano, b.Entry().Meta().MTime())

		switch {
		case aTime.Before(bTime):
			return -1
		case aTime.After(bTime):
			return 1
		default:
			return 0
		}
	})

	for _, instance := range instances {
		err = s.client.RemoveObject(
			ctx,
			req.ContainerName,
			req.BucketName,
			req.ObjectName,
			instance.Entry().Instance(),
		)
		if err != nil {
			return fmt.Errorf("remove object: %w", err)
		}

		slog.Info(
			"removed object",
			"container", req.ContainerName,
			"bucket", req.BucketName,
			"object", req.ObjectName,
			"version", instance.Entry().Instance(),
		)
	}

	// RemoveObject succeeded but the underlying raw object or omap entry may still remain.
	remainingEntryGroup, err := s.client.ListBucketIndexByObject(
		ctx,
		req.ContainerName,
		req.BucketName,
		req.ObjectName,
		shardID,
	)
	if err != nil {
		return fmt.Errorf("verify bucket index list after purge: %w", err)
	}

	if remainingEntryGroup.IsEmpty() {
		return nil
	}

	stats, err := s.client.BucketStats(ctx, req.ContainerName, req.BucketName)
	if err != nil {
		return fmt.Errorf("read bucket stats: %w", err)
	}

	zone, err := s.client.GetDefaultZone(ctx, req.ContainerName)
	if err != nil {
		return fmt.Errorf("read default zone: %w", err)
	}

	var (
		rawObjects []string
		omapKeys   []string
	)

	rawObjects = remainingEntryGroup.ExtractRawObjectNames(stats.Marker(), req.ObjectName)
	omapKeys = remainingEntryGroup.ExtractOmapKeys()

	indexObject, err := s.bucketIndexObject(
		ctx,
		req.ContainerName,
		req.BucketName,
		stats.Marker(),
		shardID,
	)
	if err != nil {
		return err
	}

	for _, rawObject := range rawObjects {
		err = s.client.RemoveRawObject(ctx, req.ContainerName, zone.DataPool(), rawObject)
		if err != nil {
			slog.Info(
				"failed to remove raw object",
				"container", req.ContainerName,
				"bucket", req.BucketName,
				"pool", zone.DataPool(),
				"marker", stats.Marker(),
				"object", req.ObjectName,
				"raw_object", rawObject,
			)

			continue
		}

		slog.Info(
			"removed raw object",
			"container", req.ContainerName,
			"bucket", req.BucketName,
			"pool", zone.DataPool(),
			"marker", stats.Marker(),
			"object", req.ObjectName,
			"raw_object", rawObject,
		)
	}

	for _, omapKey := range omapKeys {
		err = s.client.RemoveOmapKey(
			ctx,
			req.ContainerName,
			zone.IndexPool(),
			indexObject,
			omapKey,
		)
		if err != nil {
			slog.Info(
				"failed to remove omap key",
				"container", req.ContainerName,
				"bucket", req.BucketName,
				"pool", zone.IndexPool(),
				"marker", stats.Marker(),
				"shard", shardID,
				"object", req.ObjectName,
				"omap_key", omapKey,
			)

			continue
		}

		slog.Info(
			"removed omap key",
			"container", req.ContainerName,
			"bucket", req.BucketName,
			"pool", zone.IndexPool(),
			"marker", stats.Marker(),
			"shard", shardID,
			"object", req.ObjectName,
			"omap_key", omapKey,
		)
	}

	return nil
}

func (s *Service) RemoveOmapKey(
	ctx context.Context,
	req RemoveOmapKeyRequest,
) error {
	indexObject, err := s.bucketIndexObject(
		ctx,
		req.ContainerName,
		req.BucketName,
		req.Marker,
		req.ShardID,
	)
	if err != nil {
		return err
	}

	err = s.client.RemoveOmapKey(ctx, req.ContainerName, req.IndexPool, indexObject, req.Key)
	if err != nil {
		return fmt.Errorf("remove omap key: %w", err)
	}

	return nil
}

func (s *Service) bucketIndexObject(
	ctx context.Context,
	containerName, bucketName, marker string,
	shard int,
) (*domain.BucketIndexObject, error) {
	layout, err := s.client.BucketLayout(ctx, containerName, bucketName)
	if err != nil {
		return nil, fmt.Errorf("get bucket layout: %w", err)
	}

	return domain.NewBucketIndexObject(marker, layout.Generation(), shard), nil
}

func (s *Service) resolveListBIByObjectShard(
	ctx context.Context,
	req ListBIByObjectRequest,
) (int, error) {
	if req.ShardID != nil {
		return *req.ShardID, nil
	}

	totalShards := req.TotalShards
	if totalShards == nil {
		stats, err := s.client.BucketStats(ctx, req.ContainerName, req.BucketName)
		if err != nil {
			return 0, fmt.Errorf("read bucket stats: %w", err)
		}

		statsTotalShards := stats.TotalShards()
		totalShards = &statsTotalShards
	}

	shard, err := s.client.ObjectShard(ctx, req.ContainerName, req.ObjectName, *totalShards)
	if err != nil {
		return 0, fmt.Errorf("read object shard: %w", err)
	}

	return shard.Shard(), nil
}

func (s *Service) resolvePurgeObjectShard(
	ctx context.Context,
	req PurgeObjectRequest,
) (int, error) {
	totalShards := req.TotalShards
	if totalShards == nil {
		stats, err := s.client.BucketStats(ctx, req.ContainerName, req.BucketName)
		if err != nil {
			return 0, fmt.Errorf("read bucket stats: %w", err)
		}

		statsTotalShards := stats.TotalShards()
		totalShards = &statsTotalShards
	}

	shard, err := s.client.ObjectShard(ctx, req.ContainerName, req.ObjectName, *totalShards)
	if err != nil {
		return 0, fmt.Errorf("read object shard: %w", err)
	}

	return shard.Shard(), nil
}

func (s *Service) inspectRawObjects(
	ctx context.Context,
	containerName, dataPool, marker, objectName string,
	biList *domain.BIList,
) ([]*RawObjectExistence, error) {
	rawNames := rawObjectNames(marker, objectName, biList)
	results := make([]*RawObjectExistence, 0, len(rawNames))

	for _, rawName := range rawNames {
		exists, err := s.client.HasRawObject(ctx, containerName, dataPool, rawName.Value())
		if err != nil {
			return nil, fmt.Errorf("check raw object existence: %w", err)
		}

		results = append(results, NewRawObjectExistence(rawName, exists))
	}

	return results, nil
}

func rawObjectNames(marker, objectName string, biList *domain.BIList) []*domain.RawObjectName {
	names := []*domain.RawObjectName{domain.NewOLHRawObjectName(marker, objectName)}
	seenVersions := map[string]struct{}{}

	for _, entry := range biList.Entries() {
		for _, version := range rawObjectVersions(entry) {
			if version == "" {
				continue
			}

			if _, ok := seenVersions[version]; ok {
				continue
			}

			seenVersions[version] = struct{}{}
			names = append(names, domain.NewVersionRawObjectName(marker, version, objectName))
		}
	}

	return names
}

func rawObjectVersions(entry domain.BIEntry) []string {
	switch typed := entry.(type) {
	case *domain.PlainBIEntry:
		return []string{typed.Entry().Instance()}
	case *domain.InstanceBIEntry:
		return []string{typed.Entry().Instance()}
	case *domain.OLHBIEntry:
		versions := []string{typed.Entry().Key().Instance()}

		for _, pending := range typed.Entry().PendingLog() {
			for _, item := range pending.Val() {
				versions = append(versions, item.Key().Instance())
			}
		}

		return versions
	default:
		return nil
	}
}
