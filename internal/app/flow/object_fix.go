package flow

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"slices"

	"github.com/neatflowcv/cephclient/internal/pkg/domain"
)

var errObjectOLHInstanceMismatch = errors.New("object olh instance mismatch")

type fixObjectSide struct {
	ContainerName string
	BucketName    string
	ObjectName    string
	Stats         *domain.BucketStats
	ShardID       int
	EntryGroup    *domain.EntryGroup
}

func (s *Service) FixObject(ctx context.Context, req FixObjectRequest) error {
	target, err := s.loadFixObjectSide(ctx, req.TargetContainerName, req.BucketName, req.ObjectName)
	if err != nil {
		return fmt.Errorf("load target object state: %w", err)
	}

	reference, err := s.loadFixObjectSide(ctx, req.ReferenceContainerName, req.BucketName, req.ObjectName)
	if err != nil {
		return fmt.Errorf("load reference object state: %w", err)
	}

	err = validateSameOLHInstances(target, reference)
	if err != nil {
		return err
	}

	s.removeOrphanInstances(ctx, target)

	reloaded, err := s.reloadFixObjectSide(ctx, target)
	if err != nil {
		return err
	}

	return s.removeRawOrphanInstances(ctx, reloaded)
}

func (s *Service) loadFixObjectSide(
	ctx context.Context,
	containerName string,
	bucketName string,
	objectName string,
) (*fixObjectSide, error) {
	stats, err := s.client.GetBucketStats(ctx, containerName, bucketName)
	if err != nil {
		return nil, fmt.Errorf("read bucket stats: %w", err)
	}

	shard, err := s.client.ObjectShard(ctx, containerName, objectName, stats.TotalShards())
	if err != nil {
		return nil, fmt.Errorf("read object shard: %w", err)
	}

	shardID := shard.Shard()

	entryGroup, err := s.client.ListBucketIndexByObject(
		ctx,
		containerName,
		bucketName,
		objectName,
		shardID,
	)
	if err != nil {
		return nil, fmt.Errorf("read bucket index list: %w", err)
	}

	return &fixObjectSide{
		ContainerName: containerName,
		BucketName:    bucketName,
		ObjectName:    objectName,
		Stats:         stats,
		ShardID:       shardID,
		EntryGroup:    entryGroup,
	}, nil
}

func validateSameOLHInstances(target, reference *fixObjectSide) error {
	targetOLHInstances := olhInstances(target.EntryGroup)

	referenceOLHInstances := olhInstances(reference.EntryGroup)
	// If OLH points at different instances, the object may still be syncing
	// or may be in an unknown error state. Do not clean up target BI entries.
	if !slices.Equal(targetOLHInstances, referenceOLHInstances) {
		return fmt.Errorf(
			"%w: target=%v reference=%v",
			errObjectOLHInstanceMismatch,
			targetOLHInstances,
			referenceOLHInstances,
		)
	}

	return nil
}

func (s *Service) reloadFixObjectSide(ctx context.Context, target *fixObjectSide) (*fixObjectSide, error) {
	entryGroup, err := s.client.ListBucketIndexByObject(
		ctx,
		target.ContainerName,
		target.BucketName,
		target.ObjectName,
		target.ShardID,
	)
	if err != nil {
		return nil, fmt.Errorf("reload target bucket index list after fix: %w", err)
	}

	return &fixObjectSide{
		ContainerName: target.ContainerName,
		BucketName:    target.BucketName,
		ObjectName:    target.ObjectName,
		Stats:         target.Stats,
		ShardID:       target.ShardID,
		EntryGroup:    entryGroup,
	}, nil
}

func (s *Service) removeRawOrphanInstances(
	ctx context.Context,
	target *fixObjectSide,
) error {
	instances := extractOrphanInstances(target.EntryGroup)
	if len(instances) == 0 {
		return nil
	}

	zone, err := s.client.GetDefaultZone(ctx, target.ContainerName)
	if err != nil {
		return fmt.Errorf("read target default zone: %w", err)
	}

	indexObject, err := s.bucketIndexObject(
		ctx,
		target.ContainerName,
		target.BucketName,
		target.Stats.Marker(),
		target.ShardID,
		nil,
	)
	if err != nil {
		return err
	}

	s.removeRawObjectsForInstances(ctx, target, zone, instances)
	s.removeOmapKeysForInstances(ctx, target, zone, indexObject, instances)

	return nil
}

// CHECK.
func (s *Service) removeOrphanInstances(ctx context.Context, target *fixObjectSide) {
	for _, instance := range extractOrphanInstances(target.EntryGroup) {
		err := s.client.RemoveObject(
			ctx,
			target.ContainerName,
			target.BucketName,
			target.ObjectName,
			instance.Instance(),
		)
		if err != nil {
			slog.Warn(
				"failed to remove object instance without plain",
				"container", target.ContainerName,
				"bucket", target.BucketName,
				"object", target.ObjectName,
				"version", instance.Instance(),
				"error", err,
			)

			continue
		}

		slog.Info(
			"removed object instance without plain",
			"container", target.ContainerName,
			"bucket", target.BucketName,
			"object", target.ObjectName,
			"version", instance.Instance(),
		)
	}
}

func (s *Service) removeRawObjectsForInstances(
	ctx context.Context,
	target *fixObjectSide,
	zone *domain.Zone,
	instances []*domain.Instance,
) {
	for _, rawObject := range rawObjectNamesForInstances(target.Stats.Marker(), target.ObjectName, instances) {
		err := s.client.RemoveRawObject(ctx, target.ContainerName, zone.DataPool(), rawObject)
		if err != nil {
			slog.Warn(
				"failed to remove raw object for object instance without plain",
				"container", target.ContainerName,
				"bucket", target.BucketName,
				"pool", zone.DataPool(),
				"marker", target.Stats.Marker(),
				"object", target.ObjectName,
				"raw_object", rawObject,
			)

			continue
		}

		slog.Info(
			"removed raw object for object instance without plain",
			"container", target.ContainerName,
			"bucket", target.BucketName,
			"pool", zone.DataPool(),
			"marker", target.Stats.Marker(),
			"object", target.ObjectName,
			"raw_object", rawObject,
		)
	}
}

// CHECK.
func (s *Service) removeOmapKeysForInstances(
	ctx context.Context,
	target *fixObjectSide,
	zone *domain.Zone,
	indexObject *domain.BucketIndexObject,
	instances []*domain.Instance,
) {
	for _, omapKey := range omapKeysForInstances(instances) {
		err := s.client.RemoveOmapKey(
			ctx,
			target.ContainerName,
			zone.IndexPool(),
			indexObject.Raw(),
			omapKey,
		)
		if err != nil {
			slog.Warn(
				"failed to remove omap key for object instance without plain",
				"container", target.ContainerName,
				"bucket", target.BucketName,
				"pool", zone.IndexPool(),
				"marker", target.Stats.Marker(),
				"shard", target.ShardID,
				"object", target.ObjectName,
				"omap_key", omapKey,
			)

			continue
		}

		slog.Info(
			"removed omap key for object instance without plain",
			"container", target.ContainerName,
			"bucket", target.BucketName,
			"pool", zone.IndexPool(),
			"marker", target.Stats.Marker(),
			"shard", target.ShardID,
			"object", target.ObjectName,
			"omap_key", omapKey,
		)
	}
}

// CHECK.
func olhInstances(entryGroup *domain.EntryGroup) []string {
	var instances []string
	for _, olh := range entryGroup.OLHs() {
		instances = append(instances, olh.Instance())
	}

	slices.Sort(instances)

	return instances
}

// CHECK.
func extractOrphanInstances(entryGroup *domain.EntryGroup) []*domain.Instance {
	plainInstances := map[string]struct{}{}
	for _, plain := range entryGroup.Plains() {
		plainInstances[plain.Instance()] = struct{}{}
	}

	var instances []*domain.Instance

	for _, instance := range entryGroup.Instances() {
		if _, exists := plainInstances[instance.Instance()]; exists {
			continue
		}

		instances = append(instances, instance)
	}

	return instances
}

func rawObjectNamesForInstances(marker, objectName string, instances []*domain.Instance) []string {
	var rawObjects []string

	for _, instance := range instances {
		rawObject := domain.NewVersionRawObjectName(marker, instance.Instance(), objectName).Value()
		rawObjects = append(rawObjects, rawObject)
	}

	return rawObjects
}

func omapKeysForInstances(instances []*domain.Instance) []string {
	var keys []string

	seen := domain.NewSeen()

	for _, instance := range instances {
		key := instance.IDX()
		if seen.Check(key) {
			continue
		}

		seen.Set(key)
		keys = append(keys, key)
	}

	return keys
}
