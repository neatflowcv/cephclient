package flow

import (
	"context"
	"errors"
	"fmt"
	"slices"

	"github.com/neatflowcv/cephclient/internal/pkg/domain"
)

var (
	errObjectRepairOLHInstanceMismatch = errors.New("object repair olh instance mismatch")
	errObjectRepairRawObjectHit        = errors.New("object repair raw object exists")
)

type repairObjectSide struct {
	ContainerName string
	BucketName    string
	ObjectName    string
	Stats         *domain.BucketStats
	ShardID       int
	EntryGroup    *domain.EntryGroup
}

type repairOrphanEntry struct {
	version string
	omapKey string
}

func (s *Service) RepairObject(ctx context.Context, req RepairObjectRequest) (*RepairObjectResponse, error) {
	master, err := s.loadRepairObjectSide(ctx, req.MasterContainerName, req.BucketName, req.ObjectName)
	if err != nil {
		return nil, fmt.Errorf("load master object state: %w", err)
	}

	secondary, err := s.loadRepairObjectSide(ctx, req.SecondaryContainerName, req.BucketName, req.ObjectName)
	if err != nil {
		return nil, fmt.Errorf("load secondary object state: %w", err)
	}

	err = validateRepairOLHInstances(master, secondary)
	if err != nil {
		return nil, err
	}

	secondaryRemovedOmapKeys, err := s.repairObjectOmapKeys(
		ctx,
		secondary,
	)
	if err != nil {
		return nil, fmt.Errorf("repair secondary object: %w", err)
	}

	masterRemovedOmapKeys, err := s.repairObjectOmapKeys(
		ctx,
		master,
	)
	if err != nil {
		return nil, fmt.Errorf("repair master object: %w", err)
	}

	return NewRepairObjectResponse(secondaryRemovedOmapKeys, masterRemovedOmapKeys), nil
}

func (s *Service) loadRepairObjectSide(
	ctx context.Context,
	containerName string,
	bucketName string,
	objectName string,
) (*repairObjectSide, error) {
	stats, err := s.client.GetBucketStats(ctx, containerName, bucketName)
	if err != nil {
		return nil, fmt.Errorf("read bucket stats: %w", err)
	}

	shard, err := s.client.ObjectShard(ctx, containerName, objectName, stats.TotalShards())
	if err != nil {
		return nil, fmt.Errorf("read object shard: %w", err)
	}

	shardID := shard.Shard()

	entryGroup, err := s.client.ListBucketIndexByObject(ctx, containerName, bucketName, objectName, shardID)
	if err != nil {
		return nil, fmt.Errorf("read bucket index list: %w", err)
	}

	return &repairObjectSide{
		ContainerName: containerName,
		BucketName:    bucketName,
		ObjectName:    objectName,
		Stats:         stats,
		ShardID:       shardID,
		EntryGroup:    entryGroup,
	}, nil
}

func validateRepairOLHInstances(master, secondary *repairObjectSide) error {
	masterOLHInstances := repairOLHInstances(master.EntryGroup)

	secondaryOLHInstances := repairOLHInstances(secondary.EntryGroup)
	if !slices.Equal(masterOLHInstances, secondaryOLHInstances) {
		return fmt.Errorf(
			"%w: master=%v secondary=%v",
			errObjectRepairOLHInstanceMismatch,
			masterOLHInstances,
			secondaryOLHInstances,
		)
	}

	return nil
}

func (s *Service) repairObjectOmapKeys(
	ctx context.Context,
	side *repairObjectSide,
) ([]string, error) {
	orphans := repairOrphanEntries(side.EntryGroup)
	if len(orphans) == 0 {
		return nil, nil
	}

	zone, err := s.client.GetDefaultZone(ctx, side.ContainerName)
	if err != nil {
		return nil, fmt.Errorf("read default zone: %w", err)
	}

	err = s.validateRepairRawObjects(ctx, side, zone, orphans)
	if err != nil {
		return nil, err
	}

	indexObject, err := s.repairBucketIndexObject(ctx, side)
	if err != nil {
		return nil, fmt.Errorf("read bucket index object: %w", err)
	}

	return s.removeRepairOmapKeys(ctx, side, zone.IndexPool(), indexObject.Raw(), orphans)
}

func (s *Service) validateRepairRawObjects(
	ctx context.Context,
	side *repairObjectSide,
	zone *domain.Zone,
	orphans []*repairOrphanEntry,
) error {
	for _, orphan := range orphans {
		rawObject := domain.NewVersionRawObjectName(
			side.Stats.Marker(),
			orphan.version,
			side.ObjectName,
		).Value()

		exists, err := s.client.HasRawObject(ctx, side.ContainerName, zone.DataPool(), rawObject)
		if err != nil {
			return fmt.Errorf("check raw object: %w", err)
		}

		if exists {
			return fmt.Errorf("%w: raw_object=%s", errObjectRepairRawObjectHit, rawObject)
		}
	}

	return nil
}

func (s *Service) repairBucketIndexObject(
	ctx context.Context,
	side *repairObjectSide,
) (*domain.BucketIndexObject, error) {
	layout, err := s.client.GetBucketLayout(ctx, side.ContainerName, side.BucketName)
	if err != nil {
		return nil, fmt.Errorf("get bucket layout: %w", err)
	}

	return domain.NewBucketIndexObject(side.Stats.Marker(), layout.Generation(), side.ShardID), nil
}

func (s *Service) removeRepairOmapKeys(
	ctx context.Context,
	side *repairObjectSide,
	indexPool string,
	rawObject string,
	orphans []*repairOrphanEntry,
) ([]string, error) {
	var removedOmapKeys []string

	for _, orphan := range orphans {
		err := s.client.RemoveOmapKey(ctx, side.ContainerName, indexPool, rawObject, orphan.omapKey)
		if err != nil {
			return nil, fmt.Errorf("remove repair omap key %s: %w", orphan.omapKey, err)
		}

		removedOmapKeys = append(removedOmapKeys, orphan.omapKey)
	}

	return removedOmapKeys, nil
}

func repairOLHInstances(entryGroup *domain.EntryGroup) []string {
	var instances []string

	for _, olh := range entryGroup.OLHs() {
		instances = append(instances, olh.Instance())
	}

	slices.Sort(instances)

	return instances
}

func repairOrphanEntries(entryGroup *domain.EntryGroup) []*repairOrphanEntry {
	plainInstances := repairPlainInstances(entryGroup)
	instanceInstances := repairInstanceInstances(entryGroup)

	var orphans []*repairOrphanEntry

	for _, instance := range entryGroup.Instances() {
		version := instance.Instance()
		if version == "" {
			continue
		}

		if _, exists := plainInstances[version]; exists {
			continue
		}

		orphans = append(orphans, newRepairOrphanEntry(version, instance.IDX()))
	}

	for _, plain := range entryGroup.Plains() {
		version := plain.Instance()
		if version == "" {
			continue
		}

		if _, exists := instanceInstances[version]; exists {
			continue
		}

		orphans = append(orphans, newRepairOrphanEntry(version, plain.IDX()))
	}

	return repairUniqueOrphanEntries(orphans)
}

func repairPlainInstances(entryGroup *domain.EntryGroup) map[string]struct{} {
	instances := map[string]struct{}{}

	for _, plain := range entryGroup.Plains() {
		instances[plain.Instance()] = struct{}{}
	}

	return instances
}

func repairInstanceInstances(entryGroup *domain.EntryGroup) map[string]struct{} {
	instances := map[string]struct{}{}

	for _, instance := range entryGroup.Instances() {
		instances[instance.Instance()] = struct{}{}
	}

	return instances
}

func newRepairOrphanEntry(version, omapKey string) *repairOrphanEntry {
	return &repairOrphanEntry{
		version: version,
		omapKey: omapKey,
	}
}

func repairUniqueOrphanEntries(orphans []*repairOrphanEntry) []*repairOrphanEntry {
	seen := domain.NewSeen()

	var unique []*repairOrphanEntry

	for _, orphan := range orphans {
		if seen.Check(orphan.omapKey) {
			continue
		}

		seen.Set(orphan.omapKey)
		unique = append(unique, orphan)
	}

	return unique
}
