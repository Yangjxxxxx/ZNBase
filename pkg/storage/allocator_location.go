package storage

import (
	"context"
	"sort"

	"github.com/znbasedb/znbase/pkg/config"
	"github.com/znbasedb/znbase/pkg/roachpb"
	"github.com/znbasedb/znbase/pkg/util/log"
)

type allocatorLocation struct {
	ctx context.Context
}

type contextLocationSpaceKey struct{}

func setLocateSpace(ctx context.Context, value *roachpb.LocationValue) context.Context {
	return context.WithValue(ctx, contextLocationSpaceKey{}, value)
}

func (a *allocatorLocation) getLocateSpace() *roachpb.LocationValue {
	res := a.ctx.Value(contextLocationSpaceKey{})
	if res == nil {
		return nil
	}
	return res.(*roachpb.LocationValue)
}

// allocateCandidates creates a candidate list of all stores that can be used
// for allocating a new replica ordered from the best to the worst. Only
// stores that meet the criteria are included in the list.
func (a *allocatorLocation) allocateCandidates(
	sl StoreList,
	constraints analyzedConstraints,
	existing []roachpb.ReplicaDescriptor,
	rangeInfo RangeInfo,
	existingNodeLocalities map[roachpb.NodeID]roachpb.Locality,
	options scorerOptions,
) candidateList {
	var candidates candidateList
	locateSpace := a.getLocateSpace()
	for _, s := range sl.stores {
		if nodeHasReplica(s.Node.NodeID, existing) {
			continue
		}
		var constraintsOK, necessary bool
		var diversityScore float64
		constraintsOK, necessary = allocateConstraintsCheck(s, constraints)
		if locateSpace != nil && !s.Node.LocationName.Empty() {
			constraintsOK, diversityScore = s.Node.LocationName.Have(locateSpace)
		} else {
			diversityScore = diversityAllocateScore(s, existingNodeLocalities)
		}
		if !constraintsOK {
			continue
		}
		if !maxCapacityCheck(s) {
			continue
		}
		balanceScore := balanceScore(sl, s.Capacity, rangeInfo, options)
		var convergesScore int
		if options.qpsRebalanceThreshold > 0 {
			if s.Capacity.QueriesPerSecond < underfullThreshold(sl.candidateQueriesPerSecond.mean, options.qpsRebalanceThreshold) {
				convergesScore = 1
			} else if s.Capacity.QueriesPerSecond < sl.candidateQueriesPerSecond.mean {
				convergesScore = 0
			} else if s.Capacity.QueriesPerSecond < overfullThreshold(sl.candidateQueriesPerSecond.mean, options.qpsRebalanceThreshold) {
				convergesScore = -1
			} else {
				convergesScore = -2
			}
		}
		candidates = append(candidates, candidate{
			store:          s,
			valid:          constraintsOK,
			necessary:      necessary,
			diversityScore: diversityScore,
			convergesScore: convergesScore,
			balanceScore:   balanceScore,
			rangeCount:     int(s.Capacity.RangeCount),
		})
	}
	if options.deterministic {
		sort.Sort(sort.Reverse(byScoreAndID(candidates)))
	} else {
		sort.Sort(sort.Reverse(byScore(candidates)))
	}
	return candidates
}

// removeCandidates creates a candidate list of all existing replicas' stores
// ordered from least qualified for removal to most qualified. Stores that are
// marked as not valid, are in violation of a required criteria.
func (a *allocatorLocation) removeCandidates(
	sl StoreList,
	constraints analyzedConstraints,
	rangeInfo RangeInfo,
	existingNodeLocalities map[roachpb.NodeID]roachpb.Locality,
	options scorerOptions,
) candidateList {
	var candidates candidateList
	locateSpace := a.getLocateSpace()
	for _, s := range sl.stores {
		constraintsOK, necessary := removeConstraintsCheck(s, constraints)
		if !constraintsOK {
			candidates = append(candidates, candidate{
				store:     s,
				valid:     false,
				necessary: necessary,
				details:   "constraint check fail",
			})
			continue
		}
		var diversityScore float64
		if locateSpace != nil && !s.Node.LocationName.Empty() {
			constraintsOK, diversityScore = s.Node.LocationName.Have(locateSpace)
			if !constraintsOK {
				candidates = append(candidates, candidate{
					store:     s,
					valid:     false,
					necessary: necessary,
					details:   "constraint check fail",
				})
				continue
			}
		} else {
			diversityScore = diversityRemovalScore(s.Node.NodeID, existingNodeLocalities)
		}
		balanceScore := balanceScore(sl, s.Capacity, rangeInfo, options)
		var convergesScore int
		if !rebalanceFromConvergesOnMean(sl, s.Capacity) {
			// If removing this candidate replica does not converge the store
			// stats to their means, we make it less attractive for removal by
			// adding 1 to the constraint score. Note that when selecting a
			// candidate for removal the candidates with the lowest scores are
			// more likely to be removed.
			convergesScore = 1
		}
		candidates = append(candidates, candidate{
			store:          s,
			valid:          constraintsOK,
			necessary:      necessary,
			fullDisk:       !maxCapacityCheck(s),
			diversityScore: diversityScore,
			convergesScore: convergesScore,
			balanceScore:   balanceScore,
			rangeCount:     int(s.Capacity.RangeCount),
		})
	}
	if options.deterministic {
		sort.Sort(sort.Reverse(byScoreAndID(candidates)))
	} else {
		sort.Sort(sort.Reverse(byScore(candidates)))
	}
	return candidates
}

// rebalanceCandidates creates two candidate lists. The first contains all
// existing replica's stores, ordered from least qualified for rebalancing to
// most qualified. The second list is of all potential stores that could be
// used as rebalancing receivers, ordered from best to worst.
func (a *allocatorLocation) rebalanceCandidates(
	allStores StoreList,
	constraints analyzedConstraints,
	rangeInfo RangeInfo,
	existingNodeLocalities map[roachpb.NodeID]roachpb.Locality,
	localityLookupFn func(roachpb.NodeID) string,
	options scorerOptions,
) []rebalanceOptions {
	ctx := a.ctx
	// 1. Determine whether existing replicas are valid and/or necessary.
	type existingStore struct {
		cand        candidate
		localityStr string
	}
	existingStores := make(map[roachpb.StoreID]existingStore)
	var needRebalanceFrom bool
	curDiversityScore := rangeDiversityScore(existingNodeLocalities)
	locateSpace := a.getLocateSpace()
	for _, store := range allStores.stores {
		for _, repl := range rangeInfo.Desc.Replicas {
			if store.StoreID != repl.StoreID {
				continue
			}
			valid, necessary := removeConstraintsCheck(store, constraints)
			if locateSpace != nil && !store.Node.LocationName.Empty() {
				valid, curDiversityScore = store.Node.LocationName.Have(locateSpace)
				if !valid {
					if !needRebalanceFrom {
						log.VEventf(ctx, 2, "s%d: should-rebalance(invalid): LocationName:%q",
							store.StoreID, store.Node.LocationName)
					}
					needRebalanceFrom = true
				}
			} else {
				if !valid {
					if !needRebalanceFrom {
						log.VEventf(ctx, 2, "s%d: should-rebalance(invalid): locality:%q",
							store.StoreID, store.Node.Locality)
					}
					needRebalanceFrom = true
				}
			}

			fullDisk := !maxCapacityCheck(store)
			if fullDisk {
				if !needRebalanceFrom {
					log.VEventf(ctx, 2, "s%d: should-rebalance(full-disk): capacity:%q",
						store.StoreID, store.Capacity)
				}
				needRebalanceFrom = true
			}
			existingStores[store.StoreID] = existingStore{
				cand: candidate{
					store:          store,
					valid:          valid,
					necessary:      necessary,
					fullDisk:       fullDisk,
					diversityScore: curDiversityScore,
				},
				localityStr: localityLookupFn(store.Node.NodeID),
			}
		}
	}

	// 2. For each store, determine the stores that would be the best
	// replacements on the basis of constraints, disk fullness, and diversity.
	// Only the best should be included when computing balanceScores, since it
	// isn't fair to compare the fullness of stores in a valid/necessary/diverse
	// locality to those in an invalid/unnecessary/nondiverse locality (see
	// #20751).  Along the way, determine whether rebalance is needed to improve
	// the range along these critical dimensions.
	//
	// This creates groups of stores that are valid to compare with each other.
	// For example, if a range has a replica in localities A, B, and C, it's ok
	// to compare other stores in locality A with the existing store in locality
	// A, but would be bad for diversity if we were to compare them to the
	// existing stores in localities B and C (see #20751 for more background).
	//
	// NOTE: We can't just do this once per localityStr because constraints can
	// also include node Attributes or store Attributes. We could try to group
	// stores by attributes as well, but it's simplest to just run this for each
	// store.
	type comparableStoreList struct {
		existing   []roachpb.StoreDescriptor
		sl         StoreList
		candidates candidateList
	}
	var comparableStores []comparableStoreList
	var needRebalanceTo bool
	for _, existing := range existingStores {
		// If this store is equivalent in both Locality and Node/Store Attributes to
		// some other existing store, then we can treat them the same. We have to
		// include Node/Store Attributes because they affect constraints.
		var matchedOtherExisting bool
		for i, stores := range comparableStores {
			if sameLocalityAndAttrs(stores.existing[0], existing.cand.store) {
				comparableStores[i].existing = append(comparableStores[i].existing, existing.cand.store)
				matchedOtherExisting = true
				break
			}
		}
		if matchedOtherExisting {
			continue
		}
		var comparableCands candidateList
		for _, store := range allStores.stores {
			// Nodes that already have a replica on one of their stores aren't valid
			// rebalance targets. We do include stores that currently have a replica
			// because we want them to be considered as valid stores in the
			// ConvergesOnMean calculations below. This is subtle but important.
			if nodeHasReplica(store.Node.NodeID, rangeInfo.Desc.Replicas) &&
				!storeHasReplica(store.StoreID, rangeInfo.Desc.Replicas) {
				log.VEventf(ctx, 2, "nodeHasReplica(n%d, %v)=true", store.Node.NodeID, rangeInfo.Desc.Replicas)
				continue
			}
			constraintsOK, necessary := rebalanceFromConstraintsCheck(
				store, existing.cand.store.StoreID, constraints)
			var diversityScore float64
			if locateSpace != nil && !store.Node.LocationName.Empty() {
				constraintsOK, diversityScore = store.Node.LocationName.Have(locateSpace)
			} else {
				diversityScore = diversityRebalanceFromScore(
					store, existing.cand.store.Node.NodeID, existingNodeLocalities)
			}
			maxCapacityOK := maxCapacityCheck(store)
			cand := candidate{
				store:          store,
				valid:          constraintsOK,
				necessary:      necessary,
				fullDisk:       !maxCapacityOK,
				diversityScore: diversityScore,
			}
			if !cand.less(existing.cand) {
				comparableCands = append(comparableCands, cand)
				if !needRebalanceFrom && !needRebalanceTo && existing.cand.less(cand) {
					needRebalanceTo = true
					log.VEventf(ctx, 2, "s%d: should-rebalance(necessary/diversity=s%d): oldNecessary:%t, newNecessary:%t, oldDiversity:%f, newDiversity:%f, locality:%q",
						existing.cand.store.StoreID, store.StoreID, existing.cand.necessary, cand.necessary,
						existing.cand.diversityScore, cand.diversityScore, store.Node.Locality)
				}
			}
		}
		if options.deterministic {
			sort.Sort(sort.Reverse(byScoreAndID(comparableCands)))
		} else {
			sort.Sort(sort.Reverse(byScore(comparableCands)))
		}
		bestCands := comparableCands.best()
		bestStores := make([]roachpb.StoreDescriptor, len(bestCands))
		for i := range bestCands {
			bestStores[i] = bestCands[i].store
		}
		comparableStores = append(comparableStores, comparableStoreList{
			existing:   []roachpb.StoreDescriptor{existing.cand.store},
			sl:         makeStoreList(bestStores),
			candidates: bestCands,
		})
	}

	// 3. Decide whether we should try to rebalance. Note that for each existing
	// store, we only compare its fullness stats to the stats of "comparable"
	// stores, i.e. those stores that at least as valid, necessary, and diverse
	// as the existing store.
	needRebalance := needRebalanceFrom || needRebalanceTo
	var shouldRebalanceCheck bool
	if !needRebalance {
		for _, existing := range existingStores {
			var sl StoreList
		outer:
			for _, comparable := range comparableStores {
				for _, existingCand := range comparable.existing {
					if existing.cand.store.StoreID == existingCand.StoreID {
						sl = comparable.sl
						break outer
					}
				}
			}
			// TODO(a-robinson): Some moderate refactoring could extract this logic out
			// into the loop below, avoiding duplicate balanceScore calculations.
			if shouldRebalance(ctx, existing.cand.store, sl, options) {
				shouldRebalanceCheck = true
				break
			}
		}
	}
	if !needRebalance && !shouldRebalanceCheck {
		return nil
	}

	// 4. Create sets of rebalance options, i.e. groups of candidate stores and
	// the existing replicas that they could legally replace in the range.  We
	// have to make a separate set of these for each group of comparableStores.
	results := make([]rebalanceOptions, 0, len(comparableStores))
	for _, comparable := range comparableStores {
		var existingCandidates candidateList
		var candidates candidateList
		for _, existingDesc := range comparable.existing {
			existing, ok := existingStores[existingDesc.StoreID]
			if !ok {
				log.Errorf(ctx, "BUG: missing candidate for existing store %+v; stores: %+v",
					existingDesc, existingStores)
				continue
			}
			if !existing.cand.valid {
				existing.cand.details = "constraint check fail"
				existingCandidates = append(existingCandidates, existing.cand)
				continue
			}
			balanceScore := balanceScore(comparable.sl, existing.cand.store.Capacity, rangeInfo, options)
			var convergesScore int
			if !rebalanceFromConvergesOnMean(comparable.sl, existing.cand.store.Capacity) {
				// Similarly to in removeCandidates, any replica whose removal
				// would not converge the range stats to their means is given a
				// constraint score boost of 1 to make it less attractive for
				// removal.
				convergesScore = 1
			}
			existing.cand.convergesScore = convergesScore
			existing.cand.balanceScore = balanceScore
			existing.cand.rangeCount = int(existing.cand.store.Capacity.RangeCount)
			existingCandidates = append(existingCandidates, existing.cand)
		}

		for _, cand := range comparable.candidates {
			// We handled the possible candidates for removal above. Don't process
			// anymore here.
			if _, ok := existingStores[cand.store.StoreID]; ok {
				continue
			}
			// We already computed valid, necessary, fullDisk, and diversityScore
			// above, but recompute fullDisk using special rebalanceTo logic for
			// rebalance candidates.
			s := cand.store
			cand.fullDisk = !rebalanceToMaxCapacityCheck(s)
			cand.balanceScore = balanceScore(comparable.sl, s.Capacity, rangeInfo, options)
			if rebalanceToConvergesOnMean(comparable.sl, s.Capacity) {
				// This is the counterpart of !rebalanceFromConvergesOnMean from
				// the existing candidates. Candidates whose addition would
				// converge towards the range count mean are promoted.
				cand.convergesScore = 1
			} else if !needRebalance {
				// Only consider this candidate if we must rebalance due to constraint,
				// disk fullness, or diversity reasons.
				log.VEventf(ctx, 3, "not considering %+v as a candidate for range %+v: score=%s storeList=%+v",
					s, rangeInfo, cand.balanceScore, comparable.sl)
				continue
			}
			cand.rangeCount = int(s.Capacity.RangeCount)
			candidates = append(candidates, cand)
		}

		if len(existingCandidates) == 0 || len(candidates) == 0 {
			continue
		}

		if options.deterministic {
			sort.Sort(sort.Reverse(byScoreAndID(existingCandidates)))
			sort.Sort(sort.Reverse(byScoreAndID(candidates)))
		} else {
			sort.Sort(sort.Reverse(byScore(existingCandidates)))
			sort.Sort(sort.Reverse(byScore(candidates)))
		}

		// Only return candidates better than the worst existing replica.
		improvementCandidates := candidates.betterThan(existingCandidates[len(existingCandidates)-1])
		if len(improvementCandidates) == 0 {
			continue
		}
		results = append(results, rebalanceOptions{
			existingCandidates: existingCandidates,
			candidates:         improvementCandidates,
		})
		log.VEventf(ctx, 5, "rebalance candidates #%d: %s\nexisting replicas: %s",
			len(results), results[len(results)-1].candidates, results[len(results)-1].existingCandidates)
	}

	return results
}

func (a *allocatorLocation) preferredLeaseholders(
	zone *config.ZoneConfig, storePool *StorePool, existing []roachpb.ReplicaDescriptor,
) []roachpb.ReplicaDescriptor {
	locateSpace := a.getLocateSpace()
	// Go one preference at a time. As soon as we've found replicas that match a
	// preference, we don't need to look at the later preferences, because
	// they're meant to be ordered by priority.
	if locateSpace != nil && locateSpace.Leases != nil {
		var preferred []roachpb.ReplicaDescriptor
		for _, repl := range existing {
			storeDesc, ok := storePool.getStoreDescriptor(repl.StoreID)
			if !ok {
				continue
			}
			if storeDesc.Node.LocationName.HaveLease(locateSpace) {
				preferred = append(preferred, repl)
			}
		}
		if len(preferred) > 0 {
			return preferred
		}
	} else {
		for _, preference := range zone.LeasePreferences {
			var preferred []roachpb.ReplicaDescriptor
			for _, repl := range existing {
				// TODO(a-robinson): Do all these lookups at once, up front? We could
				// easily be passing a slice of StoreDescriptors around all the Allocator
				// functions instead of ReplicaDescriptors.
				storeDesc, ok := storePool.getStoreDescriptor(repl.StoreID)
				if !ok {
					continue
				}
				if subConstraintsCheck(storeDesc, preference.Constraints) {
					preferred = append(preferred, repl)
				}
			}
			if len(preferred) > 0 {
				return preferred
			}
		}
	}
	return nil
}
