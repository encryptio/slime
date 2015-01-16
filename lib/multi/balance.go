package multi

import (
	"errors"
	"git.encryptio.com/slime/lib/store"
	"math/rand"
	"sort"
)

var ErrNotEnoughTargets = errors.New("Not enough targets for redundancy level")

const (
	RandomSpace        = 1024 * 1024 * 1024     // 1GiB
	RebalanceThreshold = 2 * 1024 * 1024 * 1024 // 2GiB
)

type targetFree struct {
	t store.Target
	f int64
}

type targetFreeList []targetFree

func (t targetFreeList) Len() int           { return len(t) }
func (t targetFreeList) Swap(i, j int)      { t[i], t[j] = t[j], t[i] }
func (t targetFreeList) Less(i, j int) bool { return t[i].f > t[j].f } // NB: >

func (m *Multi) findPreferred(count int) ([]store.Target, error) {
	withFree, extra := m.targetsWithFree(RandomSpace)

	out := make([]store.Target, 0, count)
	for len(out) < count {
		// prefer targets with more free space
		if len(withFree) > 0 {
			out = append(out, withFree[0].t)
			withFree = withFree[1:]
			continue
		}

		// or targets that have failed free space
		if len(extra) > 0 {
			i := rand.Intn(len(extra))
			out = append(out, extra[i])
			copy(extra[i:], extra[i+1:])
			extra = extra[:len(extra)-1]
			continue
		}

		return nil, ErrNotEnoughTargets
	}

	return out, nil
}

func (m *Multi) findRebalanceTargets() (store.Target, store.Target) {
	withFree, _ := m.targetsWithFree(0)
	if len(withFree) < 2 {
		return nil, nil
	}

	diff := withFree[0].f - withFree[len(withFree)-1].f
	if diff < RebalanceThreshold {
		return nil, nil
	}

	return withFree[len(withFree)-1].t, withFree[0].t
}

func (m *Multi) targetsWithFree(fuzz int64) (targetFreeList, []store.Target) {
	withFree := targetFreeList(make([]targetFree, 0, len(m.targets)))
	var extra []store.Target
	for _, tgt := range m.targets {
		free, err := tgt.FreeSpace()
		if err != nil {
			extra = append(extra, tgt)
			continue
		}

		if fuzz > 0 {
			free += rand.Int63n(fuzz)
		}

		withFree = append(withFree, targetFree{tgt, free})
	}

	sort.Sort(withFree)

	return withFree, extra
}
