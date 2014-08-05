package multi

import (
	"errors"
	"git.encryptio.com/slime/lib/store"
	"math/rand"
	"sort"
)

var ErrNotEnoughTargets = errors.New("Not enough targets for redundancy level")

const RandomSpace = 1024*1024*1024 // 1GiB

type targetFree struct {
	t store.Target
	f int64
}

type targetFreeList []targetFree

func (t targetFreeList) Len() int           { return len(t) }
func (t targetFreeList) Swap(i, j int)      { t[i], t[j] = t[j], t[i] }
func (t targetFreeList) Less(i, j int) bool { return t[i].f > t[j].f } // NB: >, not <

func (m *Multi) findPreferred(count int) ([]store.Target, error) {
	withFree := targetFreeList(make([]targetFree, 0, len(m.targets)))
	var extra []store.Target
	for _, tgt := range m.targets {
		free, err := tgt.FreeSpace()
		if err != nil {
			extra = append(extra, tgt)
			continue
		}

		free += rand.Int63n(RandomSpace)

		withFree = append(withFree, targetFree{tgt, free})
	}

	sort.Sort(withFree)

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

		if !m.config.AllowOverProvision {
			return nil, ErrNotEnoughTargets
		}

		// if the user has told us to, break our redundancy principles
		out = append(out, m.targets[rand.Intn(len(m.targets))])
	}

	return out, nil
}
