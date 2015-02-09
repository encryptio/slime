package meta

import (
	"git.encryptio.com/kvl"
)

func indexFn(p kvl.Pair) []kvl.Pair {
	if p.IsZero() {
		return nil
	}

	if len(p.Key) < 1 {
		return nil
	}

	switch p.Key[0] {
	case 'f':
		var f File
		err := f.fromPair(p)
		if err != nil {
			return nil
		}
		return f.indexPairs()

	case 'l':
		var l Location
		err := l.fromPair(p)
		if err != nil {
			return nil
		}
		return l.indexPairs()

	default:
		return nil
	}
}
