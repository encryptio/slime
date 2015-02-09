package meta

import (
	"git.encryptio.com/kvl"
	"git.encryptio.com/kvl/tuple"
)

func indexFn(p kvl.Pair) []kvl.Pair {
	if p.IsZero() {
		return nil
	}

	var typ string
	_, err := tuple.UnpackIntoPartial(p.Key)
	if err != nil {
		return nil
	}

	switch typ {
	case "file":
		var f File
		err := f.fromPair(p)
		if err != nil {
			return nil
		}
		return f.indexPairs()

	case "location":
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
