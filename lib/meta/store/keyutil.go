package store

func PrefixRange(key []byte) (low, high []byte) {
	if len(key) == 0 {
		return
	}

	low = make([]byte, len(key))
	copy(low, key)

	high = PrefixNext(low)

	return
}

func PrefixNext(key []byte) []byte {
	for i := len(key) - 1; i >= 0; i-- {
		if key[i] != 0xFF {
			n := make([]byte, i+1)
			copy(n, key)
			n[i]++
			return n
		}
	}
	return nil
}
