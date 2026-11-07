package hashing

const prime64 uint64 = 1099511628211

func Fnv1a(key string) uint64 {
	// constanta FNV-1a 64-bit
	var hash uint64 = 14695981039346656037

	// Loop byte string
	for i := 0; i < len(key); i++ {
		hash ^= uint64(key[i])
		hash *= prime64
	}
	return hash
}
