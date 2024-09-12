package util

func GroupByKey[T any, K comparable](items []T, getKey func(T) K) map[K][]T {

	grouped := make(map[K][]T)

	for _, item := range items {
		key := getKey(item)
		grouped[key] = append(grouped[key], item)
	}

	return grouped
}
