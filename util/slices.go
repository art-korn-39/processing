package util

func GroupByKey[T any, K comparable](items []T, getKey func(T) K) map[K][]T {

	grouped := make(map[K][]T)

	for _, item := range items {
		key := getKey(item)
		grouped[key] = append(grouped[key], item)
	}

	return grouped
}

func Compact[T comparable](sliceList []T) []T {
	allKeys := make(map[T]bool)
	list := []T{}
	for _, item := range sliceList {
		if _, value := allKeys[item]; !value {
			allKeys[item] = true
			list = append(list, item)
		}
	}
	return list
}
