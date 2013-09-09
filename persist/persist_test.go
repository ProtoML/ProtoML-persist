package persist

import (
	"math/rand"
	"testing"
)

const RAND_TEST_SIZE = 1000

func TestHashLength(t *testing.T) {
	hashLength := 32
	for i := 0; i < RAND_TEST_SIZE; i++ {
		rand := rand.Int63()
		hashed := Hash(rand)
		if x := len(hashed); x != hashLength {
			t.Errorf("len(Hash(%d)) = %d, want %d", rand, x, hashLength)
		}
	}
}

func TestHashUnique(t *testing.T) {
	seen := make(map[string]bool)
	for i := 0; i < RAND_TEST_SIZE; i++ {
		rand := rand.Int63()
		hashed := Hash(rand)
		if seen[hashed] {
			t.Errorf("Hash(%d) already seen!", rand)
		}
		seen[hashed] = true
	}
}

func TestDataId(t *testing.T) {
	const in1, out = "doooo", "doooo-14"
	const in2 = 14
	if x := DataId(in1, in2); x != out {
		t.Errorf("DataId(%s, %d) = %s, want %s", in1, in2, x, out)
	}
}
