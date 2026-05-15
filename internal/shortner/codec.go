package shortner

import (
	"crypto/rand"
	"math/big"
	"strings"
)

// Base62 character set — 0-9, a-z, A-Z
// 62^7 = 3.5 trillion possible short codes
const charset = "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"

// Generate returns a cryptographically random Base62 string of given length.
// e.g. length=7 → "aB3xZ9q"
//
// We use crypto/rand instead of math/rand to avoid predictable codes.
// Predictable short codes can be brute-forced to enumerate all URLs — a
// real security issue in production URL shorteners.
func Generate(length int) (string, error) {
	base := big.NewInt(int64(len(charset)))
	var sb strings.Builder
	sb.Grow(length)

	for i := 0; i < length; i++ {
		// Pick a random index into charset
		idx, err := rand.Int(rand.Reader, base)
		if err != nil {
			return "", err
		}
		sb.WriteByte(charset[idx.Int64()])
	}

	return sb.String(), nil
}