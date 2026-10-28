package recovery

import "encoding/base64"

func validToken(s string) bool {
	b, err := base64.RawURLEncoding.Strict().DecodeString(s)
	return err == nil && len(b) == 16 && len(s) == 22
}
