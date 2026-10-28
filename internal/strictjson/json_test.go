package strictjson

import "testing"

func TestSyntaxAndComplexity(t *testing.T) {
	for _, data := range []string{`{"a":1,"a":2}`, `{"a":{"x":1,"x":2}}`, `{} {}`, `[[[]]]`, "\xff", `{"a":1,"b":2,"c":3}`} {
		if Validate([]byte(data), 128, 1, 3) == nil {
			t.Fatalf("accepted %s", data)
		}
	}
	if err := Validate([]byte(`{"a":[true,null,"x"]}`), 128, 4, 8); err != nil {
		t.Fatal(err)
	}
}
