// Package naming derives NATS routes and Registry resources for one deployment.
package naming

import (
	"fmt"
	"regexp"

	"github.com/go-aie/actorx"
	"github.com/go-aie/actorx-dist/internal/identity"
)

const DefaultNamespace = "default"

var validNamespace = regexp.MustCompile(`^[a-z][a-z0-9_-]{0,63}$`)

// Names is constructed once, then shared as an immutable value.
type Names struct {
	Namespace     string
	SubjectPrefix string
	TypesBucket   string
	CurrentBucket string
	ExitsBucket   string
}

func New(namespace string) (Names, error) {
	if !validNamespace.MatchString(namespace) {
		return Names{}, fmt.Errorf("%w: namespace must match [a-z][a-z0-9_-]{0,63}", actorx.ErrSetup)
	}
	prefix := "actorxdist_" + namespace
	return Names{
		Namespace:     namespace,
		SubjectPrefix: "actorxdist." + namespace,
		TypesBucket:   prefix + "_types_v1",
		CurrentBucket: prefix + "_activations_v1",
		ExitsBucket:   prefix + "_exits_v1",
	}, nil
}

func (n Names) FactorySubject(typ string) string {
	return n.SubjectPrefix + ".factory." + identity.Token(typ)
}

func (n Names) ControlSubject(typ, token string) string {
	return n.FactorySubject(typ) + "." + token + ".control.*"
}

func (n Names) ActorSubject(typ, id string) string {
	return n.SubjectPrefix + ".actor." + identity.Token(typ) + "." + identity.Token(id)
}
