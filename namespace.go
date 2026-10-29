package actorxdist

// WithNamespace sets this Runtime's distributed namespace. It scopes NATS
// subjects, Factory queue groups and JetStream KV buckets. Runtimes that
// communicate and share Actor ownership must use the same namespace.
// The default is "default". Names must match [a-z][a-z0-9_-]{0,63}.
// The last declaration wins; an invalid name returns actorx.ErrSetup before
// connecting. Namespace naming does not replace NATS account permissions.
func WithNamespace(namespace string) Option {
	return func(cfg *runtimeOptions) { cfg.namespace = namespace }
}
