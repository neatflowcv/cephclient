package podman

import "context"

type Runner interface {
	Run(ctx context.Context, args ...string) (stdout []byte, stderr string, err error)
}
