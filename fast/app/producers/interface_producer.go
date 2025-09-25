package producers

import "context"

type Producer interface {
	Run(ctx context.Context, messages []string) error
}
