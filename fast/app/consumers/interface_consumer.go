package consumers

import "context"

type Consumer interface {
	Run(ctx context.Context) error
}
