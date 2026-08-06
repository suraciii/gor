package domain

import (
	"context"

	adomain "github.com/suraciii/gor/cmd/gorgen/testfixture/aliasconflict/a/domain"
	bdomain "github.com/suraciii/gor/cmd/gorgen/testfixture/aliasconflict/b/domain"
)

//gor:entity
type Ledger interface {
	Merge(ctx context.Context, left adomain.Event, right bdomain.Event) (adomain.Event, error)
}
