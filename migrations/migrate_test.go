package migrations

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

type beginnerFunc func(context.Context) (pgx.Tx, error)

func (f beginnerFunc) Begin(ctx context.Context) (pgx.Tx, error) { return f(ctx) }

type rowFunc func(...any) error

func (f rowFunc) Scan(dest ...any) error { return f(dest...) }

type transaction struct {
	pgx.Tx
	statements                  []string
	versions                    []int
	applied                     map[int]bool
	failExec, failQuery         int
	queries, commits, rollbacks int
	failure, commitError        error
	rollbackContextError        error
}

func (tx *transaction) Exec(_ context.Context, sql string, args ...any) (pgconn.CommandTag, error) {
	tx.statements = append(tx.statements, sql)
	if tx.failExec == len(tx.statements) {
		return pgconn.CommandTag{}, tx.failure
	}
	if strings.HasPrefix(sql, "INSERT INTO schema_migrations") {
		tx.versions = append(tx.versions, args[0].(int))
	}
	return pgconn.NewCommandTag("INSERT 0 1"), nil
}
func (tx *transaction) QueryRow(_ context.Context, _ string, args ...any) pgx.Row {
	tx.queries++
	failed := tx.queries == tx.failQuery
	return rowFunc(func(dest ...any) error {
		if failed {
			return tx.failure
		}
		*dest[0].(*bool) = tx.applied[args[0].(int)]
		return nil
	})
}
func (tx *transaction) Commit(context.Context) error { tx.commits++; return tx.commitError }
func (tx *transaction) Rollback(ctx context.Context) error {
	tx.rollbacks++
	tx.rollbackContextError = ctx.Err()
	return pgx.ErrTxClosed
}
func poolFor(tx *transaction) Beginner {
	return beginnerFunc(func(context.Context) (pgx.Tx, error) { return tx, nil })
}

func TestApplyOrdersMissingVersionsAndSkipsAppliedVersions(t *testing.T) {
	for _, test := range []struct {
		name    string
		applied map[int]bool
		want    []int
	}{
		{"fresh", nil, []int{1, 2}},
		{"upgrade", map[int]bool{1: true}, []int{2}},
		{"already current", map[int]bool{1: true, 2: true}, nil},
	} {
		t.Run(test.name, func(t *testing.T) {
			tx := &transaction{applied: test.applied}
			if err := Apply(context.Background(), poolFor(tx)); err != nil {
				t.Fatal(err)
			}
			if tx.commits != 1 || tx.rollbacks != 1 || tx.rollbackContextError != nil {
				t.Fatal("transaction cleanup failed", tx)
			}
			if len(tx.versions) != len(test.want) {
				t.Fatal(tx.versions, test.want)
			}
			for i, v := range test.want {
				if tx.versions[i] != v {
					t.Fatal(tx.versions, test.want)
				}
			}
			if !strings.Contains(tx.statements[0], "pg_advisory_xact_lock") {
				t.Fatal("migration lock not acquired first")
			}
			for _, sql := range tx.statements {
				if test.applied[1] && sql == initial {
					t.Fatal("reapplied initial schema")
				}
				if test.applied[2] && sql == dropPayloadHash {
					t.Fatal("reapplied completed migration")
				}
			}
		})
	}
}

func TestApplyRollsBackEveryFailedStatementOrVersionRead(t *testing.T) {
	failure := errors.New("database statement failed")
	for operation := 1; operation <= 8; operation++ {
		tx := &transaction{failure: failure}
		if operation <= 6 {
			tx.failExec = operation
		} else {
			tx.failQuery = operation - 6
		}
		if err := Apply(context.Background(), poolFor(tx)); !errors.Is(err, failure) {
			t.Fatalf("operation %d: %v", operation, err)
		}
		if tx.commits != 0 || tx.rollbacks != 1 || tx.rollbackContextError != nil {
			t.Fatalf("operation %d failed cleanup: %+v", operation, tx)
		}
	}
}

func TestApplyBeginAndCommitFailures(t *testing.T) {
	failure := errors.New("connection closed")
	if err := Apply(context.Background(), beginnerFunc(func(context.Context) (pgx.Tx, error) { return nil, failure })); !errors.Is(err, failure) {
		t.Fatal(err)
	}
	tx := &transaction{commitError: failure}
	if err := Apply(context.Background(), poolFor(tx)); !errors.Is(err, failure) {
		t.Fatal(err)
	}
	if tx.commits != 1 || tx.rollbacks != 1 {
		t.Fatal(tx)
	}
}

func TestRollbackUsesIndependentContextAfterCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	tx := &transaction{failure: context.Canceled, failExec: 1}
	pool := beginnerFunc(func(context.Context) (pgx.Tx, error) { cancel(); return tx, nil })
	if err := Apply(ctx, pool); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if tx.rollbackContextError != nil || tx.rollbacks != 1 {
		t.Fatal("rollback inherited canceled operation", tx.rollbackContextError)
	}
}
