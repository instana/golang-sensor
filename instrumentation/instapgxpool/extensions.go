// (c) Copyright IBM Corp. 2021
// (c) Copyright Instana Inc. 2020
// (c) Copyright clevabit GmbH 2021

package instapgxpool

import (
	"context"
	"fmt"
	instana "github.com/instana/go-sensor"
	"github.com/jackc/pgconn"
	"github.com/jackc/pgproto3/v2"
	"github.com/jackc/pgx/v4"
	ot "github.com/opentracing/opentracing-go"
	"github.com/opentracing/opentracing-go/ext"
	"time"
)

// Query executes the given query against the pool, while creating an internal span
// across the query and fetch time.
func Query(pool Pool, ctx context.Context, query string, args ...interface{}) (pgx.Rows, error) {
	return query0(pool, ctx, nil, query, args...)
}

// QueryWithTimeout executes the given query against the pool, while creating an internal span
// across the query and fetch time. The query is cancelled when timeout is reached.
func QueryWithTimeout(pool Pool, ctx context.Context, timeout time.Duration, query string, args ...interface{}) (pgx.Rows, error) {
	return query0(pool, ctx, &timeout, query, args...)
}

func query0(pool Pool, ctx context.Context, timeout *time.Duration, query string, args ...interface{}) (pgx.Rows, error) {
	ctx, span := contextWithChildSpan(query, ctx)

	var cancel func()
	if timeout != nil {
		ctx, cancel = context.WithTimeout(ctx, *timeout)
	}

	rows, err := pool.Query(ctx, query, args...)
	if err != nil {
		if cancel != nil {
			cancel()
		}
		if err != pgx.ErrNoRows {
			span.SetTag(string(ext.Error), fmt.Sprintf("%+v", err))
			span.SetTag("params", args)
		}
		return nil, err
	}
	return pgRowsAdapter{span: span, rows: rows, cancel: cancel}, nil
}

// QueryRow executes the given query against the pool, while creating an internal span
// across the query and fetch time.
func QueryRow(pool Pool, ctx context.Context, query string, args ...interface{}) pgx.Row {
	return queryRow0(pool, ctx, nil, query, args...)
}

// QueryRowWithTimeout executes the given query against the pool, while creating an internal span
// across the query and fetch time. The query is cancelled when timeout is reached.
func QueryRowWithTimeout(pool Pool, ctx context.Context, timeout time.Duration, query string, args ...interface{}) pgx.Row {
	return queryRow0(pool, ctx, &timeout, query, args...)
}

func queryRow0(pool Pool, ctx context.Context, timeout *time.Duration, query string, args ...interface{}) pgx.Row {
	ctx, span := contextWithChildSpan(query, ctx)

	var cancel func()
	if timeout != nil {
		ctx, cancel = context.WithTimeout(ctx, *timeout)
	}

	row := pool.QueryRow(ctx, query, args...)
	return pgRowAdapter{span: span, row: row, args: args, cancel: cancel}
}

// Exec executes the given query against the pool, while creating an internal span
// across the query and fetch time.
func Exec(pool Pool, ctx context.Context, query string, args ...interface{}) (pgconn.CommandTag, error) {
	return exec0(pool, ctx, nil, query, args...)
}

// ExecWithTimeout executes the given query against the pool, while creating an internal span
// across the query and fetch time. The query is cancelled when timeout is reached.
func ExecWithTimeout(pool Pool, ctx context.Context, timeout time.Duration, query string, args ...interface{}) (pgconn.CommandTag, error) {
	return exec0(pool, ctx, &timeout, query, args...)
}

func exec0(pool Pool, ctx context.Context, timeout *time.Duration, query string, args ...interface{}) (pgconn.CommandTag, error) {
	ctx, span := contextWithChildSpan(query, ctx)
	defer span.Finish()

	if timeout != nil {
		childCtx, cancel := context.WithTimeout(ctx, *timeout)
		defer cancel()
		ctx = childCtx
	}

	return pool.Exec(ctx, query, args...)
}

type RowFunction = func(row pgx.Row) error

// QueryFunc executes the given query against the pool, while creating an internal span
// across the query and fetch time. The given function is called once per result row and
// can be used to simplify error handling in the PGX row api.
func QueryFunc(pool Pool, ctx context.Context, fn RowFunction, query string, args ...interface{}) error {
	return queryFunc0(pool, ctx, nil, fn, query, args...)
}

// QueryFuncWithTimeout executes the given query against the pool, while creating an internal span
// across the query and fetch time. The given function is called once per result row and
// can be used to simplify error handling in the PGX row api. The query is cancelled
// when timeout is reached.
func QueryFuncWithTimeout(pool Pool, ctx context.Context, timeout time.Duration, fn RowFunction, query string, args ...interface{}) error {
	return queryFunc0(pool, ctx, &timeout, fn, query, args...)
}

func queryFunc0(pool Pool, ctx context.Context, timeout *time.Duration, fn RowFunction, query string, args ...interface{}) error {
	ctx, span := contextWithChildSpan(query, ctx)
	defer span.Finish()

	if timeout != nil {
		childCtx, cancel := context.WithTimeout(ctx, *timeout)
		defer cancel()
		ctx = childCtx
	}

	rows, err := pool.Query(ctx, query, args...)
	if err != nil {
		span.SetTag("params", args)
		return err
	}
	defer rows.Close()

	for rows.Next() {
		if err := fn(rows); err != nil {
			span.SetTag(string(ext.Error), fmt.Sprintf("%+v", err))
			span.SetTag("params", args)
			return err
		}
	}

	err = rows.Err()
	if err != nil {
		span.SetTag(string(ext.Error), fmt.Sprintf("%+v", err))
		span.SetTag("params", args)
	}
	return err
}

type pgRowAdapter struct {
	span   ot.Span
	row    pgx.Row
	cancel func()
	args   []interface{}
}

func (p pgRowAdapter) Scan(dest ...interface{}) error {
	err := p.row.Scan(dest...)
	if err != nil && err != pgx.ErrNoRows {
		p.span.SetTag(string(ext.Error), fmt.Sprintf("%+v", err))
		p.span.SetTag("params", p.args)
	}
	if p.cancel != nil {
		p.cancel()
	}
	p.span.Finish()
	return err
}

type pgRowsAdapter struct {
	span   ot.Span
	rows   pgx.Rows
	cancel func()
}

func (p pgRowsAdapter) Close() {
	p.rows.Close()
	if p.cancel != nil {
		p.cancel()
	}
	p.span.Finish()
}

func (p pgRowsAdapter) Err() error {
	return p.rows.Err()
}

func (p pgRowsAdapter) CommandTag() pgconn.CommandTag {
	return p.rows.CommandTag()
}

func (p pgRowsAdapter) FieldDescriptions() []pgproto3.FieldDescription {
	return p.rows.FieldDescriptions()
}

func (p pgRowsAdapter) Next() bool {
	return p.rows.Next()
}

func (p pgRowsAdapter) Scan(dest ...interface{}) error {
	return p.rows.Scan(dest...)
}

func (p pgRowsAdapter) Values() ([]interface{}, error) {
	return p.rows.Values()
}

func (p pgRowsAdapter) RawValues() [][]byte {
	return p.rows.RawValues()
}

func contextWithChildSpan(sql string, ctx context.Context) (context.Context, ot.Span) {
	var spanOptions []ot.StartSpanOption
	parent, ok := instana.SpanFromContext(ctx)
	if !ok {
		return ctx, nil
	}

	spanOptions = append(spanOptions, ot.ChildOf(parent.Context()))
	span := parent.Tracer().StartSpan(sql, spanOptions...)
	span.SetTag(string(ext.SpanKind), "intermediate")
	return instana.ContextWithSpan(ctx, span), span
}
