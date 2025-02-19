/*
Copyright 2019 The Vitess Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package mysqlctl

import (
	"context"
	"fmt"
	"strings"
	"time"

	"vitess.io/vitess/go/mysql/sqlerror"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/dbconnpool"
	"vitess.io/vitess/go/vt/log"
)

// getPoolReconnect gets a connection from a pool, tests it, and reconnects if
// the connection is lost.
func getPoolReconnect(ctx context.Context, pool *dbconnpool.ConnectionPool) (*dbconnpool.PooledDBConnection, error) {
	conn, err := pool.Get(ctx)
	if err != nil {
		return conn, err
	}
	// Run a test query to see if this connection is still good.
	if _, err := conn.ExecuteFetch("SELECT 1", 1, false); err != nil {
		// If we get a connection error, try to reconnect.
		if sqlErr, ok := err.(*sqlerror.SQLError); ok && (sqlErr.Number() == sqlerror.CRServerGone || sqlErr.Number() == sqlerror.CRServerLost) {
			if err := conn.Reconnect(ctx); err != nil {
				conn.Recycle()
				return nil, err
			}
			return conn, nil
		}
		conn.Recycle()
		return nil, err
	}
	return conn, nil
}

// ExecuteSuperQuery allows the user to execute a query as a super user.
func (mysqld *Mysqld) ExecuteSuperQuery(ctx context.Context, query string) error {
	return mysqld.ExecuteSuperQueryList(ctx, []string{query})
}

// ExecuteSuperQueryList alows the user to execute queries as a super user.
func (mysqld *Mysqld) ExecuteSuperQueryList(ctx context.Context, queryList []string) error {
	conn, err := getPoolReconnect(ctx, mysqld.dbaPool)
	if err != nil {
		return err
	}
	defer conn.Recycle()

	return mysqld.executeSuperQueryListConn(ctx, conn, queryList)
}

func limitString(s string, limit int) string {
	if len(s) > limit {
		return s[:limit]
	}
	return s
}

func (mysqld *Mysqld) executeSuperQueryListConn(ctx context.Context, conn *dbconnpool.PooledDBConnection, queryList []string) error {
	const LogQueryLengthLimit = 200
	for _, query := range queryList {
		log.Infof("exec %s", limitString(redactPassword(query), LogQueryLengthLimit))
		if _, err := mysqld.executeFetchContext(ctx, conn, query, 10000, false); err != nil {
			log.Errorf("ExecuteFetch(%v) failed: %v", redactPassword(query), redactPassword(err.Error()))
			return fmt.Errorf("ExecuteFetch(%v) failed: %v", redactPassword(query), redactPassword(err.Error()))
		}
	}
	return nil
}

// FetchSuperQuery returns the results of executing a query as a super user.
func (mysqld *Mysqld) FetchSuperQuery(ctx context.Context, query string) (*sqltypes.Result, error) {
	conn, connErr := getPoolReconnect(ctx, mysqld.dbaPool)
	if connErr != nil {
		return nil, connErr
	}
	defer conn.Recycle()
	qr, err := mysqld.executeFetchContext(ctx, conn, query, 10000, true)
	if err != nil {
		return nil, err
	}
	return qr, nil
}

// executeFetchContext calls ExecuteFetch() on the given connection,
// while respecting Context deadline and cancellation.
func (mysqld *Mysqld) executeFetchContext(ctx context.Context, conn *dbconnpool.PooledDBConnection, query string, maxrows int, wantfields bool) (*sqltypes.Result, error) {
	// Fast fail if context is done.
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}

	// Execute asynchronously so we can select on both it and the context.
	var qr *sqltypes.Result
	var executeErr error
	done := make(chan struct{})
	go func() {
		defer close(done)

		qr, executeErr = conn.ExecuteFetch(query, maxrows, wantfields)
	}()

	// Wait for either the query or the context to be done.
	select {
	case <-done:
		return qr, executeErr
	case <-ctx.Done():
		// If both are done already, we may end up here anyway because select
		// chooses among multiple ready channels pseudorandomly.
		// Check the done channel and prefer that one if it's ready.
		select {
		case <-done:
			return qr, executeErr
		default:
		}

		// The context expired or was canceled.
		// Try to kill the connection to effectively cancel the ExecuteFetch().
		connID := conn.ID()
		log.Infof("Mysqld.executeFetchContext(): killing connID %v due to timeout of query: %v", connID, query)
		if killErr := mysqld.killConnection(connID); killErr != nil {
			// Log it, but go ahead and wait for the query anyway.
			log.Warningf("Mysqld.executeFetchContext(): failed to kill connID %v: %v", connID, killErr)
		}
		// Wait for the conn.ExecuteFetch() call to return.
		<-done
		// Close the connection. Upon Recycle() it will be thrown out.
		conn.Close()
		// ExecuteFetch() may have succeeded before we tried to kill it.
		// If ExecuteFetch() had returned because we canceled it,
		// then executeErr would be an error like "MySQL has gone away".
		if executeErr == nil {
			return qr, executeErr
		}
		return nil, ctx.Err()
	}
}

// killConnection issues a MySQL KILL command for the given connection ID.
func (mysqld *Mysqld) killConnection(connID int64) error {
	// There's no other interface that both types of connection implement.
	// We only care about one method anyway.
	var killConn interface {
		ExecuteFetch(query string, maxrows int, wantfields bool) (*sqltypes.Result, error)
	}

	// Get another connection with which to kill.
	// Use background context because the caller's context is likely expired,
	// which is the reason we're being asked to kill the connection.
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if poolConn, connErr := getPoolReconnect(ctx, mysqld.dbaPool); connErr == nil {
		// We got a pool connection.
		defer poolConn.Recycle()
		killConn = poolConn
	} else {
		// We couldn't get a connection from the pool.
		// It might be because the connection pool is exhausted,
		// because some connections need to be killed!
		// Try to open a new connection without the pool.
		conn, connErr := mysqld.GetDbaConnection(ctx)
		if connErr != nil {
			return connErr
		}
		defer conn.Close()
		killConn = conn
	}

	_, err := killConn.ExecuteFetch(fmt.Sprintf("kill %d", connID), 10000, false)
	return err
}

// fetchVariables returns a map from MySQL variable names to variable value
// for variables that match the given pattern.
func (mysqld *Mysqld) fetchVariables(ctx context.Context, pattern string) (map[string]string, error) {
	query := fmt.Sprintf("SHOW VARIABLES LIKE '%s'", pattern)
	qr, err := mysqld.FetchSuperQuery(ctx, query)
	if err != nil {
		return nil, err
	}
	if len(qr.Fields) != 2 {
		return nil, fmt.Errorf("query %#v returned %d columns, expected 2", query, len(qr.Fields))
	}
	varMap := make(map[string]string, len(qr.Rows))
	for _, row := range qr.Rows {
		varMap[row[0].ToString()] = row[1].ToString()
	}
	return varMap, nil
}

// fetchStatuses returns a map from MySQL status names to status value
// for variables that match the given pattern.
func (mysqld *Mysqld) fetchStatuses(ctx context.Context, pattern string) (map[string]string, error) {
	query := fmt.Sprintf("SHOW STATUS LIKE '%s'", pattern)
	qr, err := mysqld.FetchSuperQuery(ctx, query)
	if err != nil {
		return nil, err
	}
	if len(qr.Fields) != 2 {
		return nil, fmt.Errorf("query %#v returned %d columns, expected 2", query, len(qr.Fields))
	}
	varMap := make(map[string]string, len(qr.Rows))
	for _, row := range qr.Rows {
		varMap[row[0].ToString()] = row[1].ToString()
	}
	return varMap, nil
}

const (
	masterPasswordStart = "  MASTER_PASSWORD = '"
	masterPasswordEnd   = "',\n"
	passwordStart       = " PASSWORD = '"
	passwordEnd         = "'"
)

func redactPassword(input string) string {
	i := strings.Index(input, masterPasswordStart)
	// We have primary password in the query, try to redact it
	if i != -1 {
		j := strings.Index(input[i+len(masterPasswordStart):], masterPasswordEnd)
		if j == -1 {
			return input
		}
		input = input[:i+len(masterPasswordStart)] + strings.Repeat("*", 4) + input[i+len(masterPasswordStart)+j:]
	}
	// We also check if we have any password keyword in the query
	i = strings.Index(input, passwordStart)
	if i == -1 {
		return input
	}
	j := strings.Index(input[i+len(passwordStart):], passwordEnd)
	if j == -1 {
		return input
	}
	return input[:i+len(passwordStart)] + strings.Repeat("*", 4) + input[i+len(passwordStart)+j:]
}
