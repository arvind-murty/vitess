/*
Copyright 2023 The Vitess Authors.

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

package aggregation

import (
	"fmt"
	"github.com/stretchr/testify/require"
	"math/rand"
	"strings"
	"testing"
	"time"
	"vitess.io/vitess/go/mysql/collations"
	"vitess.io/vitess/go/slices2"
	"vitess.io/vitess/go/vt/key"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vtgate/simplifier"
	"vitess.io/vitess/go/vt/vtgate/vindexes"

	"golang.org/x/exp/maps"
)

type (
	column struct {
		name string
		typ  string
	}
	tableT struct {
		name    string
		columns []column
	}
)

type schemaInfo struct {
	schema map[string]tableT
}

func (s schemaInfo) FindTableOrVindex(tablename sqlparser.TableName) (*vindexes.Table, vindexes.Vindex, string, topodatapb.TabletType, key.Destination, error) {
	t := s.schema[tablename.Name.String()]
	cols := slices2.Map(t.columns, func(from column) vindexes.Column {
		return vindexes.Column{
			Name:          sqlparser.NewIdentifierCI(from.name),
			Type:          0,
			CollationName: "",
		}
	})
	tbl := &vindexes.Table{
		Type: "base",
		Name: sqlparser.IdentifierCS{},
		Keyspace: &vindexes.Keyspace{
			Name:    "ks",
			Sharded: true,
		},
		Columns:                 cols,
		ColumnListAuthoritative: true,
	}

	return tbl, nil, "ks", topodatapb.TabletType_REPLICA, nil, nil

}

func (s schemaInfo) ConnCollation() collations.ID {
	return collations.Unknown
}

func TestSimplifyBuggyQuery(t *testing.T) {
	mcmp, closer := start(t)
	defer closer()

	t1Insert := "insert into t1 (t1_id, name, value, shardKey) values (0, 'name0', 'value0', 0),(1, 'name1', 'value1', 1),(2, 'name2', 'value2', 2),(3, 'name3', 'value3', 3),(4, 'name4', 'value4', 4),(5, 'name5', 'value5', 5),(6, 'name6', 'value6', 6),(7, 'name7', 'value7', 7),(8, 'name8', 'value8', 8),(9, 'name9', 'value9', 9),(10, 'name10', 'value10', 10),(11, 'name11', 'value11', 11),(12, 'name12', 'value12', 12),(13, 'name13', 'value13', 13),(14, 'name14', 'value14', 14),(15, 'name15', 'value15', 15),(16, 'name16', 'value16', 16),(17, 'name17', 'value17', 17);"
	t2Insert := "insert into t2 (id, shardKey) values (0, 0);"

	mcmp.Exec(t1Insert)
	mcmp.Exec(t2Insert)

	query := "select /*vt+ PLANNER=Gen4 */ count(tbl0.t1_id), count(tbl0.t1_id), count(tbl0.name) from t1 as tbl0 group by tbl0.value, tbl0.value order by tbl0.value limit 14"

	stmt, _, err := sqlparser.Parse2(query)
	require.NoError(t, err)
	rewritten, _ := sqlparser.RewriteAST(sqlparser.CloneStatement(stmt), "ks", sqlparser.SQLSelectLimitUnset, "", nil, nil)

	info := schemaInfo{}
	info.schema = map[string]tableT{
		"t1": {name: "t1", columns: []column{
			{name: "t1_id", typ: "bigint"},
			{name: "name", typ: "varchar"},
			{name: "value", typ: "varchar"},
			{name: "shardKey", typ: "bigint"},
		}},
		"t2": {name: "t2", columns: []column{
			{name: "id", typ: "bigint"},
			{name: "shardKey", typ: "bigint"},
		}},
	}
	simplified := simplifier.SimplifyStatement(
		rewritten.AST.(sqlparser.SelectStatement),
		"ks",
		info,
		func(statement sqlparser.SelectStatement) (failed bool) {
			q := sqlparser.String(statement)
			t.Run(q, func(t *testing.T) {
				mcmp, closer := start(t)
				defer closer()

				_, _ = mcmp.ExecAllowAndCompareError(q)
				failed = t.Failed()
			})
			return
		},
	)

	fmt.Println(sqlparser.String(simplified))
}

func TestFuzzAggregations(t *testing.T) {
	// This test randomizes values and queries, and checks that mysql returns the same values that Vitess does
	mcmp, closer := start(t)
	defer closer()

	noOfRows := rand.Intn(20)
	var values []string
	for i := 0; i < noOfRows; i++ {
		values = append(values, fmt.Sprintf("(%d, 'name%d', 'value%d', %d)", i, i, i, i))
	}
	t1Insert := fmt.Sprintf("insert into t1 (t1_id, name, value, shardKey) values %s;", strings.Join(values, ","))
	values = nil
	noOfRows = rand.Intn(20)
	for i := 0; i < noOfRows; i++ {
		values = append(values, fmt.Sprintf("(%d, %d)", i, i))
	}
	t2Insert := fmt.Sprintf("insert into t2 (id, shardKey) values %s;", strings.Join(values, ","))

	mcmp.Exec(t1Insert)
	mcmp.Exec(t2Insert)

	t.Cleanup(func() {
		if t.Failed() {
			fmt.Println(t1Insert)
			fmt.Println(t2Insert)
		}
	})

	schema := map[string]tableT{
		"t1": {name: "t1", columns: []column{
			{name: "t1_id", typ: "bigint"},
			{name: "name", typ: "varchar"},
			{name: "value", typ: "varchar"},
			{name: "shardKey", typ: "bigint"},
		}},
		"t2": {name: "t2", columns: []column{
			{name: "id", typ: "bigint"},
			{name: "shardKey", typ: "bigint"},
		}},
	}

	endBy := time.Now().Add(1 * time.Second)
	schemaTables := maps.Values(schema)

	var queryCount int
	for time.Now().Before(endBy) || t.Failed() {
		tables := createTables(schemaTables)
		query := randomQuery(tables, 3, 3)
		mcmp.Exec(query)
		if t.Failed() {
			fmt.Println(query)
		}
		queryCount++
	}
	fmt.Printf("Queries successfully executed: %d\n", queryCount)
}

func randomQuery(tables []tableT, maxAggrs, maxGroupBy int) string {
	randomCol := func(tblIdx int) (string, string) {
		tbl := tables[tblIdx]
		col := randomEl(tbl.columns)
		return fmt.Sprintf("tbl%d.%s", tblIdx, col.name), col.typ
	}
	predicates := createPredicates(tables, randomCol)
	aggregates := createAggregations(tables, maxAggrs, randomCol)
	grouping := createGroupBy(tables, maxGroupBy, randomCol)
	sel := "select /*vt+ PLANNER=Gen4 */ " + strings.Join(aggregates, ", ") + " from "

	var tbls []string
	for i, s := range tables {
		tbls = append(tbls, fmt.Sprintf("%s as tbl%d", s.name, i))
	}
	sel += strings.Join(tbls, ", ")

	if len(predicates) > 0 {
		sel += " where "
		sel += strings.Join(predicates, " and ")
	}
	if len(grouping) > 0 {
		sel += " group by "
		sel += strings.Join(grouping, ", ")
	}
	// we do it this way so we don't have to do only `only_full_group_by` queries
	var noOfOrderBy int
	if len(grouping) > 0 {
		// panic on rand function call if value is 0
		noOfOrderBy = rand.Intn(len(grouping))
	}
	//if noOfOrderBy > 0 {
	//	noOfOrderBy = 0 // TODO turning on ORDER BY here causes lots of failures to happen
	//}
	if noOfOrderBy > 0 {
		var orderBy []string
		for noOfOrderBy > 0 {
			noOfOrderBy--
			if rand.Intn(2) == 0 || len(grouping) == 0 {
				orderBy = append(orderBy, randomEl(aggregates))
			} else {
				orderBy = append(orderBy, randomEl(grouping))
			}
		}
		sel += " order by "
		sel += strings.Join(orderBy, ", ")
	}

	if rand.Intn(2) > 0 {
		limitNum := rand.Intn(20)
		sel += fmt.Sprintf(" limit %d", limitNum)
	}

	return sel
}

func createGroupBy(tables []tableT, maxGB int, randomCol func(tblIdx int) (string, string)) (grouping []string) {
	noOfGBs := rand.Intn(maxGB)
	for i := 0; i < noOfGBs; i++ {
		tblIdx := rand.Intn(len(tables))
		col, _ := randomCol(tblIdx)
		grouping = append(grouping, col)
	}
	return
}

func createAggregations(tables []tableT, maxAggrs int, randomCol func(tblIdx int) (string, string)) (aggregates []string) {
	aggregations := []func(string) string{
		func(_ string) string { return "count(*)" },
		func(e string) string { return fmt.Sprintf("count(%s)", e) },
		//func(e string) string { return fmt.Sprintf("sum(%s)", e) },
		//func(e string) string { return fmt.Sprintf("avg(%s)", e) },
		//func(e string) string { return fmt.Sprintf("min(%s)", e) },
		//func(e string) string { return fmt.Sprintf("max(%s)", e) },
	}

	noOfAggrs := rand.Intn(maxAggrs) + 1
	for i := 0; i < noOfAggrs; i++ {
		tblIdx := rand.Intn(len(tables))
		e, _ := randomCol(tblIdx)
		aggregates = append(aggregates, randomEl(aggregations)(e))
	}
	return aggregates
}

func createTables(schemaTables []tableT) []tableT {
	noOfTables := rand.Intn(2) + 1
	var tables []tableT

	for i := 0; i < noOfTables; i++ {
		tables = append(tables, randomEl(schemaTables))
	}
	return tables
}

func createPredicates(tables []tableT, randomCol func(tblIdx int) (string, string)) (predicates []string) {
	for idx1 := range tables {
		for idx2 := range tables {
			if idx1 == idx2 {
				continue
			}
			noOfPredicates := rand.Intn(2)

			for noOfPredicates > 0 {
				col1, t1 := randomCol(idx1)
				col2, t2 := randomCol(idx2)
				if t1 != t2 {
					continue
				}
				predicates = append(predicates, fmt.Sprintf("%s = %s", col1, col2))
				noOfPredicates--
			}
		}
	}
	return predicates
}

func randomEl[K any](in []K) K {
	return in[rand.Intn(len(in))]
}
