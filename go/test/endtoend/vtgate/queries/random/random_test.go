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

package random

import (
	"fmt"
	"math/rand"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"golang.org/x/exp/maps"

	"vitess.io/vitess/go/test/endtoend/cluster"
	"vitess.io/vitess/go/test/endtoend/utils"
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

func start(t *testing.T) (utils.MySQLCompare, func()) {
	mcmp, err := utils.NewMySQLCompare(t, vtParams, mysqlParams)
	require.NoError(t, err)

	deleteAll := func() {
		_, _ = utils.ExecAllowError(t, mcmp.VtConn, "set workload = oltp")

		tables := []string{"dept", "emp"}
		for _, table := range tables {
			_, _ = mcmp.ExecAndIgnore("delete from " + table)
		}
	}

	deleteAll()

	mcmp.Exec("INSERT INTO emp(empno, ename, job, mgr, hiredate, sal, comm, deptno) VALUES (7369,'SMITH','CLERK',7902,'1980-12-17',800,NULL,20);")
	mcmp.Exec("INSERT INTO emp(empno, ename, job, mgr, hiredate, sal, comm, deptno) VALUES (7499,'ALLEN','SALESMAN',7698,'1981-02-20',1600,300,30);")
	mcmp.Exec("INSERT INTO emp(empno, ename, job, mgr, hiredate, sal, comm, deptno) VALUES (7521,'WARD','SALESMAN',7698,'1981-02-22',1250,500,30);")
	mcmp.Exec("INSERT INTO emp(empno, ename, job, mgr, hiredate, sal, comm, deptno) VALUES (7566,'JONES','MANAGER',7839,'1981-04-02',2975,NULL,20);")
	mcmp.Exec("INSERT INTO emp(empno, ename, job, mgr, hiredate, sal, comm, deptno) VALUES (7654,'MARTIN','SALESMAN',7698,'1981-09-28',1250,1400,30);")
	mcmp.Exec("INSERT INTO emp(empno, ename, job, mgr, hiredate, sal, comm, deptno) VALUES (7698,'BLAKE','MANAGER',7839,'1981-05-01',2850,NULL,30);")
	mcmp.Exec("INSERT INTO emp(empno, ename, job, mgr, hiredate, sal, comm, deptno) VALUES (7782,'CLARK','MANAGER',7839,'1981-06-09',2450,NULL,10);")
	mcmp.Exec("INSERT INTO emp(empno, ename, job, mgr, hiredate, sal, comm, deptno) VALUES (7788,'SCOTT','ANALYST',7566,'1982-12-09',3000,NULL,20);")
	mcmp.Exec("INSERT INTO emp(empno, ename, job, mgr, hiredate, sal, comm, deptno) VALUES (7839,'KING','PRESIDENT',NULL,'1981-11-17',5000,NULL,10);")
	mcmp.Exec("INSERT INTO emp(empno, ename, job, mgr, hiredate, sal, comm, deptno) VALUES (7844,'TURNER','SALESMAN',7698,'1981-09-08',1500,0,30);")
	mcmp.Exec("INSERT INTO emp(empno, ename, job, mgr, hiredate, sal, comm, deptno) VALUES (7876,'ADAMS','CLERK',7788,'1983-01-12',1100,NULL,20);")
	mcmp.Exec("INSERT INTO emp(empno, ename, job, mgr, hiredate, sal, comm, deptno) VALUES (7900,'JAMES','CLERK',7698,'1981-12-03',950,NULL,30);")
	mcmp.Exec("INSERT INTO emp(empno, ename, job, mgr, hiredate, sal, comm, deptno) VALUES (7902,'FORD','ANALYST',7566,'1981-12-03',3000,NULL,20);")
	mcmp.Exec("INSERT INTO emp(empno, ename, job, mgr, hiredate, sal, comm, deptno) VALUES (7934,'MILLER','CLERK',7782,'1982-01-23',1300,NULL,10);")
	mcmp.Exec("INSERT INTO dept(deptno, dname, loc) VALUES (10,'ACCOUNTING','NEW YORK');")
	mcmp.Exec("INSERT INTO dept(deptno, dname, loc) VALUES (20,'RESEARCH','DALLAS');")
	mcmp.Exec("INSERT INTO dept(deptno, dname, loc) VALUES (30,'SALES','CHICAGO');")
	mcmp.Exec("INSERT INTO dept(deptno, dname, loc) VALUES (40,'OPERATIONS','BOSTON');")

	return mcmp, func() {
		deleteAll()
		mcmp.Close()
		cluster.PanicHandler(t)
	}
}

func helperTest(t *testing.T, query string) {
	t.Helper()
	t.Run(query, func(t *testing.T) {
		mcmp, closer := start(t)
		defer closer()

		mcmp.Exec(query)
	})
}

func TestKnownFailures(t *testing.T) {
	require.NoError(t, utils.WaitForAuthoritative(t, clusterInstance.VtgateProcess, keyspaceName, "emp"))
	require.NoError(t, utils.WaitForAuthoritative(t, clusterInstance.VtgateProcess, keyspaceName, "dept"))

	// logs more stuff
	//clusterInstance.EnableGeneralLog()

	// succeeds
	helperTest(t, "select /*vt+ PLANNER=Gen4 */ distinct count(*), tbl0.loc from dept as tbl0 group by tbl0.loc")

	// unsupported: using aggregation on top of a *operators.Aggregator plan
	helperTest(t, "select /*vt+ PLANNER=Gen4 */ distinct count(*) from dept as tbl0 group by tbl0.loc")

	// mismatched results
	helperTest(t, "select /*vt+ PLANNER=Gen4 */ sum(tbl1.comm) from emp as tbl0, emp as tbl1")

	// cannot compare strings, collation is unknown or unsupported
	helperTest(t, "select /*vt+ PLANNER=Gen4 */ min(tbl0.loc) from dept as tbl0")

	// unsupported: in scatter query: aggregation function
	helperTest(t, "select /*vt+ PLANNER=Gen4 */ avg(tbl0.deptno) from dept as tbl0")

	// EOF (errno 2013) (sqlstate HY000)
	helperTest(t, "select /*vt+ PLANNER=Gen4 */ tbl1.mgr, tbl1.mgr, count(*) from emp as tbl1 group by tbl1.mgr")

	// EOF (same as above)
	helperTest(t, "select /*vt+ PLANNER=Gen4 */ tbl1.mgr, tbl1.mgr, count(*) from emp as tbl0, emp as tbl1 group by tbl1.mgr")

	// mismatched results
	helperTest(t, "select /*vt+ PLANNER=Gen4 */ count(*), count(*), count(tbl0.comm) from emp as tbl0, emp as tbl1 join dept as tbl2")

	// unsupported: using aggregation on top of a *planbuilder.orderedAggregate plan
	helperTest(t, "select /*vt+ PLANNER=Gen4 */ count(*) from (select count(*) from dept as tbl0) as tbl0")

	// unsupported: using aggregation on top of a *planbuilder.orderedAggregate plan
	helperTest(t, "select /*vt+ PLANNER=Gen4 */ count(*), count(*) from (select count(*) from dept as tbl0) as tbl0, dept as tbl1")

	// EOF (errno 2013) (sqlstate HY000)
	helperTest(t, "select /*vt+ PLANNER=Gen4 */ count(*), count(*) from (select count(*) from dept as tbl0 group by tbl0.deptno) as tbl0")

	// succeeds
	helperTest(t, "select /*vt+ PLANNER=Gen4 */ count(*), count(*) from (select count(*) from dept as tbl0 group by tbl0.deptno) as tbl0, dept as tbl1")

	// succeeds
	helperTest(t, "select /*vt+ PLANNER=Gen4 */ count(*) from (select count(*) from dept as tbl0 group by tbl0.deptno) as tbl0")
}

func TestRandom(t *testing.T) {
	mcmp, closer := start(t)
	defer closer()

	require.NoError(t, utils.WaitForAuthoritative(t, clusterInstance.VtgateProcess, keyspaceName, "emp"))
	require.NoError(t, utils.WaitForAuthoritative(t, clusterInstance.VtgateProcess, keyspaceName, "dept"))

	schema := map[string]tableT{
		"emp": {name: "emp", columns: []column{
			{name: "empno", typ: "bigint"},
			{name: "ename", typ: "varchar"},
			{name: "job", typ: "varchar"},
			{name: "mgr", typ: "bigint"},
			{name: "hiredate", typ: "date"},
			{name: "sal", typ: "bigint"},
			{name: "comm", typ: "bigint"},
			{name: "deptno", typ: "bigint"},
		}},
		"dept": {name: "dept", columns: []column{
			{name: "deptno", typ: "bigint"},
			{name: "dname", typ: "varchar"},
			{name: "loc", typ: "varchar"},
		}},
	}

	endBy := time.Now().Add(10 * time.Second)
	schemaTables := maps.Values(schema)

	var queryCount int
	for time.Now().Before(endBy) || t.Failed() {
		query := randomQuery(schemaTables, 3, 3)
		mcmp.Exec(query)
		if t.Failed() {
			fmt.Println(query)
		}
		queryCount++
	}
	fmt.Printf("Queries successfully executed: %d", queryCount)
}

func randomQuery(schemaTables []tableT, maxAggrs, maxGroupBy int) string {
	tables := createTables(schemaTables)

	randomCol := func(tblIdx int) (string, string) {
		tbl := tables[tblIdx]
		col := randomEl(tbl.columns)
		return fmt.Sprintf("tbl%d.%s", tblIdx, col.name), col.typ
	}

	isDerived := rand.Intn(10) < 0
	aggregates, _ := createAggregations(tables, maxAggrs, randomCol, isDerived)
	predicates := createPredicates(tables, randomCol, false)
	grouping, _ := createGroupBy(tables, maxGroupBy, randomCol)
	sel := "select /*vt+ PLANNER=Gen4 */ "

	// select distinct (fails)
	if rand.Intn(2) > 1 {
		sel += "distinct "
	}

	// select the grouping columns (fails)
	if len(grouping) > 10 {
		sel += strings.Join(grouping, ", ") + ", "
	}

	sel += strings.Join(aggregates, ", ") + " from "

	var tbls []string
	for i, s := range tables {
		tbls = append(tbls, fmt.Sprintf("%s as tbl%d", s.name, i))
	}
	sel += strings.Join(tbls, ", ")

	//// join (fails)
	//if rand.Intn(1) > 0 {
	//	tables = append(tables, randomEl(schemaTables))
	//	join := createPredicates(tables, randomCol, true)
	//	sel += " join " + fmt.Sprintf("%s as tbl%d", tables[len(tables)-1].name, len(tables)-1)
	//	if len(join) > 0 {
	//		sel += " on " + strings.Join(join, " and ")
	//	}
	//}

	if len(predicates) > 0 {
		sel += " where "
		sel += strings.Join(predicates, " and ")
	}

	if len(grouping) > 0 {
		sel += " group by "
		sel += strings.Join(grouping, ", ")
	}

	// we do it this way, so we don't have to do only `only_full_group_by` queries
	var noOfOrderBy int
	if len(grouping) > 0 {
		// panic on rand function call if value is 0 (??)
		noOfOrderBy = rand.Intn(len(grouping))
	}
	if noOfOrderBy > 0 {
		noOfOrderBy = 0 // TODO turning on ORDER BY here causes lots of failures to happen
	}
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

	// limit (fails)
	if rand.Intn(2) > 1 {
		limitNum := rand.Intn(20)
		sel += fmt.Sprintf(" limit %d", limitNum)
	}

	// add generated query to schemaTables
	schemaTables = append(schemaTables, tableT{
		name:    "(" + sel + ")",
		columns: nil,
	})

	// derived tables (fails)
	if false {
		sel = randomQuery(schemaTables, 3, 3)
	}

	// cleanup
	schemaTables = schemaTables[:len(schemaTables)-1]

	return sel
}

func createGroupBy(tables []tableT, maxGB int, randomCol func(tblIdx int) (string, string)) (grouping []string, groupTypes []string) {
	noOfGBs := rand.Intn(maxGB)
	for i := 0; i < noOfGBs; i++ {
		var tblIdx int
		for {
			tblIdx = rand.Intn(len(tables))
			if tables[tblIdx].columns != nil {
				break
			}
		}
		col, typ := randomCol(tblIdx)
		grouping = append(grouping, col)
		groupTypes = append(groupTypes, typ)
	}
	return grouping, groupTypes
}

func createAggregations(tables []tableT, maxAggrs int, randomCol func(tblIdx int) (string, string), isDerived bool) (aggregates []string, aggrTypes []string) {
	aggregations := []func(string) string{
		func(_ string) string { return "count(*)" },
		func(e string) string { return fmt.Sprintf("count(%s)", e) },
		//func (e string) string { return fmt.Sprintf("sum(%s)", e) },
		//func(e string) string { return fmt.Sprintf("avg(%s)", e) },
		//func(e string) string { return fmt.Sprintf("min(%s)", e) },
		//func(e string) string { return fmt.Sprintf("max(%s)", e) },
	}

	noOfAggrs := rand.Intn(maxAggrs) + 1
	for i := 0; i < noOfAggrs; i++ {
		var tblIdx int
		for {
			tblIdx = rand.Intn(len(tables))
			if tables[tblIdx].columns != nil {
				break
			}
		}
		e, typ := randomCol(tblIdx)
		newAggregate := randomEl(aggregations)(e)
		addAggr := true
		if isDerived {
			for _, aggr := range aggregates {
				if newAggregate == aggr {
					addAggr = false
					break
				}
			}
		}
		if addAggr {
			aggregates = append(aggregates, newAggregate)
			if newAggregate == fmt.Sprintf("count(%s)", e) && typ == "bigint" {
				aggrTypes = append(aggrTypes, "decimal")
			} else {
				aggrTypes = append(aggrTypes, typ)
			}
		}
	}
	return aggregates, aggrTypes
}

func createTables(schemaTables []tableT) []tableT {
	noOfTables := rand.Intn(2) + 1
	var tables []tableT

	for i := 0; i < noOfTables; i++ {
		tables = append(tables, randomEl(schemaTables))
	}
	return tables
}

func createPredicates(tables []tableT, randomCol func(tblIdx int) (string, string), isJoin bool) (predicates []string) {
	// if creating predicates for a join,
	// then make sure predicates are created for the last two tables (which are being joined)
	incr := 0
	if isJoin && len(tables) > 2 {
		incr += len(tables) - 2
		tables = tables[len(tables)-3 : len(tables)-1]
	}
	for idx1 := range tables {
		for idx2 := range tables {
			if idx1 == idx2 || idx1 < incr || idx2 < incr || tables[idx1].columns == nil || tables[idx2].columns == nil {
				continue
			}
			noOfPredicates := rand.Intn(2)
			if isJoin {
				noOfPredicates++
			}

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