package agegraph

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"fmt"

	//"strconv"
	"strings"
	"time"

	//"bytes"

	//ageGraph "github.com/apache/age/drivers/golang/age"
	ag "github.com/bitnine-oss/agensgraph-golang"
	"github.com/prahaladd/gograph/core"
	"github.com/prahaladd/gograph/query/cypher"
	//"gorm.io/driver/postgres"
	//"gorm.io/driver/postgres"
	//"gorm.io/gorm"
)

type ageContextKey string

// query context constants
const (
	// ContextKeyQueryTimeoutSeconds is used in context to specify the query timeout in milli-seconds
	// The value of this context parameter is currently ignored
	ContextKeyQueryTimeoutMillis = ageContextKey("QueryTimeout")
	// ContextKeyIsolationLevel is used in context to specify the isolation level for query execution
	ContextKeyIsolationLevel = ageContextKey("IsolationLevel")
	// ContextKeyReadonly is used to specify if the transaction is read-only
	ContextKeyReadOnly = ageContextKey("ReadOnly")
	// ContextKeyGraph name is used to specify the graph against which the operations should be executed
	ContextKeyGraphName = ageContextKey("graphName")
	// ContextKeyWriteModeCreate is used to specify if an vertex or edge creation should be done via CREATE instead of a  MERGE
	ContextKeyWriteModeCreate = ageContextKey("writeModeCreate")
)
const (
	AGE_USER_KEY              = "username"
	AGE_PASSWD_KEY            = "password"
	AGE_DBNAME_KEY            = "dbName"
	AGE_DEFAULT_PORT          = int32(5432)
	AGE_TLS_PROTOCOL          = "tls"
	AGE_QUERY_TIMEOUT_KEY     = "QUERY_TIMEOUT"
	AGE_DEFAULT_QUERY_TIMEOUT = 50 * time.Second
	AGE_GRAPH_NAME            = "graphName"
	AGE_SSL_MODE              = "sslMode"
	AGE_CA_FILE               = "caFile"
)

// query options is a simple struct to accumlate all settings required
// to execute the SQL query against the underlying database
type queryOptions struct {
	timeout         int64
	txOpts          *sql.TxOptions
	writeModeCreate bool
}

// AgeGraphConnection implements a connection to an [Agegraph] database.
// Agegraph is backed by PostgreSQL and hence the mechanism of obtaining the initial
// connection is equivalent to connecting to a PostgreSQL instance.
// Agegraph stores multiple graphs with a schema created for each defined graph.
// Hence the graph name must be specified when executing an operation within the context
// as a string value using the ContextKeyGraphName key.
//
// Compared to other graph databases such as Neo4J, Agegraph does not allow the MERGE
// operation to create new node or vertex labels. Only CREATE operations can create non-existent
// labels and corresponding vertex or edge.
// Hence applications must provide the correct semantics during a write operation on whether
// the generated cypher query must use CREATE or MERGE clause. By default the write operation
// would be done using a MERGE. This behavior can be overrriden by specifying a boolean
// value of true aganst the context key ContextKeyWriteModeCreate
//
// Isolation levels can be specified by specifying the correct
// sql.IsolationLevel valyes against the context key  ContextKeyIsolationLevel key. Defaults
// to the default isolation provided by the database if not specified
//
// Read only transactions can be created by specifying a boolean value of true against
// the context key ContextKeyReadOnly. Defaults to false.
//
// [Agegraph]: https://github.com/apache/age/tree/master/drivers/golang
type AgeGraphConnection struct {
	db *sql.DB
}

// QueryVertex returns a vertex from the graph for the specified label
// selectors are required to "select" a particular node within the graph. If selectors are not specified, then all nodes in the graph
// with the specified label woould be selected.
//
// filters are used to filter out the results from the set of selected nodes
func (age *AgeGraphConnection) QueryVertex(ctx context.Context, label string, selectors core.KVMap, filters core.KVMap, queryParams core.KVMap) ([]*core.Vertex, error) {
	vqb := cypher.NewVertexQueryBuilder()
	vqb.SetQueryMode(core.Read)
	vqb.SetLabel([]string{label})
	vqb.SetSelector(selectors)
	vqb.SetFilters(filters)
	vqb.SetVarName("v")

	query, err := vqb.Build()

	if err != nil {
		return nil, err
	}
	vertices := make([]*core.Vertex, 0)

	qr, err := age.ExecuteQuery(ctx, query, core.Read, nil)

	if err != nil {
		return nil, err
	}

	for _, row := range qr.Rows {
		var agVertex ag.BasicVertex
		err = ag.ScanEntity(row["v"], &agVertex)
		if err != nil {
			return nil, err
		}
		v := age.agVertexToVertex(&agVertex)
		vertices = append(vertices, v)
	}
	return vertices, nil
}

// QueryEdge returns a set of edges for the specified label
//
// selctors are required to select a particular relationship within the graph. If selectors are not specified, then all edges in the graph
// with the specified labels would be selected
//
// filters are used to filter out the results from the set of selected edges
//
// The level of detail about the start and end nodes of an edge  can be controled by the fetch mode. Currently, the library
// supports returning edges where-in the ids of the start and end vertices of the relations are available.
func (age *AgeGraphConnection) QueryEdge(ctx context.Context, startVertexLabel []string, endVertexLabel []string, label string, startVertexSelectors core.KVMap, endVertexSelectors core.KVMap, selectors core.KVMap, startVertexFilters core.KVMap, endVertexFilters core.KVMap, filters core.KVMap, queryParams core.KVMap, fetchMode core.EdgeFetchMode) ([]*core.Edge, error) {
	edgeQueryBuilder := cypher.NewEdgeQueryBuilder()
	edgeQueryBuilder.SetEdgeFetchMode(fetchMode)
	edgeQueryBuilder.SetStartVertexLabels(startVertexLabel)
	edgeQueryBuilder.SetEndVertexLabels(endVertexLabel)
	edgeQueryBuilder.SetLabel([]string{label})
	edgeQueryBuilder.SetStartVertexSelector(startVertexSelectors)
	edgeQueryBuilder.SetEndVertexSelector(endVertexSelectors)
	edgeQueryBuilder.SetSelector(selectors)
	edgeQueryBuilder.SetStartVertexFilters(startVertexFilters)
	edgeQueryBuilder.SetEndVertexFilters(endVertexFilters)
	edgeQueryBuilder.SetFilters(filters)
	edgeQueryBuilder.SetVariableName("r")
	if fetchMode == core.EdgeWithCompleteVertex {
		edgeQueryBuilder.SetStartVertexVariableName("sv")
		edgeQueryBuilder.SetEndVertexVariableName("ev")
	}

	query, err := edgeQueryBuilder.Build()

	if err != nil {
		return nil, err
	}
	qr, err := age.ExecuteQuery(ctx, query, core.Read, filters)
	if err != nil {
		return nil, err
	}
	edges := make([]*core.Edge, 0)
	for _, row := range qr.Rows {
		var agEdge ag.BasicEdge
		err := ag.ScanEntity(row["r"], &agEdge)
		if err != nil {
			return nil, err
		}
		var agSrcVertex, agDestVertex *ag.BasicVertex

		if fetchMode == core.EdgeWithCompleteVertex {
			agSrcVertex = new(ag.BasicVertex)
			agDestVertex = new(ag.BasicVertex)
			ag.ScanEntity(row["sv"], agSrcVertex)
			ag.ScanEntity(row["ev"], agDestVertex)
		}
		e := age.agEdgeToEdge(&agEdge, agSrcVertex, agDestVertex)
		edges = append(edges, e)
	}
	return edges, nil
}

// ExecuteReadQuery executes a query and transforms the native result set obtained from the DB to a QueryResult using the specified transform function
//
// The specified query must be a valid Cypher or Gremlin query.
//
// The mode parameter specifies the mode of the query - read or write. This parameter is required in
// order to handle API invocations for certain drivers such as Neo4J where-in the query mode would
// determine the access mode of the session being used to execute the query
//
// The queryParams parameter can be used to inject dynamic data into the query. This is an optional argument and can be nil
//
// The context can contain additional query and session configuration parameters required for execution

func trimSuffix(s, suffix string) string {
	if strings.HasSuffix(s, suffix) {
		s = s[:len(s)-len(suffix)]
	}
	return s
}

//func (age *AgeGraphConnection) ExecuteQuery(ctx context.Context, query string, mode core.QueryMode, queryParams map[string]interface{}) (*core.QueryResult, error) {
//sql, _ := age.db.DB()
//tx, _ := sql.Begin()

//query = strings.ReplaceAll(query, `'`, `"`)
//query = trimSuffix(query, ";")
//for key, value := range queryParams {
//switch value.(type) {
//case int64:
//va := fmt.Sprintf("%v", value)
//query = strings.ReplaceAll(query, "$"+key, va)
//case int:
//va := fmt.Sprintf("%v", value)
//query = strings.ReplaceAll(query, "$"+key, va)
//case float64:
//query = strings.ReplaceAll(query, "$"+key, value.(string))
//case string:
//query = strings.ReplaceAll(query, "$"+key, `"`+value.(string)+`"`)
//case bool:
//va := "false"
//if value.(bool) {
//va = "true"
//}
//query = strings.ReplaceAll(query, "$"+key, va)
//default:
//query = strings.ReplaceAll(query, "$"+key, `"`+value.(string)+`"`)
//}
//}
////query, _ = strconv.Unquote(query)
//fmt.Print(query)
//columnCount, ok := queryParams["columnCount"].(int)
//if !ok {
//columnCount = 0
//}
////ageGraph.GetReady(sql, "graphdb")
//ageGraph.ExecCypher(tx, "graphdb", columnCount, query)
//return nil, nil
//}

func (age *AgeGraphConnection) ExecuteQuery(ctx context.Context, query string, mode core.QueryMode, queryParams map[string]interface{}) (*core.QueryResult, error) {
	var buf bytes.Buffer
	buf.WriteString("SELECT * from ag_catalog.cypher(NULL,NULL) as (v0 agtype")
	nvt := query
	_ = nvt
	query = strings.ReplaceAll(query, `"`, `\"`)
	query = strings.ReplaceAll(query, `'`, `"`)
	query = trimSuffix(query, ";")
	for key, value := range queryParams {
		switch value.(type) {
		case int64:
			va := fmt.Sprintf("%v", value)
			query = strings.ReplaceAll(query, "$"+key, va)
		case int:
			va := fmt.Sprintf("%v", value)
			query = strings.ReplaceAll(query, "$"+key, va)
		case float64:
			query = strings.ReplaceAll(query, "$"+key, value.(string))
		case string:
			query = strings.ReplaceAll(query, "$"+key, `"`+value.(string)+`"`)
		case []string:
			if len(value.([]string)) > 0 {
				justString := strings.Join(value.([]string), "\",\"")
				query = strings.ReplaceAll(query, "$"+key, `["`+justString+`"]`)
			} else {
				query = strings.ReplaceAll(query, "$"+key, `[]`)
			}
		case bool:
			va := "false"
			if value.(bool) {
				va = "true"
			}
			query = strings.ReplaceAll(query, "$"+key, va)
		default:
			query = strings.ReplaceAll(query, "$"+key, `"`+value.(string)+`"`)
		}
	}

	columnCount, ok := queryParams["columnCount"].(int)
	if !ok {
		columnCount = 0
	}

	for i := 1; i < columnCount; i++ {
		buf.WriteString(fmt.Sprintf(", v%d agtype", i))
	}
	buf.WriteString(");")

	stmt := buf.String()

	//graphName, ok := ctx.Value(ContextKeyGraphName).(string)
	graphName := "graphdb"

	//stmt = fmt.Sprintf(stmt, graphName, query)
	//if !ok {
	//return nil, errors.New("graph name must be specified")
	//}
	tx, err := age.db.Begin()
	if err != nil {
		panic(err)
	}
	prepare_stmt := "SELECT * FROM ag_catalog.age_prepare_cypher($1, $2);"
	_, perr := tx.Exec(prepare_stmt, graphName, query)
	//fmt.Println("Writing", query)
	if perr != nil {
		tx.Rollback()
		fmt.Println(prepare_stmt + " " + graphName + " " + query)
		return nil, perr
	}

	if columnCount == 0 {
		_, err := tx.Exec(stmt)
		if err != nil {
			fmt.Println(query)
			tx.Rollback()
			return nil, err
		}
		tx.Commit()
		return nil, nil
	}
	rows, err := tx.Query(stmt)
	if err != nil {
		fmt.Println(query)
		tx.Rollback()
		return nil, err
	}

	defer rows.Close()
	queryResult := core.QueryResult{}
	keys, err := rows.Columns()
	if err != nil {
		tx.Rollback()
		return nil, err
	}
	vals := make([]interface{}, len(keys))
	rawResults := make([]sql.RawBytes, len(keys))

	for i := range keys {
		vals[i] = &rawResults[i]
	}
	for rows.Next() {
		rows.Scan(vals...)
		currentRawRow := make([]sql.RawBytes, len(keys))
		copy(currentRawRow, rawResults)
		m := make(core.Row)
		for i, key := range keys {
			data := make([]byte, len(currentRawRow[i]))
			copy(data, currentRawRow[i])
			m[key] = data
		}
		queryResult.Rows = append(queryResult.Rows, m)
	}
	tx.Commit()
	return &queryResult, nil
}

// Close closes the connection to a database.
//
// Not all implementations of the below method ould actually close a connection. For e.g. if the database is being
//
// updated using an HTTP(s) interface then there is no requirement to close the connection explicitly.
func (age *AgeGraphConnection) Close(ctx context.Context) error {
	return nil
}

// StoreVertex stores a vertex to the underlying graph database.
//
// Upon successful storage, the passed in vertex object's ID field would be set to the ID returned by the database.
// Returns an error if there is a failure when persisting the vertex
func (age *AgeGraphConnection) StoreVertex(ctx context.Context, vertex *core.Vertex) error {
	qopts := age.queryOptionsFromContext(ctx, core.Write)
	vqb := cypher.NewVertexQueryBuilder()
	vqb.SetQueryMode(core.Write)
	vqb.SetLabel(vertex.Labels)
	vqb.SetSelector(vertex.Properties)
	vqb.SetVarName("sv")
	if qopts.writeModeCreate {
		vqb.SetWriteMode(core.Create)
	}

	query, err := vqb.Build()
	if err != nil {
		return err
	}

	qr, err := age.ExecuteQuery(ctx, query, core.Write, nil)
	if err != nil {
		return err
	}
	if len(qr.Rows) == 0 {
		return errors.New("unexpected error. failed to store vertex")
	}
	// support only a single vertex store at a time. hence consider only the first returned row
	row := qr.Rows[0]
	var agVertex ag.BasicVertex
	err = ag.ScanEntity(row["sv"], &agVertex)
	if err != nil {
		return err
	}

	vertex.ID = core.NewId(agVertex.Id.String())

	return nil
}

// StoreEdge stores a connected component to the graph database. It can be used to create a new relation
// between two vertices or update the properties for an existing relation
//
// Upon successful storage, the ID field of the participating vertex and edge object are populated
// with the DB specific identifier.
// Returns an error if there is a failure when persisting the edge
func (age *AgeGraphConnection) StoreEdge(ctx context.Context, edge *core.Edge) error {
	qopts := age.queryOptionsFromContext(ctx, core.Write)

	if edge.SourceVertex == nil {
		return errors.New("source node must be specified for vertex connectivity")
	}

	eqb := cypher.NewEdgeQueryBuilder()
	eqb.SetQueryMode(core.Write)
	eqb.SetStartVertexSelector(edge.SourceVertex.Properties)
	eqb.SetStartVertexVariableName("sv")
	eqb.SetStartVertexLabels(edge.SourceVertex.Labels)

	if edge.DestinationVertex != nil {
		eqb.SetEndVertexSelector(edge.DestinationVertex.Properties)
		eqb.SetEndVertexVariableName("ev")
		eqb.SetEndVertexLabels(edge.DestinationVertex.Labels)
	}

	eqb.SetEdgeFetchMode(core.EdgeWithCompleteVertex)
	eqb.SetLabel([]string{edge.Type})
	eqb.SetVariableName("rel")
	eqb.SetSelector(edge.Properties)
	if qopts.writeModeCreate {
		eqb.SetWriteMode(core.Create)
	}

	query, err := eqb.Build()

	if err != nil {
		return err
	}

	qr, err := age.ExecuteQuery(ctx, query, core.Write, nil)

	if err != nil {
		return err
	}

	if len(qr.Rows) == 0 {
		return errors.New("unexpected error. failed to store vertex connectivity")
	}

	row := qr.Rows[0]

	var agSrcVertex, agDestVertex ag.BasicVertex
	var agEdge ag.BasicEdge

	err = ag.ScanEntity(row["sv"], &agSrcVertex)
	if err != nil {
		return err
	}
	err = ag.ScanEntity(row["ev"], &agDestVertex)
	if err != nil {
		return err
	}
	err = ag.ScanEntity(row["rel"], &agEdge)
	if err != nil {
		return err
	}
	edge.SourceVertex.ID = core.NewId(agSrcVertex.Id.String())
	edge.ID = core.NewId(agEdge.Id.String())
	edge.DestinationVertex.ID = core.NewId(agDestVertex.Id.String())

	return nil
}

func (agc *AgeGraphConnection) queryOptionsFromContext(ctx context.Context, queryMode core.QueryMode) *queryOptions {
	qopts := queryOptions{timeout: int64(5 * time.Millisecond)}
	txOpts := sql.TxOptions{}
	if timeout, ok := ctx.Value(ContextKeyQueryTimeoutMillis).(int64); ok {
		qopts.timeout = timeout
	}

	if isolation, ok := ctx.Value(ContextKeyIsolationLevel).(sql.IsolationLevel); ok {
		txOpts.Isolation = isolation
	}

	if queryMode == core.Read {
		txOpts.ReadOnly = true
	}

	if writeWithCreate, ok := ctx.Value(ContextKeyWriteModeCreate).(bool); ok {
		qopts.writeModeCreate = writeWithCreate
	}
	qopts.txOpts = &txOpts
	return &qopts
}

func (agc *AgeGraphConnection) agVertexToVertex(agVertex *ag.BasicVertex) *core.Vertex {
	v := new(core.Vertex)
	v.ID = core.NewId(agVertex.Id.String())
	v.Labels = []string{agVertex.Label}
	v.Properties = make(core.KVMap)
	for prop, val := range agVertex.Properties {
		v.Properties[prop] = val
	}
	return v
}

func (agc *AgeGraphConnection) agEdgeToEdge(agEdge *ag.BasicEdge, srcVertex, destVertex *ag.BasicVertex) *core.Edge {
	e := new(core.Edge)
	e.Properties = make(core.KVMap)
	e.ID = core.NewId(agEdge.Id.String())
	e.Type = agEdge.Label
	e.SourceVertexID = core.NewId(agEdge.Start.String())
	e.DestinationVertexID = core.NewId(agEdge.End.String())
	if srcVertex != nil {
		e.SourceVertex = agc.agVertexToVertex(srcVertex)
	}
	if destVertex != nil {
		e.DestinationVertex = agc.agVertexToVertex(destVertex)
	}
	for k, v := range agEdge.Properties {
		e.Properties[k] = v
	}
	return e
}

// GetReady prepare AGE extension
// load AGE extension
// set graph path
func GetReady(db *sql.DB, graphName string) (bool, error) {
	tx, err := db.Begin()
	if err != nil {
		return false, err
	}

	_, err = tx.Exec("LOAD 'age';")
	if err != nil {
		return false, err
	}

	_, err = tx.Exec("SET search_path = ag_catalog, '$user', public;")
	if err != nil {
		return false, err
	}

	var count int = 0

	err = tx.QueryRow("SELECT count(*) FROM ag_graph WHERE name=$1", graphName).Scan(&count)

	if err != nil {
		return false, err
	}

	if count == 0 {
		_, err = tx.Exec("SELECT create_graph($1);", graphName)
		if err != nil {
			return false, err
		}
	}

	tx.Commit()

	return true, nil
}

// NewConnection returns a new connection to the specified Agegraph database.
//
// Agegraph behind the scenes uses Postgres. Hence the connectivity parameters are similar to
// specifying the connectivity parameters to connect to a Postgres instance.
//
// The connection to postgres requires a database name. The database name must be specified within the options KV store
// using the key 'dbName'. Alternatively, clients can use the constant AGE_DBNAME_KEY as a key to the options map and the value
// as the name of the database to connect.
//
// The host, protocol, realm and port parameters are used as follows:
//
// - protocol : Set to "tls" to enable sslmode. If emtpty string ("") is passed, then sslmode is disabled.
//
// - host : Mandatory parameter. If not set then an error would be returned.
//
// - port : Optional parameter. If not specified then the default PostgreSQL port 5432 is assumed
//
// - realm : Unused parameter. may be used in future
func NewConnection(protocol, host, realm string, port *int32, auth, options map[string]interface{}) (core.Connection, error) {

	if len(host) == 0 {
		return nil, errors.New("hostname must be specified to establish connection")
	}

	if !validateAuthData(auth) {
		return nil, errors.New("specify a valid AGE_USER_KEY and AGE_PWD_KEY")
	}
	userName := auth[AGE_USER_KEY]
	pwd := auth[AGE_PASSWD_KEY]
	if port == nil {
		port = new(int32)
		*port = AGE_DEFAULT_PORT
	}
	dbName, ok := options[AGE_DBNAME_KEY]
	if !ok {
		return nil, errors.New("database connection option must contain the AGE_DB_NAME key specifying the database")
	}

	graphName, ok := options[AGE_GRAPH_NAME].(string)
	if !ok {
		return nil, errors.New("database connection option must contain the AGE_GRAPH_NAME key specifying the database")
	}
	sslMode, ok := options[AGE_SSL_MODE].(string)
	if !ok {
		sslMode = "disable"
	}

	if protocol == AGE_TLS_PROTOCOL {
		sslMode = "enable"
	}

	psqlInfo := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=%s", host, *port, userName, pwd, dbName, sslMode)
	if sslMode == "verify-ca" {
		caFile, ok := options[AGE_CA_FILE].(string)
		if !ok {
			return nil, errors.New("database connection option must contain the AGE_CA_FILE key specifying the database")
		}
		psqlInfo = fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=%s sslrootcert=%s", host, *port, userName, pwd, dbName, sslMode, caFile)
	}

	db, err := sql.Open("postgres", psqlInfo)
	if err != nil {
		return nil, err
	}
	ageConnection := AgeGraphConnection{db: db}

	// Confirm graph_path created
	_, err = GetReady(db, graphName)
	if err != nil {
		panic(err)
	}
	return &ageConnection, nil
}

func validateAuthData(auth core.KVMap) bool {
	_, userNamePresent := auth[AGE_USER_KEY]
	_, pwdPresent := auth[AGE_PASSWD_KEY]
	return userNamePresent && pwdPresent
}

func init() {
	core.RegisterConnectorFactory("agegraph", NewConnection)
}
