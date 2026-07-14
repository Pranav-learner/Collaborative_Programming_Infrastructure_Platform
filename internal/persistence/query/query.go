package query

import (
	"fmt"
	"strings"
)

// Operator represents standard query filtering operations.
type Operator string

const (
	OpEquals      Operator = "="
	OpNotEquals   Operator = "!="
	OpLike        Operator = "LIKE"
	OpGreater     Operator = ">"
	OpLess        Operator = "<"
	OpIsNull      Operator = "IS NULL"
	OpIsNotNull   Operator = "IS NOT NULL"
)

// Filter holds conditional properties for a query.
type Filter struct {
	Field    string
	Operator Operator
	Value    any
}

// Sort specifies column ordering.
type Sort struct {
	Field     string
	Ascending bool
}

// Params configures pagination, sorting, filtering, and projecting.
type Params struct {
	Filters []Filter
	Sorts   []Sort
	Limit   int
	Offset  int
}

// BuildQuery constructs standard parameterized SQL query clauses safely.
func BuildQuery(baseQuery string, params Params, startPlaceholderIndex int) (string, []any) {
	var clauses []string
	var args []any
	placeholderIdx := startPlaceholderIndex

	// 1. Map filters.
	for _, f := range params.Filters {
		switch f.Operator {
		case OpIsNull, OpIsNotNull:
			clauses = append(clauses, fmt.Sprintf("%s %s", f.Field, string(f.Operator)))
		default:
			clauses = append(clauses, fmt.Sprintf("%s %s $%d", f.Field, string(f.Operator), placeholderIdx))
			args = append(args, f.Value)
			placeholderIdx++
		}
	}

	query := baseQuery
	if len(clauses) > 0 {
		if strings.Contains(strings.ToUpper(baseQuery), " WHERE ") {
			query += " AND " + strings.Join(clauses, " AND ")
		} else {
			query += " WHERE " + strings.Join(clauses, " AND ")
		}
	}

	// 2. Sort ordering.
	if len(params.Sorts) > 0 {
		var sortClauses []string
		for _, s := range params.Sorts {
			dir := "ASC"
			if !s.Ascending {
				dir = "DESC"
			}
			sortClauses = append(sortClauses, fmt.Sprintf("%s %s", s.Field, dir))
		}
		query += " ORDER BY " + strings.Join(sortClauses, ", ")
	}

	// 3. Limit and Offset.
	if params.Limit > 0 {
		query += fmt.Sprintf(" LIMIT %d", params.Limit)
	}
	if params.Offset > 0 {
		query += fmt.Sprintf(" OFFSET %d", params.Offset)
	}

	return query, args
}
