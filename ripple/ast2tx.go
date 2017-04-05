// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Copyright 2016 The kingshard Authors. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"): you may
// not use this file except in compliance with the License. You may obtain
// a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// License for the specific language governing permissions and limitations
// under the License.

package ripple

import (
	"errors"
	"fmt"
	"strconv"
	"strings"

	kingerros "github.com/flike/kingshard/core/errors"
	"github.com/flike/kingshard/core/golog"
	"github.com/flike/kingshard/sqlparser"
)

func AstOp2RippleOp(astOp string) (string, error) {
	var rippleOp string
	var err error = nil
	if astOp == sqlparser.AST_EQ {
		rippleOp = "$eq"
	} else if astOp == sqlparser.AST_LE {
		rippleOp = "$le"
	} else if astOp == sqlparser.AST_LT {
		rippleOp = "$lt"
	} else if astOp == sqlparser.AST_GT {
		rippleOp = "$gt"
	} else if astOp == sqlparser.AST_GE {
		rippleOp = "$ge"
	} else if astOp == sqlparser.AST_NE {
		rippleOp = "$ne"
	} else if astOp == sqlparser.AST_IN {
		rippleOp = "$in"
	} else if astOp == sqlparser.AST_NOT_IN {
		rippleOp = "$nin"
	} else if astOp == sqlparser.AST_LIKE {
		rippleOp = "$regex"
	} else {
		err = errors.New("Not support Oprator.[" + astOp + "]")
	}

	return rippleOp, err
}

func HandleSelect(statement *sqlparser.Select, tx *Transaction) error {
	//var where *sqlparser.Where
	//var err error
	var tableName string

	switch v := (statement.From[0]).(type) {
	case *sqlparser.AliasedTableExpr:
		tableName = sqlparser.String(v.Expr)
	case *sqlparser.JoinTableExpr:
		if ate, ok := (v.LeftExpr).(*sqlparser.AliasedTableExpr); ok {
			tableName = sqlparser.String(ate.Expr)
		} else {
			tableName = sqlparser.String(v)
		}
	default:
		tableName = sqlparser.String(v)
	}
	tx.SetTransactionType(TxType_SQLStatement)
	tx.SetOpType(OpType_Select)
	// add table
	tx.AddTableByName(tableName, true)

	if err := handleSelectExprs(statement.SelectExprs, tx); err != nil {
		return err
	}

	if statement.Where != nil {
		if err := handleWhere(*statement.Where, tx); err != nil {
			return err
		}
	}

	if statement.Limit != nil {
		var index int64
		var count int64
		var err error
		if offSet, ok := (*statement.Limit).Offset.(sqlparser.NumVal); ok {
			index, err = strconv.ParseInt(string(offSet), 10, 64)
			if err != nil {
				golog.Error("ripple", "handleSelect", "parse limit's offset failure.["+err.Error()+"]", 0)
				return err
			}
		}

		if rowCount, ok := (*statement.Limit).Rowcount.(sqlparser.NumVal); ok {
			count, err = strconv.ParseInt(string(rowCount), 10, 64)
			if err != nil {
				golog.Error("ripple", "handleSelect", "parse limit's rowCount failure.["+err.Error()+"]", 0)
				return err
			}
		}
		tx.SetLimit(index, count)
	}

	if len(statement.OrderBy) > 0 {
		for _, order := range statement.OrderBy {
			var asc int
			if strings.EqualFold(order.Direction, "asc") {
				asc = Order_ASC
			} else if strings.EqualFold(order.Direction, "desc") {
				asc = Order_DESC
			} else {
				asc = Order_ASC
			}

			if col, ok := order.Expr.(*sqlparser.ColName); ok {
				tx.AddOneOrder(string(col.Name), asc)
			}
		}
	}

	return nil
}

func handleSelectExprs(selectExprs sqlparser.SelectExprs, tx *Transaction) error {
	fields := NewRawArrayItem()
	if len(selectExprs) == 0 {
		*fields = append(*fields, "*")
		if ok := tx.AddFields(fields); ok == false {
			return errors.New("Add query's fields failure.")
		}
		return nil
	}

	for _, expr := range selectExprs {
		switch v := expr.(type) {
		case *sqlparser.NonStarExpr:
			col := v.Expr.(*sqlparser.ColName)
			*fields = append(*fields, string(col.Name))
		case *sqlparser.StarExpr:
			*fields = append(*fields, "*")
		default:
			return fmt.Errorf("Not support type of selectExpr. %T", expr)
		}
	}
	if len(*fields) == 0 {
		// default all
		*fields = append(*fields, "*")
	}
	if ok := tx.AddFields(fields); ok == false {
		return errors.New("Add query's fields failure.")
	}
	return nil
}

func handleWhere(where sqlparser.Where, tx *Transaction) error {
	switch c := where.Expr.(type) {
	case *sqlparser.ComparisonExpr:
		condition, err := parseComparisonExpr(c)
		if err != nil {
			return err
		}
		tx.AddOneRawItem(condition)
	case *sqlparser.AndExpr:
		and_conds := NewConditionExprs()
		left_err := handleBoolExpr(1, &c.Left, &and_conds)
		if left_err != nil {
			return left_err
		}
		right_err := handleBoolExpr(1, &c.Right, &and_conds)
		if right_err != nil {
			return right_err
		}
		and, ok := MakeAndConditions(&and_conds)
		if ok != nil {
			return ok
		}
		tx.AddOneRawItem(and)
	case *sqlparser.OrExpr:
		or_conds := NewConditionExprs()
		left_err := handleBoolExpr(2, &c.Left, &or_conds)
		if left_err != nil {
			return left_err
		}
		right_err := handleBoolExpr(2, &c.Right, &or_conds)
		if right_err != nil {
			return right_err
		}

		or, ok := MakeOrConditions(&or_conds)
		if ok != nil {
			return ok
		}
		tx.AddOneRawItem(or)
	default:
		return fmt.Errorf("Not support type in handleWhere. %T, where's type: %s", c, where.Type)
	}
	return nil
}

func parseComparisonExpr(c *sqlparser.ComparisonExpr) (*JsonItem, error) {
	var operator string = c.Operator
	rippleOp, ok := AstOp2RippleOp(operator)
	if ok != nil {
		return nil, ok
	}
	condition_value := NewJsonItem()
	var colName []byte
	switch v := c.Right.(type) {
	case sqlparser.NumVal:
		i, err := strconv.ParseInt(sqlparser.String(c.Right), 10, 64)
		if err != nil {
			return nil, err
		}
		condition_value.AddField(rippleOp, i)
	case sqlparser.StrVal:
		condition_value.AddField(rippleOp, sqlparser.String(c.Right))
	default:
		return nil, fmt.Errorf("Not support type when parsing comparisonExpr. %T\n", v)
	}

	if col, ok := c.Left.(*sqlparser.ColName); ok {
		colName = col.Name
	} else {
		return nil, errors.New("left value of comparison must be colName")
	}
	condition := NewJsonItem()
	condition.AddField(string(colName), condition_value)
	return condition, nil
}

// parentExprType 0:ignore 1:AndExpr 2:OrExpr
func handleBoolExpr(parentExprType int, boolExpr *sqlparser.BoolExpr, conds *ConditionExprs) error {
	switch v := (*boolExpr).(type) {
	case *sqlparser.ComparisonExpr:
		//fmt.Printf("handleBoolExpr.sqlparser.ComparisonExpr\n")
		condition, err := parseComparisonExpr(v)
		if err != nil {
			return err
		}
		conds.AddOneExprEnty(condition)
	case *sqlparser.AndExpr:
		//fmt.Printf("handleBoolExpr.sqlparser.AndExpr\n")
		if parentExprType == 1 {
			left_err := handleBoolExpr(1, &v.Left, conds)
			if left_err != nil {
				return left_err
			}
			right_err := handleBoolExpr(1, &v.Right, conds)
			if right_err != nil {
				return right_err
			}
		} else if parentExprType == 2 {
			and_conds := NewConditionExprs()
			left_err := handleBoolExpr(1, &v.Left, &and_conds)
			if left_err != nil {
				return left_err
			}
			right_err := handleBoolExpr(1, &v.Right, &and_conds)
			if right_err != nil {
				return right_err
			}

			and, ok := MakeAndConditions(&and_conds)
			if ok != nil {
				golog.Error("ripple", "handleBoolExpr", ok.Error(), 0)
				return ok
			}
			conds.AddOneExprEnty(and)
		}

	case *sqlparser.OrExpr:
		//fmt.Printf("handleBoolExpr.sqlparser.OrExpr\n")
		if parentExprType == 2 {
			left_err := handleBoolExpr(2, &v.Left, conds)
			if left_err != nil {
				return left_err
			}
			right_err := handleBoolExpr(2, &v.Right, conds)
			if right_err != nil {
				return right_err
			}
		} else if parentExprType == 1 {
			or_conds := NewConditionExprs()
			left_err := handleBoolExpr(2, &v.Left, &or_conds)
			if left_err != nil {
				return left_err
			}
			right_err := handleBoolExpr(2, &v.Right, &or_conds)
			if right_err != nil {
				return right_err
			}

			or, ok := MakeOrConditions(&or_conds)
			if ok != nil {
				golog.Error("ripple", "handleBoolExpr", ok.Error(), 0)
				return ok
			}
			conds.AddOneExprEnty(or)
		}
	case *sqlparser.ParenBoolExpr:
		return handleBoolExpr(parentExprType, &v.Expr, conds)
	default:
		return fmt.Errorf("Not support type in handleBoolExpr. %T", v)
	}
	return nil
}

func HandleInsert(insert *sqlparser.Insert, tx *Transaction) error {
	if insert.Table == nil {
		return errors.New("Insert statement must specify table")
	}

	tx.SetTransactionType(TxType_SQLStatement)
	tx.SetOpType(OpType_Insert)
	tx.AddTableByName(string(insert.Table.Name), true)

	if len(insert.Columns) == 0 {
		return kingerros.ErrIRNoColumns
	}

	if values, ok := insert.Rows.(sqlparser.Values); ok {
		for _, value := range values {
			if valExprs, ok := value.(sqlparser.ValTuple); ok {
				// insert one record
				one := NewJsonItem()
				for index, expr := range valExprs {
					col := insert.Columns[index]
					var colName string
					if colExpr, ok := col.(*sqlparser.NonStarExpr); ok {
						colName = sqlparser.String(colExpr)
					} else {
						return fmt.Errorf("Not support type of colum in handleInsert. %T", colExpr)
					}

					switch v := expr.(type) {
					case sqlparser.NumVal:
						i, err := strconv.ParseInt(sqlparser.String(v), 10, 64)
						if err != nil {
							return fmt.Errorf("strconv execute failure. %s", sqlparser.String(v))
						}
						one.AddField(colName, i)
					case sqlparser.StrVal:
						one.AddField(colName, sqlparser.String(v))
					case *sqlparser.ColName:
						one.AddField(colName, sqlparser.String(v))
					default:
						return fmt.Errorf("Not support type in handleInsert. %T", v)
					}
				}
				tx.AddOneRawItem(one)
			}
		}
	}
	return nil
}

func HandleDelete(del *sqlparser.Delete, tx *Transaction) error {
	if del.Table == nil {
		return errors.New("delete statement must specify table")
	}

	tx.SetTransactionType(TxType_SQLStatement)
	tx.SetOpType(OpType_Delete)
	tx.AddTableByName(string(del.Table.Name), true)

	if del.Where != nil {
		if err := handleWhere(*del.Where, tx); err != nil {
			return err
		}
	}
	return nil
}

func HandleUpdate(update *sqlparser.Update, tx *Transaction) error {
	if update.Table == nil {
		return errors.New("update statement must specify table")
	}

	tx.SetTransactionType(TxType_SQLStatement)
	tx.SetOpType(OpType_Update)
	tx.AddTableByName(string(update.Table.Name), true)

	if len(update.Exprs) == 0 {
		return kingerros.ErrIRNoColumns
	}

	set := NewJsonItem()
	for _, element := range update.Exprs {
		colName := string(element.Name.Name)
		switch expr_value := element.Expr.(type) {
		case sqlparser.NumVal:
			i, err := strconv.ParseInt(sqlparser.String(expr_value), 10, 64)
			if err != nil {
				return fmt.Errorf("strconv failure in handleUpdate. %T",
					sqlparser.String(expr_value))
			}
			set.AddField(colName, i)
		case sqlparser.StrVal:
			set.AddField(colName, sqlparser.String(expr_value))
		case *sqlparser.ColName:
			set.AddField(colName, sqlparser.String(expr_value))
		default:
			return fmt.Errorf("Not support type in handleUpdate. %T", expr_value)
		}
	}
	tx.AddOneRawItem(set)

	if update.Where != nil {
		if err := handleWhere(*update.Where, tx); err != nil {
			return err
		}
	}
	return nil
}

func HandleDDL(ddl *sqlparser.DDL, tx *Transaction, nameInDb []byte) error {

	tx.SetTransactionType(TxType_TableListSet)

	if ddl.Action == sqlparser.AST_DROP {
		tx.SetOpType(OpType_DropTable)
		tableEntry := NewTableEntry()
		tableEntry.AddTableName(string(ddl.Table), true)
		//tx.AddTableByName(string(ddl.Table), true)
		if nameInDb != nil {
			tableEntry.AddNameInDB(string(nameInDb))
		}
		tx.AddTableEntry(tableEntry)
	} else if ddl.Action == sqlparser.AST_RENAME || ddl.Action == sqlparser.AST_ALTER {
		tx.SetOpType(OpType_Rename)
		tableEntry := NewTableEntry()
		tableEntry.AddTableName(string(ddl.Table), true)
		tableEntry.AddTableNewName(string(ddl.NewName), true)
		if nameInDb != nil {
			tableEntry.AddNameInDB(string(nameInDb))
		}
		tx.AddTableEntry(tableEntry)
	} else {
		return fmt.Errorf("statement %T not support now", ddl)
	}
	return nil
}

func Ast2Tx(stmt *sqlparser.Statement, tx *Transaction) error {
	switch v := (*stmt).(type) {
	case *sqlparser.Select:
		return HandleSelect(v, tx)
	case *sqlparser.Insert:
		return HandleInsert(v, tx)
	case *sqlparser.Delete:
		return HandleDelete(v, tx)
	case *sqlparser.Update:
		return HandleUpdate(v, tx)
	case *sqlparser.DDL:
		return HandleDDL(v, tx, nil)
	default:
		return fmt.Errorf("statement %T not support now", v)
	}
	return errors.New("Not implement Ast2Tx")
}

type RippleResponse struct {
	Result map[string]interface{}
	Status string
	Type   string
}
