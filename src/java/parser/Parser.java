package parser;

import java.io.StringReader;
import java.util.List;

import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.expression.operators.relational.GreaterThan;
import net.sf.jsqlparser.expression.operators.relational.GreaterThanEquals;
import net.sf.jsqlparser.expression.operators.relational.MinorThan;
import net.sf.jsqlparser.expression.operators.relational.MinorThanEquals;
import net.sf.jsqlparser.expression.operators.relational.NotEqualsTo;
import net.sf.jsqlparser.parser.CCJSqlParserManager;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.FromItem;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;

public class Parser {

	public static void main(String[] args) throws JSQLParserException {

		CCJSqlParserManager pm = new CCJSqlParserManager();
		String sql = "SELECT * FROM courses  "
				+ " WHERE ID > 5 AND age > 1 AND h < 6 AND m = 4";

		Statement statement = pm.parse(new StringReader(sql));

		System.out.println(statement.toString());

		if (statement instanceof Select) {
			PlainSelect plainSelect = (PlainSelect) ((Select) statement)
					.getSelectBody();
		

			List join = plainSelect.getJoins();
			List groupby = plainSelect.getGroupByColumnReferences();
			Expression having = plainSelect.getHaving();
			Expression where = plainSelect.getWhere();
			FromItem from = plainSelect.getFromItem();

			if (where instanceof GreaterThan) {
				GreaterThan gt = (GreaterThan) where;
				System.out.println(gt.getLeftExpression());
				System.out.println(gt.getStringExpression());

			}

			if (join == null) {
				
				//NO JOIN

				if (plainSelect.getGroupByColumnReferences() != null) {

					// GROUPBY

					StringBuilder endResultTableName;
					if (having == null)
						endResultTableName = new StringBuilder("preagg_agg_");
					else
						endResultTableName = new StringBuilder(
								"having_preagg_agg_");

					endResultTableName.append(from.toString());
					endResultTableName.append("_");
					endResultTableName.append(plainSelect
							.getGroupByColumnReferences().get(0));
					

					SelectExpressionItem firstSelectionItem = (SelectExpressionItem) plainSelect
							.getSelectItems().get(0);
					if (firstSelectionItem.getExpression() instanceof Function) {

						endResultTableName.append("_");
						
						Function f = (Function) firstSelectionItem
								.getExpression();

						
						// System.out.println(f.isAllColumns());
						
						String aggCol = f.getParameters().toString();
						aggCol = aggCol.replace("(", "");
						aggCol = aggCol.replace(")", "");
						
						endResultTableName.append(aggCol);
						
					}

						if (where != null) {
							if (where instanceof AndExpression) {

								Expression temp = where;
								String cond = "";
								while (temp instanceof AndExpression) {

									AndExpression x = (AndExpression) temp;

									cond = "_"+ transformWhereClause(x
											.getRightExpression()) + cond;

									temp = x.getLeftExpression();
								}

								
								cond = "_"+ transformWhereClause(temp) + cond;
								
								endResultTableName.append(cond);

							} else {
								endResultTableName.append("_");
								endResultTableName
										.append(transformWhereClause(where));
							}
						}

					

					System.out.println("End result table name: "
							+ endResultTableName);

				}
				else{
					//selection
					StringBuilder endResultTableName = new StringBuilder("selection_");
					endResultTableName.append(from.toString()).append("_");
					
					if (where != null) {
						if (where instanceof AndExpression) {

							Expression temp = where;
							String cond = "";
							while (temp instanceof AndExpression) {

								AndExpression x = (AndExpression) temp;

								cond = "_"+ transformWhereClause(x
										.getRightExpression()) + cond;

								temp = x.getLeftExpression();
							}

							
							cond = "_"+ transformWhereClause(temp) + cond;
							
							endResultTableName.append(cond);

						} else {
							endResultTableName.append("_");
							endResultTableName
									.append(transformWhereClause(where));
						}
					}
					
					System.out.println("End result table name: "
							+ endResultTableName);
					
				}

			} else {
				if (join.size() > 1) {
					System.out.println("Multiple joins not supported!");
				} else {
					System.out.println(join.get(0));

				}

			}

		}

		else {
			System.out.println("Only select statements are supported");
		}

	}

	public static String transformWhereClause(Expression condition) {

		if (condition instanceof GreaterThan) {
			GreaterThan gt = (GreaterThan) condition;
			return gt.getLeftExpression().toString().toLowerCase() + "GT"
					+ gt.getRightExpression().toString().toLowerCase();
		}

		if (condition instanceof GreaterThanEquals) {
			GreaterThanEquals gte = (GreaterThanEquals) condition;
			return gte.getLeftExpression().toString().toLowerCase() + "GTE"
					+ gte.getRightExpression().toString().toLowerCase();
		}

		if (condition instanceof EqualsTo) {
			EqualsTo eq = (EqualsTo) condition;
			return eq.getLeftExpression().toString().toLowerCase() + "EQ"
					+ eq.getRightExpression().toString().toLowerCase();
		}

		if (condition instanceof NotEqualsTo) {
			NotEqualsTo nq = (NotEqualsTo) condition;
			return nq.getLeftExpression().toString().toLowerCase() + "NQ"
					+ nq.getRightExpression().toString().toLowerCase();
		}

		if (condition instanceof MinorThan) {
			MinorThan lt = (MinorThan) condition;
			return lt.getLeftExpression().toString().toLowerCase() + "LT"
					+ lt.getRightExpression().toString().toLowerCase();
		}

		if (condition instanceof MinorThanEquals) {
			MinorThanEquals lte = (MinorThanEquals) condition;
			return lte.getLeftExpression().toString().toLowerCase() + "LTE"
					+ lte.getRightExpression().toString().toLowerCase();
		}

		return null;

	}
	
}
