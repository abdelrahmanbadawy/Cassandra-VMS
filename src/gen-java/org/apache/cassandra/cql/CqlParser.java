// $ANTLR 3.5.2 /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql/Cql.g 2015-05-19 16:37:52

    package org.apache.cassandra.cql;
    import java.util.Map;
    import java.util.HashMap;
    import java.util.Collections;
    import java.util.List;
    import java.util.ArrayList;
    import org.apache.cassandra.exceptions.SyntaxException;
    import org.apache.cassandra.utils.Pair;
    import org.apache.cassandra.db.ConsistencyLevel;

    import static org.apache.cassandra.cql.AlterTableStatement.OperationType;


import org.antlr.runtime.*;
import java.util.Stack;
import java.util.List;
import java.util.ArrayList;

@SuppressWarnings("all")
public class CqlParser extends Parser {
	public static final String[] tokenNames = new String[] {
		"<invalid>", "<EOR>", "<DOWN>", "<UP>", "A", "B", "C", "COMMENT", "COMPIDENT", 
		"D", "DIGIT", "E", "F", "FLOAT", "G", "H", "HEX", "I", "IDENT", "INTEGER", 
		"J", "K", "K_ADD", "K_ALTER", "K_AND", "K_APPLY", "K_BATCH", "K_BEGIN", 
		"K_COLUMNFAMILY", "K_CONSISTENCY", "K_COUNT", "K_CREATE", "K_DELETE", 
		"K_DROP", "K_FIRST", "K_FROM", "K_IN", "K_INDEX", "K_INSERT", "K_INTO", 
		"K_KEY", "K_KEYSPACE", "K_LEVEL", "K_LIMIT", "K_ON", "K_PRIMARY", "K_REVERSED", 
		"K_SELECT", "K_SET", "K_TIMESTAMP", "K_TRUNCATE", "K_TTL", "K_TYPE", "K_UPDATE", 
		"K_USE", "K_USING", "K_VALUES", "K_WHERE", "K_WITH", "L", "LETTER", "M", 
		"MULTILINE_COMMENT", "N", "O", "P", "Q", "QMARK", "R", "RANGEOP", "S", 
		"STRING_LITERAL", "T", "U", "UUID", "V", "W", "WS", "X", "Y", "Z", "'('", 
		"')'", "'+'", "','", "'-'", "'.'", "';'", "'<'", "'<='", "'='", "'>'", 
		"'>='", "'\\*'", "'ascii'", "'bigint'", "'blob'", "'boolean'", "'counter'", 
		"'decimal'", "'double'", "'float'", "'int'", "'text'", "'timestamp'", 
		"'uuid'", "'varchar'", "'varint'"
	};
	public static final int EOF=-1;
	public static final int T__81=81;
	public static final int T__82=82;
	public static final int T__83=83;
	public static final int T__84=84;
	public static final int T__85=85;
	public static final int T__86=86;
	public static final int T__87=87;
	public static final int T__88=88;
	public static final int T__89=89;
	public static final int T__90=90;
	public static final int T__91=91;
	public static final int T__92=92;
	public static final int T__93=93;
	public static final int T__94=94;
	public static final int T__95=95;
	public static final int T__96=96;
	public static final int T__97=97;
	public static final int T__98=98;
	public static final int T__99=99;
	public static final int T__100=100;
	public static final int T__101=101;
	public static final int T__102=102;
	public static final int T__103=103;
	public static final int T__104=104;
	public static final int T__105=105;
	public static final int T__106=106;
	public static final int T__107=107;
	public static final int A=4;
	public static final int B=5;
	public static final int C=6;
	public static final int COMMENT=7;
	public static final int COMPIDENT=8;
	public static final int D=9;
	public static final int DIGIT=10;
	public static final int E=11;
	public static final int F=12;
	public static final int FLOAT=13;
	public static final int G=14;
	public static final int H=15;
	public static final int HEX=16;
	public static final int I=17;
	public static final int IDENT=18;
	public static final int INTEGER=19;
	public static final int J=20;
	public static final int K=21;
	public static final int K_ADD=22;
	public static final int K_ALTER=23;
	public static final int K_AND=24;
	public static final int K_APPLY=25;
	public static final int K_BATCH=26;
	public static final int K_BEGIN=27;
	public static final int K_COLUMNFAMILY=28;
	public static final int K_CONSISTENCY=29;
	public static final int K_COUNT=30;
	public static final int K_CREATE=31;
	public static final int K_DELETE=32;
	public static final int K_DROP=33;
	public static final int K_FIRST=34;
	public static final int K_FROM=35;
	public static final int K_IN=36;
	public static final int K_INDEX=37;
	public static final int K_INSERT=38;
	public static final int K_INTO=39;
	public static final int K_KEY=40;
	public static final int K_KEYSPACE=41;
	public static final int K_LEVEL=42;
	public static final int K_LIMIT=43;
	public static final int K_ON=44;
	public static final int K_PRIMARY=45;
	public static final int K_REVERSED=46;
	public static final int K_SELECT=47;
	public static final int K_SET=48;
	public static final int K_TIMESTAMP=49;
	public static final int K_TRUNCATE=50;
	public static final int K_TTL=51;
	public static final int K_TYPE=52;
	public static final int K_UPDATE=53;
	public static final int K_USE=54;
	public static final int K_USING=55;
	public static final int K_VALUES=56;
	public static final int K_WHERE=57;
	public static final int K_WITH=58;
	public static final int L=59;
	public static final int LETTER=60;
	public static final int M=61;
	public static final int MULTILINE_COMMENT=62;
	public static final int N=63;
	public static final int O=64;
	public static final int P=65;
	public static final int Q=66;
	public static final int QMARK=67;
	public static final int R=68;
	public static final int RANGEOP=69;
	public static final int S=70;
	public static final int STRING_LITERAL=71;
	public static final int T=72;
	public static final int U=73;
	public static final int UUID=74;
	public static final int V=75;
	public static final int W=76;
	public static final int WS=77;
	public static final int X=78;
	public static final int Y=79;
	public static final int Z=80;

	// delegates
	public Parser[] getDelegates() {
		return new Parser[] {};
	}

	// delegators


	public CqlParser(TokenStream input) {
		this(input, new RecognizerSharedState());
	}
	public CqlParser(TokenStream input, RecognizerSharedState state) {
		super(input, state);
	}

	@Override public String[] getTokenNames() { return CqlParser.tokenNames; }
	@Override public String getGrammarFileName() { return "/home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql/Cql.g"; }


	    private List<String> recognitionErrors = new ArrayList<String>();
	    private int currentBindMarkerIdx = -1;
	    
	    public void displayRecognitionError(String[] tokenNames, RecognitionException e)
	    {
	        String hdr = getErrorHeader(e);
	        String msg = getErrorMessage(e, tokenNames);
	        recognitionErrors.add(hdr + " " + msg);
	    }
	    
	    public List<String> getRecognitionErrors()
	    {
	        return recognitionErrors;
	    }
	    
	    public void throwLastRecognitionError() throws SyntaxException
	    {
	        if (recognitionErrors.size() > 0)
	            throw new SyntaxException(recognitionErrors.get((recognitionErrors.size()-1)));
	    }

	    // used by UPDATE of the counter columns to validate if '-' was supplied by user
	    public void validateMinusSupplied(Object op, final Term value, IntStream stream) throws MissingTokenException
	    {
	        if (op == null && Long.parseLong(value.getText()) > 0)
	            throw new MissingTokenException(102, stream, value);
	    }



	// $ANTLR start "query"
	// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql/Cql.g:114:1: query returns [CQLStatement stmnt] : ( selectStatement | insertStatement endStmnt | updateStatement endStmnt | batchStatement | useStatement | truncateStatement | deleteStatement endStmnt | createKeyspaceStatement | createColumnFamilyStatement | createIndexStatement | dropIndexStatement | dropKeyspaceStatement | dropColumnFamilyStatement | alterTableStatement );
	public final CQLStatement query() throws RecognitionException {
		CQLStatement stmnt = null;


		SelectStatement selectStatement1 =null;
		UpdateStatement insertStatement2 =null;
		UpdateStatement updateStatement3 =null;
		BatchStatement batchStatement4 =null;
		String useStatement5 =null;
		Pair<String,String> truncateStatement6 =null;
		DeleteStatement deleteStatement7 =null;
		CreateKeyspaceStatement createKeyspaceStatement8 =null;
		CreateColumnFamilyStatement createColumnFamilyStatement9 =null;
		CreateIndexStatement createIndexStatement10 =null;
		DropIndexStatement dropIndexStatement11 =null;
		String dropKeyspaceStatement12 =null;
		String dropColumnFamilyStatement13 =null;
		AlterTableStatement alterTableStatement14 =null;

		try {
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql/Cql.g:115:5: ( selectStatement | insertStatement endStmnt | updateStatement endStmnt | batchStatement | useStatement | truncateStatement | deleteStatement endStmnt | createKeyspaceStatement | createColumnFamilyStatement | createIndexStatement | dropIndexStatement | dropKeyspaceStatement | dropColumnFamilyStatement | alterTableStatement )
			int alt1=14;
			switch ( input.LA(1) ) {
			case K_SELECT:
				{
				alt1=1;
				}
				break;
			case K_INSERT:
				{
				alt1=2;
				}
				break;
			case K_UPDATE:
				{
				alt1=3;
				}
				break;
			case K_BEGIN:
				{
				alt1=4;
				}
				break;
			case K_USE:
				{
				alt1=5;
				}
				break;
			case K_TRUNCATE:
				{
				alt1=6;
				}
				break;
			case K_DELETE:
				{
				alt1=7;
				}
				break;
			case K_CREATE:
				{
				switch ( input.LA(2) ) {
				case K_KEYSPACE:
					{
					alt1=8;
					}
					break;
				case K_COLUMNFAMILY:
					{
					alt1=9;
					}
					break;
				case K_INDEX:
					{
					alt1=10;
					}
					break;
				default:
					int nvaeMark = input.mark();
					try {
						input.consume();
						NoViableAltException nvae =
							new NoViableAltException("", 1, 8, input);
						throw nvae;
					} finally {
						input.rewind(nvaeMark);
					}
				}
				}
				break;
			case K_DROP:
				{
				switch ( input.LA(2) ) {
				case K_INDEX:
					{
					alt1=11;
					}
					break;
				case K_KEYSPACE:
					{
					alt1=12;
					}
					break;
				case K_COLUMNFAMILY:
					{
					alt1=13;
					}
					break;
				default:
					int nvaeMark = input.mark();
					try {
						input.consume();
						NoViableAltException nvae =
							new NoViableAltException("", 1, 9, input);
						throw nvae;
					} finally {
						input.rewind(nvaeMark);
					}
				}
				}
				break;
			case K_ALTER:
				{
				alt1=14;
				}
				break;
			default:
				NoViableAltException nvae =
					new NoViableAltException("", 1, 0, input);
				throw nvae;
			}
			switch (alt1) {
				case 1 :
					// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql/Cql.g:115:7: selectStatement
					{
					pushFollow(FOLLOW_selectStatement_in_query69);
					selectStatement1=selectStatement();
					state._fsp--;

					 stmnt = new CQLStatement(StatementType.SELECT, selectStatement1, currentBindMarkerIdx); 
					}
					break;
				case 2 :
					// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql/Cql.g:116:7: insertStatement endStmnt
					{
					pushFollow(FOLLOW_insertStatement_in_query81);
					insertStatement2=insertStatement();
					state._fsp--;

					pushFollow(FOLLOW_endStmnt_in_query83);
					endStmnt();
					state._fsp--;

					 stmnt = new CQLStatement(StatementType.INSERT, insertStatement2, currentBindMarkerIdx); 
					}
					break;
				case 3 :
					// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql/Cql.g:117:7: updateStatement endStmnt
					{
					pushFollow(FOLLOW_updateStatement_in_query93);
					updateStatement3=updateStatement();
					state._fsp--;

					pushFollow(FOLLOW_endStmnt_in_query95);
					endStmnt();
					state._fsp--;

					 stmnt = new CQLStatement(StatementType.UPDATE, updateStatement3, currentBindMarkerIdx); 
					}
					break;
				case 4 :
					// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql/Cql.g:118:7: batchStatement
					{
					pushFollow(FOLLOW_batchStatement_in_query105);
					batchStatement4=batchStatement();
					state._fsp--;

					 stmnt = new CQLStatement(StatementType.BATCH, batchStatement4, currentBindMarkerIdx); 
					}
					break;
				case 5 :
					// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql/Cql.g:119:7: useStatement
					{
					pushFollow(FOLLOW_useStatement_in_query115);
					useStatement5=useStatement();
					state._fsp--;

					 stmnt = new CQLStatement(StatementType.USE, useStatement5, currentBindMarkerIdx); 
					}
					break;
				case 6 :
					// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql/Cql.g:120:7: truncateStatement
					{
					pushFollow(FOLLOW_truncateStatement_in_query130);
					truncateStatement6=truncateStatement();
					state._fsp--;

					 stmnt = new CQLStatement(StatementType.TRUNCATE, truncateStatement6, currentBindMarkerIdx); 
					}
					break;
				case 7 :
					// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql/Cql.g:121:7: deleteStatement endStmnt
					{
					pushFollow(FOLLOW_deleteStatement_in_query140);
					deleteStatement7=deleteStatement();
					state._fsp--;

					pushFollow(FOLLOW_endStmnt_in_query142);
					endStmnt();
					state._fsp--;

					 stmnt = new CQLStatement(StatementType.DELETE, deleteStatement7, currentBindMarkerIdx); 
					}
					break;
				case 8 :
					// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql/Cql.g:122:7: createKeyspaceStatement
					{
					pushFollow(FOLLOW_createKeyspaceStatement_in_query152);
					createKeyspaceStatement8=createKeyspaceStatement();
					state._fsp--;

					 stmnt = new CQLStatement(StatementType.CREATE_KEYSPACE, createKeyspaceStatement8, currentBindMarkerIdx); 
					}
					break;
				case 9 :
					// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql/Cql.g:123:7: createColumnFamilyStatement
					{
					pushFollow(FOLLOW_createColumnFamilyStatement_in_query162);
					createColumnFamilyStatement9=createColumnFamilyStatement();
					state._fsp--;

					 stmnt = new CQLStatement(StatementType.CREATE_COLUMNFAMILY, createColumnFamilyStatement9, currentBindMarkerIdx); 
					}
					break;
				case 10 :
					// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql/Cql.g:124:7: createIndexStatement
					{
					pushFollow(FOLLOW_createIndexStatement_in_query172);
					createIndexStatement10=createIndexStatement();
					state._fsp--;

					 stmnt = new CQLStatement(StatementType.CREATE_INDEX, createIndexStatement10, currentBindMarkerIdx); 
					}
					break;
				case 11 :
					// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql/Cql.g:125:7: dropIndexStatement
					{
					pushFollow(FOLLOW_dropIndexStatement_in_query182);
					dropIndexStatement11=dropIndexStatement();
					state._fsp--;

					 stmnt = new CQLStatement(StatementType.DROP_INDEX, dropIndexStatement11, currentBindMarkerIdx); 
					}
					break;
				case 12 :
					// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql/Cql.g:126:7: dropKeyspaceStatement
					{
					pushFollow(FOLLOW_dropKeyspaceStatement_in_query194);
					dropKeyspaceStatement12=dropKeyspaceStatement();
					state._fsp--;

					 stmnt = new CQLStatement(StatementType.DROP_KEYSPACE, dropKeyspaceStatement12, currentBindMarkerIdx); 
					}
					break;
				case 13 :
					// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql/Cql.g:127:7: dropColumnFamilyStatement
					{
					pushFollow(FOLLOW_dropColumnFamilyStatement_in_query204);
					dropColumnFamilyStatement13=dropColumnFamilyStatement();
					state._fsp--;

					 stmnt = new CQLStatement(StatementType.DROP_COLUMNFAMILY, dropColumnFamilyStatement13, currentBindMarkerIdx); 
					}
					break;
				case 14 :
					// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql/Cql.g:128:7: alterTableStatement
					{
					pushFollow(FOLLOW_alterTableStatement_in_query214);
					alterTableStatement14=alterTableStatement();
					state._fsp--;

					 stmnt = new CQLStatement(StatementType.ALTER_TABLE, alterTableStatement14, currentBindMarkerIdx); 
					}
					break;

			}
		}
		catch (RecognitionException re) {
			reportError(re);
			recover(input,re);
		}
		finally {
			// do for sure before leaving
		}
		return stmnt;
	}
	// $ANTLR end "query"



	// $ANTLR start "useStatement"
	// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql/Cql.g:132:1: useStatement returns [String keyspace] : K_USE name= ( IDENT | INTEGER | STRING_LITERAL ) endStmnt ;
	public final String useStatement() throws RecognitionException {
		String keyspace = null;


		Token name=null;

		try {
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql/Cql.g:133:5: ( K_USE name= ( IDENT | INTEGER | STRING_LITERAL ) endStmnt )
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql/Cql.g:133:7: K_USE name= ( IDENT | INTEGER | STRING_LITERAL ) endStmnt
			{
			match(input,K_USE,FOLLOW_K_USE_in_useStatement238); 
			name=input.LT(1);
			if ( (input.LA(1) >= IDENT && input.LA(1) <= INTEGER)||input.LA(1)==STRING_LITERAL ) {
				input.consume();
				state.errorRecovery=false;
			}
			else {
				MismatchedSetException mse = new MismatchedSetException(null,input);
				throw mse;
			}
			 keyspace = (name!=null?name.getText():null); 
			pushFollow(FOLLOW_endStmnt_in_useStatement258);
			endStmnt();
			state._fsp--;

			}

		}
		catch (RecognitionException re) {
			reportError(re);
			recover(input,re);
		}
		finally {
			// do for sure before leaving
		}
		return keyspace;
	}
	// $ANTLR end "useStatement"



	// $ANTLR start "selectStatement"
	// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql/Cql.g:148:1: selectStatement returns [SelectStatement expr] : K_SELECT (s1= selectExpression | K_COUNT '(' s2= selectExpression ')' ) K_FROM (keyspace= ( IDENT | STRING_LITERAL | INTEGER ) '.' )? columnFamily= ( IDENT | STRING_LITERAL | INTEGER ) ( K_USING K_CONSISTENCY K_LEVEL )? ( K_WHERE whereClause )? ( K_LIMIT rows= INTEGER )? endStmnt ;
	public final SelectStatement selectStatement() throws RecognitionException {
		SelectStatement expr = null;


		Token keyspace=null;
		Token columnFamily=null;
		Token rows=null;
		Token K_LEVEL15=null;
		SelectExpression s1 =null;
		SelectExpression s2 =null;
		WhereClause whereClause16 =null;

		try {
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql/Cql.g:149:5: ( K_SELECT (s1= selectExpression | K_COUNT '(' s2= selectExpression ')' ) K_FROM (keyspace= ( IDENT | STRING_LITERAL | INTEGER ) '.' )? columnFamily= ( IDENT | STRING_LITERAL | INTEGER ) ( K_USING K_CONSISTENCY K_LEVEL )? ( K_WHERE whereClause )? ( K_LIMIT rows= INTEGER )? endStmnt )
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql/Cql.g:149:7: K_SELECT (s1= selectExpression | K_COUNT '(' s2= selectExpression ')' ) K_FROM (keyspace= ( IDENT | STRING_LITERAL | INTEGER ) '.' )? columnFamily= ( IDENT | STRING_LITERAL | INTEGER ) ( K_USING K_CONSISTENCY K_LEVEL )? ( K_WHERE whereClause )? ( K_LIMIT rows= INTEGER )? endStmnt
			{
			 
			          int numRecords = 10000;
			          SelectExpression expression = null;
			          boolean isCountOp = false;
			          ConsistencyLevel cLevel = ConsistencyLevel.ONE;
			      
			match(input,K_SELECT,FOLLOW_K_SELECT_in_selectStatement289); 
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql/Cql.g:156:11: (s1= selectExpression | K_COUNT '(' s2= selectExpression ')' )
			int alt2=2;
			int LA2_0 = input.LA(1);
			if ( (LA2_0==FLOAT||(LA2_0 >= IDENT && LA2_0 <= INTEGER)||LA2_0==K_FIRST||LA2_0==K_KEY||LA2_0==K_REVERSED||LA2_0==QMARK||LA2_0==STRING_LITERAL||LA2_0==UUID||LA2_0==93) ) {
				alt2=1;
			}
			else if ( (LA2_0==K_COUNT) ) {
				alt2=2;
			}

			else {
				NoViableAltException nvae =
					new NoViableAltException("", 2, 0, input);
				throw nvae;
			}

			switch (alt2) {
				case 1 :
					// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql/Cql.g:156:13: s1= selectExpression
					{
					pushFollow(FOLLOW_selectExpression_in_selectStatement305);
					s1=selectExpression();
					state._fsp--;

					 expression = s1; 
					}
					break;
				case 2 :
					// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql/Cql.g:157:13: K_COUNT '(' s2= selectExpression ')'
					{
					match(input,K_COUNT,FOLLOW_K_COUNT_in_selectStatement337); 
					match(input,81,FOLLOW_81_in_selectStatement339); 
					pushFollow(FOLLOW_selectExpression_in_selectStatement343);
					s2=selectExpression();
					state._fsp--;

					match(input,82,FOLLOW_82_in_selectStatement345); 
					 expression = s2; isCountOp = true; 
					}
					break;

			}

			match(input,K_FROM,FOLLOW_K_FROM_in_selectStatement371); 
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql/Cql.g:159:18: (keyspace= ( IDENT | STRING_LITERAL | INTEGER ) '.' )?
			int alt3=2;
			int LA3_0 = input.LA(1);
			if ( ((LA3_0 >= IDENT && LA3_0 <= INTEGER)||LA3_0==STRING_LITERAL) ) {
				int LA3_1 = input.LA(2);
				if ( (LA3_1==86) ) {
					alt3=1;
				}
			}
			switch (alt3) {
				case 1 :
					// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql/Cql.g:159:19: keyspace= ( IDENT | STRING_LITERAL | INTEGER ) '.'
					{
					keyspace=input.LT(1);
					if ( (input.LA(1) >= IDENT && input.LA(1) <= INTEGER)||input.LA(1)==STRING_LITERAL ) {
						input.consume();
						state.errorRecovery=false;
					}
					else {
						MismatchedSetException mse = new MismatchedSetException(null,input);
						throw mse;
					}
					match(input,86,FOLLOW_86_in_selectStatement388); 
					}
					break;

			}

			columnFamily=input.LT(1);
			if ( (input.LA(1) >= IDENT && input.LA(1) <= INTEGER)||input.LA(1)==STRING_LITERAL ) {
				input.consume();
				state.errorRecovery=false;
			}
			else {
				MismatchedSetException mse = new MismatchedSetException(null,input);
				throw mse;
			}
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql/Cql.g:160:11: ( K_USING K_CONSISTENCY K_LEVEL )?
			int alt4=2;
			int LA4_0 = input.LA(1);
			if ( (LA4_0==K_USING) ) {
				alt4=1;
			}
			switch (alt4) {
				case 1 :
					// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql/Cql.g:160:13: K_USING K_CONSISTENCY K_LEVEL
					{
					match(input,K_USING,FOLLOW_K_USING_in_selectStatement420); 
					match(input,K_CONSISTENCY,FOLLOW_K_CONSISTENCY_in_selectStatement422); 
					K_LEVEL15=(Token)match(input,K_LEVEL,FOLLOW_K_LEVEL_in_selectStatement424); 
					 cLevel = ConsistencyLevel.valueOf((K_LEVEL15!=null?K_LEVEL15.getText():null).toUpperCase()); 
					}
					break;

			}

			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql/Cql.g:161:11: ( K_WHERE whereClause )?
			int alt5=2;
			int LA5_0 = input.LA(1);
			if ( (LA5_0==K_WHERE) ) {
				alt5=1;
			}
			switch (alt5) {
				case 1 :
					// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql/Cql.g:161:13: K_WHERE whereClause
					{
					match(input,K_WHERE,FOLLOW_K_WHERE_in_selectStatement443); 
					pushFollow(FOLLOW_whereClause_in_selectStatement445);
					whereClause16=whereClause();
					state._fsp--;

					}
					break;

			}

			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql/Cql.g:162:11: ( K_LIMIT rows= INTEGER )?
			int alt6=2;
			int LA6_0 = input.LA(1);
			if ( (LA6_0==K_LIMIT) ) {
				alt6=1;
			}
			switch (alt6) {
				case 1 :
					// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql/Cql.g:162:13: K_LIMIT rows= INTEGER
					{
					match(input,K_LIMIT,FOLLOW_K_LIMIT_in_selectStatement462); 
					rows=(Token)match(input,INTEGER,FOLLOW_INTEGER_in_selectStatement466); 
					 numRecords = Integer.parseInt((rows!=null?rows.getText():null)); 
					}
					break;

			}

			pushFollow(FOLLOW_endStmnt_in_selectStatement483);
			endStmnt();
			state._fsp--;


			          return new SelectStatement(expression,
			                                     isCountOp,
			                                     (keyspace!=null?keyspace.getText():null),
			                                     (columnFamily!=null?columnFamily.getText():null),
			                                     cLevel,
			                                     whereClause16,
			                                     numRecords);
			      
			}

		}
		catch (RecognitionException re) {
			reportError(re);
			recover(input,re);
		}
		finally {
			// do for sure before leaving
		}
		return expr;
	}
	// $ANTLR end "selectStatement"



	// $ANTLR start "selectExpression"
	// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql/Cql.g:177:1: selectExpression returns [SelectExpression expr] : ( K_FIRST cols= INTEGER )? ( K_REVERSED )? (first= term ( ',' next= term )* |start= term RANGEOP finish= term | '\\*' ) ;
	public final SelectExpression selectExpression() throws RecognitionException {
		SelectExpression expr = null;


		Token cols=null;
		Term first =null;
		Term next =null;
		Term start =null;
		Term finish =null;

		try {
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql/Cql.g:178:5: ( ( K_FIRST cols= INTEGER )? ( K_REVERSED )? (first= term ( ',' next= term )* |start= term RANGEOP finish= term | '\\*' ) )
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql/Cql.g:178:7: ( K_FIRST cols= INTEGER )? ( K_REVERSED )? (first= term ( ',' next= term )* |start= term RANGEOP finish= term | '\\*' )
			{

			          int count = 10000;
			          boolean reversed = false;
			          boolean hasFirstSet = false;
			      
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql/Cql.g:183:7: ( K_FIRST cols= INTEGER )?
			int alt7=2;
			int LA7_0 = input.LA(1);
			if ( (LA7_0==K_FIRST) ) {
				alt7=1;
			}
			switch (alt7) {
				case 1 :
					// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql/Cql.g:183:9: K_FIRST cols= INTEGER
					{
					match(input,K_FIRST,FOLLOW_K_FIRST_in_selectExpression524); 
					 hasFirstSet = true; 
					cols=(Token)match(input,INTEGER,FOLLOW_INTEGER_in_selectExpression530); 
					 count = Integer.parseInt((cols!=null?cols.getText():null)); 
					}
					break;

			}

			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql/Cql.g:184:7: ( K_REVERSED )?
			int alt8=2;
			int LA8_0 = input.LA(1);
			if ( (LA8_0==K_REVERSED) ) {
				alt8=1;
			}
			switch (alt8) {
				case 1 :
					// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql/Cql.g:184:9: K_REVERSED
					{
					match(input,K_REVERSED,FOLLOW_K_REVERSED_in_selectExpression545); 
					 reversed = true; 
					}
					break;

			}

			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql/Cql.g:185:7: (first= term ( ',' next= term )* |start= term RANGEOP finish= term | '\\*' )
			int alt10=3;
			switch ( input.LA(1) ) {
			case K_KEY:
				{
				int LA10_1 = input.LA(2);
				if ( (LA10_1==K_FROM||LA10_1==82||LA10_1==84) ) {
					alt10=1;
				}
				else if ( (LA10_1==RANGEOP) ) {
					alt10=2;
				}

				else {
					int nvaeMark = input.mark();
					try {
						input.consume();
						NoViableAltException nvae =
							new NoViableAltException("", 10, 1, input);
						throw nvae;
					} finally {
						input.rewind(nvaeMark);
					}
				}

				}
				break;
			case STRING_LITERAL:
				{
				int LA10_2 = input.LA(2);
				if ( (LA10_2==K_FROM||LA10_2==82||LA10_2==84) ) {
					alt10=1;
				}
				else if ( (LA10_2==RANGEOP) ) {
					alt10=2;
				}

				else {
					int nvaeMark = input.mark();
					try {
						input.consume();
						NoViableAltException nvae =
							new NoViableAltException("", 10, 2, input);
						throw nvae;
					} finally {
						input.rewind(nvaeMark);
					}
				}

				}
				break;
			case INTEGER:
				{
				int LA10_3 = input.LA(2);
				if ( (LA10_3==K_FROM||LA10_3==82||LA10_3==84) ) {
					alt10=1;
				}
				else if ( (LA10_3==RANGEOP) ) {
					alt10=2;
				}

				else {
					int nvaeMark = input.mark();
					try {
						input.consume();
						NoViableAltException nvae =
							new NoViableAltException("", 10, 3, input);
						throw nvae;
					} finally {
						input.rewind(nvaeMark);
					}
				}

				}
				break;
			case UUID:
				{
				int LA10_4 = input.LA(2);
				if ( (LA10_4==K_FROM||LA10_4==82||LA10_4==84) ) {
					alt10=1;
				}
				else if ( (LA10_4==RANGEOP) ) {
					alt10=2;
				}

				else {
					int nvaeMark = input.mark();
					try {
						input.consume();
						NoViableAltException nvae =
							new NoViableAltException("", 10, 4, input);
						throw nvae;
					} finally {
						input.rewind(nvaeMark);
					}
				}

				}
				break;
			case IDENT:
				{
				int LA10_5 = input.LA(2);
				if ( (LA10_5==K_FROM||LA10_5==82||LA10_5==84) ) {
					alt10=1;
				}
				else if ( (LA10_5==RANGEOP) ) {
					alt10=2;
				}

				else {
					int nvaeMark = input.mark();
					try {
						input.consume();
						NoViableAltException nvae =
							new NoViableAltException("", 10, 5, input);
						throw nvae;
					} finally {
						input.rewind(nvaeMark);
					}
				}

				}
				break;
			case FLOAT:
				{
				int LA10_6 = input.LA(2);
				if ( (LA10_6==K_FROM||LA10_6==82||LA10_6==84) ) {
					alt10=1;
				}
				else if ( (LA10_6==RANGEOP) ) {
					alt10=2;
				}

				else {
					int nvaeMark = input.mark();
					try {
						input.consume();
						NoViableAltException nvae =
							new NoViableAltException("", 10, 6, input);
						throw nvae;
					} finally {
						input.rewind(nvaeMark);
					}
				}

				}
				break;
			case QMARK:
				{
				int LA10_7 = input.LA(2);
				if ( (LA10_7==K_FROM||LA10_7==82||LA10_7==84) ) {
					alt10=1;
				}
				else if ( (LA10_7==RANGEOP) ) {
					alt10=2;
				}

				else {
					int nvaeMark = input.mark();
					try {
						input.consume();
						NoViableAltException nvae =
							new NoViableAltException("", 10, 7, input);
						throw nvae;
					} finally {
						input.rewind(nvaeMark);
					}
				}

				}
				break;
			case 93:
				{
				alt10=3;
				}
				break;
			default:
				NoViableAltException nvae =
					new NoViableAltException("", 10, 0, input);
				throw nvae;
			}
			switch (alt10) {
				case 1 :
					// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql/Cql.g:185:9: first= term ( ',' next= term )*
					{
					pushFollow(FOLLOW_term_in_selectExpression562);
					first=term();
					state._fsp--;

					 expr = new SelectExpression(first, count, reversed, hasFirstSet); 
					// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql/Cql.g:186:13: ( ',' next= term )*
					loop9:
					while (true) {
						int alt9=2;
						int LA9_0 = input.LA(1);
						if ( (LA9_0==84) ) {
							alt9=1;
						}

						switch (alt9) {
						case 1 :
							// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql/Cql.g:186:14: ',' next= term
							{
							match(input,84,FOLLOW_84_in_selectExpression579); 
							pushFollow(FOLLOW_term_in_selectExpression583);
							next=term();
							state._fsp--;

							 expr.and(next); 
							}
							break;

						default :
							break loop9;
						}
					}

					}
					break;
				case 2 :
					// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql/Cql.g:187:9: start= term RANGEOP finish= term
					{
					pushFollow(FOLLOW_term_in_selectExpression599);
					start=term();
					state._fsp--;

					match(input,RANGEOP,FOLLOW_RANGEOP_in_selectExpression601); 
					pushFollow(FOLLOW_term_in_selectExpression605);
					finish=term();
					state._fsp--;

					 expr = new SelectExpression(start, finish, count, reversed, false, hasFirstSet); 
					}
					break;
				case 3 :
					// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql/Cql.g:188:9: '\\*'
					{
					match(input,93,FOLLOW_93_in_selectExpression617); 
					 expr = new SelectExpression(new Term(), new Term(), count, reversed, true, hasFirstSet); 
					}
					break;

			}

			}

		}
		catch (RecognitionException re) {
			reportError(re);
			recover(input,re);
		}
		finally {
			// do for sure before leaving
		}
		return expr;
	}
	// $ANTLR end "selectExpression"



	// $ANTLR start "whereClause"
	// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql/Cql.g:193:1: whereClause returns [WhereClause clause] : (first= relation ( K_AND next= relation )* |key_alias= term K_IN '(' f1= term ( ',' fN= term )* ')' );
	public final WhereClause whereClause() throws RecognitionException {
		WhereClause clause = null;


		Relation first =null;
		Relation next =null;
		Term key_alias =null;
		Term f1 =null;
		Term fN =null;


		        WhereClause inClause = new WhereClause();
		    
		try {
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql/Cql.g:197:5: (first= relation ( K_AND next= relation )* |key_alias= term K_IN '(' f1= term ( ',' fN= term )* ')' )
			int alt13=2;
			switch ( input.LA(1) ) {
			case K_KEY:
				{
				int LA13_1 = input.LA(2);
				if ( ((LA13_1 >= 88 && LA13_1 <= 92)) ) {
					alt13=1;
				}
				else if ( (LA13_1==K_IN) ) {
					alt13=2;
				}

				else {
					int nvaeMark = input.mark();
					try {
						input.consume();
						NoViableAltException nvae =
							new NoViableAltException("", 13, 1, input);
						throw nvae;
					} finally {
						input.rewind(nvaeMark);
					}
				}

				}
				break;
			case STRING_LITERAL:
				{
				int LA13_2 = input.LA(2);
				if ( ((LA13_2 >= 88 && LA13_2 <= 92)) ) {
					alt13=1;
				}
				else if ( (LA13_2==K_IN) ) {
					alt13=2;
				}

				else {
					int nvaeMark = input.mark();
					try {
						input.consume();
						NoViableAltException nvae =
							new NoViableAltException("", 13, 2, input);
						throw nvae;
					} finally {
						input.rewind(nvaeMark);
					}
				}

				}
				break;
			case INTEGER:
				{
				int LA13_3 = input.LA(2);
				if ( ((LA13_3 >= 88 && LA13_3 <= 92)) ) {
					alt13=1;
				}
				else if ( (LA13_3==K_IN) ) {
					alt13=2;
				}

				else {
					int nvaeMark = input.mark();
					try {
						input.consume();
						NoViableAltException nvae =
							new NoViableAltException("", 13, 3, input);
						throw nvae;
					} finally {
						input.rewind(nvaeMark);
					}
				}

				}
				break;
			case UUID:
				{
				int LA13_4 = input.LA(2);
				if ( ((LA13_4 >= 88 && LA13_4 <= 92)) ) {
					alt13=1;
				}
				else if ( (LA13_4==K_IN) ) {
					alt13=2;
				}

				else {
					int nvaeMark = input.mark();
					try {
						input.consume();
						NoViableAltException nvae =
							new NoViableAltException("", 13, 4, input);
						throw nvae;
					} finally {
						input.rewind(nvaeMark);
					}
				}

				}
				break;
			case IDENT:
				{
				int LA13_5 = input.LA(2);
				if ( ((LA13_5 >= 88 && LA13_5 <= 92)) ) {
					alt13=1;
				}
				else if ( (LA13_5==K_IN) ) {
					alt13=2;
				}

				else {
					int nvaeMark = input.mark();
					try {
						input.consume();
						NoViableAltException nvae =
							new NoViableAltException("", 13, 5, input);
						throw nvae;
					} finally {
						input.rewind(nvaeMark);
					}
				}

				}
				break;
			case FLOAT:
				{
				int LA13_6 = input.LA(2);
				if ( ((LA13_6 >= 88 && LA13_6 <= 92)) ) {
					alt13=1;
				}
				else if ( (LA13_6==K_IN) ) {
					alt13=2;
				}

				else {
					int nvaeMark = input.mark();
					try {
						input.consume();
						NoViableAltException nvae =
							new NoViableAltException("", 13, 6, input);
						throw nvae;
					} finally {
						input.rewind(nvaeMark);
					}
				}

				}
				break;
			case QMARK:
				{
				int LA13_7 = input.LA(2);
				if ( ((LA13_7 >= 88 && LA13_7 <= 92)) ) {
					alt13=1;
				}
				else if ( (LA13_7==K_IN) ) {
					alt13=2;
				}

				else {
					int nvaeMark = input.mark();
					try {
						input.consume();
						NoViableAltException nvae =
							new NoViableAltException("", 13, 7, input);
						throw nvae;
					} finally {
						input.rewind(nvaeMark);
					}
				}

				}
				break;
			default:
				NoViableAltException nvae =
					new NoViableAltException("", 13, 0, input);
				throw nvae;
			}
			switch (alt13) {
				case 1 :
					// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql/Cql.g:197:7: first= relation ( K_AND next= relation )*
					{
					pushFollow(FOLLOW_relation_in_whereClause660);
					first=relation();
					state._fsp--;

					 clause = new WhereClause(first); 
					// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql/Cql.g:198:11: ( K_AND next= relation )*
					loop11:
					while (true) {
						int alt11=2;
						int LA11_0 = input.LA(1);
						if ( (LA11_0==K_AND) ) {
							alt11=1;
						}

						switch (alt11) {
						case 1 :
							// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql/Cql.g:198:12: K_AND next= relation
							{
							match(input,K_AND,FOLLOW_K_AND_in_whereClause676); 
							pushFollow(FOLLOW_relation_in_whereClause680);
							next=relation();
							state._fsp--;

							 clause.and(next); 
							}
							break;

						default :
							break loop11;
						}
					}

					}
					break;
				case 2 :
					// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql/Cql.g:199:9: key_alias= term K_IN '(' f1= term ( ',' fN= term )* ')'
					{
					pushFollow(FOLLOW_term_in_whereClause696);
					key_alias=term();
					state._fsp--;

					 inClause.setKeyAlias(key_alias.getText()); 
					match(input,K_IN,FOLLOW_K_IN_in_whereClause711); 
					match(input,81,FOLLOW_81_in_whereClause713); 
					pushFollow(FOLLOW_term_in_whereClause717);
					f1=term();
					state._fsp--;

					 inClause.andKeyEquals(f1); 
					// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql/Cql.g:201:19: ( ',' fN= term )*
					loop12:
					while (true) {
						int alt12=2;
						int LA12_0 = input.LA(1);
						if ( (LA12_0==84) ) {
							alt12=1;
						}

						switch (alt12) {
						case 1 :
							// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql/Cql.g:201:20: ',' fN= term
							{
							match(input,84,FOLLOW_84_in_whereClause740); 
							pushFollow(FOLLOW_term_in_whereClause744);
							fN=term();
							state._fsp--;

							 inClause.andKeyEquals(fN); 
							}
							break;

						default :
							break loop12;
						}
					}

					match(input,82,FOLLOW_82_in_whereClause751); 
					 inClause.setMultiKey(true); clause = inClause; 
					}
					break;

			}
		}
		catch (RecognitionException re) {
			reportError(re);
			recover(input,re);
		}
		finally {
			// do for sure before leaving
		}
		return clause;
	}
	// $ANTLR end "whereClause"



	// $ANTLR start "insertStatement"
	// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql/Cql.g:218:1: insertStatement returns [UpdateStatement expr] : K_INSERT K_INTO (keyspace= ( IDENT | STRING_LITERAL | INTEGER ) '.' )? columnFamily= ( IDENT | STRING_LITERAL | INTEGER ) '(' key_alias= term ( ',' column_name= term )+ ')' K_VALUES '(' key= term ( ',' column_value= term )+ ')' ( usingClause[attrs] )? ;
	public final UpdateStatement insertStatement() throws RecognitionException {
		UpdateStatement expr = null;


		Token keyspace=null;
		Token columnFamily=null;
		Term key_alias =null;
		Term column_name =null;
		Term key =null;
		Term column_value =null;

		try {
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql/Cql.g:219:5: ( K_INSERT K_INTO (keyspace= ( IDENT | STRING_LITERAL | INTEGER ) '.' )? columnFamily= ( IDENT | STRING_LITERAL | INTEGER ) '(' key_alias= term ( ',' column_name= term )+ ')' K_VALUES '(' key= term ( ',' column_value= term )+ ')' ( usingClause[attrs] )? )
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql/Cql.g:219:7: K_INSERT K_INTO (keyspace= ( IDENT | STRING_LITERAL | INTEGER ) '.' )? columnFamily= ( IDENT | STRING_LITERAL | INTEGER ) '(' key_alias= term ( ',' column_name= term )+ ')' K_VALUES '(' key= term ( ',' column_value= term )+ ')' ( usingClause[attrs] )?
			{

			          Attributes attrs = new Attributes();

			          List<Term> columnNames  = new ArrayList<Term>();
			          List<Term> columnValues = new ArrayList<Term>();
			      
			match(input,K_INSERT,FOLLOW_K_INSERT_in_insertStatement792); 
			match(input,K_INTO,FOLLOW_K_INTO_in_insertStatement794); 
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql/Cql.g:225:23: (keyspace= ( IDENT | STRING_LITERAL | INTEGER ) '.' )?
			int alt14=2;
			int LA14_0 = input.LA(1);
			if ( ((LA14_0 >= IDENT && LA14_0 <= INTEGER)||LA14_0==STRING_LITERAL) ) {
				int LA14_1 = input.LA(2);
				if ( (LA14_1==86) ) {
					alt14=1;
				}
			}
			switch (alt14) {
				case 1 :
					// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql/Cql.g:225:24: keyspace= ( IDENT | STRING_LITERAL | INTEGER ) '.'
					{
					keyspace=input.LT(1);
					if ( (input.LA(1) >= IDENT && input.LA(1) <= INTEGER)||input.LA(1)==STRING_LITERAL ) {
						input.consume();
						state.errorRecovery=false;
					}
					else {
						MismatchedSetException mse = new MismatchedSetException(null,input);
						throw mse;
					}
					match(input,86,FOLLOW_86_in_insertStatement811); 
					}
					break;

			}

			columnFamily=input.LT(1);
			if ( (input.LA(1) >= IDENT && input.LA(1) <= INTEGER)||input.LA(1)==STRING_LITERAL ) {
				input.consume();
				state.errorRecovery=false;
			}
			else {
				MismatchedSetException mse = new MismatchedSetException(null,input);
				throw mse;
			}
			match(input,81,FOLLOW_81_in_insertStatement841); 
			pushFollow(FOLLOW_term_in_insertStatement845);
			key_alias=term();
			state._fsp--;

			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql/Cql.g:226:30: ( ',' column_name= term )+
			int cnt15=0;
			loop15:
			while (true) {
				int alt15=2;
				int LA15_0 = input.LA(1);
				if ( (LA15_0==84) ) {
					alt15=1;
				}

				switch (alt15) {
				case 1 :
					// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql/Cql.g:226:32: ',' column_name= term
					{
					match(input,84,FOLLOW_84_in_insertStatement849); 
					pushFollow(FOLLOW_term_in_insertStatement853);
					column_name=term();
					state._fsp--;

					 columnNames.add(column_name); 
					}
					break;

				default :
					if ( cnt15 >= 1 ) break loop15;
					EarlyExitException eee = new EarlyExitException(15, input);
					throw eee;
				}
				cnt15++;
			}

			match(input,82,FOLLOW_82_in_insertStatement861); 
			match(input,K_VALUES,FOLLOW_K_VALUES_in_insertStatement871); 
			match(input,81,FOLLOW_81_in_insertStatement883); 
			pushFollow(FOLLOW_term_in_insertStatement887);
			key=term();
			state._fsp--;

			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql/Cql.g:228:24: ( ',' column_value= term )+
			int cnt16=0;
			loop16:
			while (true) {
				int alt16=2;
				int LA16_0 = input.LA(1);
				if ( (LA16_0==84) ) {
					alt16=1;
				}

				switch (alt16) {
				case 1 :
					// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql/Cql.g:228:26: ',' column_value= term
					{
					match(input,84,FOLLOW_84_in_insertStatement891); 
					pushFollow(FOLLOW_term_in_insertStatement895);
					column_value=term();
					state._fsp--;

					 columnValues.add(column_value); 
					}
					break;

				default :
					if ( cnt16 >= 1 ) break loop16;
					EarlyExitException eee = new EarlyExitException(16, input);
					throw eee;
				}
				cnt16++;
			}

			match(input,82,FOLLOW_82_in_insertStatement901); 
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql/Cql.g:229:9: ( usingClause[attrs] )?
			int alt17=2;
			int LA17_0 = input.LA(1);
			if ( (LA17_0==K_USING) ) {
				alt17=1;
			}
			switch (alt17) {
				case 1 :
					// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql/Cql.g:229:11: usingClause[attrs]
					{
					pushFollow(FOLLOW_usingClause_in_insertStatement913);
					usingClause(attrs);
					state._fsp--;

					}
					break;

			}


			          return new UpdateStatement((keyspace!=null?keyspace.getText():null), (columnFamily!=null?columnFamily.getText():null), key_alias.getText(), columnNames, columnValues, Collections.singletonList(key), attrs);
			      
			}

		}
		catch (RecognitionException re) {
			reportError(re);
			recover(input,re);
		}
		finally {
			// do for sure before leaving
		}
		return expr;
	}
	// $ANTLR end "insertStatement"



	// $ANTLR start "usingClause"
	// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql/Cql.g:235:1: usingClause[Attributes attrs] : K_USING usingClauseObjective[attrs] ( ( K_AND )? usingClauseObjective[attrs] )* ;
	public final void usingClause(Attributes attrs) throws RecognitionException {
		try {
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql/Cql.g:236:5: ( K_USING usingClauseObjective[attrs] ( ( K_AND )? usingClauseObjective[attrs] )* )
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql/Cql.g:236:7: K_USING usingClauseObjective[attrs] ( ( K_AND )? usingClauseObjective[attrs] )*
			{
			match(input,K_USING,FOLLOW_K_USING_in_usingClause943); 
			pushFollow(FOLLOW_usingClauseObjective_in_usingClause945);
			usingClauseObjective(attrs);
			state._fsp--;

			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql/Cql.g:236:43: ( ( K_AND )? usingClauseObjective[attrs] )*
			loop19:
			while (true) {
				int alt19=2;
				int LA19_0 = input.LA(1);
				if ( (LA19_0==K_AND||LA19_0==K_CONSISTENCY||LA19_0==K_TIMESTAMP||LA19_0==K_TTL) ) {
					alt19=1;
				}

				switch (alt19) {
				case 1 :
					// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql/Cql.g:236:45: ( K_AND )? usingClauseObjective[attrs]
					{
					// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql/Cql.g:236:45: ( K_AND )?
					int alt18=2;
					int LA18_0 = input.LA(1);
					if ( (LA18_0==K_AND) ) {
						alt18=1;
					}
					switch (alt18) {
						case 1 :
							// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql/Cql.g:236:45: K_AND
							{
							match(input,K_AND,FOLLOW_K_AND_in_usingClause950); 
							}
							break;

					}

					pushFollow(FOLLOW_usingClauseObjective_in_usingClause953);
					usingClauseObjective(attrs);
					state._fsp--;

					}
					break;

				default :
					break loop19;
				}
			}

			}

		}
		catch (RecognitionException re) {
			reportError(re);
			recover(input,re);
		}
		finally {
			// do for sure before leaving
		}
	}
	// $ANTLR end "usingClause"



	// $ANTLR start "usingClauseDelete"
	// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql/Cql.g:239:1: usingClauseDelete[Attributes attrs] : K_USING usingClauseDeleteObjective[attrs] ( ( K_AND )? usingClauseDeleteObjective[attrs] )* ;
	public final void usingClauseDelete(Attributes attrs) throws RecognitionException {
		try {
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql/Cql.g:240:5: ( K_USING usingClauseDeleteObjective[attrs] ( ( K_AND )? usingClauseDeleteObjective[attrs] )* )
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql/Cql.g:240:7: K_USING usingClauseDeleteObjective[attrs] ( ( K_AND )? usingClauseDeleteObjective[attrs] )*
			{
			match(input,K_USING,FOLLOW_K_USING_in_usingClauseDelete975); 
			pushFollow(FOLLOW_usingClauseDeleteObjective_in_usingClauseDelete977);
			usingClauseDeleteObjective(attrs);
			state._fsp--;

			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql/Cql.g:240:49: ( ( K_AND )? usingClauseDeleteObjective[attrs] )*
			loop21:
			while (true) {
				int alt21=2;
				int LA21_0 = input.LA(1);
				if ( (LA21_0==K_AND||LA21_0==K_CONSISTENCY||LA21_0==K_TIMESTAMP) ) {
					alt21=1;
				}

				switch (alt21) {
				case 1 :
					// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql/Cql.g:240:51: ( K_AND )? usingClauseDeleteObjective[attrs]
					{
					// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql/Cql.g:240:51: ( K_AND )?
					int alt20=2;
					int LA20_0 = input.LA(1);
					if ( (LA20_0==K_AND) ) {
						alt20=1;
					}
					switch (alt20) {
						case 1 :
							// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql/Cql.g:240:51: K_AND
							{
							match(input,K_AND,FOLLOW_K_AND_in_usingClauseDelete982); 
							}
							break;

					}

					pushFollow(FOLLOW_usingClauseDeleteObjective_in_usingClauseDelete985);
					usingClauseDeleteObjective(attrs);
					state._fsp--;

					}
					break;

				default :
					break loop21;
				}
			}

			}

		}
		catch (RecognitionException re) {
			reportError(re);
			recover(input,re);
		}
		finally {
			// do for sure before leaving
		}
	}
	// $ANTLR end "usingClauseDelete"



	// $ANTLR start "usingClauseDeleteObjective"
	// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql/Cql.g:243:1: usingClauseDeleteObjective[Attributes attrs] : ( K_CONSISTENCY K_LEVEL | K_TIMESTAMP ts= INTEGER );
	public final void usingClauseDeleteObjective(Attributes attrs) throws RecognitionException {
		Token ts=null;
		Token K_LEVEL17=null;

		try {
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql/Cql.g:244:5: ( K_CONSISTENCY K_LEVEL | K_TIMESTAMP ts= INTEGER )
			int alt22=2;
			int LA22_0 = input.LA(1);
			if ( (LA22_0==K_CONSISTENCY) ) {
				alt22=1;
			}
			else if ( (LA22_0==K_TIMESTAMP) ) {
				alt22=2;
			}

			else {
				NoViableAltException nvae =
					new NoViableAltException("", 22, 0, input);
				throw nvae;
			}

			switch (alt22) {
				case 1 :
					// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql/Cql.g:244:7: K_CONSISTENCY K_LEVEL
					{
					match(input,K_CONSISTENCY,FOLLOW_K_CONSISTENCY_in_usingClauseDeleteObjective1007); 
					K_LEVEL17=(Token)match(input,K_LEVEL,FOLLOW_K_LEVEL_in_usingClauseDeleteObjective1009); 
					 attrs.setConsistencyLevel(ConsistencyLevel.valueOf((K_LEVEL17!=null?K_LEVEL17.getText():null).toUpperCase())); 
					}
					break;
				case 2 :
					// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql/Cql.g:245:7: K_TIMESTAMP ts= INTEGER
					{
					match(input,K_TIMESTAMP,FOLLOW_K_TIMESTAMP_in_usingClauseDeleteObjective1020); 
					ts=(Token)match(input,INTEGER,FOLLOW_INTEGER_in_usingClauseDeleteObjective1024); 
					 attrs.setTimestamp(Long.valueOf((ts!=null?ts.getText():null))); 
					}
					break;

			}
		}
		catch (RecognitionException re) {
			reportError(re);
			recover(input,re);
		}
		finally {
			// do for sure before leaving
		}
	}
	// $ANTLR end "usingClauseDeleteObjective"



	// $ANTLR start "usingClauseObjective"
	// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql/Cql.g:248:1: usingClauseObjective[Attributes attrs] : ( usingClauseDeleteObjective[attrs] | K_TTL t= INTEGER );
	public final void usingClauseObjective(Attributes attrs) throws RecognitionException {
		Token t=null;

		try {
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql/Cql.g:249:5: ( usingClauseDeleteObjective[attrs] | K_TTL t= INTEGER )
			int alt23=2;
			int LA23_0 = input.LA(1);
			if ( (LA23_0==K_CONSISTENCY||LA23_0==K_TIMESTAMP) ) {
				alt23=1;
			}
			else if ( (LA23_0==K_TTL) ) {
				alt23=2;
			}

			else {
				NoViableAltException nvae =
					new NoViableAltException("", 23, 0, input);
				throw nvae;
			}

			switch (alt23) {
				case 1 :
					// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql/Cql.g:249:7: usingClauseDeleteObjective[attrs]
					{
					pushFollow(FOLLOW_usingClauseDeleteObjective_in_usingClauseObjective1044);
					usingClauseDeleteObjective(attrs);
					state._fsp--;

					}
					break;
				case 2 :
					// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql/Cql.g:250:7: K_TTL t= INTEGER
					{
					match(input,K_TTL,FOLLOW_K_TTL_in_usingClauseObjective1053); 
					t=(Token)match(input,INTEGER,FOLLOW_INTEGER_in_usingClauseObjective1057); 
					 attrs.setTimeToLive(Integer.parseInt((t!=null?t.getText():null))); 
					}
					break;

			}
		}
		catch (RecognitionException re) {
			reportError(re);
			recover(input,re);
		}
		finally {
			// do for sure before leaving
		}
	}
	// $ANTLR end "usingClauseObjective"



	// $ANTLR start "batchStatement"
	// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql/Cql.g:277:1: batchStatement returns [BatchStatement expr] : K_BEGIN K_BATCH ( usingClause[attrs] )? s1= batchStatementObjective ( ';' )? (sN= batchStatementObjective ( ';' )? )* K_APPLY K_BATCH endStmnt ;
	public final BatchStatement batchStatement() throws RecognitionException {
		BatchStatement expr = null;


		AbstractModification s1 =null;
		AbstractModification sN =null;

		try {
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql/Cql.g:278:5: ( K_BEGIN K_BATCH ( usingClause[attrs] )? s1= batchStatementObjective ( ';' )? (sN= batchStatementObjective ( ';' )? )* K_APPLY K_BATCH endStmnt )
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql/Cql.g:278:7: K_BEGIN K_BATCH ( usingClause[attrs] )? s1= batchStatementObjective ( ';' )? (sN= batchStatementObjective ( ';' )? )* K_APPLY K_BATCH endStmnt
			{

			          Attributes attrs = new Attributes();
			          attrs.setConsistencyLevel(ConsistencyLevel.ONE);

			          List<AbstractModification> statements = new ArrayList<AbstractModification>();
			      
			match(input,K_BEGIN,FOLLOW_K_BEGIN_in_batchStatement1097); 
			match(input,K_BATCH,FOLLOW_K_BATCH_in_batchStatement1099); 
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql/Cql.g:284:23: ( usingClause[attrs] )?
			int alt24=2;
			int LA24_0 = input.LA(1);
			if ( (LA24_0==K_USING) ) {
				alt24=1;
			}
			switch (alt24) {
				case 1 :
					// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql/Cql.g:284:25: usingClause[attrs]
					{
					pushFollow(FOLLOW_usingClause_in_batchStatement1103);
					usingClause(attrs);
					state._fsp--;

					}
					break;

			}

			pushFollow(FOLLOW_batchStatementObjective_in_batchStatement1121);
			s1=batchStatementObjective();
			state._fsp--;

			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql/Cql.g:285:38: ( ';' )?
			int alt25=2;
			int LA25_0 = input.LA(1);
			if ( (LA25_0==87) ) {
				alt25=1;
			}
			switch (alt25) {
				case 1 :
					// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql/Cql.g:285:38: ';'
					{
					match(input,87,FOLLOW_87_in_batchStatement1123); 
					}
					break;

			}

			 statements.add(s1); 
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql/Cql.g:285:67: (sN= batchStatementObjective ( ';' )? )*
			loop27:
			while (true) {
				int alt27=2;
				int LA27_0 = input.LA(1);
				if ( (LA27_0==K_DELETE||LA27_0==K_INSERT||LA27_0==K_UPDATE) ) {
					alt27=1;
				}

				switch (alt27) {
				case 1 :
					// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql/Cql.g:285:69: sN= batchStatementObjective ( ';' )?
					{
					pushFollow(FOLLOW_batchStatementObjective_in_batchStatement1132);
					sN=batchStatementObjective();
					state._fsp--;

					// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql/Cql.g:285:96: ( ';' )?
					int alt26=2;
					int LA26_0 = input.LA(1);
					if ( (LA26_0==87) ) {
						alt26=1;
					}
					switch (alt26) {
						case 1 :
							// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql/Cql.g:285:96: ';'
							{
							match(input,87,FOLLOW_87_in_batchStatement1134); 
							}
							break;

					}

					 statements.add(sN); 
					}
					break;

				default :
					break loop27;
				}
			}

			match(input,K_APPLY,FOLLOW_K_APPLY_in_batchStatement1148); 
			match(input,K_BATCH,FOLLOW_K_BATCH_in_batchStatement1150); 
			pushFollow(FOLLOW_endStmnt_in_batchStatement1152);
			endStmnt();
			state._fsp--;


			          return new BatchStatement(statements, attrs);
			      
			}

		}
		catch (RecognitionException re) {
			reportError(re);
			recover(input,re);
		}
		finally {
			// do for sure before leaving
		}
		return expr;
	}
	// $ANTLR end "batchStatement"



	// $ANTLR start "batchStatementObjective"
	// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql/Cql.g:292:1: batchStatementObjective returns [AbstractModification statement] : (i= insertStatement |u= updateStatement |d= deleteStatement );
	public final AbstractModification batchStatementObjective() throws RecognitionException {
		AbstractModification statement = null;


		UpdateStatement i =null;
		UpdateStatement u =null;
		DeleteStatement d =null;

		try {
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql/Cql.g:293:5: (i= insertStatement |u= updateStatement |d= deleteStatement )
			int alt28=3;
			switch ( input.LA(1) ) {
			case K_INSERT:
				{
				alt28=1;
				}
				break;
			case K_UPDATE:
				{
				alt28=2;
				}
				break;
			case K_DELETE:
				{
				alt28=3;
				}
				break;
			default:
				NoViableAltException nvae =
					new NoViableAltException("", 28, 0, input);
				throw nvae;
			}
			switch (alt28) {
				case 1 :
					// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql/Cql.g:293:7: i= insertStatement
					{
					pushFollow(FOLLOW_insertStatement_in_batchStatementObjective1183);
					i=insertStatement();
					state._fsp--;

					 statement = i; 
					}
					break;
				case 2 :
					// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql/Cql.g:294:7: u= updateStatement
					{
					pushFollow(FOLLOW_updateStatement_in_batchStatementObjective1196);
					u=updateStatement();
					state._fsp--;

					 statement = u; 
					}
					break;
				case 3 :
					// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql/Cql.g:295:7: d= deleteStatement
					{
					pushFollow(FOLLOW_deleteStatement_in_batchStatementObjective1209);
					d=deleteStatement();
					state._fsp--;

					 statement = d; 
					}
					break;

			}
		}
		catch (RecognitionException re) {
			reportError(re);
			recover(input,re);
		}
		finally {
			// do for sure before leaving
		}
		return statement;
	}
	// $ANTLR end "batchStatementObjective"



	// $ANTLR start "updateStatement"
	// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql/Cql.g:311:1: updateStatement returns [UpdateStatement expr] : K_UPDATE (keyspace= ( IDENT | STRING_LITERAL | INTEGER ) '.' )? columnFamily= ( IDENT | STRING_LITERAL | INTEGER ) ( usingClause[attrs] )? K_SET termPairWithOperation[columns] ( ',' termPairWithOperation[columns] )* K_WHERE (key_alias= term ( '=' key= term | K_IN '(' keys= termList ')' ) ) ;
	public final UpdateStatement updateStatement() throws RecognitionException {
		UpdateStatement expr = null;


		Token keyspace=null;
		Token columnFamily=null;
		Term key_alias =null;
		Term key =null;
		List<Term> keys =null;

		try {
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql/Cql.g:312:5: ( K_UPDATE (keyspace= ( IDENT | STRING_LITERAL | INTEGER ) '.' )? columnFamily= ( IDENT | STRING_LITERAL | INTEGER ) ( usingClause[attrs] )? K_SET termPairWithOperation[columns] ( ',' termPairWithOperation[columns] )* K_WHERE (key_alias= term ( '=' key= term | K_IN '(' keys= termList ')' ) ) )
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql/Cql.g:312:7: K_UPDATE (keyspace= ( IDENT | STRING_LITERAL | INTEGER ) '.' )? columnFamily= ( IDENT | STRING_LITERAL | INTEGER ) ( usingClause[attrs] )? K_SET termPairWithOperation[columns] ( ',' termPairWithOperation[columns] )* K_WHERE (key_alias= term ( '=' key= term | K_IN '(' keys= termList ')' ) )
			{

			          Attributes attrs = new Attributes();
			          Map<Term, Operation> columns = new HashMap<Term, Operation>();
			          List<Term> keyList = null;
			      
			match(input,K_UPDATE,FOLLOW_K_UPDATE_in_updateStatement1243); 
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql/Cql.g:317:16: (keyspace= ( IDENT | STRING_LITERAL | INTEGER ) '.' )?
			int alt29=2;
			int LA29_0 = input.LA(1);
			if ( ((LA29_0 >= IDENT && LA29_0 <= INTEGER)||LA29_0==STRING_LITERAL) ) {
				int LA29_1 = input.LA(2);
				if ( (LA29_1==86) ) {
					alt29=1;
				}
			}
			switch (alt29) {
				case 1 :
					// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql/Cql.g:317:17: keyspace= ( IDENT | STRING_LITERAL | INTEGER ) '.'
					{
					keyspace=input.LT(1);
					if ( (input.LA(1) >= IDENT && input.LA(1) <= INTEGER)||input.LA(1)==STRING_LITERAL ) {
						input.consume();
						state.errorRecovery=false;
					}
					else {
						MismatchedSetException mse = new MismatchedSetException(null,input);
						throw mse;
					}
					match(input,86,FOLLOW_86_in_updateStatement1260); 
					}
					break;

			}

			columnFamily=input.LT(1);
			if ( (input.LA(1) >= IDENT && input.LA(1) <= INTEGER)||input.LA(1)==STRING_LITERAL ) {
				input.consume();
				state.errorRecovery=false;
			}
			else {
				MismatchedSetException mse = new MismatchedSetException(null,input);
				throw mse;
			}
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql/Cql.g:318:11: ( usingClause[attrs] )?
			int alt30=2;
			int LA30_0 = input.LA(1);
			if ( (LA30_0==K_USING) ) {
				alt30=1;
			}
			switch (alt30) {
				case 1 :
					// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql/Cql.g:318:13: usingClause[attrs]
					{
					pushFollow(FOLLOW_usingClause_in_updateStatement1292);
					usingClause(attrs);
					state._fsp--;

					}
					break;

			}

			match(input,K_SET,FOLLOW_K_SET_in_updateStatement1308); 
			pushFollow(FOLLOW_termPairWithOperation_in_updateStatement1310);
			termPairWithOperation(columns);
			state._fsp--;

			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql/Cql.g:319:48: ( ',' termPairWithOperation[columns] )*
			loop31:
			while (true) {
				int alt31=2;
				int LA31_0 = input.LA(1);
				if ( (LA31_0==84) ) {
					alt31=1;
				}

				switch (alt31) {
				case 1 :
					// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql/Cql.g:319:49: ',' termPairWithOperation[columns]
					{
					match(input,84,FOLLOW_84_in_updateStatement1314); 
					pushFollow(FOLLOW_termPairWithOperation_in_updateStatement1316);
					termPairWithOperation(columns);
					state._fsp--;

					}
					break;

				default :
					break loop31;
				}
			}

			match(input,K_WHERE,FOLLOW_K_WHERE_in_updateStatement1331); 
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql/Cql.g:320:19: (key_alias= term ( '=' key= term | K_IN '(' keys= termList ')' ) )
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql/Cql.g:320:21: key_alias= term ( '=' key= term | K_IN '(' keys= termList ')' )
			{
			pushFollow(FOLLOW_term_in_updateStatement1337);
			key_alias=term();
			state._fsp--;

			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql/Cql.g:320:36: ( '=' key= term | K_IN '(' keys= termList ')' )
			int alt32=2;
			int LA32_0 = input.LA(1);
			if ( (LA32_0==90) ) {
				alt32=1;
			}
			else if ( (LA32_0==K_IN) ) {
				alt32=2;
			}

			else {
				NoViableAltException nvae =
					new NoViableAltException("", 32, 0, input);
				throw nvae;
			}

			switch (alt32) {
				case 1 :
					// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql/Cql.g:320:37: '=' key= term
					{
					match(input,90,FOLLOW_90_in_updateStatement1340); 
					pushFollow(FOLLOW_term_in_updateStatement1344);
					key=term();
					state._fsp--;

					 keyList = Collections.singletonList(key); 
					}
					break;
				case 2 :
					// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql/Cql.g:322:37: K_IN '(' keys= termList ')'
					{
					match(input,K_IN,FOLLOW_K_IN_in_updateStatement1422); 
					match(input,81,FOLLOW_81_in_updateStatement1424); 
					pushFollow(FOLLOW_termList_in_updateStatement1428);
					keys=termList();
					state._fsp--;

					 keyList = keys; 
					match(input,82,FOLLOW_82_in_updateStatement1432); 
					}
					break;

			}

			}


			          return new UpdateStatement((keyspace!=null?keyspace.getText():null), (columnFamily!=null?columnFamily.getText():null), key_alias.getText(), columns, keyList, attrs);
			      
			}

		}
		catch (RecognitionException re) {
			reportError(re);
			recover(input,re);
		}
		finally {
			// do for sure before leaving
		}
		return expr;
	}
	// $ANTLR end "updateStatement"



	// $ANTLR start "deleteStatement"
	// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql/Cql.g:338:1: deleteStatement returns [DeleteStatement expr] : K_DELETE (cols= termList )? K_FROM (keyspace= ( IDENT | STRING_LITERAL | INTEGER ) '.' )? columnFamily= ( IDENT | STRING_LITERAL | INTEGER ) ( usingClauseDelete[attrs] )? ( K_WHERE key_alias= term ( '=' key= term | K_IN '(' keys= termList ')' ) )? ;
	public final DeleteStatement deleteStatement() throws RecognitionException {
		DeleteStatement expr = null;


		Token keyspace=null;
		Token columnFamily=null;
		List<Term> cols =null;
		Term key_alias =null;
		Term key =null;
		List<Term> keys =null;

		try {
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql/Cql.g:339:5: ( K_DELETE (cols= termList )? K_FROM (keyspace= ( IDENT | STRING_LITERAL | INTEGER ) '.' )? columnFamily= ( IDENT | STRING_LITERAL | INTEGER ) ( usingClauseDelete[attrs] )? ( K_WHERE key_alias= term ( '=' key= term | K_IN '(' keys= termList ')' ) )? )
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql/Cql.g:339:7: K_DELETE (cols= termList )? K_FROM (keyspace= ( IDENT | STRING_LITERAL | INTEGER ) '.' )? columnFamily= ( IDENT | STRING_LITERAL | INTEGER ) ( usingClauseDelete[attrs] )? ( K_WHERE key_alias= term ( '=' key= term | K_IN '(' keys= termList ')' ) )?
			{

			          Attributes attrs = new Attributes();
			          List<Term> keyList = null;
			          List<Term> columnsList = Collections.emptyList();
			      
			match(input,K_DELETE,FOLLOW_K_DELETE_in_deleteStatement1474); 
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql/Cql.g:345:11: (cols= termList )?
			int alt33=2;
			int LA33_0 = input.LA(1);
			if ( (LA33_0==FLOAT||(LA33_0 >= IDENT && LA33_0 <= INTEGER)||LA33_0==K_KEY||LA33_0==QMARK||LA33_0==STRING_LITERAL||LA33_0==UUID) ) {
				alt33=1;
			}
			switch (alt33) {
				case 1 :
					// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql/Cql.g:345:13: cols= termList
					{
					pushFollow(FOLLOW_termList_in_deleteStatement1490);
					cols=termList();
					state._fsp--;

					 columnsList = cols; 
					}
					break;

			}

			match(input,K_FROM,FOLLOW_K_FROM_in_deleteStatement1506); 
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql/Cql.g:346:18: (keyspace= ( IDENT | STRING_LITERAL | INTEGER ) '.' )?
			int alt34=2;
			int LA34_0 = input.LA(1);
			if ( ((LA34_0 >= IDENT && LA34_0 <= INTEGER)||LA34_0==STRING_LITERAL) ) {
				int LA34_1 = input.LA(2);
				if ( (LA34_1==86) ) {
					alt34=1;
				}
			}
			switch (alt34) {
				case 1 :
					// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql/Cql.g:346:19: keyspace= ( IDENT | STRING_LITERAL | INTEGER ) '.'
					{
					keyspace=input.LT(1);
					if ( (input.LA(1) >= IDENT && input.LA(1) <= INTEGER)||input.LA(1)==STRING_LITERAL ) {
						input.consume();
						state.errorRecovery=false;
					}
					else {
						MismatchedSetException mse = new MismatchedSetException(null,input);
						throw mse;
					}
					match(input,86,FOLLOW_86_in_deleteStatement1523); 
					}
					break;

			}

			columnFamily=input.LT(1);
			if ( (input.LA(1) >= IDENT && input.LA(1) <= INTEGER)||input.LA(1)==STRING_LITERAL ) {
				input.consume();
				state.errorRecovery=false;
			}
			else {
				MismatchedSetException mse = new MismatchedSetException(null,input);
				throw mse;
			}
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql/Cql.g:347:11: ( usingClauseDelete[attrs] )?
			int alt35=2;
			int LA35_0 = input.LA(1);
			if ( (LA35_0==K_USING) ) {
				alt35=1;
			}
			switch (alt35) {
				case 1 :
					// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql/Cql.g:347:13: usingClauseDelete[attrs]
					{
					pushFollow(FOLLOW_usingClauseDelete_in_deleteStatement1555);
					usingClauseDelete(attrs);
					state._fsp--;

					}
					break;

			}

			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql/Cql.g:348:11: ( K_WHERE key_alias= term ( '=' key= term | K_IN '(' keys= termList ')' ) )?
			int alt37=2;
			int LA37_0 = input.LA(1);
			if ( (LA37_0==K_WHERE) ) {
				alt37=1;
			}
			switch (alt37) {
				case 1 :
					// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql/Cql.g:348:13: K_WHERE key_alias= term ( '=' key= term | K_IN '(' keys= termList ')' )
					{
					match(input,K_WHERE,FOLLOW_K_WHERE_in_deleteStatement1573); 
					pushFollow(FOLLOW_term_in_deleteStatement1577);
					key_alias=term();
					state._fsp--;

					// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql/Cql.g:348:36: ( '=' key= term | K_IN '(' keys= termList ')' )
					int alt36=2;
					int LA36_0 = input.LA(1);
					if ( (LA36_0==90) ) {
						alt36=1;
					}
					else if ( (LA36_0==K_IN) ) {
						alt36=2;
					}

					else {
						NoViableAltException nvae =
							new NoViableAltException("", 36, 0, input);
						throw nvae;
					}

					switch (alt36) {
						case 1 :
							// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql/Cql.g:348:37: '=' key= term
							{
							match(input,90,FOLLOW_90_in_deleteStatement1580); 
							pushFollow(FOLLOW_term_in_deleteStatement1584);
							key=term();
							state._fsp--;

							 keyList = Collections.singletonList(key); 
							}
							break;
						case 2 :
							// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql/Cql.g:349:38: K_IN '(' keys= termList ')'
							{
							match(input,K_IN,FOLLOW_K_IN_in_deleteStatement1635); 
							match(input,81,FOLLOW_81_in_deleteStatement1637); 
							pushFollow(FOLLOW_termList_in_deleteStatement1641);
							keys=termList();
							state._fsp--;

							 keyList = keys; 
							match(input,82,FOLLOW_82_in_deleteStatement1645); 
							}
							break;

					}

					}
					break;

			}


			          return new DeleteStatement(columnsList, (keyspace!=null?keyspace.getText():null), (columnFamily!=null?columnFamily.getText():null), key_alias.getText(), keyList, attrs);
			      
			}

		}
		catch (RecognitionException re) {
			reportError(re);
			recover(input,re);
		}
		finally {
			// do for sure before leaving
		}
		return expr;
	}
	// $ANTLR end "deleteStatement"



	// $ANTLR start "createKeyspaceStatement"
	// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql/Cql.g:359:1: createKeyspaceStatement returns [CreateKeyspaceStatement expr] : K_CREATE K_KEYSPACE keyspace= ( IDENT | STRING_LITERAL | INTEGER ) K_WITH a1= ( COMPIDENT | IDENT ) '=' v1= ( STRING_LITERAL | INTEGER | IDENT ) ( K_AND aN= ( COMPIDENT | IDENT ) '=' vN= ( STRING_LITERAL | INTEGER | IDENT ) )* endStmnt ;
	public final CreateKeyspaceStatement createKeyspaceStatement() throws RecognitionException {
		CreateKeyspaceStatement expr = null;


		Token keyspace=null;
		Token a1=null;
		Token v1=null;
		Token aN=null;
		Token vN=null;

		try {
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql/Cql.g:360:5: ( K_CREATE K_KEYSPACE keyspace= ( IDENT | STRING_LITERAL | INTEGER ) K_WITH a1= ( COMPIDENT | IDENT ) '=' v1= ( STRING_LITERAL | INTEGER | IDENT ) ( K_AND aN= ( COMPIDENT | IDENT ) '=' vN= ( STRING_LITERAL | INTEGER | IDENT ) )* endStmnt )
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql/Cql.g:360:7: K_CREATE K_KEYSPACE keyspace= ( IDENT | STRING_LITERAL | INTEGER ) K_WITH a1= ( COMPIDENT | IDENT ) '=' v1= ( STRING_LITERAL | INTEGER | IDENT ) ( K_AND aN= ( COMPIDENT | IDENT ) '=' vN= ( STRING_LITERAL | INTEGER | IDENT ) )* endStmnt
			{

			          Map<String, String> attrs = new HashMap<String, String>();
			      
			match(input,K_CREATE,FOLLOW_K_CREATE_in_createKeyspaceStatement1708); 
			match(input,K_KEYSPACE,FOLLOW_K_KEYSPACE_in_createKeyspaceStatement1710); 
			keyspace=input.LT(1);
			if ( (input.LA(1) >= IDENT && input.LA(1) <= INTEGER)||input.LA(1)==STRING_LITERAL ) {
				input.consume();
				state.errorRecovery=false;
			}
			else {
				MismatchedSetException mse = new MismatchedSetException(null,input);
				throw mse;
			}
			match(input,K_WITH,FOLLOW_K_WITH_in_createKeyspaceStatement1738); 
			a1=input.LT(1);
			if ( input.LA(1)==COMPIDENT||input.LA(1)==IDENT ) {
				input.consume();
				state.errorRecovery=false;
			}
			else {
				MismatchedSetException mse = new MismatchedSetException(null,input);
				throw mse;
			}
			match(input,90,FOLLOW_90_in_createKeyspaceStatement1753); 
			v1=input.LT(1);
			if ( (input.LA(1) >= IDENT && input.LA(1) <= INTEGER)||input.LA(1)==STRING_LITERAL ) {
				input.consume();
				state.errorRecovery=false;
			}
			else {
				MismatchedSetException mse = new MismatchedSetException(null,input);
				throw mse;
			}
			 attrs.put((a1!=null?a1.getText():null), (v1!=null?v1.getText():null)); 
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql/Cql.g:365:11: ( K_AND aN= ( COMPIDENT | IDENT ) '=' vN= ( STRING_LITERAL | INTEGER | IDENT ) )*
			loop38:
			while (true) {
				int alt38=2;
				int LA38_0 = input.LA(1);
				if ( (LA38_0==K_AND) ) {
					alt38=1;
				}

				switch (alt38) {
				case 1 :
					// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql/Cql.g:365:13: K_AND aN= ( COMPIDENT | IDENT ) '=' vN= ( STRING_LITERAL | INTEGER | IDENT )
					{
					match(input,K_AND,FOLLOW_K_AND_in_createKeyspaceStatement1785); 
					aN=input.LT(1);
					if ( input.LA(1)==COMPIDENT||input.LA(1)==IDENT ) {
						input.consume();
						state.errorRecovery=false;
					}
					else {
						MismatchedSetException mse = new MismatchedSetException(null,input);
						throw mse;
					}
					match(input,90,FOLLOW_90_in_createKeyspaceStatement1799); 
					vN=input.LT(1);
					if ( (input.LA(1) >= IDENT && input.LA(1) <= INTEGER)||input.LA(1)==STRING_LITERAL ) {
						input.consume();
						state.errorRecovery=false;
					}
					else {
						MismatchedSetException mse = new MismatchedSetException(null,input);
						throw mse;
					}
					 attrs.put((aN!=null?aN.getText():null), (vN!=null?vN.getText():null)); 
					}
					break;

				default :
					break loop38;
				}
			}

			pushFollow(FOLLOW_endStmnt_in_createKeyspaceStatement1832);
			endStmnt();
			state._fsp--;


			          return new CreateKeyspaceStatement((keyspace!=null?keyspace.getText():null), attrs);
			      
			}

		}
		catch (RecognitionException re) {
			reportError(re);
			recover(input,re);
		}
		finally {
			// do for sure before leaving
		}
		return expr;
	}
	// $ANTLR end "createKeyspaceStatement"



	// $ANTLR start "createColumnFamilyStatement"
	// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql/Cql.g:379:1: createColumnFamilyStatement returns [CreateColumnFamilyStatement expr] : K_CREATE K_COLUMNFAMILY name= ( IDENT | STRING_LITERAL | INTEGER ) ( '(' createCfamColumns[expr] ( ',' createCfamColumns[expr] )* ')' )? ( K_WITH prop1= ( COMPIDENT | IDENT ) '=' arg1= createCfamKeywordArgument ( K_AND propN= ( COMPIDENT | IDENT ) '=' argN= createCfamKeywordArgument )* )? endStmnt ;
	public final CreateColumnFamilyStatement createColumnFamilyStatement() throws RecognitionException {
		CreateColumnFamilyStatement expr = null;


		Token name=null;
		Token prop1=null;
		Token propN=null;
		String arg1 =null;
		String argN =null;

		try {
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql/Cql.g:380:5: ( K_CREATE K_COLUMNFAMILY name= ( IDENT | STRING_LITERAL | INTEGER ) ( '(' createCfamColumns[expr] ( ',' createCfamColumns[expr] )* ')' )? ( K_WITH prop1= ( COMPIDENT | IDENT ) '=' arg1= createCfamKeywordArgument ( K_AND propN= ( COMPIDENT | IDENT ) '=' argN= createCfamKeywordArgument )* )? endStmnt )
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql/Cql.g:380:7: K_CREATE K_COLUMNFAMILY name= ( IDENT | STRING_LITERAL | INTEGER ) ( '(' createCfamColumns[expr] ( ',' createCfamColumns[expr] )* ')' )? ( K_WITH prop1= ( COMPIDENT | IDENT ) '=' arg1= createCfamKeywordArgument ( K_AND propN= ( COMPIDENT | IDENT ) '=' argN= createCfamKeywordArgument )* )? endStmnt
			{
			match(input,K_CREATE,FOLLOW_K_CREATE_in_createColumnFamilyStatement1867); 
			match(input,K_COLUMNFAMILY,FOLLOW_K_COLUMNFAMILY_in_createColumnFamilyStatement1869); 
			name=input.LT(1);
			if ( (input.LA(1) >= IDENT && input.LA(1) <= INTEGER)||input.LA(1)==STRING_LITERAL ) {
				input.consume();
				state.errorRecovery=false;
			}
			else {
				MismatchedSetException mse = new MismatchedSetException(null,input);
				throw mse;
			}
			 expr = new CreateColumnFamilyStatement((name!=null?name.getText():null)); 
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql/Cql.g:381:7: ( '(' createCfamColumns[expr] ( ',' createCfamColumns[expr] )* ')' )?
			int alt40=2;
			int LA40_0 = input.LA(1);
			if ( (LA40_0==81) ) {
				alt40=1;
			}
			switch (alt40) {
				case 1 :
					// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql/Cql.g:381:9: '(' createCfamColumns[expr] ( ',' createCfamColumns[expr] )* ')'
					{
					match(input,81,FOLLOW_81_in_createColumnFamilyStatement1897); 
					pushFollow(FOLLOW_createCfamColumns_in_createColumnFamilyStatement1899);
					createCfamColumns(expr);
					state._fsp--;

					// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql/Cql.g:381:37: ( ',' createCfamColumns[expr] )*
					loop39:
					while (true) {
						int alt39=2;
						int LA39_0 = input.LA(1);
						if ( (LA39_0==84) ) {
							alt39=1;
						}

						switch (alt39) {
						case 1 :
							// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql/Cql.g:381:39: ',' createCfamColumns[expr]
							{
							match(input,84,FOLLOW_84_in_createColumnFamilyStatement1904); 
							pushFollow(FOLLOW_createCfamColumns_in_createColumnFamilyStatement1906);
							createCfamColumns(expr);
							state._fsp--;

							}
							break;

						default :
							break loop39;
						}
					}

					match(input,82,FOLLOW_82_in_createColumnFamilyStatement1912); 
					}
					break;

			}

			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql/Cql.g:382:7: ( K_WITH prop1= ( COMPIDENT | IDENT ) '=' arg1= createCfamKeywordArgument ( K_AND propN= ( COMPIDENT | IDENT ) '=' argN= createCfamKeywordArgument )* )?
			int alt42=2;
			int LA42_0 = input.LA(1);
			if ( (LA42_0==K_WITH) ) {
				alt42=1;
			}
			switch (alt42) {
				case 1 :
					// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql/Cql.g:382:9: K_WITH prop1= ( COMPIDENT | IDENT ) '=' arg1= createCfamKeywordArgument ( K_AND propN= ( COMPIDENT | IDENT ) '=' argN= createCfamKeywordArgument )*
					{
					match(input,K_WITH,FOLLOW_K_WITH_in_createColumnFamilyStatement1925); 
					prop1=input.LT(1);
					if ( input.LA(1)==COMPIDENT||input.LA(1)==IDENT ) {
						input.consume();
						state.errorRecovery=false;
					}
					else {
						MismatchedSetException mse = new MismatchedSetException(null,input);
						throw mse;
					}
					match(input,90,FOLLOW_90_in_createColumnFamilyStatement1937); 
					pushFollow(FOLLOW_createCfamKeywordArgument_in_createColumnFamilyStatement1941);
					arg1=createCfamKeywordArgument();
					state._fsp--;

					 expr.addProperty((prop1!=null?prop1.getText():null), arg1); 
					// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql/Cql.g:383:11: ( K_AND propN= ( COMPIDENT | IDENT ) '=' argN= createCfamKeywordArgument )*
					loop41:
					while (true) {
						int alt41=2;
						int LA41_0 = input.LA(1);
						if ( (LA41_0==K_AND) ) {
							alt41=1;
						}

						switch (alt41) {
						case 1 :
							// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql/Cql.g:383:13: K_AND propN= ( COMPIDENT | IDENT ) '=' argN= createCfamKeywordArgument
							{
							match(input,K_AND,FOLLOW_K_AND_in_createColumnFamilyStatement1957); 
							propN=input.LT(1);
							if ( input.LA(1)==COMPIDENT||input.LA(1)==IDENT ) {
								input.consume();
								state.errorRecovery=false;
							}
							else {
								MismatchedSetException mse = new MismatchedSetException(null,input);
								throw mse;
							}
							match(input,90,FOLLOW_90_in_createColumnFamilyStatement1969); 
							pushFollow(FOLLOW_createCfamKeywordArgument_in_createColumnFamilyStatement1973);
							argN=createCfamKeywordArgument();
							state._fsp--;

							 expr.addProperty((propN!=null?propN.getText():null), argN); 
							}
							break;

						default :
							break loop41;
						}
					}

					}
					break;

			}

			pushFollow(FOLLOW_endStmnt_in_createColumnFamilyStatement1995);
			endStmnt();
			state._fsp--;

			}

		}
		catch (RecognitionException re) {
			reportError(re);
			recover(input,re);
		}
		finally {
			// do for sure before leaving
		}
		return expr;
	}
	// $ANTLR end "createColumnFamilyStatement"



	// $ANTLR start "createCfamColumns"
	// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql/Cql.g:388:1: createCfamColumns[CreateColumnFamilyStatement expr] : (n= term v= createCfamColumnValidator |k= term v= createCfamColumnValidator K_PRIMARY K_KEY );
	public final void createCfamColumns(CreateColumnFamilyStatement expr) throws RecognitionException {
		Term n =null;
		String v =null;
		Term k =null;

		try {
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql/Cql.g:389:5: (n= term v= createCfamColumnValidator |k= term v= createCfamColumnValidator K_PRIMARY K_KEY )
			int alt43=2;
			switch ( input.LA(1) ) {
			case K_KEY:
				{
				int LA43_1 = input.LA(2);
				if ( ((LA43_1 >= 94 && LA43_1 <= 107)) ) {
					int LA43_8 = input.LA(3);
					if ( (LA43_8==82||LA43_8==84) ) {
						alt43=1;
					}
					else if ( (LA43_8==K_PRIMARY) ) {
						alt43=2;
					}

					else {
						int nvaeMark = input.mark();
						try {
							for (int nvaeConsume = 0; nvaeConsume < 3 - 1; nvaeConsume++) {
								input.consume();
							}
							NoViableAltException nvae =
								new NoViableAltException("", 43, 8, input);
							throw nvae;
						} finally {
							input.rewind(nvaeMark);
						}
					}

				}
				else if ( (LA43_1==STRING_LITERAL) ) {
					int LA43_9 = input.LA(3);
					if ( (LA43_9==82||LA43_9==84) ) {
						alt43=1;
					}
					else if ( (LA43_9==K_PRIMARY) ) {
						alt43=2;
					}

					else {
						int nvaeMark = input.mark();
						try {
							for (int nvaeConsume = 0; nvaeConsume < 3 - 1; nvaeConsume++) {
								input.consume();
							}
							NoViableAltException nvae =
								new NoViableAltException("", 43, 9, input);
							throw nvae;
						} finally {
							input.rewind(nvaeMark);
						}
					}

				}

				else {
					int nvaeMark = input.mark();
					try {
						input.consume();
						NoViableAltException nvae =
							new NoViableAltException("", 43, 1, input);
						throw nvae;
					} finally {
						input.rewind(nvaeMark);
					}
				}

				}
				break;
			case STRING_LITERAL:
				{
				int LA43_2 = input.LA(2);
				if ( ((LA43_2 >= 94 && LA43_2 <= 107)) ) {
					int LA43_8 = input.LA(3);
					if ( (LA43_8==82||LA43_8==84) ) {
						alt43=1;
					}
					else if ( (LA43_8==K_PRIMARY) ) {
						alt43=2;
					}

					else {
						int nvaeMark = input.mark();
						try {
							for (int nvaeConsume = 0; nvaeConsume < 3 - 1; nvaeConsume++) {
								input.consume();
							}
							NoViableAltException nvae =
								new NoViableAltException("", 43, 8, input);
							throw nvae;
						} finally {
							input.rewind(nvaeMark);
						}
					}

				}
				else if ( (LA43_2==STRING_LITERAL) ) {
					int LA43_9 = input.LA(3);
					if ( (LA43_9==82||LA43_9==84) ) {
						alt43=1;
					}
					else if ( (LA43_9==K_PRIMARY) ) {
						alt43=2;
					}

					else {
						int nvaeMark = input.mark();
						try {
							for (int nvaeConsume = 0; nvaeConsume < 3 - 1; nvaeConsume++) {
								input.consume();
							}
							NoViableAltException nvae =
								new NoViableAltException("", 43, 9, input);
							throw nvae;
						} finally {
							input.rewind(nvaeMark);
						}
					}

				}

				else {
					int nvaeMark = input.mark();
					try {
						input.consume();
						NoViableAltException nvae =
							new NoViableAltException("", 43, 2, input);
						throw nvae;
					} finally {
						input.rewind(nvaeMark);
					}
				}

				}
				break;
			case INTEGER:
				{
				int LA43_3 = input.LA(2);
				if ( ((LA43_3 >= 94 && LA43_3 <= 107)) ) {
					int LA43_8 = input.LA(3);
					if ( (LA43_8==82||LA43_8==84) ) {
						alt43=1;
					}
					else if ( (LA43_8==K_PRIMARY) ) {
						alt43=2;
					}

					else {
						int nvaeMark = input.mark();
						try {
							for (int nvaeConsume = 0; nvaeConsume < 3 - 1; nvaeConsume++) {
								input.consume();
							}
							NoViableAltException nvae =
								new NoViableAltException("", 43, 8, input);
							throw nvae;
						} finally {
							input.rewind(nvaeMark);
						}
					}

				}
				else if ( (LA43_3==STRING_LITERAL) ) {
					int LA43_9 = input.LA(3);
					if ( (LA43_9==82||LA43_9==84) ) {
						alt43=1;
					}
					else if ( (LA43_9==K_PRIMARY) ) {
						alt43=2;
					}

					else {
						int nvaeMark = input.mark();
						try {
							for (int nvaeConsume = 0; nvaeConsume < 3 - 1; nvaeConsume++) {
								input.consume();
							}
							NoViableAltException nvae =
								new NoViableAltException("", 43, 9, input);
							throw nvae;
						} finally {
							input.rewind(nvaeMark);
						}
					}

				}

				else {
					int nvaeMark = input.mark();
					try {
						input.consume();
						NoViableAltException nvae =
							new NoViableAltException("", 43, 3, input);
						throw nvae;
					} finally {
						input.rewind(nvaeMark);
					}
				}

				}
				break;
			case UUID:
				{
				int LA43_4 = input.LA(2);
				if ( ((LA43_4 >= 94 && LA43_4 <= 107)) ) {
					int LA43_8 = input.LA(3);
					if ( (LA43_8==82||LA43_8==84) ) {
						alt43=1;
					}
					else if ( (LA43_8==K_PRIMARY) ) {
						alt43=2;
					}

					else {
						int nvaeMark = input.mark();
						try {
							for (int nvaeConsume = 0; nvaeConsume < 3 - 1; nvaeConsume++) {
								input.consume();
							}
							NoViableAltException nvae =
								new NoViableAltException("", 43, 8, input);
							throw nvae;
						} finally {
							input.rewind(nvaeMark);
						}
					}

				}
				else if ( (LA43_4==STRING_LITERAL) ) {
					int LA43_9 = input.LA(3);
					if ( (LA43_9==82||LA43_9==84) ) {
						alt43=1;
					}
					else if ( (LA43_9==K_PRIMARY) ) {
						alt43=2;
					}

					else {
						int nvaeMark = input.mark();
						try {
							for (int nvaeConsume = 0; nvaeConsume < 3 - 1; nvaeConsume++) {
								input.consume();
							}
							NoViableAltException nvae =
								new NoViableAltException("", 43, 9, input);
							throw nvae;
						} finally {
							input.rewind(nvaeMark);
						}
					}

				}

				else {
					int nvaeMark = input.mark();
					try {
						input.consume();
						NoViableAltException nvae =
							new NoViableAltException("", 43, 4, input);
						throw nvae;
					} finally {
						input.rewind(nvaeMark);
					}
				}

				}
				break;
			case IDENT:
				{
				int LA43_5 = input.LA(2);
				if ( ((LA43_5 >= 94 && LA43_5 <= 107)) ) {
					int LA43_8 = input.LA(3);
					if ( (LA43_8==82||LA43_8==84) ) {
						alt43=1;
					}
					else if ( (LA43_8==K_PRIMARY) ) {
						alt43=2;
					}

					else {
						int nvaeMark = input.mark();
						try {
							for (int nvaeConsume = 0; nvaeConsume < 3 - 1; nvaeConsume++) {
								input.consume();
							}
							NoViableAltException nvae =
								new NoViableAltException("", 43, 8, input);
							throw nvae;
						} finally {
							input.rewind(nvaeMark);
						}
					}

				}
				else if ( (LA43_5==STRING_LITERAL) ) {
					int LA43_9 = input.LA(3);
					if ( (LA43_9==82||LA43_9==84) ) {
						alt43=1;
					}
					else if ( (LA43_9==K_PRIMARY) ) {
						alt43=2;
					}

					else {
						int nvaeMark = input.mark();
						try {
							for (int nvaeConsume = 0; nvaeConsume < 3 - 1; nvaeConsume++) {
								input.consume();
							}
							NoViableAltException nvae =
								new NoViableAltException("", 43, 9, input);
							throw nvae;
						} finally {
							input.rewind(nvaeMark);
						}
					}

				}

				else {
					int nvaeMark = input.mark();
					try {
						input.consume();
						NoViableAltException nvae =
							new NoViableAltException("", 43, 5, input);
						throw nvae;
					} finally {
						input.rewind(nvaeMark);
					}
				}

				}
				break;
			case FLOAT:
				{
				int LA43_6 = input.LA(2);
				if ( ((LA43_6 >= 94 && LA43_6 <= 107)) ) {
					int LA43_8 = input.LA(3);
					if ( (LA43_8==82||LA43_8==84) ) {
						alt43=1;
					}
					else if ( (LA43_8==K_PRIMARY) ) {
						alt43=2;
					}

					else {
						int nvaeMark = input.mark();
						try {
							for (int nvaeConsume = 0; nvaeConsume < 3 - 1; nvaeConsume++) {
								input.consume();
							}
							NoViableAltException nvae =
								new NoViableAltException("", 43, 8, input);
							throw nvae;
						} finally {
							input.rewind(nvaeMark);
						}
					}

				}
				else if ( (LA43_6==STRING_LITERAL) ) {
					int LA43_9 = input.LA(3);
					if ( (LA43_9==82||LA43_9==84) ) {
						alt43=1;
					}
					else if ( (LA43_9==K_PRIMARY) ) {
						alt43=2;
					}

					else {
						int nvaeMark = input.mark();
						try {
							for (int nvaeConsume = 0; nvaeConsume < 3 - 1; nvaeConsume++) {
								input.consume();
							}
							NoViableAltException nvae =
								new NoViableAltException("", 43, 9, input);
							throw nvae;
						} finally {
							input.rewind(nvaeMark);
						}
					}

				}

				else {
					int nvaeMark = input.mark();
					try {
						input.consume();
						NoViableAltException nvae =
							new NoViableAltException("", 43, 6, input);
						throw nvae;
					} finally {
						input.rewind(nvaeMark);
					}
				}

				}
				break;
			case QMARK:
				{
				int LA43_7 = input.LA(2);
				if ( ((LA43_7 >= 94 && LA43_7 <= 107)) ) {
					int LA43_8 = input.LA(3);
					if ( (LA43_8==82||LA43_8==84) ) {
						alt43=1;
					}
					else if ( (LA43_8==K_PRIMARY) ) {
						alt43=2;
					}

					else {
						int nvaeMark = input.mark();
						try {
							for (int nvaeConsume = 0; nvaeConsume < 3 - 1; nvaeConsume++) {
								input.consume();
							}
							NoViableAltException nvae =
								new NoViableAltException("", 43, 8, input);
							throw nvae;
						} finally {
							input.rewind(nvaeMark);
						}
					}

				}
				else if ( (LA43_7==STRING_LITERAL) ) {
					int LA43_9 = input.LA(3);
					if ( (LA43_9==82||LA43_9==84) ) {
						alt43=1;
					}
					else if ( (LA43_9==K_PRIMARY) ) {
						alt43=2;
					}

					else {
						int nvaeMark = input.mark();
						try {
							for (int nvaeConsume = 0; nvaeConsume < 3 - 1; nvaeConsume++) {
								input.consume();
							}
							NoViableAltException nvae =
								new NoViableAltException("", 43, 9, input);
							throw nvae;
						} finally {
							input.rewind(nvaeMark);
						}
					}

				}

				else {
					int nvaeMark = input.mark();
					try {
						input.consume();
						NoViableAltException nvae =
							new NoViableAltException("", 43, 7, input);
						throw nvae;
					} finally {
						input.rewind(nvaeMark);
					}
				}

				}
				break;
			default:
				NoViableAltException nvae =
					new NoViableAltException("", 43, 0, input);
				throw nvae;
			}
			switch (alt43) {
				case 1 :
					// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql/Cql.g:389:7: n= term v= createCfamColumnValidator
					{
					pushFollow(FOLLOW_term_in_createCfamColumns2015);
					n=term();
					state._fsp--;

					pushFollow(FOLLOW_createCfamColumnValidator_in_createCfamColumns2019);
					v=createCfamColumnValidator();
					state._fsp--;

					 expr.addColumn(n, v); 
					}
					break;
				case 2 :
					// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql/Cql.g:390:7: k= term v= createCfamColumnValidator K_PRIMARY K_KEY
					{
					pushFollow(FOLLOW_term_in_createCfamColumns2031);
					k=term();
					state._fsp--;

					pushFollow(FOLLOW_createCfamColumnValidator_in_createCfamColumns2035);
					v=createCfamColumnValidator();
					state._fsp--;

					match(input,K_PRIMARY,FOLLOW_K_PRIMARY_in_createCfamColumns2037); 
					match(input,K_KEY,FOLLOW_K_KEY_in_createCfamColumns2039); 
					 expr.setKeyAlias(k.getText()); expr.setKeyType(v); 
					}
					break;

			}
		}
		catch (RecognitionException re) {
			reportError(re);
			recover(input,re);
		}
		finally {
			// do for sure before leaving
		}
	}
	// $ANTLR end "createCfamColumns"



	// $ANTLR start "createCfamColumnValidator"
	// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql/Cql.g:393:1: createCfamColumnValidator returns [String validator] : ( comparatorType | STRING_LITERAL );
	public final String createCfamColumnValidator() throws RecognitionException {
		String validator = null;


		Token STRING_LITERAL19=null;
		ParserRuleReturnScope comparatorType18 =null;

		try {
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql/Cql.g:394:5: ( comparatorType | STRING_LITERAL )
			int alt44=2;
			int LA44_0 = input.LA(1);
			if ( ((LA44_0 >= 94 && LA44_0 <= 107)) ) {
				alt44=1;
			}
			else if ( (LA44_0==STRING_LITERAL) ) {
				alt44=2;
			}

			else {
				NoViableAltException nvae =
					new NoViableAltException("", 44, 0, input);
				throw nvae;
			}

			switch (alt44) {
				case 1 :
					// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql/Cql.g:394:7: comparatorType
					{
					pushFollow(FOLLOW_comparatorType_in_createCfamColumnValidator2062);
					comparatorType18=comparatorType();
					state._fsp--;

					 validator = (comparatorType18!=null?input.toString(comparatorType18.start,comparatorType18.stop):null); 
					}
					break;
				case 2 :
					// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql/Cql.g:395:7: STRING_LITERAL
					{
					STRING_LITERAL19=(Token)match(input,STRING_LITERAL,FOLLOW_STRING_LITERAL_in_createCfamColumnValidator2072); 
					 validator = (STRING_LITERAL19!=null?STRING_LITERAL19.getText():null); 
					}
					break;

			}
		}
		catch (RecognitionException re) {
			reportError(re);
			recover(input,re);
		}
		finally {
			// do for sure before leaving
		}
		return validator;
	}
	// $ANTLR end "createCfamColumnValidator"



	// $ANTLR start "createCfamKeywordArgument"
	// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql/Cql.g:398:1: createCfamKeywordArgument returns [String arg] : ( comparatorType |value= ( STRING_LITERAL | IDENT | INTEGER | FLOAT ) );
	public final String createCfamKeywordArgument() throws RecognitionException {
		String arg = null;


		Token value=null;
		ParserRuleReturnScope comparatorType20 =null;

		try {
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql/Cql.g:399:5: ( comparatorType |value= ( STRING_LITERAL | IDENT | INTEGER | FLOAT ) )
			int alt45=2;
			int LA45_0 = input.LA(1);
			if ( ((LA45_0 >= 94 && LA45_0 <= 107)) ) {
				alt45=1;
			}
			else if ( (LA45_0==FLOAT||(LA45_0 >= IDENT && LA45_0 <= INTEGER)||LA45_0==STRING_LITERAL) ) {
				alt45=2;
			}

			else {
				NoViableAltException nvae =
					new NoViableAltException("", 45, 0, input);
				throw nvae;
			}

			switch (alt45) {
				case 1 :
					// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql/Cql.g:399:7: comparatorType
					{
					pushFollow(FOLLOW_comparatorType_in_createCfamKeywordArgument2095);
					comparatorType20=comparatorType();
					state._fsp--;

					 arg = (comparatorType20!=null?input.toString(comparatorType20.start,comparatorType20.stop):null); 
					}
					break;
				case 2 :
					// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql/Cql.g:400:7: value= ( STRING_LITERAL | IDENT | INTEGER | FLOAT )
					{
					value=input.LT(1);
					if ( input.LA(1)==FLOAT||(input.LA(1) >= IDENT && input.LA(1) <= INTEGER)||input.LA(1)==STRING_LITERAL ) {
						input.consume();
						state.errorRecovery=false;
					}
					else {
						MismatchedSetException mse = new MismatchedSetException(null,input);
						throw mse;
					}
					 arg = (value!=null?value.getText():null); 
					}
					break;

			}
		}
		catch (RecognitionException re) {
			reportError(re);
			recover(input,re);
		}
		finally {
			// do for sure before leaving
		}
		return arg;
	}
	// $ANTLR end "createCfamKeywordArgument"



	// $ANTLR start "createIndexStatement"
	// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql/Cql.g:404:1: createIndexStatement returns [CreateIndexStatement expr] : K_CREATE K_INDEX (idxName= IDENT )? K_ON cf= ( IDENT | STRING_LITERAL | INTEGER ) '(' columnName= term ')' endStmnt ;
	public final CreateIndexStatement createIndexStatement() throws RecognitionException {
		CreateIndexStatement expr = null;


		Token idxName=null;
		Token cf=null;
		Term columnName =null;

		try {
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql/Cql.g:405:5: ( K_CREATE K_INDEX (idxName= IDENT )? K_ON cf= ( IDENT | STRING_LITERAL | INTEGER ) '(' columnName= term ')' endStmnt )
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql/Cql.g:405:7: K_CREATE K_INDEX (idxName= IDENT )? K_ON cf= ( IDENT | STRING_LITERAL | INTEGER ) '(' columnName= term ')' endStmnt
			{
			match(input,K_CREATE,FOLLOW_K_CREATE_in_createIndexStatement2148); 
			match(input,K_INDEX,FOLLOW_K_INDEX_in_createIndexStatement2150); 
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql/Cql.g:405:24: (idxName= IDENT )?
			int alt46=2;
			int LA46_0 = input.LA(1);
			if ( (LA46_0==IDENT) ) {
				alt46=1;
			}
			switch (alt46) {
				case 1 :
					// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql/Cql.g:405:25: idxName= IDENT
					{
					idxName=(Token)match(input,IDENT,FOLLOW_IDENT_in_createIndexStatement2155); 
					}
					break;

			}

			match(input,K_ON,FOLLOW_K_ON_in_createIndexStatement2159); 
			cf=input.LT(1);
			if ( (input.LA(1) >= IDENT && input.LA(1) <= INTEGER)||input.LA(1)==STRING_LITERAL ) {
				input.consume();
				state.errorRecovery=false;
			}
			else {
				MismatchedSetException mse = new MismatchedSetException(null,input);
				throw mse;
			}
			match(input,81,FOLLOW_81_in_createIndexStatement2177); 
			pushFollow(FOLLOW_term_in_createIndexStatement2181);
			columnName=term();
			state._fsp--;

			match(input,82,FOLLOW_82_in_createIndexStatement2183); 
			pushFollow(FOLLOW_endStmnt_in_createIndexStatement2185);
			endStmnt();
			state._fsp--;

			 expr = new CreateIndexStatement((idxName!=null?idxName.getText():null), (cf!=null?cf.getText():null), columnName); 
			}

		}
		catch (RecognitionException re) {
			reportError(re);
			recover(input,re);
		}
		finally {
			// do for sure before leaving
		}
		return expr;
	}
	// $ANTLR end "createIndexStatement"



	// $ANTLR start "dropIndexStatement"
	// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql/Cql.g:412:1: dropIndexStatement returns [DropIndexStatement expr] : K_DROP K_INDEX index= ( IDENT | STRING_LITERAL | INTEGER ) endStmnt ;
	public final DropIndexStatement dropIndexStatement() throws RecognitionException {
		DropIndexStatement expr = null;


		Token index=null;

		try {
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql/Cql.g:413:5: ( K_DROP K_INDEX index= ( IDENT | STRING_LITERAL | INTEGER ) endStmnt )
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql/Cql.g:414:7: K_DROP K_INDEX index= ( IDENT | STRING_LITERAL | INTEGER ) endStmnt
			{
			match(input,K_DROP,FOLLOW_K_DROP_in_dropIndexStatement2221); 
			match(input,K_INDEX,FOLLOW_K_INDEX_in_dropIndexStatement2223); 
			index=input.LT(1);
			if ( (input.LA(1) >= IDENT && input.LA(1) <= INTEGER)||input.LA(1)==STRING_LITERAL ) {
				input.consume();
				state.errorRecovery=false;
			}
			else {
				MismatchedSetException mse = new MismatchedSetException(null,input);
				throw mse;
			}
			pushFollow(FOLLOW_endStmnt_in_dropIndexStatement2241);
			endStmnt();
			state._fsp--;

			 expr = new DropIndexStatement((index!=null?index.getText():null)); 
			}

		}
		catch (RecognitionException re) {
			reportError(re);
			recover(input,re);
		}
		finally {
			// do for sure before leaving
		}
		return expr;
	}
	// $ANTLR end "dropIndexStatement"



	// $ANTLR start "dropKeyspaceStatement"
	// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql/Cql.g:419:1: dropKeyspaceStatement returns [String ksp] : K_DROP K_KEYSPACE name= ( IDENT | STRING_LITERAL | INTEGER ) endStmnt ;
	public final String dropKeyspaceStatement() throws RecognitionException {
		String ksp = null;


		Token name=null;

		try {
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql/Cql.g:420:5: ( K_DROP K_KEYSPACE name= ( IDENT | STRING_LITERAL | INTEGER ) endStmnt )
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql/Cql.g:420:7: K_DROP K_KEYSPACE name= ( IDENT | STRING_LITERAL | INTEGER ) endStmnt
			{
			match(input,K_DROP,FOLLOW_K_DROP_in_dropKeyspaceStatement2272); 
			match(input,K_KEYSPACE,FOLLOW_K_KEYSPACE_in_dropKeyspaceStatement2274); 
			name=input.LT(1);
			if ( (input.LA(1) >= IDENT && input.LA(1) <= INTEGER)||input.LA(1)==STRING_LITERAL ) {
				input.consume();
				state.errorRecovery=false;
			}
			else {
				MismatchedSetException mse = new MismatchedSetException(null,input);
				throw mse;
			}
			pushFollow(FOLLOW_endStmnt_in_dropKeyspaceStatement2292);
			endStmnt();
			state._fsp--;

			 ksp = (name!=null?name.getText():null); 
			}

		}
		catch (RecognitionException re) {
			reportError(re);
			recover(input,re);
		}
		finally {
			// do for sure before leaving
		}
		return ksp;
	}
	// $ANTLR end "dropKeyspaceStatement"



	// $ANTLR start "alterTableStatement"
	// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql/Cql.g:424:1: alterTableStatement returns [AlterTableStatement expr] : K_ALTER K_COLUMNFAMILY name= ( IDENT | STRING_LITERAL | INTEGER ) ( K_ALTER (col= ( IDENT | STRING_LITERAL | INTEGER ) ) K_TYPE alterValidator= comparatorType | K_ADD (col= ( IDENT | STRING_LITERAL | INTEGER ) ) addValidator= comparatorType | K_DROP (col= ( IDENT | STRING_LITERAL | INTEGER ) ) | K_WITH prop1= ( COMPIDENT | IDENT ) '=' arg1= createCfamKeywordArgument ( K_AND propN= ( COMPIDENT | IDENT ) '=' argN= createCfamKeywordArgument )* ) endStmnt ;
	public final AlterTableStatement alterTableStatement() throws RecognitionException {
		AlterTableStatement expr = null;


		Token name=null;
		Token col=null;
		Token prop1=null;
		Token propN=null;
		ParserRuleReturnScope alterValidator =null;
		ParserRuleReturnScope addValidator =null;
		String arg1 =null;
		String argN =null;

		try {
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql/Cql.g:425:5: ( K_ALTER K_COLUMNFAMILY name= ( IDENT | STRING_LITERAL | INTEGER ) ( K_ALTER (col= ( IDENT | STRING_LITERAL | INTEGER ) ) K_TYPE alterValidator= comparatorType | K_ADD (col= ( IDENT | STRING_LITERAL | INTEGER ) ) addValidator= comparatorType | K_DROP (col= ( IDENT | STRING_LITERAL | INTEGER ) ) | K_WITH prop1= ( COMPIDENT | IDENT ) '=' arg1= createCfamKeywordArgument ( K_AND propN= ( COMPIDENT | IDENT ) '=' argN= createCfamKeywordArgument )* ) endStmnt )
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql/Cql.g:426:5: K_ALTER K_COLUMNFAMILY name= ( IDENT | STRING_LITERAL | INTEGER ) ( K_ALTER (col= ( IDENT | STRING_LITERAL | INTEGER ) ) K_TYPE alterValidator= comparatorType | K_ADD (col= ( IDENT | STRING_LITERAL | INTEGER ) ) addValidator= comparatorType | K_DROP (col= ( IDENT | STRING_LITERAL | INTEGER ) ) | K_WITH prop1= ( COMPIDENT | IDENT ) '=' arg1= createCfamKeywordArgument ( K_AND propN= ( COMPIDENT | IDENT ) '=' argN= createCfamKeywordArgument )* ) endStmnt
			{

			        OperationType type = null;
			        String columnFamily = null, columnName = null, validator = null;
			        Map<String, String> propertyMap = null;
			    
			match(input,K_ALTER,FOLLOW_K_ALTER_in_alterTableStatement2326); 
			match(input,K_COLUMNFAMILY,FOLLOW_K_COLUMNFAMILY_in_alterTableStatement2328); 
			name=input.LT(1);
			if ( (input.LA(1) >= IDENT && input.LA(1) <= INTEGER)||input.LA(1)==STRING_LITERAL ) {
				input.consume();
				state.errorRecovery=false;
			}
			else {
				MismatchedSetException mse = new MismatchedSetException(null,input);
				throw mse;
			}
			 columnFamily = (name!=null?name.getText():null); 
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql/Cql.g:432:11: ( K_ALTER (col= ( IDENT | STRING_LITERAL | INTEGER ) ) K_TYPE alterValidator= comparatorType | K_ADD (col= ( IDENT | STRING_LITERAL | INTEGER ) ) addValidator= comparatorType | K_DROP (col= ( IDENT | STRING_LITERAL | INTEGER ) ) | K_WITH prop1= ( COMPIDENT | IDENT ) '=' arg1= createCfamKeywordArgument ( K_AND propN= ( COMPIDENT | IDENT ) '=' argN= createCfamKeywordArgument )* )
			int alt48=4;
			switch ( input.LA(1) ) {
			case K_ALTER:
				{
				alt48=1;
				}
				break;
			case K_ADD:
				{
				alt48=2;
				}
				break;
			case K_DROP:
				{
				alt48=3;
				}
				break;
			case K_WITH:
				{
				alt48=4;
				}
				break;
			default:
				NoViableAltException nvae =
					new NoViableAltException("", 48, 0, input);
				throw nvae;
			}
			switch (alt48) {
				case 1 :
					// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql/Cql.g:432:13: K_ALTER (col= ( IDENT | STRING_LITERAL | INTEGER ) ) K_TYPE alterValidator= comparatorType
					{
					match(input,K_ALTER,FOLLOW_K_ALTER_in_alterTableStatement2360); 
					 type = OperationType.ALTER; 
					// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql/Cql.g:433:16: (col= ( IDENT | STRING_LITERAL | INTEGER ) )
					// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql/Cql.g:433:17: col= ( IDENT | STRING_LITERAL | INTEGER )
					{
					col=input.LT(1);
					if ( (input.LA(1) >= IDENT && input.LA(1) <= INTEGER)||input.LA(1)==STRING_LITERAL ) {
						input.consume();
						state.errorRecovery=false;
					}
					else {
						MismatchedSetException mse = new MismatchedSetException(null,input);
						throw mse;
					}
					 columnName = (col!=null?col.getText():null); 
					}

					match(input,K_TYPE,FOLLOW_K_TYPE_in_alterTableStatement2414); 
					pushFollow(FOLLOW_comparatorType_in_alterTableStatement2418);
					alterValidator=comparatorType();
					state._fsp--;

					 validator = (alterValidator!=null?input.toString(alterValidator.start,alterValidator.stop):null); 
					}
					break;
				case 2 :
					// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql/Cql.g:435:13: K_ADD (col= ( IDENT | STRING_LITERAL | INTEGER ) ) addValidator= comparatorType
					{
					match(input,K_ADD,FOLLOW_K_ADD_in_alterTableStatement2434); 
					 type = OperationType.ADD; 
					// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql/Cql.g:436:16: (col= ( IDENT | STRING_LITERAL | INTEGER ) )
					// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql/Cql.g:436:17: col= ( IDENT | STRING_LITERAL | INTEGER )
					{
					col=input.LT(1);
					if ( (input.LA(1) >= IDENT && input.LA(1) <= INTEGER)||input.LA(1)==STRING_LITERAL ) {
						input.consume();
						state.errorRecovery=false;
					}
					else {
						MismatchedSetException mse = new MismatchedSetException(null,input);
						throw mse;
					}
					 columnName = (col!=null?col.getText():null); 
					}

					pushFollow(FOLLOW_comparatorType_in_alterTableStatement2490);
					addValidator=comparatorType();
					state._fsp--;

					 validator = (addValidator!=null?input.toString(addValidator.start,addValidator.stop):null); 
					}
					break;
				case 3 :
					// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql/Cql.g:438:13: K_DROP (col= ( IDENT | STRING_LITERAL | INTEGER ) )
					{
					match(input,K_DROP,FOLLOW_K_DROP_in_alterTableStatement2506); 
					 type = OperationType.DROP; 
					// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql/Cql.g:439:16: (col= ( IDENT | STRING_LITERAL | INTEGER ) )
					// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql/Cql.g:439:17: col= ( IDENT | STRING_LITERAL | INTEGER )
					{
					col=input.LT(1);
					if ( (input.LA(1) >= IDENT && input.LA(1) <= INTEGER)||input.LA(1)==STRING_LITERAL ) {
						input.consume();
						state.errorRecovery=false;
					}
					else {
						MismatchedSetException mse = new MismatchedSetException(null,input);
						throw mse;
					}
					 columnName = (col!=null?col.getText():null); 
					}

					}
					break;
				case 4 :
					// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql/Cql.g:440:13: K_WITH prop1= ( COMPIDENT | IDENT ) '=' arg1= createCfamKeywordArgument ( K_AND propN= ( COMPIDENT | IDENT ) '=' argN= createCfamKeywordArgument )*
					{
					match(input,K_WITH,FOLLOW_K_WITH_in_alterTableStatement2557); 
					 type = OperationType.OPTS; propertyMap = new HashMap<String, String>(); 
					prop1=input.LT(1);
					if ( input.LA(1)==COMPIDENT||input.LA(1)==IDENT ) {
						input.consume();
						state.errorRecovery=false;
					}
					else {
						MismatchedSetException mse = new MismatchedSetException(null,input);
						throw mse;
					}
					match(input,90,FOLLOW_90_in_alterTableStatement2586); 
					pushFollow(FOLLOW_createCfamKeywordArgument_in_alterTableStatement2590);
					arg1=createCfamKeywordArgument();
					state._fsp--;

					 propertyMap.put((prop1!=null?prop1.getText():null), arg1); 
					// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql/Cql.g:442:16: ( K_AND propN= ( COMPIDENT | IDENT ) '=' argN= createCfamKeywordArgument )*
					loop47:
					while (true) {
						int alt47=2;
						int LA47_0 = input.LA(1);
						if ( (LA47_0==K_AND) ) {
							alt47=1;
						}

						switch (alt47) {
						case 1 :
							// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql/Cql.g:442:18: K_AND propN= ( COMPIDENT | IDENT ) '=' argN= createCfamKeywordArgument
							{
							match(input,K_AND,FOLLOW_K_AND_in_alterTableStatement2611); 
							propN=input.LT(1);
							if ( input.LA(1)==COMPIDENT||input.LA(1)==IDENT ) {
								input.consume();
								state.errorRecovery=false;
							}
							else {
								MismatchedSetException mse = new MismatchedSetException(null,input);
								throw mse;
							}
							match(input,90,FOLLOW_90_in_alterTableStatement2623); 
							pushFollow(FOLLOW_createCfamKeywordArgument_in_alterTableStatement2627);
							argN=createCfamKeywordArgument();
							state._fsp--;

							 propertyMap.put((propN!=null?propN.getText():null), argN); 
							}
							break;

						default :
							break loop47;
						}
					}

					}
					break;

			}

			pushFollow(FOLLOW_endStmnt_in_alterTableStatement2640);
			endStmnt();
			state._fsp--;


			          expr = new AlterTableStatement(columnFamily, type, columnName, validator, propertyMap);
			      
			}

		}
		catch (RecognitionException re) {
			reportError(re);
			recover(input,re);
		}
		finally {
			// do for sure before leaving
		}
		return expr;
	}
	// $ANTLR end "alterTableStatement"



	// $ANTLR start "dropColumnFamilyStatement"
	// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql/Cql.g:450:1: dropColumnFamilyStatement returns [String cfam] : K_DROP K_COLUMNFAMILY name= ( IDENT | STRING_LITERAL | INTEGER ) endStmnt ;
	public final String dropColumnFamilyStatement() throws RecognitionException {
		String cfam = null;


		Token name=null;

		try {
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql/Cql.g:451:5: ( K_DROP K_COLUMNFAMILY name= ( IDENT | STRING_LITERAL | INTEGER ) endStmnt )
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql/Cql.g:451:7: K_DROP K_COLUMNFAMILY name= ( IDENT | STRING_LITERAL | INTEGER ) endStmnt
			{
			match(input,K_DROP,FOLLOW_K_DROP_in_dropColumnFamilyStatement2671); 
			match(input,K_COLUMNFAMILY,FOLLOW_K_COLUMNFAMILY_in_dropColumnFamilyStatement2673); 
			name=input.LT(1);
			if ( (input.LA(1) >= IDENT && input.LA(1) <= INTEGER)||input.LA(1)==STRING_LITERAL ) {
				input.consume();
				state.errorRecovery=false;
			}
			else {
				MismatchedSetException mse = new MismatchedSetException(null,input);
				throw mse;
			}
			pushFollow(FOLLOW_endStmnt_in_dropColumnFamilyStatement2691);
			endStmnt();
			state._fsp--;

			 cfam = (name!=null?name.getText():null); 
			}

		}
		catch (RecognitionException re) {
			reportError(re);
			recover(input,re);
		}
		finally {
			// do for sure before leaving
		}
		return cfam;
	}
	// $ANTLR end "dropColumnFamilyStatement"


	public static class comparatorType_return extends ParserRuleReturnScope {
	};


	// $ANTLR start "comparatorType"
	// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql/Cql.g:454:1: comparatorType : ( 'blob' | 'ascii' | 'text' | 'varchar' | 'int' | 'varint' | 'bigint' | 'uuid' | 'counter' | 'boolean' | 'timestamp' | 'float' | 'double' | 'decimal' );
	public final CqlParser.comparatorType_return comparatorType() throws RecognitionException {
		CqlParser.comparatorType_return retval = new CqlParser.comparatorType_return();
		retval.start = input.LT(1);

		try {
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql/Cql.g:455:5: ( 'blob' | 'ascii' | 'text' | 'varchar' | 'int' | 'varint' | 'bigint' | 'uuid' | 'counter' | 'boolean' | 'timestamp' | 'float' | 'double' | 'decimal' )
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql/Cql.g:
			{
			if ( (input.LA(1) >= 94 && input.LA(1) <= 107) ) {
				input.consume();
				state.errorRecovery=false;
			}
			else {
				MismatchedSetException mse = new MismatchedSetException(null,input);
				throw mse;
			}
			}

			retval.stop = input.LT(-1);

		}
		catch (RecognitionException re) {
			reportError(re);
			recover(input,re);
		}
		finally {
			// do for sure before leaving
		}
		return retval;
	}
	// $ANTLR end "comparatorType"



	// $ANTLR start "term"
	// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql/Cql.g:458:1: term returns [Term item] : ( (t= K_KEY |t= STRING_LITERAL |t= INTEGER |t= UUID |t= IDENT |t= FLOAT ) |t= QMARK ) ;
	public final Term term() throws RecognitionException {
		Term item = null;


		Token t=null;

		try {
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql/Cql.g:459:5: ( ( (t= K_KEY |t= STRING_LITERAL |t= INTEGER |t= UUID |t= IDENT |t= FLOAT ) |t= QMARK ) )
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql/Cql.g:459:7: ( (t= K_KEY |t= STRING_LITERAL |t= INTEGER |t= UUID |t= IDENT |t= FLOAT ) |t= QMARK )
			{
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql/Cql.g:459:7: ( (t= K_KEY |t= STRING_LITERAL |t= INTEGER |t= UUID |t= IDENT |t= FLOAT ) |t= QMARK )
			int alt50=2;
			int LA50_0 = input.LA(1);
			if ( (LA50_0==FLOAT||(LA50_0 >= IDENT && LA50_0 <= INTEGER)||LA50_0==K_KEY||LA50_0==STRING_LITERAL||LA50_0==UUID) ) {
				alt50=1;
			}
			else if ( (LA50_0==QMARK) ) {
				alt50=2;
			}

			else {
				NoViableAltException nvae =
					new NoViableAltException("", 50, 0, input);
				throw nvae;
			}

			switch (alt50) {
				case 1 :
					// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql/Cql.g:459:8: (t= K_KEY |t= STRING_LITERAL |t= INTEGER |t= UUID |t= IDENT |t= FLOAT )
					{
					// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql/Cql.g:459:8: (t= K_KEY |t= STRING_LITERAL |t= INTEGER |t= UUID |t= IDENT |t= FLOAT )
					int alt49=6;
					switch ( input.LA(1) ) {
					case K_KEY:
						{
						alt49=1;
						}
						break;
					case STRING_LITERAL:
						{
						alt49=2;
						}
						break;
					case INTEGER:
						{
						alt49=3;
						}
						break;
					case UUID:
						{
						alt49=4;
						}
						break;
					case IDENT:
						{
						alt49=5;
						}
						break;
					case FLOAT:
						{
						alt49=6;
						}
						break;
					default:
						NoViableAltException nvae =
							new NoViableAltException("", 49, 0, input);
						throw nvae;
					}
					switch (alt49) {
						case 1 :
							// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql/Cql.g:459:10: t= K_KEY
							{
							t=(Token)match(input,K_KEY,FOLLOW_K_KEY_in_term2788); 
							}
							break;
						case 2 :
							// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql/Cql.g:459:20: t= STRING_LITERAL
							{
							t=(Token)match(input,STRING_LITERAL,FOLLOW_STRING_LITERAL_in_term2794); 
							}
							break;
						case 3 :
							// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql/Cql.g:459:39: t= INTEGER
							{
							t=(Token)match(input,INTEGER,FOLLOW_INTEGER_in_term2800); 
							}
							break;
						case 4 :
							// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql/Cql.g:459:51: t= UUID
							{
							t=(Token)match(input,UUID,FOLLOW_UUID_in_term2806); 
							}
							break;
						case 5 :
							// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql/Cql.g:459:60: t= IDENT
							{
							t=(Token)match(input,IDENT,FOLLOW_IDENT_in_term2812); 
							}
							break;
						case 6 :
							// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql/Cql.g:459:70: t= FLOAT
							{
							t=(Token)match(input,FLOAT,FOLLOW_FLOAT_in_term2818); 
							}
							break;

					}

					 item = new Term((t!=null?t.getText():null), (t!=null?t.getType():0)); 
					}
					break;
				case 2 :
					// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql/Cql.g:460:10: t= QMARK
					{
					t=(Token)match(input,QMARK,FOLLOW_QMARK_in_term2835); 
					 item = new Term((t!=null?t.getText():null), (t!=null?t.getType():0), ++currentBindMarkerIdx); 
					}
					break;

			}

			}

		}
		catch (RecognitionException re) {
			reportError(re);
			recover(input,re);
		}
		finally {
			// do for sure before leaving
		}
		return item;
	}
	// $ANTLR end "term"



	// $ANTLR start "termList"
	// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql/Cql.g:464:1: termList returns [List<Term> items] :t1= term ( ',' tN= term )* ;
	public final List<Term> termList() throws RecognitionException {
		List<Term> items = null;


		Term t1 =null;
		Term tN =null;

		try {
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql/Cql.g:465:5: (t1= term ( ',' tN= term )* )
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql/Cql.g:465:7: t1= term ( ',' tN= term )*
			{
			 items = new ArrayList<Term>(); 
			pushFollow(FOLLOW_term_in_termList2876);
			t1=term();
			state._fsp--;

			 items.add(t1); 
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql/Cql.g:466:35: ( ',' tN= term )*
			loop51:
			while (true) {
				int alt51=2;
				int LA51_0 = input.LA(1);
				if ( (LA51_0==84) ) {
					alt51=1;
				}

				switch (alt51) {
				case 1 :
					// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql/Cql.g:466:36: ',' tN= term
					{
					match(input,84,FOLLOW_84_in_termList2881); 
					pushFollow(FOLLOW_term_in_termList2885);
					tN=term();
					state._fsp--;

					 items.add(tN); 
					}
					break;

				default :
					break loop51;
				}
			}

			}

		}
		catch (RecognitionException re) {
			reportError(re);
			recover(input,re);
		}
		finally {
			// do for sure before leaving
		}
		return items;
	}
	// $ANTLR end "termList"



	// $ANTLR start "termPair"
	// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql/Cql.g:470:1: termPair[Map<Term, Term> columns] : key= term '=' value= term ;
	public final void termPair(Map<Term, Term> columns) throws RecognitionException {
		Term key =null;
		Term value =null;

		try {
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql/Cql.g:471:5: (key= term '=' value= term )
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql/Cql.g:471:9: key= term '=' value= term
			{
			pushFollow(FOLLOW_term_in_termPair2912);
			key=term();
			state._fsp--;

			match(input,90,FOLLOW_90_in_termPair2914); 
			pushFollow(FOLLOW_term_in_termPair2918);
			value=term();
			state._fsp--;

			 columns.put(key, value); 
			}

		}
		catch (RecognitionException re) {
			reportError(re);
			recover(input,re);
		}
		finally {
			// do for sure before leaving
		}
	}
	// $ANTLR end "termPair"



	// $ANTLR start "intTerm"
	// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql/Cql.g:474:1: intTerm returns [Term integer] : t= INTEGER ;
	public final Term intTerm() throws RecognitionException {
		Term integer = null;


		Token t=null;

		try {
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql/Cql.g:475:5: (t= INTEGER )
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql/Cql.g:475:7: t= INTEGER
			{
			t=(Token)match(input,INTEGER,FOLLOW_INTEGER_in_intTerm2943); 
			 integer = new Term((t!=null?t.getText():null), (t!=null?t.getType():0)); 
			}

		}
		catch (RecognitionException re) {
			reportError(re);
			recover(input,re);
		}
		finally {
			// do for sure before leaving
		}
		return integer;
	}
	// $ANTLR end "intTerm"



	// $ANTLR start "termPairWithOperation"
	// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql/Cql.g:478:1: termPairWithOperation[Map<Term, Operation> columns] : key= term '=' (value= term |c= term ( '+' v= term | (op= '-' )? v= intTerm ) ) ;
	public final void termPairWithOperation(Map<Term, Operation> columns) throws RecognitionException {
		Token op=null;
		Term key =null;
		Term value =null;
		Term c =null;
		Term v =null;

		try {
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql/Cql.g:479:5: (key= term '=' (value= term |c= term ( '+' v= term | (op= '-' )? v= intTerm ) ) )
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql/Cql.g:479:7: key= term '=' (value= term |c= term ( '+' v= term | (op= '-' )? v= intTerm ) )
			{
			pushFollow(FOLLOW_term_in_termPairWithOperation2965);
			key=term();
			state._fsp--;

			match(input,90,FOLLOW_90_in_termPairWithOperation2967); 
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql/Cql.g:479:20: (value= term |c= term ( '+' v= term | (op= '-' )? v= intTerm ) )
			int alt54=2;
			switch ( input.LA(1) ) {
			case K_KEY:
				{
				int LA54_1 = input.LA(2);
				if ( (LA54_1==K_WHERE||LA54_1==84) ) {
					alt54=1;
				}
				else if ( (LA54_1==INTEGER||LA54_1==83||LA54_1==85) ) {
					alt54=2;
				}

				else {
					int nvaeMark = input.mark();
					try {
						input.consume();
						NoViableAltException nvae =
							new NoViableAltException("", 54, 1, input);
						throw nvae;
					} finally {
						input.rewind(nvaeMark);
					}
				}

				}
				break;
			case STRING_LITERAL:
				{
				int LA54_2 = input.LA(2);
				if ( (LA54_2==K_WHERE||LA54_2==84) ) {
					alt54=1;
				}
				else if ( (LA54_2==INTEGER||LA54_2==83||LA54_2==85) ) {
					alt54=2;
				}

				else {
					int nvaeMark = input.mark();
					try {
						input.consume();
						NoViableAltException nvae =
							new NoViableAltException("", 54, 2, input);
						throw nvae;
					} finally {
						input.rewind(nvaeMark);
					}
				}

				}
				break;
			case INTEGER:
				{
				int LA54_3 = input.LA(2);
				if ( (LA54_3==K_WHERE||LA54_3==84) ) {
					alt54=1;
				}
				else if ( (LA54_3==INTEGER||LA54_3==83||LA54_3==85) ) {
					alt54=2;
				}

				else {
					int nvaeMark = input.mark();
					try {
						input.consume();
						NoViableAltException nvae =
							new NoViableAltException("", 54, 3, input);
						throw nvae;
					} finally {
						input.rewind(nvaeMark);
					}
				}

				}
				break;
			case UUID:
				{
				int LA54_4 = input.LA(2);
				if ( (LA54_4==K_WHERE||LA54_4==84) ) {
					alt54=1;
				}
				else if ( (LA54_4==INTEGER||LA54_4==83||LA54_4==85) ) {
					alt54=2;
				}

				else {
					int nvaeMark = input.mark();
					try {
						input.consume();
						NoViableAltException nvae =
							new NoViableAltException("", 54, 4, input);
						throw nvae;
					} finally {
						input.rewind(nvaeMark);
					}
				}

				}
				break;
			case IDENT:
				{
				int LA54_5 = input.LA(2);
				if ( (LA54_5==K_WHERE||LA54_5==84) ) {
					alt54=1;
				}
				else if ( (LA54_5==INTEGER||LA54_5==83||LA54_5==85) ) {
					alt54=2;
				}

				else {
					int nvaeMark = input.mark();
					try {
						input.consume();
						NoViableAltException nvae =
							new NoViableAltException("", 54, 5, input);
						throw nvae;
					} finally {
						input.rewind(nvaeMark);
					}
				}

				}
				break;
			case FLOAT:
				{
				int LA54_6 = input.LA(2);
				if ( (LA54_6==K_WHERE||LA54_6==84) ) {
					alt54=1;
				}
				else if ( (LA54_6==INTEGER||LA54_6==83||LA54_6==85) ) {
					alt54=2;
				}

				else {
					int nvaeMark = input.mark();
					try {
						input.consume();
						NoViableAltException nvae =
							new NoViableAltException("", 54, 6, input);
						throw nvae;
					} finally {
						input.rewind(nvaeMark);
					}
				}

				}
				break;
			case QMARK:
				{
				int LA54_7 = input.LA(2);
				if ( (LA54_7==K_WHERE||LA54_7==84) ) {
					alt54=1;
				}
				else if ( (LA54_7==INTEGER||LA54_7==83||LA54_7==85) ) {
					alt54=2;
				}

				else {
					int nvaeMark = input.mark();
					try {
						input.consume();
						NoViableAltException nvae =
							new NoViableAltException("", 54, 7, input);
						throw nvae;
					} finally {
						input.rewind(nvaeMark);
					}
				}

				}
				break;
			default:
				NoViableAltException nvae =
					new NoViableAltException("", 54, 0, input);
				throw nvae;
			}
			switch (alt54) {
				case 1 :
					// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql/Cql.g:479:21: value= term
					{
					pushFollow(FOLLOW_term_in_termPairWithOperation2972);
					value=term();
					state._fsp--;

					 columns.put(key, new Operation(value)); 
					}
					break;
				case 2 :
					// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql/Cql.g:480:20: c= term ( '+' v= term | (op= '-' )? v= intTerm )
					{
					pushFollow(FOLLOW_term_in_termPairWithOperation2997);
					c=term();
					state._fsp--;

					// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql/Cql.g:480:27: ( '+' v= term | (op= '-' )? v= intTerm )
					int alt53=2;
					int LA53_0 = input.LA(1);
					if ( (LA53_0==83) ) {
						alt53=1;
					}
					else if ( (LA53_0==INTEGER||LA53_0==85) ) {
						alt53=2;
					}

					else {
						NoViableAltException nvae =
							new NoViableAltException("", 53, 0, input);
						throw nvae;
					}

					switch (alt53) {
						case 1 :
							// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql/Cql.g:480:29: '+' v= term
							{
							match(input,83,FOLLOW_83_in_termPairWithOperation3001); 
							pushFollow(FOLLOW_term_in_termPairWithOperation3006);
							v=term();
							state._fsp--;

							 columns.put(key, new Operation(c, org.apache.cassandra.cql.Operation.OperationType.PLUS, v)); 
							}
							break;
						case 2 :
							// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql/Cql.g:481:31: (op= '-' )? v= intTerm
							{
							// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql/Cql.g:481:33: (op= '-' )?
							int alt52=2;
							int LA52_0 = input.LA(1);
							if ( (LA52_0==85) ) {
								alt52=1;
							}
							switch (alt52) {
								case 1 :
									// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql/Cql.g:481:33: op= '-'
									{
									op=(Token)match(input,85,FOLLOW_85_in_termPairWithOperation3042); 
									}
									break;

							}

							pushFollow(FOLLOW_intTerm_in_termPairWithOperation3047);
							v=intTerm();
							state._fsp--;

							 validateMinusSupplied(op, v, input);
							                                    if (op != null) v = new Term(-(Long.valueOf(v.getText())), v.getType());
							                                    columns.put(key, new Operation(c, org.apache.cassandra.cql.Operation.OperationType.MINUS, v)); 
							}
							break;

					}

					}
					break;

			}

			}

		}
		catch (RecognitionException re) {
			reportError(re);
			recover(input,re);
		}
		finally {
			// do for sure before leaving
		}
	}
	// $ANTLR end "termPairWithOperation"



	// $ANTLR start "relation"
	// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql/Cql.g:488:1: relation returns [Relation rel] : name= term type= ( '=' | '<' | '<=' | '>=' | '>' ) t= term ;
	public final Relation relation() throws RecognitionException {
		Relation rel = null;


		Token type=null;
		Term name =null;
		Term t =null;

		try {
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql/Cql.g:489:5: (name= term type= ( '=' | '<' | '<=' | '>=' | '>' ) t= term )
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql/Cql.g:489:7: name= term type= ( '=' | '<' | '<=' | '>=' | '>' ) t= term
			{
			pushFollow(FOLLOW_term_in_relation3110);
			name=term();
			state._fsp--;

			type=input.LT(1);
			if ( (input.LA(1) >= 88 && input.LA(1) <= 92) ) {
				input.consume();
				state.errorRecovery=false;
			}
			else {
				MismatchedSetException mse = new MismatchedSetException(null,input);
				throw mse;
			}
			pushFollow(FOLLOW_term_in_relation3136);
			t=term();
			state._fsp--;

			 return new Relation(name, (type!=null?type.getText():null), t); 
			}

		}
		catch (RecognitionException re) {
			reportError(re);
			recover(input,re);
		}
		finally {
			// do for sure before leaving
		}
		return rel;
	}
	// $ANTLR end "relation"



	// $ANTLR start "truncateStatement"
	// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql/Cql.g:494:1: truncateStatement returns [Pair<String,String> cf] : K_TRUNCATE (keyspace= ( IDENT | STRING_LITERAL | INTEGER ) '.' )? columnFamily= ( IDENT | STRING_LITERAL | INTEGER ) endStmnt ;
	public final Pair<String,String> truncateStatement() throws RecognitionException {
		Pair<String,String> cf = null;


		Token keyspace=null;
		Token columnFamily=null;

		try {
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql/Cql.g:495:5: ( K_TRUNCATE (keyspace= ( IDENT | STRING_LITERAL | INTEGER ) '.' )? columnFamily= ( IDENT | STRING_LITERAL | INTEGER ) endStmnt )
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql/Cql.g:495:7: K_TRUNCATE (keyspace= ( IDENT | STRING_LITERAL | INTEGER ) '.' )? columnFamily= ( IDENT | STRING_LITERAL | INTEGER ) endStmnt
			{
			match(input,K_TRUNCATE,FOLLOW_K_TRUNCATE_in_truncateStatement3166); 
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql/Cql.g:495:18: (keyspace= ( IDENT | STRING_LITERAL | INTEGER ) '.' )?
			int alt55=2;
			int LA55_0 = input.LA(1);
			if ( ((LA55_0 >= IDENT && LA55_0 <= INTEGER)||LA55_0==STRING_LITERAL) ) {
				int LA55_1 = input.LA(2);
				if ( (LA55_1==86) ) {
					alt55=1;
				}
			}
			switch (alt55) {
				case 1 :
					// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql/Cql.g:495:19: keyspace= ( IDENT | STRING_LITERAL | INTEGER ) '.'
					{
					keyspace=input.LT(1);
					if ( (input.LA(1) >= IDENT && input.LA(1) <= INTEGER)||input.LA(1)==STRING_LITERAL ) {
						input.consume();
						state.errorRecovery=false;
					}
					else {
						MismatchedSetException mse = new MismatchedSetException(null,input);
						throw mse;
					}
					match(input,86,FOLLOW_86_in_truncateStatement3185); 
					}
					break;

			}

			columnFamily=input.LT(1);
			if ( (input.LA(1) >= IDENT && input.LA(1) <= INTEGER)||input.LA(1)==STRING_LITERAL ) {
				input.consume();
				state.errorRecovery=false;
			}
			else {
				MismatchedSetException mse = new MismatchedSetException(null,input);
				throw mse;
			}
			 cf = Pair.create((keyspace!=null?keyspace.getText():null), (columnFamily!=null?columnFamily.getText():null)); 
			pushFollow(FOLLOW_endStmnt_in_truncateStatement3207);
			endStmnt();
			state._fsp--;

			}

		}
		catch (RecognitionException re) {
			reportError(re);
			recover(input,re);
		}
		finally {
			// do for sure before leaving
		}
		return cf;
	}
	// $ANTLR end "truncateStatement"



	// $ANTLR start "endStmnt"
	// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql/Cql.g:498:1: endStmnt : ( ';' )? EOF ;
	public final void endStmnt() throws RecognitionException {
		try {
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql/Cql.g:499:5: ( ( ';' )? EOF )
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql/Cql.g:499:7: ( ';' )? EOF
			{
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql/Cql.g:499:7: ( ';' )?
			int alt56=2;
			int LA56_0 = input.LA(1);
			if ( (LA56_0==87) ) {
				alt56=1;
			}
			switch (alt56) {
				case 1 :
					// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql/Cql.g:499:7: ';'
					{
					match(input,87,FOLLOW_87_in_endStmnt3224); 
					}
					break;

			}

			match(input,EOF,FOLLOW_EOF_in_endStmnt3228); 
			}

		}
		catch (RecognitionException re) {
			reportError(re);
			recover(input,re);
		}
		finally {
			// do for sure before leaving
		}
	}
	// $ANTLR end "endStmnt"

	// Delegated rules



	public static final BitSet FOLLOW_selectStatement_in_query69 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_insertStatement_in_query81 = new BitSet(new long[]{0x0000000000000000L,0x0000000000800000L});
	public static final BitSet FOLLOW_endStmnt_in_query83 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_updateStatement_in_query93 = new BitSet(new long[]{0x0000000000000000L,0x0000000000800000L});
	public static final BitSet FOLLOW_endStmnt_in_query95 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_batchStatement_in_query105 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_useStatement_in_query115 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_truncateStatement_in_query130 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_deleteStatement_in_query140 = new BitSet(new long[]{0x0000000000000000L,0x0000000000800000L});
	public static final BitSet FOLLOW_endStmnt_in_query142 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_createKeyspaceStatement_in_query152 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_createColumnFamilyStatement_in_query162 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_createIndexStatement_in_query172 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_dropIndexStatement_in_query182 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_dropKeyspaceStatement_in_query194 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_dropColumnFamilyStatement_in_query204 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_alterTableStatement_in_query214 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_K_USE_in_useStatement238 = new BitSet(new long[]{0x00000000000C0000L,0x0000000000000080L});
	public static final BitSet FOLLOW_set_in_useStatement242 = new BitSet(new long[]{0x0000000000000000L,0x0000000000800000L});
	public static final BitSet FOLLOW_endStmnt_in_useStatement258 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_K_SELECT_in_selectStatement289 = new BitSet(new long[]{0x00004104400C2000L,0x0000000020000488L});
	public static final BitSet FOLLOW_selectExpression_in_selectStatement305 = new BitSet(new long[]{0x0000000800000000L});
	public static final BitSet FOLLOW_K_COUNT_in_selectStatement337 = new BitSet(new long[]{0x0000000000000000L,0x0000000000020000L});
	public static final BitSet FOLLOW_81_in_selectStatement339 = new BitSet(new long[]{0x00004104000C2000L,0x0000000020000488L});
	public static final BitSet FOLLOW_selectExpression_in_selectStatement343 = new BitSet(new long[]{0x0000000000000000L,0x0000000000040000L});
	public static final BitSet FOLLOW_82_in_selectStatement345 = new BitSet(new long[]{0x0000000800000000L});
	public static final BitSet FOLLOW_K_FROM_in_selectStatement371 = new BitSet(new long[]{0x00000000000C0000L,0x0000000000000080L});
	public static final BitSet FOLLOW_set_in_selectStatement376 = new BitSet(new long[]{0x0000000000000000L,0x0000000000400000L});
	public static final BitSet FOLLOW_86_in_selectStatement388 = new BitSet(new long[]{0x00000000000C0000L,0x0000000000000080L});
	public static final BitSet FOLLOW_set_in_selectStatement394 = new BitSet(new long[]{0x0280080000000000L,0x0000000000800000L});
	public static final BitSet FOLLOW_K_USING_in_selectStatement420 = new BitSet(new long[]{0x0000000020000000L});
	public static final BitSet FOLLOW_K_CONSISTENCY_in_selectStatement422 = new BitSet(new long[]{0x0000040000000000L});
	public static final BitSet FOLLOW_K_LEVEL_in_selectStatement424 = new BitSet(new long[]{0x0200080000000000L,0x0000000000800000L});
	public static final BitSet FOLLOW_K_WHERE_in_selectStatement443 = new BitSet(new long[]{0x00000100000C2000L,0x0000000000000488L});
	public static final BitSet FOLLOW_whereClause_in_selectStatement445 = new BitSet(new long[]{0x0000080000000000L,0x0000000000800000L});
	public static final BitSet FOLLOW_K_LIMIT_in_selectStatement462 = new BitSet(new long[]{0x0000000000080000L});
	public static final BitSet FOLLOW_INTEGER_in_selectStatement466 = new BitSet(new long[]{0x0000000000000000L,0x0000000000800000L});
	public static final BitSet FOLLOW_endStmnt_in_selectStatement483 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_K_FIRST_in_selectExpression524 = new BitSet(new long[]{0x0000000000080000L});
	public static final BitSet FOLLOW_INTEGER_in_selectExpression530 = new BitSet(new long[]{0x00004100000C2000L,0x0000000020000488L});
	public static final BitSet FOLLOW_K_REVERSED_in_selectExpression545 = new BitSet(new long[]{0x00000100000C2000L,0x0000000020000488L});
	public static final BitSet FOLLOW_term_in_selectExpression562 = new BitSet(new long[]{0x0000000000000002L,0x0000000000100000L});
	public static final BitSet FOLLOW_84_in_selectExpression579 = new BitSet(new long[]{0x00000100000C2000L,0x0000000000000488L});
	public static final BitSet FOLLOW_term_in_selectExpression583 = new BitSet(new long[]{0x0000000000000002L,0x0000000000100000L});
	public static final BitSet FOLLOW_term_in_selectExpression599 = new BitSet(new long[]{0x0000000000000000L,0x0000000000000020L});
	public static final BitSet FOLLOW_RANGEOP_in_selectExpression601 = new BitSet(new long[]{0x00000100000C2000L,0x0000000000000488L});
	public static final BitSet FOLLOW_term_in_selectExpression605 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_93_in_selectExpression617 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_relation_in_whereClause660 = new BitSet(new long[]{0x0000000001000002L});
	public static final BitSet FOLLOW_K_AND_in_whereClause676 = new BitSet(new long[]{0x00000100000C2000L,0x0000000000000488L});
	public static final BitSet FOLLOW_relation_in_whereClause680 = new BitSet(new long[]{0x0000000001000002L});
	public static final BitSet FOLLOW_term_in_whereClause696 = new BitSet(new long[]{0x0000001000000000L});
	public static final BitSet FOLLOW_K_IN_in_whereClause711 = new BitSet(new long[]{0x0000000000000000L,0x0000000000020000L});
	public static final BitSet FOLLOW_81_in_whereClause713 = new BitSet(new long[]{0x00000100000C2000L,0x0000000000000488L});
	public static final BitSet FOLLOW_term_in_whereClause717 = new BitSet(new long[]{0x0000000000000000L,0x0000000000140000L});
	public static final BitSet FOLLOW_84_in_whereClause740 = new BitSet(new long[]{0x00000100000C2000L,0x0000000000000488L});
	public static final BitSet FOLLOW_term_in_whereClause744 = new BitSet(new long[]{0x0000000000000000L,0x0000000000140000L});
	public static final BitSet FOLLOW_82_in_whereClause751 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_K_INSERT_in_insertStatement792 = new BitSet(new long[]{0x0000008000000000L});
	public static final BitSet FOLLOW_K_INTO_in_insertStatement794 = new BitSet(new long[]{0x00000000000C0000L,0x0000000000000080L});
	public static final BitSet FOLLOW_set_in_insertStatement799 = new BitSet(new long[]{0x0000000000000000L,0x0000000000400000L});
	public static final BitSet FOLLOW_86_in_insertStatement811 = new BitSet(new long[]{0x00000000000C0000L,0x0000000000000080L});
	public static final BitSet FOLLOW_set_in_insertStatement817 = new BitSet(new long[]{0x0000000000000000L,0x0000000000020000L});
	public static final BitSet FOLLOW_81_in_insertStatement841 = new BitSet(new long[]{0x00000100000C2000L,0x0000000000000488L});
	public static final BitSet FOLLOW_term_in_insertStatement845 = new BitSet(new long[]{0x0000000000000000L,0x0000000000100000L});
	public static final BitSet FOLLOW_84_in_insertStatement849 = new BitSet(new long[]{0x00000100000C2000L,0x0000000000000488L});
	public static final BitSet FOLLOW_term_in_insertStatement853 = new BitSet(new long[]{0x0000000000000000L,0x0000000000140000L});
	public static final BitSet FOLLOW_82_in_insertStatement861 = new BitSet(new long[]{0x0100000000000000L});
	public static final BitSet FOLLOW_K_VALUES_in_insertStatement871 = new BitSet(new long[]{0x0000000000000000L,0x0000000000020000L});
	public static final BitSet FOLLOW_81_in_insertStatement883 = new BitSet(new long[]{0x00000100000C2000L,0x0000000000000488L});
	public static final BitSet FOLLOW_term_in_insertStatement887 = new BitSet(new long[]{0x0000000000000000L,0x0000000000100000L});
	public static final BitSet FOLLOW_84_in_insertStatement891 = new BitSet(new long[]{0x00000100000C2000L,0x0000000000000488L});
	public static final BitSet FOLLOW_term_in_insertStatement895 = new BitSet(new long[]{0x0000000000000000L,0x0000000000140000L});
	public static final BitSet FOLLOW_82_in_insertStatement901 = new BitSet(new long[]{0x0080000000000002L});
	public static final BitSet FOLLOW_usingClause_in_insertStatement913 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_K_USING_in_usingClause943 = new BitSet(new long[]{0x000A000020000000L});
	public static final BitSet FOLLOW_usingClauseObjective_in_usingClause945 = new BitSet(new long[]{0x000A000021000002L});
	public static final BitSet FOLLOW_K_AND_in_usingClause950 = new BitSet(new long[]{0x000A000020000000L});
	public static final BitSet FOLLOW_usingClauseObjective_in_usingClause953 = new BitSet(new long[]{0x000A000021000002L});
	public static final BitSet FOLLOW_K_USING_in_usingClauseDelete975 = new BitSet(new long[]{0x0002000020000000L});
	public static final BitSet FOLLOW_usingClauseDeleteObjective_in_usingClauseDelete977 = new BitSet(new long[]{0x0002000021000002L});
	public static final BitSet FOLLOW_K_AND_in_usingClauseDelete982 = new BitSet(new long[]{0x0002000020000000L});
	public static final BitSet FOLLOW_usingClauseDeleteObjective_in_usingClauseDelete985 = new BitSet(new long[]{0x0002000021000002L});
	public static final BitSet FOLLOW_K_CONSISTENCY_in_usingClauseDeleteObjective1007 = new BitSet(new long[]{0x0000040000000000L});
	public static final BitSet FOLLOW_K_LEVEL_in_usingClauseDeleteObjective1009 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_K_TIMESTAMP_in_usingClauseDeleteObjective1020 = new BitSet(new long[]{0x0000000000080000L});
	public static final BitSet FOLLOW_INTEGER_in_usingClauseDeleteObjective1024 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_usingClauseDeleteObjective_in_usingClauseObjective1044 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_K_TTL_in_usingClauseObjective1053 = new BitSet(new long[]{0x0000000000080000L});
	public static final BitSet FOLLOW_INTEGER_in_usingClauseObjective1057 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_K_BEGIN_in_batchStatement1097 = new BitSet(new long[]{0x0000000004000000L});
	public static final BitSet FOLLOW_K_BATCH_in_batchStatement1099 = new BitSet(new long[]{0x00A0004100000000L});
	public static final BitSet FOLLOW_usingClause_in_batchStatement1103 = new BitSet(new long[]{0x0020004100000000L});
	public static final BitSet FOLLOW_batchStatementObjective_in_batchStatement1121 = new BitSet(new long[]{0x0020004102000000L,0x0000000000800000L});
	public static final BitSet FOLLOW_87_in_batchStatement1123 = new BitSet(new long[]{0x0020004102000000L});
	public static final BitSet FOLLOW_batchStatementObjective_in_batchStatement1132 = new BitSet(new long[]{0x0020004102000000L,0x0000000000800000L});
	public static final BitSet FOLLOW_87_in_batchStatement1134 = new BitSet(new long[]{0x0020004102000000L});
	public static final BitSet FOLLOW_K_APPLY_in_batchStatement1148 = new BitSet(new long[]{0x0000000004000000L});
	public static final BitSet FOLLOW_K_BATCH_in_batchStatement1150 = new BitSet(new long[]{0x0000000000000000L,0x0000000000800000L});
	public static final BitSet FOLLOW_endStmnt_in_batchStatement1152 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_insertStatement_in_batchStatementObjective1183 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_updateStatement_in_batchStatementObjective1196 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_deleteStatement_in_batchStatementObjective1209 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_K_UPDATE_in_updateStatement1243 = new BitSet(new long[]{0x00000000000C0000L,0x0000000000000080L});
	public static final BitSet FOLLOW_set_in_updateStatement1248 = new BitSet(new long[]{0x0000000000000000L,0x0000000000400000L});
	public static final BitSet FOLLOW_86_in_updateStatement1260 = new BitSet(new long[]{0x00000000000C0000L,0x0000000000000080L});
	public static final BitSet FOLLOW_set_in_updateStatement1266 = new BitSet(new long[]{0x0081000000000000L});
	public static final BitSet FOLLOW_usingClause_in_updateStatement1292 = new BitSet(new long[]{0x0001000000000000L});
	public static final BitSet FOLLOW_K_SET_in_updateStatement1308 = new BitSet(new long[]{0x00000100000C2000L,0x0000000000000488L});
	public static final BitSet FOLLOW_termPairWithOperation_in_updateStatement1310 = new BitSet(new long[]{0x0200000000000000L,0x0000000000100000L});
	public static final BitSet FOLLOW_84_in_updateStatement1314 = new BitSet(new long[]{0x00000100000C2000L,0x0000000000000488L});
	public static final BitSet FOLLOW_termPairWithOperation_in_updateStatement1316 = new BitSet(new long[]{0x0200000000000000L,0x0000000000100000L});
	public static final BitSet FOLLOW_K_WHERE_in_updateStatement1331 = new BitSet(new long[]{0x00000100000C2000L,0x0000000000000488L});
	public static final BitSet FOLLOW_term_in_updateStatement1337 = new BitSet(new long[]{0x0000001000000000L,0x0000000004000000L});
	public static final BitSet FOLLOW_90_in_updateStatement1340 = new BitSet(new long[]{0x00000100000C2000L,0x0000000000000488L});
	public static final BitSet FOLLOW_term_in_updateStatement1344 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_K_IN_in_updateStatement1422 = new BitSet(new long[]{0x0000000000000000L,0x0000000000020000L});
	public static final BitSet FOLLOW_81_in_updateStatement1424 = new BitSet(new long[]{0x00000100000C2000L,0x0000000000000488L});
	public static final BitSet FOLLOW_termList_in_updateStatement1428 = new BitSet(new long[]{0x0000000000000000L,0x0000000000040000L});
	public static final BitSet FOLLOW_82_in_updateStatement1432 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_K_DELETE_in_deleteStatement1474 = new BitSet(new long[]{0x00000108000C2000L,0x0000000000000488L});
	public static final BitSet FOLLOW_termList_in_deleteStatement1490 = new BitSet(new long[]{0x0000000800000000L});
	public static final BitSet FOLLOW_K_FROM_in_deleteStatement1506 = new BitSet(new long[]{0x00000000000C0000L,0x0000000000000080L});
	public static final BitSet FOLLOW_set_in_deleteStatement1511 = new BitSet(new long[]{0x0000000000000000L,0x0000000000400000L});
	public static final BitSet FOLLOW_86_in_deleteStatement1523 = new BitSet(new long[]{0x00000000000C0000L,0x0000000000000080L});
	public static final BitSet FOLLOW_set_in_deleteStatement1529 = new BitSet(new long[]{0x0280000000000002L});
	public static final BitSet FOLLOW_usingClauseDelete_in_deleteStatement1555 = new BitSet(new long[]{0x0200000000000002L});
	public static final BitSet FOLLOW_K_WHERE_in_deleteStatement1573 = new BitSet(new long[]{0x00000100000C2000L,0x0000000000000488L});
	public static final BitSet FOLLOW_term_in_deleteStatement1577 = new BitSet(new long[]{0x0000001000000000L,0x0000000004000000L});
	public static final BitSet FOLLOW_90_in_deleteStatement1580 = new BitSet(new long[]{0x00000100000C2000L,0x0000000000000488L});
	public static final BitSet FOLLOW_term_in_deleteStatement1584 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_K_IN_in_deleteStatement1635 = new BitSet(new long[]{0x0000000000000000L,0x0000000000020000L});
	public static final BitSet FOLLOW_81_in_deleteStatement1637 = new BitSet(new long[]{0x00000100000C2000L,0x0000000000000488L});
	public static final BitSet FOLLOW_termList_in_deleteStatement1641 = new BitSet(new long[]{0x0000000000000000L,0x0000000000040000L});
	public static final BitSet FOLLOW_82_in_deleteStatement1645 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_K_CREATE_in_createKeyspaceStatement1708 = new BitSet(new long[]{0x0000020000000000L});
	public static final BitSet FOLLOW_K_KEYSPACE_in_createKeyspaceStatement1710 = new BitSet(new long[]{0x00000000000C0000L,0x0000000000000080L});
	public static final BitSet FOLLOW_set_in_createKeyspaceStatement1714 = new BitSet(new long[]{0x0400000000000000L});
	public static final BitSet FOLLOW_K_WITH_in_createKeyspaceStatement1738 = new BitSet(new long[]{0x0000000000040100L});
	public static final BitSet FOLLOW_set_in_createKeyspaceStatement1743 = new BitSet(new long[]{0x0000000000000000L,0x0000000004000000L});
	public static final BitSet FOLLOW_90_in_createKeyspaceStatement1753 = new BitSet(new long[]{0x00000000000C0000L,0x0000000000000080L});
	public static final BitSet FOLLOW_set_in_createKeyspaceStatement1757 = new BitSet(new long[]{0x0000000001000000L,0x0000000000800000L});
	public static final BitSet FOLLOW_K_AND_in_createKeyspaceStatement1785 = new BitSet(new long[]{0x0000000000040100L});
	public static final BitSet FOLLOW_set_in_createKeyspaceStatement1789 = new BitSet(new long[]{0x0000000000000000L,0x0000000004000000L});
	public static final BitSet FOLLOW_90_in_createKeyspaceStatement1799 = new BitSet(new long[]{0x00000000000C0000L,0x0000000000000080L});
	public static final BitSet FOLLOW_set_in_createKeyspaceStatement1803 = new BitSet(new long[]{0x0000000001000000L,0x0000000000800000L});
	public static final BitSet FOLLOW_endStmnt_in_createKeyspaceStatement1832 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_K_CREATE_in_createColumnFamilyStatement1867 = new BitSet(new long[]{0x0000000010000000L});
	public static final BitSet FOLLOW_K_COLUMNFAMILY_in_createColumnFamilyStatement1869 = new BitSet(new long[]{0x00000000000C0000L,0x0000000000000080L});
	public static final BitSet FOLLOW_set_in_createColumnFamilyStatement1873 = new BitSet(new long[]{0x0400000000000000L,0x0000000000820000L});
	public static final BitSet FOLLOW_81_in_createColumnFamilyStatement1897 = new BitSet(new long[]{0x00000100000C2000L,0x0000000000000488L});
	public static final BitSet FOLLOW_createCfamColumns_in_createColumnFamilyStatement1899 = new BitSet(new long[]{0x0000000000000000L,0x0000000000140000L});
	public static final BitSet FOLLOW_84_in_createColumnFamilyStatement1904 = new BitSet(new long[]{0x00000100000C2000L,0x0000000000000488L});
	public static final BitSet FOLLOW_createCfamColumns_in_createColumnFamilyStatement1906 = new BitSet(new long[]{0x0000000000000000L,0x0000000000140000L});
	public static final BitSet FOLLOW_82_in_createColumnFamilyStatement1912 = new BitSet(new long[]{0x0400000000000000L,0x0000000000800000L});
	public static final BitSet FOLLOW_K_WITH_in_createColumnFamilyStatement1925 = new BitSet(new long[]{0x0000000000040100L});
	public static final BitSet FOLLOW_set_in_createColumnFamilyStatement1929 = new BitSet(new long[]{0x0000000000000000L,0x0000000004000000L});
	public static final BitSet FOLLOW_90_in_createColumnFamilyStatement1937 = new BitSet(new long[]{0x00000000000C2000L,0x00000FFFC0000080L});
	public static final BitSet FOLLOW_createCfamKeywordArgument_in_createColumnFamilyStatement1941 = new BitSet(new long[]{0x0000000001000000L,0x0000000000800000L});
	public static final BitSet FOLLOW_K_AND_in_createColumnFamilyStatement1957 = new BitSet(new long[]{0x0000000000040100L});
	public static final BitSet FOLLOW_set_in_createColumnFamilyStatement1961 = new BitSet(new long[]{0x0000000000000000L,0x0000000004000000L});
	public static final BitSet FOLLOW_90_in_createColumnFamilyStatement1969 = new BitSet(new long[]{0x00000000000C2000L,0x00000FFFC0000080L});
	public static final BitSet FOLLOW_createCfamKeywordArgument_in_createColumnFamilyStatement1973 = new BitSet(new long[]{0x0000000001000000L,0x0000000000800000L});
	public static final BitSet FOLLOW_endStmnt_in_createColumnFamilyStatement1995 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_term_in_createCfamColumns2015 = new BitSet(new long[]{0x0000000000000000L,0x00000FFFC0000080L});
	public static final BitSet FOLLOW_createCfamColumnValidator_in_createCfamColumns2019 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_term_in_createCfamColumns2031 = new BitSet(new long[]{0x0000000000000000L,0x00000FFFC0000080L});
	public static final BitSet FOLLOW_createCfamColumnValidator_in_createCfamColumns2035 = new BitSet(new long[]{0x0000200000000000L});
	public static final BitSet FOLLOW_K_PRIMARY_in_createCfamColumns2037 = new BitSet(new long[]{0x0000010000000000L});
	public static final BitSet FOLLOW_K_KEY_in_createCfamColumns2039 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_comparatorType_in_createCfamColumnValidator2062 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_STRING_LITERAL_in_createCfamColumnValidator2072 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_comparatorType_in_createCfamKeywordArgument2095 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_set_in_createCfamKeywordArgument2107 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_K_CREATE_in_createIndexStatement2148 = new BitSet(new long[]{0x0000002000000000L});
	public static final BitSet FOLLOW_K_INDEX_in_createIndexStatement2150 = new BitSet(new long[]{0x0000100000040000L});
	public static final BitSet FOLLOW_IDENT_in_createIndexStatement2155 = new BitSet(new long[]{0x0000100000000000L});
	public static final BitSet FOLLOW_K_ON_in_createIndexStatement2159 = new BitSet(new long[]{0x00000000000C0000L,0x0000000000000080L});
	public static final BitSet FOLLOW_set_in_createIndexStatement2163 = new BitSet(new long[]{0x0000000000000000L,0x0000000000020000L});
	public static final BitSet FOLLOW_81_in_createIndexStatement2177 = new BitSet(new long[]{0x00000100000C2000L,0x0000000000000488L});
	public static final BitSet FOLLOW_term_in_createIndexStatement2181 = new BitSet(new long[]{0x0000000000000000L,0x0000000000040000L});
	public static final BitSet FOLLOW_82_in_createIndexStatement2183 = new BitSet(new long[]{0x0000000000000000L,0x0000000000800000L});
	public static final BitSet FOLLOW_endStmnt_in_createIndexStatement2185 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_K_DROP_in_dropIndexStatement2221 = new BitSet(new long[]{0x0000002000000000L});
	public static final BitSet FOLLOW_K_INDEX_in_dropIndexStatement2223 = new BitSet(new long[]{0x00000000000C0000L,0x0000000000000080L});
	public static final BitSet FOLLOW_set_in_dropIndexStatement2227 = new BitSet(new long[]{0x0000000000000000L,0x0000000000800000L});
	public static final BitSet FOLLOW_endStmnt_in_dropIndexStatement2241 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_K_DROP_in_dropKeyspaceStatement2272 = new BitSet(new long[]{0x0000020000000000L});
	public static final BitSet FOLLOW_K_KEYSPACE_in_dropKeyspaceStatement2274 = new BitSet(new long[]{0x00000000000C0000L,0x0000000000000080L});
	public static final BitSet FOLLOW_set_in_dropKeyspaceStatement2278 = new BitSet(new long[]{0x0000000000000000L,0x0000000000800000L});
	public static final BitSet FOLLOW_endStmnt_in_dropKeyspaceStatement2292 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_K_ALTER_in_alterTableStatement2326 = new BitSet(new long[]{0x0000000010000000L});
	public static final BitSet FOLLOW_K_COLUMNFAMILY_in_alterTableStatement2328 = new BitSet(new long[]{0x00000000000C0000L,0x0000000000000080L});
	public static final BitSet FOLLOW_set_in_alterTableStatement2332 = new BitSet(new long[]{0x0400000200C00000L});
	public static final BitSet FOLLOW_K_ALTER_in_alterTableStatement2360 = new BitSet(new long[]{0x00000000000C0000L,0x0000000000000080L});
	public static final BitSet FOLLOW_set_in_alterTableStatement2382 = new BitSet(new long[]{0x0010000000000000L});
	public static final BitSet FOLLOW_K_TYPE_in_alterTableStatement2414 = new BitSet(new long[]{0x0000000000000000L,0x00000FFFC0000000L});
	public static final BitSet FOLLOW_comparatorType_in_alterTableStatement2418 = new BitSet(new long[]{0x0000000000000000L,0x0000000000800000L});
	public static final BitSet FOLLOW_K_ADD_in_alterTableStatement2434 = new BitSet(new long[]{0x00000000000C0000L,0x0000000000000080L});
	public static final BitSet FOLLOW_set_in_alterTableStatement2456 = new BitSet(new long[]{0x0000000000000000L,0x00000FFFC0000000L});
	public static final BitSet FOLLOW_comparatorType_in_alterTableStatement2490 = new BitSet(new long[]{0x0000000000000000L,0x0000000000800000L});
	public static final BitSet FOLLOW_K_DROP_in_alterTableStatement2506 = new BitSet(new long[]{0x00000000000C0000L,0x0000000000000080L});
	public static final BitSet FOLLOW_set_in_alterTableStatement2528 = new BitSet(new long[]{0x0000000000000000L,0x0000000000800000L});
	public static final BitSet FOLLOW_K_WITH_in_alterTableStatement2557 = new BitSet(new long[]{0x0000000000040100L});
	public static final BitSet FOLLOW_set_in_alterTableStatement2578 = new BitSet(new long[]{0x0000000000000000L,0x0000000004000000L});
	public static final BitSet FOLLOW_90_in_alterTableStatement2586 = new BitSet(new long[]{0x00000000000C2000L,0x00000FFFC0000080L});
	public static final BitSet FOLLOW_createCfamKeywordArgument_in_alterTableStatement2590 = new BitSet(new long[]{0x0000000001000000L,0x0000000000800000L});
	public static final BitSet FOLLOW_K_AND_in_alterTableStatement2611 = new BitSet(new long[]{0x0000000000040100L});
	public static final BitSet FOLLOW_set_in_alterTableStatement2615 = new BitSet(new long[]{0x0000000000000000L,0x0000000004000000L});
	public static final BitSet FOLLOW_90_in_alterTableStatement2623 = new BitSet(new long[]{0x00000000000C2000L,0x00000FFFC0000080L});
	public static final BitSet FOLLOW_createCfamKeywordArgument_in_alterTableStatement2627 = new BitSet(new long[]{0x0000000001000000L,0x0000000000800000L});
	public static final BitSet FOLLOW_endStmnt_in_alterTableStatement2640 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_K_DROP_in_dropColumnFamilyStatement2671 = new BitSet(new long[]{0x0000000010000000L});
	public static final BitSet FOLLOW_K_COLUMNFAMILY_in_dropColumnFamilyStatement2673 = new BitSet(new long[]{0x00000000000C0000L,0x0000000000000080L});
	public static final BitSet FOLLOW_set_in_dropColumnFamilyStatement2677 = new BitSet(new long[]{0x0000000000000000L,0x0000000000800000L});
	public static final BitSet FOLLOW_endStmnt_in_dropColumnFamilyStatement2691 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_K_KEY_in_term2788 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_STRING_LITERAL_in_term2794 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_INTEGER_in_term2800 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_UUID_in_term2806 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_IDENT_in_term2812 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_FLOAT_in_term2818 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_QMARK_in_term2835 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_term_in_termList2876 = new BitSet(new long[]{0x0000000000000002L,0x0000000000100000L});
	public static final BitSet FOLLOW_84_in_termList2881 = new BitSet(new long[]{0x00000100000C2000L,0x0000000000000488L});
	public static final BitSet FOLLOW_term_in_termList2885 = new BitSet(new long[]{0x0000000000000002L,0x0000000000100000L});
	public static final BitSet FOLLOW_term_in_termPair2912 = new BitSet(new long[]{0x0000000000000000L,0x0000000004000000L});
	public static final BitSet FOLLOW_90_in_termPair2914 = new BitSet(new long[]{0x00000100000C2000L,0x0000000000000488L});
	public static final BitSet FOLLOW_term_in_termPair2918 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_INTEGER_in_intTerm2943 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_term_in_termPairWithOperation2965 = new BitSet(new long[]{0x0000000000000000L,0x0000000004000000L});
	public static final BitSet FOLLOW_90_in_termPairWithOperation2967 = new BitSet(new long[]{0x00000100000C2000L,0x0000000000000488L});
	public static final BitSet FOLLOW_term_in_termPairWithOperation2972 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_term_in_termPairWithOperation2997 = new BitSet(new long[]{0x0000000000080000L,0x0000000000280000L});
	public static final BitSet FOLLOW_83_in_termPairWithOperation3001 = new BitSet(new long[]{0x00000100000C2000L,0x0000000000000488L});
	public static final BitSet FOLLOW_term_in_termPairWithOperation3006 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_85_in_termPairWithOperation3042 = new BitSet(new long[]{0x0000000000080000L});
	public static final BitSet FOLLOW_intTerm_in_termPairWithOperation3047 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_term_in_relation3110 = new BitSet(new long[]{0x0000000000000000L,0x000000001F000000L});
	public static final BitSet FOLLOW_set_in_relation3114 = new BitSet(new long[]{0x00000100000C2000L,0x0000000000000488L});
	public static final BitSet FOLLOW_term_in_relation3136 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_K_TRUNCATE_in_truncateStatement3166 = new BitSet(new long[]{0x00000000000C0000L,0x0000000000000080L});
	public static final BitSet FOLLOW_set_in_truncateStatement3171 = new BitSet(new long[]{0x0000000000000000L,0x0000000000400000L});
	public static final BitSet FOLLOW_86_in_truncateStatement3185 = new BitSet(new long[]{0x00000000000C0000L,0x0000000000000080L});
	public static final BitSet FOLLOW_set_in_truncateStatement3191 = new BitSet(new long[]{0x0000000000000000L,0x0000000000800000L});
	public static final BitSet FOLLOW_endStmnt_in_truncateStatement3207 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_87_in_endStmnt3224 = new BitSet(new long[]{0x0000000000000000L});
	public static final BitSet FOLLOW_EOF_in_endStmnt3228 = new BitSet(new long[]{0x0000000000000002L});
}
