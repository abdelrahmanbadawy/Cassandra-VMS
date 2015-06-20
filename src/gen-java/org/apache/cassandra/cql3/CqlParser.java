// $ANTLR 3.5.2 /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g 2015-05-19 16:37:59

    package org.apache.cassandra.cql3;

    import java.util.ArrayList;
    import java.util.Arrays;
    import java.util.Collections;
    import java.util.EnumSet;
    import java.util.HashSet;
    import java.util.HashMap;
    import java.util.LinkedHashMap;
    import java.util.List;
    import java.util.Map;
    import java.util.Set;

    import org.apache.cassandra.auth.Permission;
    import org.apache.cassandra.auth.DataResource;
    import org.apache.cassandra.auth.IResource;
    import org.apache.cassandra.cql3.*;
    import org.apache.cassandra.cql3.statements.*;
    import org.apache.cassandra.cql3.functions.FunctionCall;
    import org.apache.cassandra.db.marshal.CollectionType;
    import org.apache.cassandra.exceptions.ConfigurationException;
    import org.apache.cassandra.exceptions.InvalidRequestException;
    import org.apache.cassandra.exceptions.SyntaxException;
    import org.apache.cassandra.utils.Pair;


import org.antlr.runtime.*;
import java.util.Stack;
import java.util.List;
import java.util.ArrayList;

@SuppressWarnings("all")
public class CqlParser extends Parser {
	public static final String[] tokenNames = new String[] {
		"<invalid>", "<EOR>", "<DOWN>", "<UP>", "A", "B", "BOOLEAN", "C", "COMMENT", 
		"D", "DIGIT", "E", "EXPONENT", "F", "FLOAT", "G", "H", "HEX", "HEXNUMBER", 
		"I", "IDENT", "INTEGER", "J", "K", "K_ADD", "K_ALL", "K_ALLOW", "K_ALTER", 
		"K_AND", "K_APPLY", "K_AS", "K_ASC", "K_ASCII", "K_AUTHORIZE", "K_BATCH", 
		"K_BEGIN", "K_BIGINT", "K_BLOB", "K_BOOLEAN", "K_BY", "K_CLUSTERING", 
		"K_COLUMNFAMILY", "K_COMPACT", "K_CONTAINS", "K_COUNT", "K_COUNTER", "K_CREATE", 
		"K_CUSTOM", "K_DECIMAL", "K_DELETE", "K_DESC", "K_DISTINCT", "K_DOUBLE", 
		"K_DROP", "K_EXISTS", "K_FILTERING", "K_FLOAT", "K_FROM", "K_FROZEN", 
		"K_FULL", "K_GRANT", "K_IF", "K_IN", "K_INDEX", "K_INET", "K_INFINITY", 
		"K_INSERT", "K_INT", "K_INTO", "K_KEY", "K_KEYS", "K_KEYSPACE", "K_KEYSPACES", 
		"K_LIMIT", "K_LIST", "K_MAP", "K_MODIFY", "K_NAN", "K_NORECURSIVE", "K_NOSUPERUSER", 
		"K_NOT", "K_NULL", "K_OF", "K_ON", "K_ORDER", "K_PASSWORD", "K_PERMISSION", 
		"K_PERMISSIONS", "K_PRIMARY", "K_RENAME", "K_REVOKE", "K_SELECT", "K_SET", 
		"K_STATIC", "K_STORAGE", "K_SUPERUSER", "K_TEXT", "K_TIMESTAMP", "K_TIMEUUID", 
		"K_TO", "K_TOKEN", "K_TRIGGER", "K_TRUNCATE", "K_TTL", "K_TUPLE", "K_TYPE", 
		"K_UNLOGGED", "K_UPDATE", "K_USE", "K_USER", "K_USERS", "K_USING", "K_UUID", 
		"K_VALUES", "K_VARCHAR", "K_VARINT", "K_WHERE", "K_WITH", "K_WRITETIME", 
		"L", "LETTER", "M", "MULTILINE_COMMENT", "N", "O", "P", "Q", "QMARK", 
		"QUOTED_NAME", "R", "S", "STRING_LITERAL", "T", "U", "UUID", "V", "W", 
		"WS", "X", "Y", "Z", "'!='", "'('", "')'", "'+'", "','", "'-'", "'.'", 
		"':'", "';'", "'<'", "'<='", "'='", "'>'", "'>='", "'['", "'\\*'", "']'", 
		"'{'", "'}'"
	};
	public static final int EOF=-1;
	public static final int T__141=141;
	public static final int T__142=142;
	public static final int T__143=143;
	public static final int T__144=144;
	public static final int T__145=145;
	public static final int T__146=146;
	public static final int T__147=147;
	public static final int T__148=148;
	public static final int T__149=149;
	public static final int T__150=150;
	public static final int T__151=151;
	public static final int T__152=152;
	public static final int T__153=153;
	public static final int T__154=154;
	public static final int T__155=155;
	public static final int T__156=156;
	public static final int T__157=157;
	public static final int T__158=158;
	public static final int T__159=159;
	public static final int A=4;
	public static final int B=5;
	public static final int BOOLEAN=6;
	public static final int C=7;
	public static final int COMMENT=8;
	public static final int D=9;
	public static final int DIGIT=10;
	public static final int E=11;
	public static final int EXPONENT=12;
	public static final int F=13;
	public static final int FLOAT=14;
	public static final int G=15;
	public static final int H=16;
	public static final int HEX=17;
	public static final int HEXNUMBER=18;
	public static final int I=19;
	public static final int IDENT=20;
	public static final int INTEGER=21;
	public static final int J=22;
	public static final int K=23;
	public static final int K_ADD=24;
	public static final int K_ALL=25;
	public static final int K_ALLOW=26;
	public static final int K_ALTER=27;
	public static final int K_AND=28;
	public static final int K_APPLY=29;
	public static final int K_AS=30;
	public static final int K_ASC=31;
	public static final int K_ASCII=32;
	public static final int K_AUTHORIZE=33;
	public static final int K_BATCH=34;
	public static final int K_BEGIN=35;
	public static final int K_BIGINT=36;
	public static final int K_BLOB=37;
	public static final int K_BOOLEAN=38;
	public static final int K_BY=39;
	public static final int K_CLUSTERING=40;
	public static final int K_COLUMNFAMILY=41;
	public static final int K_COMPACT=42;
	public static final int K_CONTAINS=43;
	public static final int K_COUNT=44;
	public static final int K_COUNTER=45;
	public static final int K_CREATE=46;
	public static final int K_CUSTOM=47;
	public static final int K_DECIMAL=48;
	public static final int K_DELETE=49;
	public static final int K_DESC=50;
	public static final int K_DISTINCT=51;
	public static final int K_DOUBLE=52;
	public static final int K_DROP=53;
	public static final int K_EXISTS=54;
	public static final int K_FILTERING=55;
	public static final int K_FLOAT=56;
	public static final int K_FROM=57;
	public static final int K_FROZEN=58;
	public static final int K_FULL=59;
	public static final int K_GRANT=60;
	public static final int K_IF=61;
	public static final int K_IN=62;
	public static final int K_INDEX=63;
	public static final int K_INET=64;
	public static final int K_INFINITY=65;
	public static final int K_INSERT=66;
	public static final int K_INT=67;
	public static final int K_INTO=68;
	public static final int K_KEY=69;
	public static final int K_KEYS=70;
	public static final int K_KEYSPACE=71;
	public static final int K_KEYSPACES=72;
	public static final int K_LIMIT=73;
	public static final int K_LIST=74;
	public static final int K_MAP=75;
	public static final int K_MODIFY=76;
	public static final int K_NAN=77;
	public static final int K_NORECURSIVE=78;
	public static final int K_NOSUPERUSER=79;
	public static final int K_NOT=80;
	public static final int K_NULL=81;
	public static final int K_OF=82;
	public static final int K_ON=83;
	public static final int K_ORDER=84;
	public static final int K_PASSWORD=85;
	public static final int K_PERMISSION=86;
	public static final int K_PERMISSIONS=87;
	public static final int K_PRIMARY=88;
	public static final int K_RENAME=89;
	public static final int K_REVOKE=90;
	public static final int K_SELECT=91;
	public static final int K_SET=92;
	public static final int K_STATIC=93;
	public static final int K_STORAGE=94;
	public static final int K_SUPERUSER=95;
	public static final int K_TEXT=96;
	public static final int K_TIMESTAMP=97;
	public static final int K_TIMEUUID=98;
	public static final int K_TO=99;
	public static final int K_TOKEN=100;
	public static final int K_TRIGGER=101;
	public static final int K_TRUNCATE=102;
	public static final int K_TTL=103;
	public static final int K_TUPLE=104;
	public static final int K_TYPE=105;
	public static final int K_UNLOGGED=106;
	public static final int K_UPDATE=107;
	public static final int K_USE=108;
	public static final int K_USER=109;
	public static final int K_USERS=110;
	public static final int K_USING=111;
	public static final int K_UUID=112;
	public static final int K_VALUES=113;
	public static final int K_VARCHAR=114;
	public static final int K_VARINT=115;
	public static final int K_WHERE=116;
	public static final int K_WITH=117;
	public static final int K_WRITETIME=118;
	public static final int L=119;
	public static final int LETTER=120;
	public static final int M=121;
	public static final int MULTILINE_COMMENT=122;
	public static final int N=123;
	public static final int O=124;
	public static final int P=125;
	public static final int Q=126;
	public static final int QMARK=127;
	public static final int QUOTED_NAME=128;
	public static final int R=129;
	public static final int S=130;
	public static final int STRING_LITERAL=131;
	public static final int T=132;
	public static final int U=133;
	public static final int UUID=134;
	public static final int V=135;
	public static final int W=136;
	public static final int WS=137;
	public static final int X=138;
	public static final int Y=139;
	public static final int Z=140;

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
	@Override public String getGrammarFileName() { return "/home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g"; }


	    private final List<ErrorListener> listeners = new ArrayList<ErrorListener>();
	    private final List<ColumnIdentifier> bindVariables = new ArrayList<ColumnIdentifier>();

	    public static final Set<String> reservedTypeNames = new HashSet<String>()
	    {{
	        add("byte");
	        add("smallint");
	        add("complex");
	        add("enum");
	        add("date");
	        add("interval");
	        add("macaddr");
	        add("bitstring");
	    }};

	    public AbstractMarker.Raw newBindVariables(ColumnIdentifier name)
	    {
	        AbstractMarker.Raw marker = new AbstractMarker.Raw(bindVariables.size());
	        bindVariables.add(name);
	        return marker;
	    }

	    public AbstractMarker.INRaw newINBindVariables(ColumnIdentifier name)
	    {
	        AbstractMarker.INRaw marker = new AbstractMarker.INRaw(bindVariables.size());
	        bindVariables.add(name);
	        return marker;
	    }

	    public Tuples.Raw newTupleBindVariables(ColumnIdentifier name)
	    {
	        Tuples.Raw marker = new Tuples.Raw(bindVariables.size());
	        bindVariables.add(name);
	        return marker;
	    }

	    public Tuples.INRaw newTupleINBindVariables(ColumnIdentifier name)
	    {
	        Tuples.INRaw marker = new Tuples.INRaw(bindVariables.size());
	        bindVariables.add(name);
	        return marker;
	    }

	    public void addErrorListener(ErrorListener listener)
	    {
	        this.listeners.add(listener);
	    }

	    public void removeErrorListener(ErrorListener listener)
	    {
	        this.listeners.remove(listener);
	    }

	    public void displayRecognitionError(String[] tokenNames, RecognitionException e)
	    {
	        for (int i = 0, m = listeners.size(); i < m; i++)
	            listeners.get(i).syntaxError(this, tokenNames, e);
	    }

	    private void addRecognitionError(String msg)
	    {
	        for (int i = 0, m = listeners.size(); i < m; i++)
	            listeners.get(i).syntaxError(this, msg);
	    }

	    public Map<String, String> convertPropertyMap(Maps.Literal map)
	    {
	        if (map == null || map.entries == null || map.entries.isEmpty())
	            return Collections.<String, String>emptyMap();

	        Map<String, String> res = new HashMap<String, String>(map.entries.size());

	        for (Pair<Term.Raw, Term.Raw> entry : map.entries)
	        {
	            // Because the parser tries to be smart and recover on error (to
	            // allow displaying more than one error I suppose), we have null
	            // entries in there. Just skip those, a proper error will be thrown in the end.
	            if (entry.left == null || entry.right == null)
	                break;

	            if (!(entry.left instanceof Constants.Literal))
	            {
	                String msg = "Invalid property name: " + entry.left;
	                if (entry.left instanceof AbstractMarker.Raw)
	                    msg += " (bind variables are not supported in DDL queries)";
	                addRecognitionError(msg);
	                break;
	            }
	            if (!(entry.right instanceof Constants.Literal))
	            {
	                String msg = "Invalid property value: " + entry.right + " for property: " + entry.left;
	                if (entry.right instanceof AbstractMarker.Raw)
	                    msg += " (bind variables are not supported in DDL queries)";
	                addRecognitionError(msg);
	                break;
	            }

	            res.put(((Constants.Literal)entry.left).getRawText(), ((Constants.Literal)entry.right).getRawText());
	        }

	        return res;
	    }

	    public void addRawUpdate(List<Pair<ColumnIdentifier.Raw, Operation.RawUpdate>> operations, ColumnIdentifier.Raw key, Operation.RawUpdate update)
	    {
	        for (Pair<ColumnIdentifier.Raw, Operation.RawUpdate> p : operations)
	        {
	            if (p.left.equals(key) && !p.right.isCompatibleWith(update))
	                addRecognitionError("Multiple incompatible setting of column " + key);
	        }
	        operations.add(Pair.create(key, update));
	    }



	// $ANTLR start "query"
	// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:212:1: query returns [ParsedStatement stmnt] : st= cqlStatement ( ';' )* EOF ;
	public final ParsedStatement query() throws RecognitionException {
		ParsedStatement stmnt = null;


		ParsedStatement st =null;

		try {
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:213:5: (st= cqlStatement ( ';' )* EOF )
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:213:7: st= cqlStatement ( ';' )* EOF
			{
			pushFollow(FOLLOW_cqlStatement_in_query72);
			st=cqlStatement();
			state._fsp--;

			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:213:23: ( ';' )*
			loop1:
			while (true) {
				int alt1=2;
				int LA1_0 = input.LA(1);
				if ( (LA1_0==149) ) {
					alt1=1;
				}

				switch (alt1) {
				case 1 :
					// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:213:24: ';'
					{
					match(input,149,FOLLOW_149_in_query75); 
					}
					break;

				default :
					break loop1;
				}
			}

			match(input,EOF,FOLLOW_EOF_in_query79); 
			 stmnt = st; 
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



	// $ANTLR start "cqlStatement"
	// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:216:1: cqlStatement returns [ParsedStatement stmt] : (st1= selectStatement |st2= insertStatement |st3= updateStatement |st4= batchStatement |st5= deleteStatement |st6= useStatement |st7= truncateStatement |st8= createKeyspaceStatement |st9= createTableStatement |st10= createIndexStatement |st11= dropKeyspaceStatement |st12= dropTableStatement |st13= dropIndexStatement |st14= alterTableStatement |st15= alterKeyspaceStatement |st16= grantStatement |st17= revokeStatement |st18= listPermissionsStatement |st19= createUserStatement |st20= alterUserStatement |st21= dropUserStatement |st22= listUsersStatement |st23= createTriggerStatement |st24= dropTriggerStatement |st25= createTypeStatement |st26= alterTypeStatement |st27= dropTypeStatement );
	public final ParsedStatement cqlStatement() throws RecognitionException {
		ParsedStatement stmt = null;


		SelectStatement.RawStatement st1 =null;
		UpdateStatement.ParsedInsert st2 =null;
		UpdateStatement.ParsedUpdate st3 =null;
		BatchStatement.Parsed st4 =null;
		DeleteStatement.Parsed st5 =null;
		UseStatement st6 =null;
		TruncateStatement st7 =null;
		CreateKeyspaceStatement st8 =null;
		CreateTableStatement.RawStatement st9 =null;
		CreateIndexStatement st10 =null;
		DropKeyspaceStatement st11 =null;
		DropTableStatement st12 =null;
		DropIndexStatement st13 =null;
		AlterTableStatement st14 =null;
		AlterKeyspaceStatement st15 =null;
		GrantStatement st16 =null;
		RevokeStatement st17 =null;
		ListPermissionsStatement st18 =null;
		CreateUserStatement st19 =null;
		AlterUserStatement st20 =null;
		DropUserStatement st21 =null;
		ListUsersStatement st22 =null;
		CreateTriggerStatement st23 =null;
		DropTriggerStatement st24 =null;
		CreateTypeStatement st25 =null;
		AlterTypeStatement st26 =null;
		DropTypeStatement st27 =null;

		try {
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:218:5: (st1= selectStatement |st2= insertStatement |st3= updateStatement |st4= batchStatement |st5= deleteStatement |st6= useStatement |st7= truncateStatement |st8= createKeyspaceStatement |st9= createTableStatement |st10= createIndexStatement |st11= dropKeyspaceStatement |st12= dropTableStatement |st13= dropIndexStatement |st14= alterTableStatement |st15= alterKeyspaceStatement |st16= grantStatement |st17= revokeStatement |st18= listPermissionsStatement |st19= createUserStatement |st20= alterUserStatement |st21= dropUserStatement |st22= listUsersStatement |st23= createTriggerStatement |st24= dropTriggerStatement |st25= createTypeStatement |st26= alterTypeStatement |st27= dropTypeStatement )
			int alt2=27;
			switch ( input.LA(1) ) {
			case K_SELECT:
				{
				alt2=1;
				}
				break;
			case K_INSERT:
				{
				alt2=2;
				}
				break;
			case K_UPDATE:
				{
				alt2=3;
				}
				break;
			case K_BEGIN:
				{
				alt2=4;
				}
				break;
			case K_DELETE:
				{
				alt2=5;
				}
				break;
			case K_USE:
				{
				alt2=6;
				}
				break;
			case K_TRUNCATE:
				{
				alt2=7;
				}
				break;
			case K_CREATE:
				{
				switch ( input.LA(2) ) {
				case K_KEYSPACE:
					{
					alt2=8;
					}
					break;
				case K_COLUMNFAMILY:
					{
					alt2=9;
					}
					break;
				case K_USER:
					{
					alt2=19;
					}
					break;
				case K_TRIGGER:
					{
					alt2=23;
					}
					break;
				case K_TYPE:
					{
					alt2=25;
					}
					break;
				case K_CUSTOM:
				case K_INDEX:
					{
					alt2=10;
					}
					break;
				default:
					int nvaeMark = input.mark();
					try {
						input.consume();
						NoViableAltException nvae =
							new NoViableAltException("", 2, 8, input);
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
				case K_KEYSPACE:
					{
					alt2=11;
					}
					break;
				case K_COLUMNFAMILY:
					{
					alt2=12;
					}
					break;
				case K_INDEX:
					{
					alt2=13;
					}
					break;
				case K_USER:
					{
					alt2=21;
					}
					break;
				case K_TRIGGER:
					{
					alt2=24;
					}
					break;
				case K_TYPE:
					{
					alt2=27;
					}
					break;
				default:
					int nvaeMark = input.mark();
					try {
						input.consume();
						NoViableAltException nvae =
							new NoViableAltException("", 2, 9, input);
						throw nvae;
					} finally {
						input.rewind(nvaeMark);
					}
				}
				}
				break;
			case K_ALTER:
				{
				switch ( input.LA(2) ) {
				case K_COLUMNFAMILY:
					{
					alt2=14;
					}
					break;
				case K_KEYSPACE:
					{
					alt2=15;
					}
					break;
				case K_USER:
					{
					alt2=20;
					}
					break;
				case K_TYPE:
					{
					alt2=26;
					}
					break;
				default:
					int nvaeMark = input.mark();
					try {
						input.consume();
						NoViableAltException nvae =
							new NoViableAltException("", 2, 10, input);
						throw nvae;
					} finally {
						input.rewind(nvaeMark);
					}
				}
				}
				break;
			case K_GRANT:
				{
				alt2=16;
				}
				break;
			case K_REVOKE:
				{
				alt2=17;
				}
				break;
			case K_LIST:
				{
				int LA2_13 = input.LA(2);
				if ( (LA2_13==K_USERS) ) {
					alt2=22;
				}
				else if ( (LA2_13==K_ALL||LA2_13==K_ALTER||LA2_13==K_AUTHORIZE||LA2_13==K_CREATE||LA2_13==K_DROP||LA2_13==K_MODIFY||LA2_13==K_SELECT) ) {
					alt2=18;
				}

				else {
					int nvaeMark = input.mark();
					try {
						input.consume();
						NoViableAltException nvae =
							new NoViableAltException("", 2, 13, input);
						throw nvae;
					} finally {
						input.rewind(nvaeMark);
					}
				}

				}
				break;
			default:
				NoViableAltException nvae =
					new NoViableAltException("", 2, 0, input);
				throw nvae;
			}
			switch (alt2) {
				case 1 :
					// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:218:7: st1= selectStatement
					{
					pushFollow(FOLLOW_selectStatement_in_cqlStatement113);
					st1=selectStatement();
					state._fsp--;

					 stmt = st1; 
					}
					break;
				case 2 :
					// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:219:7: st2= insertStatement
					{
					pushFollow(FOLLOW_insertStatement_in_cqlStatement138);
					st2=insertStatement();
					state._fsp--;

					 stmt = st2; 
					}
					break;
				case 3 :
					// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:220:7: st3= updateStatement
					{
					pushFollow(FOLLOW_updateStatement_in_cqlStatement163);
					st3=updateStatement();
					state._fsp--;

					 stmt = st3; 
					}
					break;
				case 4 :
					// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:221:7: st4= batchStatement
					{
					pushFollow(FOLLOW_batchStatement_in_cqlStatement188);
					st4=batchStatement();
					state._fsp--;

					 stmt = st4; 
					}
					break;
				case 5 :
					// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:222:7: st5= deleteStatement
					{
					pushFollow(FOLLOW_deleteStatement_in_cqlStatement214);
					st5=deleteStatement();
					state._fsp--;

					 stmt = st5; 
					}
					break;
				case 6 :
					// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:223:7: st6= useStatement
					{
					pushFollow(FOLLOW_useStatement_in_cqlStatement239);
					st6=useStatement();
					state._fsp--;

					 stmt = st6; 
					}
					break;
				case 7 :
					// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:224:7: st7= truncateStatement
					{
					pushFollow(FOLLOW_truncateStatement_in_cqlStatement267);
					st7=truncateStatement();
					state._fsp--;

					 stmt = st7; 
					}
					break;
				case 8 :
					// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:225:7: st8= createKeyspaceStatement
					{
					pushFollow(FOLLOW_createKeyspaceStatement_in_cqlStatement290);
					st8=createKeyspaceStatement();
					state._fsp--;

					 stmt = st8; 
					}
					break;
				case 9 :
					// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:226:7: st9= createTableStatement
					{
					pushFollow(FOLLOW_createTableStatement_in_cqlStatement307);
					st9=createTableStatement();
					state._fsp--;

					 stmt = st9; 
					}
					break;
				case 10 :
					// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:227:7: st10= createIndexStatement
					{
					pushFollow(FOLLOW_createIndexStatement_in_cqlStatement326);
					st10=createIndexStatement();
					state._fsp--;

					 stmt = st10; 
					}
					break;
				case 11 :
					// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:228:7: st11= dropKeyspaceStatement
					{
					pushFollow(FOLLOW_dropKeyspaceStatement_in_cqlStatement345);
					st11=dropKeyspaceStatement();
					state._fsp--;

					 stmt = st11; 
					}
					break;
				case 12 :
					// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:229:7: st12= dropTableStatement
					{
					pushFollow(FOLLOW_dropTableStatement_in_cqlStatement363);
					st12=dropTableStatement();
					state._fsp--;

					 stmt = st12; 
					}
					break;
				case 13 :
					// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:230:7: st13= dropIndexStatement
					{
					pushFollow(FOLLOW_dropIndexStatement_in_cqlStatement384);
					st13=dropIndexStatement();
					state._fsp--;

					 stmt = st13; 
					}
					break;
				case 14 :
					// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:231:7: st14= alterTableStatement
					{
					pushFollow(FOLLOW_alterTableStatement_in_cqlStatement405);
					st14=alterTableStatement();
					state._fsp--;

					 stmt = st14; 
					}
					break;
				case 15 :
					// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:232:7: st15= alterKeyspaceStatement
					{
					pushFollow(FOLLOW_alterKeyspaceStatement_in_cqlStatement425);
					st15=alterKeyspaceStatement();
					state._fsp--;

					 stmt = st15; 
					}
					break;
				case 16 :
					// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:233:7: st16= grantStatement
					{
					pushFollow(FOLLOW_grantStatement_in_cqlStatement442);
					st16=grantStatement();
					state._fsp--;

					 stmt = st16; 
					}
					break;
				case 17 :
					// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:234:7: st17= revokeStatement
					{
					pushFollow(FOLLOW_revokeStatement_in_cqlStatement467);
					st17=revokeStatement();
					state._fsp--;

					 stmt = st17; 
					}
					break;
				case 18 :
					// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:235:7: st18= listPermissionsStatement
					{
					pushFollow(FOLLOW_listPermissionsStatement_in_cqlStatement491);
					st18=listPermissionsStatement();
					state._fsp--;

					 stmt = st18; 
					}
					break;
				case 19 :
					// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:236:7: st19= createUserStatement
					{
					pushFollow(FOLLOW_createUserStatement_in_cqlStatement506);
					st19=createUserStatement();
					state._fsp--;

					 stmt = st19; 
					}
					break;
				case 20 :
					// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:237:7: st20= alterUserStatement
					{
					pushFollow(FOLLOW_alterUserStatement_in_cqlStatement526);
					st20=alterUserStatement();
					state._fsp--;

					 stmt = st20; 
					}
					break;
				case 21 :
					// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:238:7: st21= dropUserStatement
					{
					pushFollow(FOLLOW_dropUserStatement_in_cqlStatement547);
					st21=dropUserStatement();
					state._fsp--;

					 stmt = st21; 
					}
					break;
				case 22 :
					// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:239:7: st22= listUsersStatement
					{
					pushFollow(FOLLOW_listUsersStatement_in_cqlStatement569);
					st22=listUsersStatement();
					state._fsp--;

					 stmt = st22; 
					}
					break;
				case 23 :
					// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:240:7: st23= createTriggerStatement
					{
					pushFollow(FOLLOW_createTriggerStatement_in_cqlStatement590);
					st23=createTriggerStatement();
					state._fsp--;

					 stmt = st23; 
					}
					break;
				case 24 :
					// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:241:7: st24= dropTriggerStatement
					{
					pushFollow(FOLLOW_dropTriggerStatement_in_cqlStatement607);
					st24=dropTriggerStatement();
					state._fsp--;

					 stmt = st24; 
					}
					break;
				case 25 :
					// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:242:7: st25= createTypeStatement
					{
					pushFollow(FOLLOW_createTypeStatement_in_cqlStatement626);
					st25=createTypeStatement();
					state._fsp--;

					 stmt = st25; 
					}
					break;
				case 26 :
					// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:243:7: st26= alterTypeStatement
					{
					pushFollow(FOLLOW_alterTypeStatement_in_cqlStatement646);
					st26=alterTypeStatement();
					state._fsp--;

					 stmt = st26; 
					}
					break;
				case 27 :
					// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:244:7: st27= dropTypeStatement
					{
					pushFollow(FOLLOW_dropTypeStatement_in_cqlStatement667);
					st27=dropTypeStatement();
					state._fsp--;

					 stmt = st27; 
					}
					break;

			}
			 if (stmt != null) stmt.setBoundVariables(bindVariables); 
		}
		catch (RecognitionException re) {
			reportError(re);
			recover(input,re);
		}
		finally {
			// do for sure before leaving
		}
		return stmt;
	}
	// $ANTLR end "cqlStatement"



	// $ANTLR start "useStatement"
	// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:250:1: useStatement returns [UseStatement stmt] : K_USE ks= keyspaceName ;
	public final UseStatement useStatement() throws RecognitionException {
		UseStatement stmt = null;


		String ks =null;

		try {
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:251:5: ( K_USE ks= keyspaceName )
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:251:7: K_USE ks= keyspaceName
			{
			match(input,K_USE,FOLLOW_K_USE_in_useStatement702); 
			pushFollow(FOLLOW_keyspaceName_in_useStatement706);
			ks=keyspaceName();
			state._fsp--;

			 stmt = new UseStatement(ks); 
			}

		}
		catch (RecognitionException re) {
			reportError(re);
			recover(input,re);
		}
		finally {
			// do for sure before leaving
		}
		return stmt;
	}
	// $ANTLR end "useStatement"



	// $ANTLR start "selectStatement"
	// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:260:1: selectStatement returns [SelectStatement.RawStatement expr] : K_SELECT ( ( K_DISTINCT )? sclause= selectClause | ( K_COUNT '(' sclause= selectCountClause ')' ( K_AS c= ident )? ) ) K_FROM cf= columnFamilyName ( K_WHERE wclause= whereClause )? ( K_ORDER K_BY orderByClause[orderings] ( ',' orderByClause[orderings] )* )? ( K_LIMIT rows= intValue )? ( K_ALLOW K_FILTERING )? ;
	public final SelectStatement.RawStatement selectStatement() throws RecognitionException {
		SelectStatement.RawStatement expr = null;


		List<RawSelector> sclause =null;
		ColumnIdentifier c =null;
		CFName cf =null;
		List<Relation> wclause =null;
		Term.Raw rows =null;


		        boolean isDistinct = false;
		        boolean isCount = false;
		        ColumnIdentifier countAlias = null;
		        Term.Raw limit = null;
		        Map<ColumnIdentifier.Raw, Boolean> orderings = new LinkedHashMap<ColumnIdentifier.Raw, Boolean>();
		        boolean allowFiltering = false;
		    
		try {
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:269:5: ( K_SELECT ( ( K_DISTINCT )? sclause= selectClause | ( K_COUNT '(' sclause= selectCountClause ')' ( K_AS c= ident )? ) ) K_FROM cf= columnFamilyName ( K_WHERE wclause= whereClause )? ( K_ORDER K_BY orderByClause[orderings] ( ',' orderByClause[orderings] )* )? ( K_LIMIT rows= intValue )? ( K_ALLOW K_FILTERING )? )
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:269:7: K_SELECT ( ( K_DISTINCT )? sclause= selectClause | ( K_COUNT '(' sclause= selectCountClause ')' ( K_AS c= ident )? ) ) K_FROM cf= columnFamilyName ( K_WHERE wclause= whereClause )? ( K_ORDER K_BY orderByClause[orderings] ( ',' orderByClause[orderings] )* )? ( K_LIMIT rows= intValue )? ( K_ALLOW K_FILTERING )?
			{
			match(input,K_SELECT,FOLLOW_K_SELECT_in_selectStatement740); 
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:269:16: ( ( K_DISTINCT )? sclause= selectClause | ( K_COUNT '(' sclause= selectCountClause ')' ( K_AS c= ident )? ) )
			int alt5=2;
			int LA5_0 = input.LA(1);
			if ( (LA5_0==IDENT||LA5_0==K_ALL||LA5_0==K_AS||LA5_0==K_ASCII||(LA5_0 >= K_BIGINT && LA5_0 <= K_BOOLEAN)||LA5_0==K_CLUSTERING||(LA5_0 >= K_COMPACT && LA5_0 <= K_CONTAINS)||LA5_0==K_COUNTER||(LA5_0 >= K_CUSTOM && LA5_0 <= K_DECIMAL)||(LA5_0 >= K_DISTINCT && LA5_0 <= K_DOUBLE)||(LA5_0 >= K_EXISTS && LA5_0 <= K_FLOAT)||LA5_0==K_FROZEN||LA5_0==K_INET||LA5_0==K_INT||(LA5_0 >= K_KEY && LA5_0 <= K_KEYS)||LA5_0==K_KEYSPACES||(LA5_0 >= K_LIST && LA5_0 <= K_MAP)||LA5_0==K_NOSUPERUSER||(LA5_0 >= K_PASSWORD && LA5_0 <= K_PERMISSIONS)||(LA5_0 >= K_STATIC && LA5_0 <= K_TIMEUUID)||(LA5_0 >= K_TOKEN && LA5_0 <= K_TRIGGER)||(LA5_0 >= K_TTL && LA5_0 <= K_TYPE)||(LA5_0 >= K_USER && LA5_0 <= K_USERS)||(LA5_0 >= K_UUID && LA5_0 <= K_VARINT)||LA5_0==K_WRITETIME||LA5_0==QUOTED_NAME||LA5_0==156) ) {
				alt5=1;
			}
			else if ( (LA5_0==K_COUNT) ) {
				int LA5_2 = input.LA(2);
				if ( (LA5_2==142) ) {
					alt5=2;
				}
				else if ( (LA5_2==K_AS||LA5_2==K_FROM||LA5_2==145||LA5_2==147) ) {
					alt5=1;
				}

				else {
					int nvaeMark = input.mark();
					try {
						input.consume();
						NoViableAltException nvae =
							new NoViableAltException("", 5, 2, input);
						throw nvae;
					} finally {
						input.rewind(nvaeMark);
					}
				}

			}

			else {
				NoViableAltException nvae =
					new NoViableAltException("", 5, 0, input);
				throw nvae;
			}

			switch (alt5) {
				case 1 :
					// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:269:18: ( K_DISTINCT )? sclause= selectClause
					{
					// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:269:18: ( K_DISTINCT )?
					int alt3=2;
					int LA3_0 = input.LA(1);
					if ( (LA3_0==K_DISTINCT) ) {
						int LA3_1 = input.LA(2);
						if ( (LA3_1==IDENT||LA3_1==K_ALL||LA3_1==K_ASCII||(LA3_1 >= K_BIGINT && LA3_1 <= K_BOOLEAN)||LA3_1==K_CLUSTERING||(LA3_1 >= K_COMPACT && LA3_1 <= K_COUNTER)||(LA3_1 >= K_CUSTOM && LA3_1 <= K_DECIMAL)||(LA3_1 >= K_DISTINCT && LA3_1 <= K_DOUBLE)||(LA3_1 >= K_EXISTS && LA3_1 <= K_FLOAT)||LA3_1==K_FROZEN||LA3_1==K_INET||LA3_1==K_INT||(LA3_1 >= K_KEY && LA3_1 <= K_KEYS)||LA3_1==K_KEYSPACES||(LA3_1 >= K_LIST && LA3_1 <= K_MAP)||LA3_1==K_NOSUPERUSER||(LA3_1 >= K_PASSWORD && LA3_1 <= K_PERMISSIONS)||(LA3_1 >= K_STATIC && LA3_1 <= K_TIMEUUID)||(LA3_1 >= K_TOKEN && LA3_1 <= K_TRIGGER)||(LA3_1 >= K_TTL && LA3_1 <= K_TYPE)||(LA3_1 >= K_USER && LA3_1 <= K_USERS)||(LA3_1 >= K_UUID && LA3_1 <= K_VARINT)||LA3_1==K_WRITETIME||LA3_1==QUOTED_NAME||LA3_1==156) ) {
							alt3=1;
						}
						else if ( (LA3_1==K_AS) ) {
							int LA3_4 = input.LA(3);
							if ( (LA3_4==K_FROM||LA3_4==142||LA3_4==145||LA3_4==147) ) {
								alt3=1;
							}
							else if ( (LA3_4==K_AS) ) {
								int LA3_5 = input.LA(4);
								if ( (LA3_5==IDENT||LA3_5==K_ALL||LA3_5==K_AS||LA3_5==K_ASCII||(LA3_5 >= K_BIGINT && LA3_5 <= K_BOOLEAN)||LA3_5==K_CLUSTERING||(LA3_5 >= K_COMPACT && LA3_5 <= K_COUNTER)||(LA3_5 >= K_CUSTOM && LA3_5 <= K_DECIMAL)||(LA3_5 >= K_DISTINCT && LA3_5 <= K_DOUBLE)||(LA3_5 >= K_EXISTS && LA3_5 <= K_FLOAT)||LA3_5==K_FROZEN||LA3_5==K_INET||LA3_5==K_INT||(LA3_5 >= K_KEY && LA3_5 <= K_KEYS)||LA3_5==K_KEYSPACES||(LA3_5 >= K_LIST && LA3_5 <= K_MAP)||LA3_5==K_NOSUPERUSER||(LA3_5 >= K_PASSWORD && LA3_5 <= K_PERMISSIONS)||(LA3_5 >= K_STATIC && LA3_5 <= K_TIMEUUID)||LA3_5==K_TRIGGER||(LA3_5 >= K_TTL && LA3_5 <= K_TYPE)||(LA3_5 >= K_USER && LA3_5 <= K_USERS)||(LA3_5 >= K_UUID && LA3_5 <= K_VARINT)||LA3_5==K_WRITETIME||LA3_5==QUOTED_NAME) ) {
									alt3=1;
								}
							}
						}
					}
					switch (alt3) {
						case 1 :
							// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:269:20: K_DISTINCT
							{
							match(input,K_DISTINCT,FOLLOW_K_DISTINCT_in_selectStatement746); 
							 isDistinct = true; 
							}
							break;

					}

					pushFollow(FOLLOW_selectClause_in_selectStatement755);
					sclause=selectClause();
					state._fsp--;

					}
					break;
				case 2 :
					// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:270:18: ( K_COUNT '(' sclause= selectCountClause ')' ( K_AS c= ident )? )
					{
					// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:270:18: ( K_COUNT '(' sclause= selectCountClause ')' ( K_AS c= ident )? )
					// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:270:19: K_COUNT '(' sclause= selectCountClause ')' ( K_AS c= ident )?
					{
					match(input,K_COUNT,FOLLOW_K_COUNT_in_selectStatement775); 
					match(input,142,FOLLOW_142_in_selectStatement777); 
					pushFollow(FOLLOW_selectCountClause_in_selectStatement781);
					sclause=selectCountClause();
					state._fsp--;

					match(input,143,FOLLOW_143_in_selectStatement783); 
					 isCount = true; 
					// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:270:81: ( K_AS c= ident )?
					int alt4=2;
					int LA4_0 = input.LA(1);
					if ( (LA4_0==K_AS) ) {
						alt4=1;
					}
					switch (alt4) {
						case 1 :
							// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:270:82: K_AS c= ident
							{
							match(input,K_AS,FOLLOW_K_AS_in_selectStatement788); 
							pushFollow(FOLLOW_ident_in_selectStatement792);
							c=ident();
							state._fsp--;

							 countAlias = c; 
							}
							break;

					}

					}

					}
					break;

			}

			match(input,K_FROM,FOLLOW_K_FROM_in_selectStatement807); 
			pushFollow(FOLLOW_columnFamilyName_in_selectStatement811);
			cf=columnFamilyName();
			state._fsp--;

			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:272:7: ( K_WHERE wclause= whereClause )?
			int alt6=2;
			int LA6_0 = input.LA(1);
			if ( (LA6_0==K_WHERE) ) {
				alt6=1;
			}
			switch (alt6) {
				case 1 :
					// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:272:9: K_WHERE wclause= whereClause
					{
					match(input,K_WHERE,FOLLOW_K_WHERE_in_selectStatement821); 
					pushFollow(FOLLOW_whereClause_in_selectStatement825);
					wclause=whereClause();
					state._fsp--;

					}
					break;

			}

			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:273:7: ( K_ORDER K_BY orderByClause[orderings] ( ',' orderByClause[orderings] )* )?
			int alt8=2;
			int LA8_0 = input.LA(1);
			if ( (LA8_0==K_ORDER) ) {
				alt8=1;
			}
			switch (alt8) {
				case 1 :
					// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:273:9: K_ORDER K_BY orderByClause[orderings] ( ',' orderByClause[orderings] )*
					{
					match(input,K_ORDER,FOLLOW_K_ORDER_in_selectStatement838); 
					match(input,K_BY,FOLLOW_K_BY_in_selectStatement840); 
					pushFollow(FOLLOW_orderByClause_in_selectStatement842);
					orderByClause(orderings);
					state._fsp--;

					// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:273:47: ( ',' orderByClause[orderings] )*
					loop7:
					while (true) {
						int alt7=2;
						int LA7_0 = input.LA(1);
						if ( (LA7_0==145) ) {
							alt7=1;
						}

						switch (alt7) {
						case 1 :
							// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:273:49: ',' orderByClause[orderings]
							{
							match(input,145,FOLLOW_145_in_selectStatement847); 
							pushFollow(FOLLOW_orderByClause_in_selectStatement849);
							orderByClause(orderings);
							state._fsp--;

							}
							break;

						default :
							break loop7;
						}
					}

					}
					break;

			}

			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:274:7: ( K_LIMIT rows= intValue )?
			int alt9=2;
			int LA9_0 = input.LA(1);
			if ( (LA9_0==K_LIMIT) ) {
				alt9=1;
			}
			switch (alt9) {
				case 1 :
					// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:274:9: K_LIMIT rows= intValue
					{
					match(input,K_LIMIT,FOLLOW_K_LIMIT_in_selectStatement866); 
					pushFollow(FOLLOW_intValue_in_selectStatement870);
					rows=intValue();
					state._fsp--;

					 limit = rows; 
					}
					break;

			}

			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:275:7: ( K_ALLOW K_FILTERING )?
			int alt10=2;
			int LA10_0 = input.LA(1);
			if ( (LA10_0==K_ALLOW) ) {
				alt10=1;
			}
			switch (alt10) {
				case 1 :
					// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:275:9: K_ALLOW K_FILTERING
					{
					match(input,K_ALLOW,FOLLOW_K_ALLOW_in_selectStatement885); 
					match(input,K_FILTERING,FOLLOW_K_FILTERING_in_selectStatement887); 
					 allowFiltering = true; 
					}
					break;

			}


			          SelectStatement.Parameters params = new SelectStatement.Parameters(orderings,
			                                                                             isDistinct,
			                                                                             isCount,
			                                                                             countAlias,
			                                                                             allowFiltering);
			          expr = new SelectStatement.RawStatement(cf, params, sclause, wclause, limit);
			      
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



	// $ANTLR start "selectClause"
	// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:286:1: selectClause returns [List<RawSelector> expr] : (t1= selector ( ',' tN= selector )* | '\\*' );
	public final List<RawSelector> selectClause() throws RecognitionException {
		List<RawSelector> expr = null;


		RawSelector t1 =null;
		RawSelector tN =null;

		try {
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:287:5: (t1= selector ( ',' tN= selector )* | '\\*' )
			int alt12=2;
			int LA12_0 = input.LA(1);
			if ( (LA12_0==IDENT||LA12_0==K_ALL||LA12_0==K_AS||LA12_0==K_ASCII||(LA12_0 >= K_BIGINT && LA12_0 <= K_BOOLEAN)||LA12_0==K_CLUSTERING||(LA12_0 >= K_COMPACT && LA12_0 <= K_COUNTER)||(LA12_0 >= K_CUSTOM && LA12_0 <= K_DECIMAL)||(LA12_0 >= K_DISTINCT && LA12_0 <= K_DOUBLE)||(LA12_0 >= K_EXISTS && LA12_0 <= K_FLOAT)||LA12_0==K_FROZEN||LA12_0==K_INET||LA12_0==K_INT||(LA12_0 >= K_KEY && LA12_0 <= K_KEYS)||LA12_0==K_KEYSPACES||(LA12_0 >= K_LIST && LA12_0 <= K_MAP)||LA12_0==K_NOSUPERUSER||(LA12_0 >= K_PASSWORD && LA12_0 <= K_PERMISSIONS)||(LA12_0 >= K_STATIC && LA12_0 <= K_TIMEUUID)||(LA12_0 >= K_TOKEN && LA12_0 <= K_TRIGGER)||(LA12_0 >= K_TTL && LA12_0 <= K_TYPE)||(LA12_0 >= K_USER && LA12_0 <= K_USERS)||(LA12_0 >= K_UUID && LA12_0 <= K_VARINT)||LA12_0==K_WRITETIME||LA12_0==QUOTED_NAME) ) {
				alt12=1;
			}
			else if ( (LA12_0==156) ) {
				alt12=2;
			}

			else {
				NoViableAltException nvae =
					new NoViableAltException("", 12, 0, input);
				throw nvae;
			}

			switch (alt12) {
				case 1 :
					// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:287:7: t1= selector ( ',' tN= selector )*
					{
					pushFollow(FOLLOW_selector_in_selectClause924);
					t1=selector();
					state._fsp--;

					 expr = new ArrayList<RawSelector>(); expr.add(t1); 
					// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:287:76: ( ',' tN= selector )*
					loop11:
					while (true) {
						int alt11=2;
						int LA11_0 = input.LA(1);
						if ( (LA11_0==145) ) {
							alt11=1;
						}

						switch (alt11) {
						case 1 :
							// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:287:77: ',' tN= selector
							{
							match(input,145,FOLLOW_145_in_selectClause929); 
							pushFollow(FOLLOW_selector_in_selectClause933);
							tN=selector();
							state._fsp--;

							 expr.add(tN); 
							}
							break;

						default :
							break loop11;
						}
					}

					}
					break;
				case 2 :
					// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:288:7: '\\*'
					{
					match(input,156,FOLLOW_156_in_selectClause945); 
					 expr = Collections.<RawSelector>emptyList();
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
		return expr;
	}
	// $ANTLR end "selectClause"



	// $ANTLR start "selector"
	// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:291:1: selector returns [RawSelector s] : us= unaliasedSelector ( K_AS c= ident )? ;
	public final RawSelector selector() throws RecognitionException {
		RawSelector s = null;


		Selectable.Raw us =null;
		ColumnIdentifier c =null;

		 ColumnIdentifier alias = null; 
		try {
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:293:5: (us= unaliasedSelector ( K_AS c= ident )? )
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:293:7: us= unaliasedSelector ( K_AS c= ident )?
			{
			pushFollow(FOLLOW_unaliasedSelector_in_selector978);
			us=unaliasedSelector();
			state._fsp--;

			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:293:28: ( K_AS c= ident )?
			int alt13=2;
			int LA13_0 = input.LA(1);
			if ( (LA13_0==K_AS) ) {
				alt13=1;
			}
			switch (alt13) {
				case 1 :
					// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:293:29: K_AS c= ident
					{
					match(input,K_AS,FOLLOW_K_AS_in_selector981); 
					pushFollow(FOLLOW_ident_in_selector985);
					c=ident();
					state._fsp--;

					 alias = c; 
					}
					break;

			}

			 s = new RawSelector(us, alias); 
			}

		}
		catch (RecognitionException re) {
			reportError(re);
			recover(input,re);
		}
		finally {
			// do for sure before leaving
		}
		return s;
	}
	// $ANTLR end "selector"



	// $ANTLR start "unaliasedSelector"
	// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:296:1: unaliasedSelector returns [Selectable.Raw s] : (c= cident | K_WRITETIME '(' c= cident ')' | K_TTL '(' c= cident ')' |f= functionName args= selectionFunctionArgs ) ( '.' fi= cident )* ;
	public final Selectable.Raw unaliasedSelector() throws RecognitionException {
		Selectable.Raw s = null;


		ColumnIdentifier.Raw c =null;
		String f =null;
		List<Selectable.Raw> args =null;
		ColumnIdentifier.Raw fi =null;

		 Selectable.Raw tmp = null; 
		try {
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:298:5: ( (c= cident | K_WRITETIME '(' c= cident ')' | K_TTL '(' c= cident ')' |f= functionName args= selectionFunctionArgs ) ( '.' fi= cident )* )
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:298:8: (c= cident | K_WRITETIME '(' c= cident ')' | K_TTL '(' c= cident ')' |f= functionName args= selectionFunctionArgs ) ( '.' fi= cident )*
			{
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:298:8: (c= cident | K_WRITETIME '(' c= cident ')' | K_TTL '(' c= cident ')' |f= functionName args= selectionFunctionArgs )
			int alt14=4;
			switch ( input.LA(1) ) {
			case IDENT:
				{
				int LA14_1 = input.LA(2);
				if ( (LA14_1==K_AS||LA14_1==K_FROM||LA14_1==143||LA14_1==145||LA14_1==147) ) {
					alt14=1;
				}
				else if ( (LA14_1==142) ) {
					alt14=4;
				}

				else {
					int nvaeMark = input.mark();
					try {
						input.consume();
						NoViableAltException nvae =
							new NoViableAltException("", 14, 1, input);
						throw nvae;
					} finally {
						input.rewind(nvaeMark);
					}
				}

				}
				break;
			case K_COUNT:
			case K_KEY:
			case QUOTED_NAME:
				{
				alt14=1;
				}
				break;
			case K_ALL:
			case K_AS:
			case K_CLUSTERING:
			case K_COMPACT:
			case K_CONTAINS:
			case K_CUSTOM:
			case K_DISTINCT:
			case K_EXISTS:
			case K_FILTERING:
			case K_FROZEN:
			case K_KEYS:
			case K_KEYSPACES:
			case K_LIST:
			case K_MAP:
			case K_NOSUPERUSER:
			case K_PASSWORD:
			case K_PERMISSION:
			case K_PERMISSIONS:
			case K_STATIC:
			case K_STORAGE:
			case K_SUPERUSER:
			case K_TRIGGER:
			case K_TUPLE:
			case K_TYPE:
			case K_USER:
			case K_USERS:
			case K_VALUES:
				{
				int LA14_3 = input.LA(2);
				if ( (LA14_3==K_AS||LA14_3==K_FROM||LA14_3==143||LA14_3==145||LA14_3==147) ) {
					alt14=1;
				}
				else if ( (LA14_3==142) ) {
					alt14=4;
				}

				else {
					int nvaeMark = input.mark();
					try {
						input.consume();
						NoViableAltException nvae =
							new NoViableAltException("", 14, 3, input);
						throw nvae;
					} finally {
						input.rewind(nvaeMark);
					}
				}

				}
				break;
			case K_ASCII:
				{
				int LA14_4 = input.LA(2);
				if ( (LA14_4==K_AS||LA14_4==K_FROM||LA14_4==143||LA14_4==145||LA14_4==147) ) {
					alt14=1;
				}
				else if ( (LA14_4==142) ) {
					alt14=4;
				}

				else {
					int nvaeMark = input.mark();
					try {
						input.consume();
						NoViableAltException nvae =
							new NoViableAltException("", 14, 4, input);
						throw nvae;
					} finally {
						input.rewind(nvaeMark);
					}
				}

				}
				break;
			case K_BIGINT:
				{
				int LA14_5 = input.LA(2);
				if ( (LA14_5==K_AS||LA14_5==K_FROM||LA14_5==143||LA14_5==145||LA14_5==147) ) {
					alt14=1;
				}
				else if ( (LA14_5==142) ) {
					alt14=4;
				}

				else {
					int nvaeMark = input.mark();
					try {
						input.consume();
						NoViableAltException nvae =
							new NoViableAltException("", 14, 5, input);
						throw nvae;
					} finally {
						input.rewind(nvaeMark);
					}
				}

				}
				break;
			case K_BLOB:
				{
				int LA14_6 = input.LA(2);
				if ( (LA14_6==K_AS||LA14_6==K_FROM||LA14_6==143||LA14_6==145||LA14_6==147) ) {
					alt14=1;
				}
				else if ( (LA14_6==142) ) {
					alt14=4;
				}

				else {
					int nvaeMark = input.mark();
					try {
						input.consume();
						NoViableAltException nvae =
							new NoViableAltException("", 14, 6, input);
						throw nvae;
					} finally {
						input.rewind(nvaeMark);
					}
				}

				}
				break;
			case K_BOOLEAN:
				{
				int LA14_7 = input.LA(2);
				if ( (LA14_7==K_AS||LA14_7==K_FROM||LA14_7==143||LA14_7==145||LA14_7==147) ) {
					alt14=1;
				}
				else if ( (LA14_7==142) ) {
					alt14=4;
				}

				else {
					int nvaeMark = input.mark();
					try {
						input.consume();
						NoViableAltException nvae =
							new NoViableAltException("", 14, 7, input);
						throw nvae;
					} finally {
						input.rewind(nvaeMark);
					}
				}

				}
				break;
			case K_COUNTER:
				{
				int LA14_8 = input.LA(2);
				if ( (LA14_8==K_AS||LA14_8==K_FROM||LA14_8==143||LA14_8==145||LA14_8==147) ) {
					alt14=1;
				}
				else if ( (LA14_8==142) ) {
					alt14=4;
				}

				else {
					int nvaeMark = input.mark();
					try {
						input.consume();
						NoViableAltException nvae =
							new NoViableAltException("", 14, 8, input);
						throw nvae;
					} finally {
						input.rewind(nvaeMark);
					}
				}

				}
				break;
			case K_DECIMAL:
				{
				int LA14_9 = input.LA(2);
				if ( (LA14_9==K_AS||LA14_9==K_FROM||LA14_9==143||LA14_9==145||LA14_9==147) ) {
					alt14=1;
				}
				else if ( (LA14_9==142) ) {
					alt14=4;
				}

				else {
					int nvaeMark = input.mark();
					try {
						input.consume();
						NoViableAltException nvae =
							new NoViableAltException("", 14, 9, input);
						throw nvae;
					} finally {
						input.rewind(nvaeMark);
					}
				}

				}
				break;
			case K_DOUBLE:
				{
				int LA14_10 = input.LA(2);
				if ( (LA14_10==K_AS||LA14_10==K_FROM||LA14_10==143||LA14_10==145||LA14_10==147) ) {
					alt14=1;
				}
				else if ( (LA14_10==142) ) {
					alt14=4;
				}

				else {
					int nvaeMark = input.mark();
					try {
						input.consume();
						NoViableAltException nvae =
							new NoViableAltException("", 14, 10, input);
						throw nvae;
					} finally {
						input.rewind(nvaeMark);
					}
				}

				}
				break;
			case K_FLOAT:
				{
				int LA14_11 = input.LA(2);
				if ( (LA14_11==K_AS||LA14_11==K_FROM||LA14_11==143||LA14_11==145||LA14_11==147) ) {
					alt14=1;
				}
				else if ( (LA14_11==142) ) {
					alt14=4;
				}

				else {
					int nvaeMark = input.mark();
					try {
						input.consume();
						NoViableAltException nvae =
							new NoViableAltException("", 14, 11, input);
						throw nvae;
					} finally {
						input.rewind(nvaeMark);
					}
				}

				}
				break;
			case K_INET:
				{
				int LA14_12 = input.LA(2);
				if ( (LA14_12==K_AS||LA14_12==K_FROM||LA14_12==143||LA14_12==145||LA14_12==147) ) {
					alt14=1;
				}
				else if ( (LA14_12==142) ) {
					alt14=4;
				}

				else {
					int nvaeMark = input.mark();
					try {
						input.consume();
						NoViableAltException nvae =
							new NoViableAltException("", 14, 12, input);
						throw nvae;
					} finally {
						input.rewind(nvaeMark);
					}
				}

				}
				break;
			case K_INT:
				{
				int LA14_13 = input.LA(2);
				if ( (LA14_13==K_AS||LA14_13==K_FROM||LA14_13==143||LA14_13==145||LA14_13==147) ) {
					alt14=1;
				}
				else if ( (LA14_13==142) ) {
					alt14=4;
				}

				else {
					int nvaeMark = input.mark();
					try {
						input.consume();
						NoViableAltException nvae =
							new NoViableAltException("", 14, 13, input);
						throw nvae;
					} finally {
						input.rewind(nvaeMark);
					}
				}

				}
				break;
			case K_TEXT:
				{
				int LA14_14 = input.LA(2);
				if ( (LA14_14==K_AS||LA14_14==K_FROM||LA14_14==143||LA14_14==145||LA14_14==147) ) {
					alt14=1;
				}
				else if ( (LA14_14==142) ) {
					alt14=4;
				}

				else {
					int nvaeMark = input.mark();
					try {
						input.consume();
						NoViableAltException nvae =
							new NoViableAltException("", 14, 14, input);
						throw nvae;
					} finally {
						input.rewind(nvaeMark);
					}
				}

				}
				break;
			case K_TIMESTAMP:
				{
				int LA14_15 = input.LA(2);
				if ( (LA14_15==K_AS||LA14_15==K_FROM||LA14_15==143||LA14_15==145||LA14_15==147) ) {
					alt14=1;
				}
				else if ( (LA14_15==142) ) {
					alt14=4;
				}

				else {
					int nvaeMark = input.mark();
					try {
						input.consume();
						NoViableAltException nvae =
							new NoViableAltException("", 14, 15, input);
						throw nvae;
					} finally {
						input.rewind(nvaeMark);
					}
				}

				}
				break;
			case K_UUID:
				{
				int LA14_16 = input.LA(2);
				if ( (LA14_16==K_AS||LA14_16==K_FROM||LA14_16==143||LA14_16==145||LA14_16==147) ) {
					alt14=1;
				}
				else if ( (LA14_16==142) ) {
					alt14=4;
				}

				else {
					int nvaeMark = input.mark();
					try {
						input.consume();
						NoViableAltException nvae =
							new NoViableAltException("", 14, 16, input);
						throw nvae;
					} finally {
						input.rewind(nvaeMark);
					}
				}

				}
				break;
			case K_VARCHAR:
				{
				int LA14_17 = input.LA(2);
				if ( (LA14_17==K_AS||LA14_17==K_FROM||LA14_17==143||LA14_17==145||LA14_17==147) ) {
					alt14=1;
				}
				else if ( (LA14_17==142) ) {
					alt14=4;
				}

				else {
					int nvaeMark = input.mark();
					try {
						input.consume();
						NoViableAltException nvae =
							new NoViableAltException("", 14, 17, input);
						throw nvae;
					} finally {
						input.rewind(nvaeMark);
					}
				}

				}
				break;
			case K_VARINT:
				{
				int LA14_18 = input.LA(2);
				if ( (LA14_18==K_AS||LA14_18==K_FROM||LA14_18==143||LA14_18==145||LA14_18==147) ) {
					alt14=1;
				}
				else if ( (LA14_18==142) ) {
					alt14=4;
				}

				else {
					int nvaeMark = input.mark();
					try {
						input.consume();
						NoViableAltException nvae =
							new NoViableAltException("", 14, 18, input);
						throw nvae;
					} finally {
						input.rewind(nvaeMark);
					}
				}

				}
				break;
			case K_TIMEUUID:
				{
				int LA14_19 = input.LA(2);
				if ( (LA14_19==K_AS||LA14_19==K_FROM||LA14_19==143||LA14_19==145||LA14_19==147) ) {
					alt14=1;
				}
				else if ( (LA14_19==142) ) {
					alt14=4;
				}

				else {
					int nvaeMark = input.mark();
					try {
						input.consume();
						NoViableAltException nvae =
							new NoViableAltException("", 14, 19, input);
						throw nvae;
					} finally {
						input.rewind(nvaeMark);
					}
				}

				}
				break;
			case K_WRITETIME:
				{
				int LA14_20 = input.LA(2);
				if ( (LA14_20==142) ) {
					alt14=2;
				}
				else if ( (LA14_20==K_AS||LA14_20==K_FROM||LA14_20==143||LA14_20==145||LA14_20==147) ) {
					alt14=1;
				}

				else {
					int nvaeMark = input.mark();
					try {
						input.consume();
						NoViableAltException nvae =
							new NoViableAltException("", 14, 20, input);
						throw nvae;
					} finally {
						input.rewind(nvaeMark);
					}
				}

				}
				break;
			case K_TTL:
				{
				int LA14_21 = input.LA(2);
				if ( (LA14_21==142) ) {
					alt14=3;
				}
				else if ( (LA14_21==K_AS||LA14_21==K_FROM||LA14_21==143||LA14_21==145||LA14_21==147) ) {
					alt14=1;
				}

				else {
					int nvaeMark = input.mark();
					try {
						input.consume();
						NoViableAltException nvae =
							new NoViableAltException("", 14, 21, input);
						throw nvae;
					} finally {
						input.rewind(nvaeMark);
					}
				}

				}
				break;
			case K_TOKEN:
				{
				alt14=4;
				}
				break;
			default:
				NoViableAltException nvae =
					new NoViableAltException("", 14, 0, input);
				throw nvae;
			}
			switch (alt14) {
				case 1 :
					// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:298:10: c= cident
					{
					pushFollow(FOLLOW_cident_in_unaliasedSelector1026);
					c=cident();
					state._fsp--;

					 tmp = c; 
					}
					break;
				case 2 :
					// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:299:10: K_WRITETIME '(' c= cident ')'
					{
					match(input,K_WRITETIME,FOLLOW_K_WRITETIME_in_unaliasedSelector1072); 
					match(input,142,FOLLOW_142_in_unaliasedSelector1074); 
					pushFollow(FOLLOW_cident_in_unaliasedSelector1078);
					c=cident();
					state._fsp--;

					match(input,143,FOLLOW_143_in_unaliasedSelector1080); 
					 tmp = new Selectable.WritetimeOrTTL.Raw(c, true); 
					}
					break;
				case 3 :
					// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:300:10: K_TTL '(' c= cident ')'
					{
					match(input,K_TTL,FOLLOW_K_TTL_in_unaliasedSelector1106); 
					match(input,142,FOLLOW_142_in_unaliasedSelector1114); 
					pushFollow(FOLLOW_cident_in_unaliasedSelector1118);
					c=cident();
					state._fsp--;

					match(input,143,FOLLOW_143_in_unaliasedSelector1120); 
					 tmp = new Selectable.WritetimeOrTTL.Raw(c, false); 
					}
					break;
				case 4 :
					// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:301:10: f= functionName args= selectionFunctionArgs
					{
					pushFollow(FOLLOW_functionName_in_unaliasedSelector1148);
					f=functionName();
					state._fsp--;

					pushFollow(FOLLOW_selectionFunctionArgs_in_unaliasedSelector1152);
					args=selectionFunctionArgs();
					state._fsp--;

					 tmp = new Selectable.WithFunction.Raw(f, args); 
					}
					break;

			}

			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:302:10: ( '.' fi= cident )*
			loop15:
			while (true) {
				int alt15=2;
				int LA15_0 = input.LA(1);
				if ( (LA15_0==147) ) {
					alt15=1;
				}

				switch (alt15) {
				case 1 :
					// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:302:12: '.' fi= cident
					{
					match(input,147,FOLLOW_147_in_unaliasedSelector1167); 
					pushFollow(FOLLOW_cident_in_unaliasedSelector1171);
					fi=cident();
					state._fsp--;

					 tmp = new Selectable.WithFieldSelection.Raw(tmp, fi); 
					}
					break;

				default :
					break loop15;
				}
			}

			 s = tmp; 
			}

		}
		catch (RecognitionException re) {
			reportError(re);
			recover(input,re);
		}
		finally {
			// do for sure before leaving
		}
		return s;
	}
	// $ANTLR end "unaliasedSelector"



	// $ANTLR start "selectionFunctionArgs"
	// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:305:1: selectionFunctionArgs returns [List<Selectable.Raw> a] : ( '(' ')' | '(' s1= unaliasedSelector ( ',' sn= unaliasedSelector )* ')' );
	public final List<Selectable.Raw> selectionFunctionArgs() throws RecognitionException {
		List<Selectable.Raw> a = null;


		Selectable.Raw s1 =null;
		Selectable.Raw sn =null;

		try {
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:306:5: ( '(' ')' | '(' s1= unaliasedSelector ( ',' sn= unaliasedSelector )* ')' )
			int alt17=2;
			int LA17_0 = input.LA(1);
			if ( (LA17_0==142) ) {
				int LA17_1 = input.LA(2);
				if ( (LA17_1==143) ) {
					alt17=1;
				}
				else if ( (LA17_1==IDENT||LA17_1==K_ALL||LA17_1==K_AS||LA17_1==K_ASCII||(LA17_1 >= K_BIGINT && LA17_1 <= K_BOOLEAN)||LA17_1==K_CLUSTERING||(LA17_1 >= K_COMPACT && LA17_1 <= K_COUNTER)||(LA17_1 >= K_CUSTOM && LA17_1 <= K_DECIMAL)||(LA17_1 >= K_DISTINCT && LA17_1 <= K_DOUBLE)||(LA17_1 >= K_EXISTS && LA17_1 <= K_FLOAT)||LA17_1==K_FROZEN||LA17_1==K_INET||LA17_1==K_INT||(LA17_1 >= K_KEY && LA17_1 <= K_KEYS)||LA17_1==K_KEYSPACES||(LA17_1 >= K_LIST && LA17_1 <= K_MAP)||LA17_1==K_NOSUPERUSER||(LA17_1 >= K_PASSWORD && LA17_1 <= K_PERMISSIONS)||(LA17_1 >= K_STATIC && LA17_1 <= K_TIMEUUID)||(LA17_1 >= K_TOKEN && LA17_1 <= K_TRIGGER)||(LA17_1 >= K_TTL && LA17_1 <= K_TYPE)||(LA17_1 >= K_USER && LA17_1 <= K_USERS)||(LA17_1 >= K_UUID && LA17_1 <= K_VARINT)||LA17_1==K_WRITETIME||LA17_1==QUOTED_NAME) ) {
					alt17=2;
				}

				else {
					int nvaeMark = input.mark();
					try {
						input.consume();
						NoViableAltException nvae =
							new NoViableAltException("", 17, 1, input);
						throw nvae;
					} finally {
						input.rewind(nvaeMark);
					}
				}

			}

			else {
				NoViableAltException nvae =
					new NoViableAltException("", 17, 0, input);
				throw nvae;
			}

			switch (alt17) {
				case 1 :
					// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:306:7: '(' ')'
					{
					match(input,142,FOLLOW_142_in_selectionFunctionArgs1199); 
					match(input,143,FOLLOW_143_in_selectionFunctionArgs1201); 
					 a = Collections.emptyList(); 
					}
					break;
				case 2 :
					// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:307:7: '(' s1= unaliasedSelector ( ',' sn= unaliasedSelector )* ')'
					{
					match(input,142,FOLLOW_142_in_selectionFunctionArgs1211); 
					pushFollow(FOLLOW_unaliasedSelector_in_selectionFunctionArgs1215);
					s1=unaliasedSelector();
					state._fsp--;

					 List<Selectable.Raw> args = new ArrayList<Selectable.Raw>(); args.add(s1); 
					// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:308:11: ( ',' sn= unaliasedSelector )*
					loop16:
					while (true) {
						int alt16=2;
						int LA16_0 = input.LA(1);
						if ( (LA16_0==145) ) {
							alt16=1;
						}

						switch (alt16) {
						case 1 :
							// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:308:13: ',' sn= unaliasedSelector
							{
							match(input,145,FOLLOW_145_in_selectionFunctionArgs1231); 
							pushFollow(FOLLOW_unaliasedSelector_in_selectionFunctionArgs1235);
							sn=unaliasedSelector();
							state._fsp--;

							 args.add(sn); 
							}
							break;

						default :
							break loop16;
						}
					}

					match(input,143,FOLLOW_143_in_selectionFunctionArgs1248); 
					 a = args; 
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
		return a;
	}
	// $ANTLR end "selectionFunctionArgs"



	// $ANTLR start "selectCountClause"
	// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:312:1: selectCountClause returns [List<RawSelector> expr] : ( '\\*' |i= INTEGER );
	public final List<RawSelector> selectCountClause() throws RecognitionException {
		List<RawSelector> expr = null;


		Token i=null;

		try {
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:313:5: ( '\\*' |i= INTEGER )
			int alt18=2;
			int LA18_0 = input.LA(1);
			if ( (LA18_0==156) ) {
				alt18=1;
			}
			else if ( (LA18_0==INTEGER) ) {
				alt18=2;
			}

			else {
				NoViableAltException nvae =
					new NoViableAltException("", 18, 0, input);
				throw nvae;
			}

			switch (alt18) {
				case 1 :
					// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:313:7: '\\*'
					{
					match(input,156,FOLLOW_156_in_selectCountClause1271); 
					 expr = Collections.<RawSelector>emptyList();
					}
					break;
				case 2 :
					// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:314:7: i= INTEGER
					{
					i=(Token)match(input,INTEGER,FOLLOW_INTEGER_in_selectCountClause1293); 
					 if (!i.getText().equals("1")) addRecognitionError("Only COUNT(1) is supported, got COUNT(" + i.getText() + ")"); expr = Collections.<RawSelector>emptyList();
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
		return expr;
	}
	// $ANTLR end "selectCountClause"



	// $ANTLR start "whereClause"
	// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:317:1: whereClause returns [List<Relation> clause] : relation[$clause] ( K_AND relation[$clause] )* ;
	public final List<Relation> whereClause() throws RecognitionException {
		List<Relation> clause = null;


		 clause = new ArrayList<Relation>(); 
		try {
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:319:5: ( relation[$clause] ( K_AND relation[$clause] )* )
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:319:7: relation[$clause] ( K_AND relation[$clause] )*
			{
			pushFollow(FOLLOW_relation_in_whereClause1329);
			relation(clause);
			state._fsp--;

			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:319:25: ( K_AND relation[$clause] )*
			loop19:
			while (true) {
				int alt19=2;
				int LA19_0 = input.LA(1);
				if ( (LA19_0==K_AND) ) {
					alt19=1;
				}

				switch (alt19) {
				case 1 :
					// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:319:26: K_AND relation[$clause]
					{
					match(input,K_AND,FOLLOW_K_AND_in_whereClause1333); 
					pushFollow(FOLLOW_relation_in_whereClause1335);
					relation(clause);
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
		return clause;
	}
	// $ANTLR end "whereClause"



	// $ANTLR start "orderByClause"
	// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:322:1: orderByClause[Map<ColumnIdentifier.Raw, Boolean> orderings] : c= cident ( K_ASC | K_DESC )? ;
	public final void orderByClause(Map<ColumnIdentifier.Raw, Boolean> orderings) throws RecognitionException {
		ColumnIdentifier.Raw c =null;


		        ColumnIdentifier.Raw orderBy = null;
		        boolean reversed = false;
		    
		try {
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:327:5: (c= cident ( K_ASC | K_DESC )? )
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:327:7: c= cident ( K_ASC | K_DESC )?
			{
			pushFollow(FOLLOW_cident_in_orderByClause1366);
			c=cident();
			state._fsp--;

			 orderBy = c; 
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:327:33: ( K_ASC | K_DESC )?
			int alt20=3;
			int LA20_0 = input.LA(1);
			if ( (LA20_0==K_ASC) ) {
				alt20=1;
			}
			else if ( (LA20_0==K_DESC) ) {
				alt20=2;
			}
			switch (alt20) {
				case 1 :
					// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:327:34: K_ASC
					{
					match(input,K_ASC,FOLLOW_K_ASC_in_orderByClause1371); 
					}
					break;
				case 2 :
					// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:327:42: K_DESC
					{
					match(input,K_DESC,FOLLOW_K_DESC_in_orderByClause1375); 
					 reversed = true; 
					}
					break;

			}

			 orderings.put(c, reversed); 
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
	// $ANTLR end "orderByClause"



	// $ANTLR start "insertStatement"
	// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:336:1: insertStatement returns [UpdateStatement.ParsedInsert expr] : K_INSERT K_INTO cf= columnFamilyName '(' c1= cident ( ',' cn= cident )* ')' K_VALUES '(' v1= term ( ',' vn= term )* ')' ( K_IF K_NOT K_EXISTS )? ( usingClause[attrs] )? ;
	public final UpdateStatement.ParsedInsert insertStatement() throws RecognitionException {
		UpdateStatement.ParsedInsert expr = null;


		CFName cf =null;
		ColumnIdentifier.Raw c1 =null;
		ColumnIdentifier.Raw cn =null;
		Term.Raw v1 =null;
		Term.Raw vn =null;


		        Attributes.Raw attrs = new Attributes.Raw();
		        List<ColumnIdentifier.Raw> columnNames  = new ArrayList<ColumnIdentifier.Raw>();
		        List<Term.Raw> values = new ArrayList<Term.Raw>();
		        boolean ifNotExists = false;
		    
		try {
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:343:5: ( K_INSERT K_INTO cf= columnFamilyName '(' c1= cident ( ',' cn= cident )* ')' K_VALUES '(' v1= term ( ',' vn= term )* ')' ( K_IF K_NOT K_EXISTS )? ( usingClause[attrs] )? )
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:343:7: K_INSERT K_INTO cf= columnFamilyName '(' c1= cident ( ',' cn= cident )* ')' K_VALUES '(' v1= term ( ',' vn= term )* ')' ( K_IF K_NOT K_EXISTS )? ( usingClause[attrs] )?
			{
			match(input,K_INSERT,FOLLOW_K_INSERT_in_insertStatement1413); 
			match(input,K_INTO,FOLLOW_K_INTO_in_insertStatement1415); 
			pushFollow(FOLLOW_columnFamilyName_in_insertStatement1419);
			cf=columnFamilyName();
			state._fsp--;

			match(input,142,FOLLOW_142_in_insertStatement1431); 
			pushFollow(FOLLOW_cident_in_insertStatement1435);
			c1=cident();
			state._fsp--;

			 columnNames.add(c1); 
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:344:51: ( ',' cn= cident )*
			loop21:
			while (true) {
				int alt21=2;
				int LA21_0 = input.LA(1);
				if ( (LA21_0==145) ) {
					alt21=1;
				}

				switch (alt21) {
				case 1 :
					// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:344:53: ',' cn= cident
					{
					match(input,145,FOLLOW_145_in_insertStatement1442); 
					pushFollow(FOLLOW_cident_in_insertStatement1446);
					cn=cident();
					state._fsp--;

					 columnNames.add(cn); 
					}
					break;

				default :
					break loop21;
				}
			}

			match(input,143,FOLLOW_143_in_insertStatement1453); 
			match(input,K_VALUES,FOLLOW_K_VALUES_in_insertStatement1463); 
			match(input,142,FOLLOW_142_in_insertStatement1475); 
			pushFollow(FOLLOW_term_in_insertStatement1479);
			v1=term();
			state._fsp--;

			 values.add(v1); 
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:346:43: ( ',' vn= term )*
			loop22:
			while (true) {
				int alt22=2;
				int LA22_0 = input.LA(1);
				if ( (LA22_0==145) ) {
					alt22=1;
				}

				switch (alt22) {
				case 1 :
					// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:346:45: ',' vn= term
					{
					match(input,145,FOLLOW_145_in_insertStatement1485); 
					pushFollow(FOLLOW_term_in_insertStatement1489);
					vn=term();
					state._fsp--;

					 values.add(vn); 
					}
					break;

				default :
					break loop22;
				}
			}

			match(input,143,FOLLOW_143_in_insertStatement1496); 
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:348:9: ( K_IF K_NOT K_EXISTS )?
			int alt23=2;
			int LA23_0 = input.LA(1);
			if ( (LA23_0==K_IF) ) {
				alt23=1;
			}
			switch (alt23) {
				case 1 :
					// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:348:11: K_IF K_NOT K_EXISTS
					{
					match(input,K_IF,FOLLOW_K_IF_in_insertStatement1509); 
					match(input,K_NOT,FOLLOW_K_NOT_in_insertStatement1511); 
					match(input,K_EXISTS,FOLLOW_K_EXISTS_in_insertStatement1513); 
					 ifNotExists = true; 
					}
					break;

			}

			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:349:9: ( usingClause[attrs] )?
			int alt24=2;
			int LA24_0 = input.LA(1);
			if ( (LA24_0==K_USING) ) {
				alt24=1;
			}
			switch (alt24) {
				case 1 :
					// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:349:11: usingClause[attrs]
					{
					pushFollow(FOLLOW_usingClause_in_insertStatement1530);
					usingClause(attrs);
					state._fsp--;

					}
					break;

			}


			          expr = new UpdateStatement.ParsedInsert(cf,
			                                                   attrs,
			                                                   columnNames,
			                                                   values,
			                                                   ifNotExists);
			      
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
	// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:359:1: usingClause[Attributes.Raw attrs] : K_USING usingClauseObjective[attrs] ( K_AND usingClauseObjective[attrs] )* ;
	public final void usingClause(Attributes.Raw attrs) throws RecognitionException {
		try {
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:360:5: ( K_USING usingClauseObjective[attrs] ( K_AND usingClauseObjective[attrs] )* )
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:360:7: K_USING usingClauseObjective[attrs] ( K_AND usingClauseObjective[attrs] )*
			{
			match(input,K_USING,FOLLOW_K_USING_in_usingClause1560); 
			pushFollow(FOLLOW_usingClauseObjective_in_usingClause1562);
			usingClauseObjective(attrs);
			state._fsp--;

			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:360:43: ( K_AND usingClauseObjective[attrs] )*
			loop25:
			while (true) {
				int alt25=2;
				int LA25_0 = input.LA(1);
				if ( (LA25_0==K_AND) ) {
					alt25=1;
				}

				switch (alt25) {
				case 1 :
					// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:360:45: K_AND usingClauseObjective[attrs]
					{
					match(input,K_AND,FOLLOW_K_AND_in_usingClause1567); 
					pushFollow(FOLLOW_usingClauseObjective_in_usingClause1569);
					usingClauseObjective(attrs);
					state._fsp--;

					}
					break;

				default :
					break loop25;
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



	// $ANTLR start "usingClauseObjective"
	// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:363:1: usingClauseObjective[Attributes.Raw attrs] : ( K_TIMESTAMP ts= intValue | K_TTL t= intValue );
	public final void usingClauseObjective(Attributes.Raw attrs) throws RecognitionException {
		Term.Raw ts =null;
		Term.Raw t =null;

		try {
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:364:5: ( K_TIMESTAMP ts= intValue | K_TTL t= intValue )
			int alt26=2;
			int LA26_0 = input.LA(1);
			if ( (LA26_0==K_TIMESTAMP) ) {
				alt26=1;
			}
			else if ( (LA26_0==K_TTL) ) {
				alt26=2;
			}

			else {
				NoViableAltException nvae =
					new NoViableAltException("", 26, 0, input);
				throw nvae;
			}

			switch (alt26) {
				case 1 :
					// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:364:7: K_TIMESTAMP ts= intValue
					{
					match(input,K_TIMESTAMP,FOLLOW_K_TIMESTAMP_in_usingClauseObjective1591); 
					pushFollow(FOLLOW_intValue_in_usingClauseObjective1595);
					ts=intValue();
					state._fsp--;

					 attrs.timestamp = ts; 
					}
					break;
				case 2 :
					// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:365:7: K_TTL t= intValue
					{
					match(input,K_TTL,FOLLOW_K_TTL_in_usingClauseObjective1605); 
					pushFollow(FOLLOW_intValue_in_usingClauseObjective1609);
					t=intValue();
					state._fsp--;

					 attrs.timeToLive = t; 
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



	// $ANTLR start "updateStatement"
	// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:375:1: updateStatement returns [UpdateStatement.ParsedUpdate expr] : K_UPDATE cf= columnFamilyName ( usingClause[attrs] )? K_SET columnOperation[operations] ( ',' columnOperation[operations] )* K_WHERE wclause= whereClause ( K_IF ( K_EXISTS |conditions= updateConditions ) )? ;
	public final UpdateStatement.ParsedUpdate updateStatement() throws RecognitionException {
		UpdateStatement.ParsedUpdate expr = null;


		CFName cf =null;
		List<Relation> wclause =null;
		List<Pair<ColumnIdentifier.Raw, ColumnCondition.Raw>> conditions =null;


		        Attributes.Raw attrs = new Attributes.Raw();
		        List<Pair<ColumnIdentifier.Raw, Operation.RawUpdate>> operations = new ArrayList<Pair<ColumnIdentifier.Raw, Operation.RawUpdate>>();
		        boolean ifExists = false;
		    
		try {
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:381:5: ( K_UPDATE cf= columnFamilyName ( usingClause[attrs] )? K_SET columnOperation[operations] ( ',' columnOperation[operations] )* K_WHERE wclause= whereClause ( K_IF ( K_EXISTS |conditions= updateConditions ) )? )
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:381:7: K_UPDATE cf= columnFamilyName ( usingClause[attrs] )? K_SET columnOperation[operations] ( ',' columnOperation[operations] )* K_WHERE wclause= whereClause ( K_IF ( K_EXISTS |conditions= updateConditions ) )?
			{
			match(input,K_UPDATE,FOLLOW_K_UPDATE_in_updateStatement1643); 
			pushFollow(FOLLOW_columnFamilyName_in_updateStatement1647);
			cf=columnFamilyName();
			state._fsp--;

			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:382:7: ( usingClause[attrs] )?
			int alt27=2;
			int LA27_0 = input.LA(1);
			if ( (LA27_0==K_USING) ) {
				alt27=1;
			}
			switch (alt27) {
				case 1 :
					// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:382:9: usingClause[attrs]
					{
					pushFollow(FOLLOW_usingClause_in_updateStatement1657);
					usingClause(attrs);
					state._fsp--;

					}
					break;

			}

			match(input,K_SET,FOLLOW_K_SET_in_updateStatement1669); 
			pushFollow(FOLLOW_columnOperation_in_updateStatement1671);
			columnOperation(operations);
			state._fsp--;

			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:383:41: ( ',' columnOperation[operations] )*
			loop28:
			while (true) {
				int alt28=2;
				int LA28_0 = input.LA(1);
				if ( (LA28_0==145) ) {
					alt28=1;
				}

				switch (alt28) {
				case 1 :
					// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:383:42: ',' columnOperation[operations]
					{
					match(input,145,FOLLOW_145_in_updateStatement1675); 
					pushFollow(FOLLOW_columnOperation_in_updateStatement1677);
					columnOperation(operations);
					state._fsp--;

					}
					break;

				default :
					break loop28;
				}
			}

			match(input,K_WHERE,FOLLOW_K_WHERE_in_updateStatement1688); 
			pushFollow(FOLLOW_whereClause_in_updateStatement1692);
			wclause=whereClause();
			state._fsp--;

			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:385:7: ( K_IF ( K_EXISTS |conditions= updateConditions ) )?
			int alt30=2;
			int LA30_0 = input.LA(1);
			if ( (LA30_0==K_IF) ) {
				alt30=1;
			}
			switch (alt30) {
				case 1 :
					// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:385:9: K_IF ( K_EXISTS |conditions= updateConditions )
					{
					match(input,K_IF,FOLLOW_K_IF_in_updateStatement1702); 
					// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:385:14: ( K_EXISTS |conditions= updateConditions )
					int alt29=2;
					int LA29_0 = input.LA(1);
					if ( (LA29_0==K_EXISTS) ) {
						int LA29_1 = input.LA(2);
						if ( (LA29_1==EOF||LA29_1==K_APPLY||LA29_1==K_DELETE||LA29_1==K_INSERT||LA29_1==K_UPDATE||LA29_1==149) ) {
							alt29=1;
						}
						else if ( (LA29_1==K_IN||LA29_1==141||(LA29_1 >= 150 && LA29_1 <= 155)) ) {
							alt29=2;
						}

						else {
							int nvaeMark = input.mark();
							try {
								input.consume();
								NoViableAltException nvae =
									new NoViableAltException("", 29, 1, input);
								throw nvae;
							} finally {
								input.rewind(nvaeMark);
							}
						}

					}
					else if ( (LA29_0==IDENT||LA29_0==K_ALL||LA29_0==K_AS||LA29_0==K_ASCII||(LA29_0 >= K_BIGINT && LA29_0 <= K_BOOLEAN)||LA29_0==K_CLUSTERING||(LA29_0 >= K_COMPACT && LA29_0 <= K_COUNTER)||(LA29_0 >= K_CUSTOM && LA29_0 <= K_DECIMAL)||(LA29_0 >= K_DISTINCT && LA29_0 <= K_DOUBLE)||(LA29_0 >= K_FILTERING && LA29_0 <= K_FLOAT)||LA29_0==K_FROZEN||LA29_0==K_INET||LA29_0==K_INT||(LA29_0 >= K_KEY && LA29_0 <= K_KEYS)||LA29_0==K_KEYSPACES||(LA29_0 >= K_LIST && LA29_0 <= K_MAP)||LA29_0==K_NOSUPERUSER||(LA29_0 >= K_PASSWORD && LA29_0 <= K_PERMISSIONS)||(LA29_0 >= K_STATIC && LA29_0 <= K_TIMEUUID)||LA29_0==K_TRIGGER||(LA29_0 >= K_TTL && LA29_0 <= K_TYPE)||(LA29_0 >= K_USER && LA29_0 <= K_USERS)||(LA29_0 >= K_UUID && LA29_0 <= K_VARINT)||LA29_0==K_WRITETIME||LA29_0==QUOTED_NAME) ) {
						alt29=2;
					}

					else {
						NoViableAltException nvae =
							new NoViableAltException("", 29, 0, input);
						throw nvae;
					}

					switch (alt29) {
						case 1 :
							// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:385:16: K_EXISTS
							{
							match(input,K_EXISTS,FOLLOW_K_EXISTS_in_updateStatement1706); 
							 ifExists = true; 
							}
							break;
						case 2 :
							// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:385:48: conditions= updateConditions
							{
							pushFollow(FOLLOW_updateConditions_in_updateStatement1714);
							conditions=updateConditions();
							state._fsp--;

							}
							break;

					}

					}
					break;

			}


			          return new UpdateStatement.ParsedUpdate(cf,
			                                                  attrs,
			                                                  operations,
			                                                  wclause,
			                                                  conditions == null ? Collections.<Pair<ColumnIdentifier.Raw, ColumnCondition.Raw>>emptyList() : conditions,
			                                                  ifExists);
			     
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



	// $ANTLR start "updateConditions"
	// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:396:1: updateConditions returns [List<Pair<ColumnIdentifier.Raw, ColumnCondition.Raw>> conditions] : columnCondition[conditions] ( K_AND columnCondition[conditions] )* ;
	public final List<Pair<ColumnIdentifier.Raw, ColumnCondition.Raw>> updateConditions() throws RecognitionException {
		List<Pair<ColumnIdentifier.Raw, ColumnCondition.Raw>> conditions = null;


		 conditions = new ArrayList<Pair<ColumnIdentifier.Raw, ColumnCondition.Raw>>(); 
		try {
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:398:5: ( columnCondition[conditions] ( K_AND columnCondition[conditions] )* )
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:398:7: columnCondition[conditions] ( K_AND columnCondition[conditions] )*
			{
			pushFollow(FOLLOW_columnCondition_in_updateConditions1756);
			columnCondition(conditions);
			state._fsp--;

			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:398:35: ( K_AND columnCondition[conditions] )*
			loop31:
			while (true) {
				int alt31=2;
				int LA31_0 = input.LA(1);
				if ( (LA31_0==K_AND) ) {
					alt31=1;
				}

				switch (alt31) {
				case 1 :
					// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:398:37: K_AND columnCondition[conditions]
					{
					match(input,K_AND,FOLLOW_K_AND_in_updateConditions1761); 
					pushFollow(FOLLOW_columnCondition_in_updateConditions1763);
					columnCondition(conditions);
					state._fsp--;

					}
					break;

				default :
					break loop31;
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
		return conditions;
	}
	// $ANTLR end "updateConditions"



	// $ANTLR start "deleteStatement"
	// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:409:1: deleteStatement returns [DeleteStatement.Parsed expr] : K_DELETE (dels= deleteSelection )? K_FROM cf= columnFamilyName ( usingClauseDelete[attrs] )? K_WHERE wclause= whereClause ( K_IF ( K_EXISTS |conditions= updateConditions ) )? ;
	public final DeleteStatement.Parsed deleteStatement() throws RecognitionException {
		DeleteStatement.Parsed expr = null;


		List<Operation.RawDeletion> dels =null;
		CFName cf =null;
		List<Relation> wclause =null;
		List<Pair<ColumnIdentifier.Raw, ColumnCondition.Raw>> conditions =null;


		        Attributes.Raw attrs = new Attributes.Raw();
		        List<Operation.RawDeletion> columnDeletions = Collections.emptyList();
		        boolean ifExists = false;
		    
		try {
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:415:5: ( K_DELETE (dels= deleteSelection )? K_FROM cf= columnFamilyName ( usingClauseDelete[attrs] )? K_WHERE wclause= whereClause ( K_IF ( K_EXISTS |conditions= updateConditions ) )? )
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:415:7: K_DELETE (dels= deleteSelection )? K_FROM cf= columnFamilyName ( usingClauseDelete[attrs] )? K_WHERE wclause= whereClause ( K_IF ( K_EXISTS |conditions= updateConditions ) )?
			{
			match(input,K_DELETE,FOLLOW_K_DELETE_in_deleteStatement1800); 
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:415:16: (dels= deleteSelection )?
			int alt32=2;
			int LA32_0 = input.LA(1);
			if ( (LA32_0==IDENT||LA32_0==K_ALL||LA32_0==K_AS||LA32_0==K_ASCII||(LA32_0 >= K_BIGINT && LA32_0 <= K_BOOLEAN)||LA32_0==K_CLUSTERING||(LA32_0 >= K_COMPACT && LA32_0 <= K_COUNTER)||(LA32_0 >= K_CUSTOM && LA32_0 <= K_DECIMAL)||(LA32_0 >= K_DISTINCT && LA32_0 <= K_DOUBLE)||(LA32_0 >= K_EXISTS && LA32_0 <= K_FLOAT)||LA32_0==K_FROZEN||LA32_0==K_INET||LA32_0==K_INT||(LA32_0 >= K_KEY && LA32_0 <= K_KEYS)||LA32_0==K_KEYSPACES||(LA32_0 >= K_LIST && LA32_0 <= K_MAP)||LA32_0==K_NOSUPERUSER||(LA32_0 >= K_PASSWORD && LA32_0 <= K_PERMISSIONS)||(LA32_0 >= K_STATIC && LA32_0 <= K_TIMEUUID)||LA32_0==K_TRIGGER||(LA32_0 >= K_TTL && LA32_0 <= K_TYPE)||(LA32_0 >= K_USER && LA32_0 <= K_USERS)||(LA32_0 >= K_UUID && LA32_0 <= K_VARINT)||LA32_0==K_WRITETIME||LA32_0==QUOTED_NAME) ) {
				alt32=1;
			}
			switch (alt32) {
				case 1 :
					// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:415:18: dels= deleteSelection
					{
					pushFollow(FOLLOW_deleteSelection_in_deleteStatement1806);
					dels=deleteSelection();
					state._fsp--;

					 columnDeletions = dels; 
					}
					break;

			}

			match(input,K_FROM,FOLLOW_K_FROM_in_deleteStatement1819); 
			pushFollow(FOLLOW_columnFamilyName_in_deleteStatement1823);
			cf=columnFamilyName();
			state._fsp--;

			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:417:7: ( usingClauseDelete[attrs] )?
			int alt33=2;
			int LA33_0 = input.LA(1);
			if ( (LA33_0==K_USING) ) {
				alt33=1;
			}
			switch (alt33) {
				case 1 :
					// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:417:9: usingClauseDelete[attrs]
					{
					pushFollow(FOLLOW_usingClauseDelete_in_deleteStatement1833);
					usingClauseDelete(attrs);
					state._fsp--;

					}
					break;

			}

			match(input,K_WHERE,FOLLOW_K_WHERE_in_deleteStatement1845); 
			pushFollow(FOLLOW_whereClause_in_deleteStatement1849);
			wclause=whereClause();
			state._fsp--;

			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:419:7: ( K_IF ( K_EXISTS |conditions= updateConditions ) )?
			int alt35=2;
			int LA35_0 = input.LA(1);
			if ( (LA35_0==K_IF) ) {
				alt35=1;
			}
			switch (alt35) {
				case 1 :
					// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:419:9: K_IF ( K_EXISTS |conditions= updateConditions )
					{
					match(input,K_IF,FOLLOW_K_IF_in_deleteStatement1859); 
					// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:419:14: ( K_EXISTS |conditions= updateConditions )
					int alt34=2;
					int LA34_0 = input.LA(1);
					if ( (LA34_0==K_EXISTS) ) {
						int LA34_1 = input.LA(2);
						if ( (LA34_1==EOF||LA34_1==K_APPLY||LA34_1==K_DELETE||LA34_1==K_INSERT||LA34_1==K_UPDATE||LA34_1==149) ) {
							alt34=1;
						}
						else if ( (LA34_1==K_IN||LA34_1==141||(LA34_1 >= 150 && LA34_1 <= 155)) ) {
							alt34=2;
						}

						else {
							int nvaeMark = input.mark();
							try {
								input.consume();
								NoViableAltException nvae =
									new NoViableAltException("", 34, 1, input);
								throw nvae;
							} finally {
								input.rewind(nvaeMark);
							}
						}

					}
					else if ( (LA34_0==IDENT||LA34_0==K_ALL||LA34_0==K_AS||LA34_0==K_ASCII||(LA34_0 >= K_BIGINT && LA34_0 <= K_BOOLEAN)||LA34_0==K_CLUSTERING||(LA34_0 >= K_COMPACT && LA34_0 <= K_COUNTER)||(LA34_0 >= K_CUSTOM && LA34_0 <= K_DECIMAL)||(LA34_0 >= K_DISTINCT && LA34_0 <= K_DOUBLE)||(LA34_0 >= K_FILTERING && LA34_0 <= K_FLOAT)||LA34_0==K_FROZEN||LA34_0==K_INET||LA34_0==K_INT||(LA34_0 >= K_KEY && LA34_0 <= K_KEYS)||LA34_0==K_KEYSPACES||(LA34_0 >= K_LIST && LA34_0 <= K_MAP)||LA34_0==K_NOSUPERUSER||(LA34_0 >= K_PASSWORD && LA34_0 <= K_PERMISSIONS)||(LA34_0 >= K_STATIC && LA34_0 <= K_TIMEUUID)||LA34_0==K_TRIGGER||(LA34_0 >= K_TTL && LA34_0 <= K_TYPE)||(LA34_0 >= K_USER && LA34_0 <= K_USERS)||(LA34_0 >= K_UUID && LA34_0 <= K_VARINT)||LA34_0==K_WRITETIME||LA34_0==QUOTED_NAME) ) {
						alt34=2;
					}

					else {
						NoViableAltException nvae =
							new NoViableAltException("", 34, 0, input);
						throw nvae;
					}

					switch (alt34) {
						case 1 :
							// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:419:16: K_EXISTS
							{
							match(input,K_EXISTS,FOLLOW_K_EXISTS_in_deleteStatement1863); 
							 ifExists = true; 
							}
							break;
						case 2 :
							// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:419:48: conditions= updateConditions
							{
							pushFollow(FOLLOW_updateConditions_in_deleteStatement1871);
							conditions=updateConditions();
							state._fsp--;

							}
							break;

					}

					}
					break;

			}


			          return new DeleteStatement.Parsed(cf,
			                                            attrs,
			                                            columnDeletions,
			                                            wclause,
			                                            conditions == null ? Collections.<Pair<ColumnIdentifier.Raw, ColumnCondition.Raw>>emptyList() : conditions,
			                                            ifExists);
			      
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



	// $ANTLR start "deleteSelection"
	// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:430:1: deleteSelection returns [List<Operation.RawDeletion> operations] :t1= deleteOp ( ',' tN= deleteOp )* ;
	public final List<Operation.RawDeletion> deleteSelection() throws RecognitionException {
		List<Operation.RawDeletion> operations = null;


		Operation.RawDeletion t1 =null;
		Operation.RawDeletion tN =null;

		try {
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:431:5: (t1= deleteOp ( ',' tN= deleteOp )* )
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:431:7: t1= deleteOp ( ',' tN= deleteOp )*
			{
			 operations = new ArrayList<Operation.RawDeletion>(); 
			pushFollow(FOLLOW_deleteOp_in_deleteSelection1918);
			t1=deleteOp();
			state._fsp--;

			 operations.add(t1); 
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:433:11: ( ',' tN= deleteOp )*
			loop36:
			while (true) {
				int alt36=2;
				int LA36_0 = input.LA(1);
				if ( (LA36_0==145) ) {
					alt36=1;
				}

				switch (alt36) {
				case 1 :
					// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:433:12: ',' tN= deleteOp
					{
					match(input,145,FOLLOW_145_in_deleteSelection1933); 
					pushFollow(FOLLOW_deleteOp_in_deleteSelection1937);
					tN=deleteOp();
					state._fsp--;

					 operations.add(tN); 
					}
					break;

				default :
					break loop36;
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
		return operations;
	}
	// $ANTLR end "deleteSelection"



	// $ANTLR start "deleteOp"
	// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:436:1: deleteOp returns [Operation.RawDeletion op] : (c= cident |c= cident '[' t= term ']' );
	public final Operation.RawDeletion deleteOp() throws RecognitionException {
		Operation.RawDeletion op = null;


		ColumnIdentifier.Raw c =null;
		Term.Raw t =null;

		try {
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:437:5: (c= cident |c= cident '[' t= term ']' )
			int alt37=2;
			switch ( input.LA(1) ) {
			case IDENT:
				{
				int LA37_1 = input.LA(2);
				if ( (LA37_1==K_FROM||LA37_1==145) ) {
					alt37=1;
				}
				else if ( (LA37_1==155) ) {
					alt37=2;
				}

				else {
					int nvaeMark = input.mark();
					try {
						input.consume();
						NoViableAltException nvae =
							new NoViableAltException("", 37, 1, input);
						throw nvae;
					} finally {
						input.rewind(nvaeMark);
					}
				}

				}
				break;
			case QUOTED_NAME:
				{
				int LA37_2 = input.LA(2);
				if ( (LA37_2==K_FROM||LA37_2==145) ) {
					alt37=1;
				}
				else if ( (LA37_2==155) ) {
					alt37=2;
				}

				else {
					int nvaeMark = input.mark();
					try {
						input.consume();
						NoViableAltException nvae =
							new NoViableAltException("", 37, 2, input);
						throw nvae;
					} finally {
						input.rewind(nvaeMark);
					}
				}

				}
				break;
			case K_ALL:
			case K_AS:
			case K_CLUSTERING:
			case K_COMPACT:
			case K_CONTAINS:
			case K_CUSTOM:
			case K_DISTINCT:
			case K_EXISTS:
			case K_FILTERING:
			case K_FROZEN:
			case K_KEYS:
			case K_KEYSPACES:
			case K_LIST:
			case K_MAP:
			case K_NOSUPERUSER:
			case K_PASSWORD:
			case K_PERMISSION:
			case K_PERMISSIONS:
			case K_STATIC:
			case K_STORAGE:
			case K_SUPERUSER:
			case K_TRIGGER:
			case K_TUPLE:
			case K_TYPE:
			case K_USER:
			case K_USERS:
			case K_VALUES:
				{
				int LA37_3 = input.LA(2);
				if ( (LA37_3==K_FROM||LA37_3==145) ) {
					alt37=1;
				}
				else if ( (LA37_3==155) ) {
					alt37=2;
				}

				else {
					int nvaeMark = input.mark();
					try {
						input.consume();
						NoViableAltException nvae =
							new NoViableAltException("", 37, 3, input);
						throw nvae;
					} finally {
						input.rewind(nvaeMark);
					}
				}

				}
				break;
			case K_ASCII:
				{
				int LA37_4 = input.LA(2);
				if ( (LA37_4==K_FROM||LA37_4==145) ) {
					alt37=1;
				}
				else if ( (LA37_4==155) ) {
					alt37=2;
				}

				else {
					int nvaeMark = input.mark();
					try {
						input.consume();
						NoViableAltException nvae =
							new NoViableAltException("", 37, 4, input);
						throw nvae;
					} finally {
						input.rewind(nvaeMark);
					}
				}

				}
				break;
			case K_BIGINT:
				{
				int LA37_5 = input.LA(2);
				if ( (LA37_5==K_FROM||LA37_5==145) ) {
					alt37=1;
				}
				else if ( (LA37_5==155) ) {
					alt37=2;
				}

				else {
					int nvaeMark = input.mark();
					try {
						input.consume();
						NoViableAltException nvae =
							new NoViableAltException("", 37, 5, input);
						throw nvae;
					} finally {
						input.rewind(nvaeMark);
					}
				}

				}
				break;
			case K_BLOB:
				{
				int LA37_6 = input.LA(2);
				if ( (LA37_6==K_FROM||LA37_6==145) ) {
					alt37=1;
				}
				else if ( (LA37_6==155) ) {
					alt37=2;
				}

				else {
					int nvaeMark = input.mark();
					try {
						input.consume();
						NoViableAltException nvae =
							new NoViableAltException("", 37, 6, input);
						throw nvae;
					} finally {
						input.rewind(nvaeMark);
					}
				}

				}
				break;
			case K_BOOLEAN:
				{
				int LA37_7 = input.LA(2);
				if ( (LA37_7==K_FROM||LA37_7==145) ) {
					alt37=1;
				}
				else if ( (LA37_7==155) ) {
					alt37=2;
				}

				else {
					int nvaeMark = input.mark();
					try {
						input.consume();
						NoViableAltException nvae =
							new NoViableAltException("", 37, 7, input);
						throw nvae;
					} finally {
						input.rewind(nvaeMark);
					}
				}

				}
				break;
			case K_COUNTER:
				{
				int LA37_8 = input.LA(2);
				if ( (LA37_8==K_FROM||LA37_8==145) ) {
					alt37=1;
				}
				else if ( (LA37_8==155) ) {
					alt37=2;
				}

				else {
					int nvaeMark = input.mark();
					try {
						input.consume();
						NoViableAltException nvae =
							new NoViableAltException("", 37, 8, input);
						throw nvae;
					} finally {
						input.rewind(nvaeMark);
					}
				}

				}
				break;
			case K_DECIMAL:
				{
				int LA37_9 = input.LA(2);
				if ( (LA37_9==K_FROM||LA37_9==145) ) {
					alt37=1;
				}
				else if ( (LA37_9==155) ) {
					alt37=2;
				}

				else {
					int nvaeMark = input.mark();
					try {
						input.consume();
						NoViableAltException nvae =
							new NoViableAltException("", 37, 9, input);
						throw nvae;
					} finally {
						input.rewind(nvaeMark);
					}
				}

				}
				break;
			case K_DOUBLE:
				{
				int LA37_10 = input.LA(2);
				if ( (LA37_10==K_FROM||LA37_10==145) ) {
					alt37=1;
				}
				else if ( (LA37_10==155) ) {
					alt37=2;
				}

				else {
					int nvaeMark = input.mark();
					try {
						input.consume();
						NoViableAltException nvae =
							new NoViableAltException("", 37, 10, input);
						throw nvae;
					} finally {
						input.rewind(nvaeMark);
					}
				}

				}
				break;
			case K_FLOAT:
				{
				int LA37_11 = input.LA(2);
				if ( (LA37_11==K_FROM||LA37_11==145) ) {
					alt37=1;
				}
				else if ( (LA37_11==155) ) {
					alt37=2;
				}

				else {
					int nvaeMark = input.mark();
					try {
						input.consume();
						NoViableAltException nvae =
							new NoViableAltException("", 37, 11, input);
						throw nvae;
					} finally {
						input.rewind(nvaeMark);
					}
				}

				}
				break;
			case K_INET:
				{
				int LA37_12 = input.LA(2);
				if ( (LA37_12==K_FROM||LA37_12==145) ) {
					alt37=1;
				}
				else if ( (LA37_12==155) ) {
					alt37=2;
				}

				else {
					int nvaeMark = input.mark();
					try {
						input.consume();
						NoViableAltException nvae =
							new NoViableAltException("", 37, 12, input);
						throw nvae;
					} finally {
						input.rewind(nvaeMark);
					}
				}

				}
				break;
			case K_INT:
				{
				int LA37_13 = input.LA(2);
				if ( (LA37_13==K_FROM||LA37_13==145) ) {
					alt37=1;
				}
				else if ( (LA37_13==155) ) {
					alt37=2;
				}

				else {
					int nvaeMark = input.mark();
					try {
						input.consume();
						NoViableAltException nvae =
							new NoViableAltException("", 37, 13, input);
						throw nvae;
					} finally {
						input.rewind(nvaeMark);
					}
				}

				}
				break;
			case K_TEXT:
				{
				int LA37_14 = input.LA(2);
				if ( (LA37_14==K_FROM||LA37_14==145) ) {
					alt37=1;
				}
				else if ( (LA37_14==155) ) {
					alt37=2;
				}

				else {
					int nvaeMark = input.mark();
					try {
						input.consume();
						NoViableAltException nvae =
							new NoViableAltException("", 37, 14, input);
						throw nvae;
					} finally {
						input.rewind(nvaeMark);
					}
				}

				}
				break;
			case K_TIMESTAMP:
				{
				int LA37_15 = input.LA(2);
				if ( (LA37_15==K_FROM||LA37_15==145) ) {
					alt37=1;
				}
				else if ( (LA37_15==155) ) {
					alt37=2;
				}

				else {
					int nvaeMark = input.mark();
					try {
						input.consume();
						NoViableAltException nvae =
							new NoViableAltException("", 37, 15, input);
						throw nvae;
					} finally {
						input.rewind(nvaeMark);
					}
				}

				}
				break;
			case K_UUID:
				{
				int LA37_16 = input.LA(2);
				if ( (LA37_16==K_FROM||LA37_16==145) ) {
					alt37=1;
				}
				else if ( (LA37_16==155) ) {
					alt37=2;
				}

				else {
					int nvaeMark = input.mark();
					try {
						input.consume();
						NoViableAltException nvae =
							new NoViableAltException("", 37, 16, input);
						throw nvae;
					} finally {
						input.rewind(nvaeMark);
					}
				}

				}
				break;
			case K_VARCHAR:
				{
				int LA37_17 = input.LA(2);
				if ( (LA37_17==K_FROM||LA37_17==145) ) {
					alt37=1;
				}
				else if ( (LA37_17==155) ) {
					alt37=2;
				}

				else {
					int nvaeMark = input.mark();
					try {
						input.consume();
						NoViableAltException nvae =
							new NoViableAltException("", 37, 17, input);
						throw nvae;
					} finally {
						input.rewind(nvaeMark);
					}
				}

				}
				break;
			case K_VARINT:
				{
				int LA37_18 = input.LA(2);
				if ( (LA37_18==K_FROM||LA37_18==145) ) {
					alt37=1;
				}
				else if ( (LA37_18==155) ) {
					alt37=2;
				}

				else {
					int nvaeMark = input.mark();
					try {
						input.consume();
						NoViableAltException nvae =
							new NoViableAltException("", 37, 18, input);
						throw nvae;
					} finally {
						input.rewind(nvaeMark);
					}
				}

				}
				break;
			case K_TIMEUUID:
				{
				int LA37_19 = input.LA(2);
				if ( (LA37_19==K_FROM||LA37_19==145) ) {
					alt37=1;
				}
				else if ( (LA37_19==155) ) {
					alt37=2;
				}

				else {
					int nvaeMark = input.mark();
					try {
						input.consume();
						NoViableAltException nvae =
							new NoViableAltException("", 37, 19, input);
						throw nvae;
					} finally {
						input.rewind(nvaeMark);
					}
				}

				}
				break;
			case K_COUNT:
			case K_KEY:
			case K_TTL:
			case K_WRITETIME:
				{
				int LA37_20 = input.LA(2);
				if ( (LA37_20==K_FROM||LA37_20==145) ) {
					alt37=1;
				}
				else if ( (LA37_20==155) ) {
					alt37=2;
				}

				else {
					int nvaeMark = input.mark();
					try {
						input.consume();
						NoViableAltException nvae =
							new NoViableAltException("", 37, 20, input);
						throw nvae;
					} finally {
						input.rewind(nvaeMark);
					}
				}

				}
				break;
			default:
				NoViableAltException nvae =
					new NoViableAltException("", 37, 0, input);
				throw nvae;
			}
			switch (alt37) {
				case 1 :
					// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:437:7: c= cident
					{
					pushFollow(FOLLOW_cident_in_deleteOp1964);
					c=cident();
					state._fsp--;

					 op = new Operation.ColumnDeletion(c); 
					}
					break;
				case 2 :
					// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:438:7: c= cident '[' t= term ']'
					{
					pushFollow(FOLLOW_cident_in_deleteOp1991);
					c=cident();
					state._fsp--;

					match(input,155,FOLLOW_155_in_deleteOp1993); 
					pushFollow(FOLLOW_term_in_deleteOp1997);
					t=term();
					state._fsp--;

					match(input,157,FOLLOW_157_in_deleteOp1999); 
					 op = new Operation.ElementDeletion(c, t); 
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
		return op;
	}
	// $ANTLR end "deleteOp"



	// $ANTLR start "usingClauseDelete"
	// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:441:1: usingClauseDelete[Attributes.Raw attrs] : K_USING K_TIMESTAMP ts= intValue ;
	public final void usingClauseDelete(Attributes.Raw attrs) throws RecognitionException {
		Term.Raw ts =null;

		try {
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:442:5: ( K_USING K_TIMESTAMP ts= intValue )
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:442:7: K_USING K_TIMESTAMP ts= intValue
			{
			match(input,K_USING,FOLLOW_K_USING_in_usingClauseDelete2019); 
			match(input,K_TIMESTAMP,FOLLOW_K_TIMESTAMP_in_usingClauseDelete2021); 
			pushFollow(FOLLOW_intValue_in_usingClauseDelete2025);
			ts=intValue();
			state._fsp--;

			 attrs.timestamp = ts; 
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



	// $ANTLR start "batchStatement"
	// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:469:1: batchStatement returns [BatchStatement.Parsed expr] : K_BEGIN ( K_UNLOGGED | K_COUNTER )? K_BATCH ( usingClause[attrs] )? (s= batchStatementObjective ( ';' )? )* K_APPLY K_BATCH ;
	public final BatchStatement.Parsed batchStatement() throws RecognitionException {
		BatchStatement.Parsed expr = null;


		ModificationStatement.Parsed s =null;


		        BatchStatement.Type type = BatchStatement.Type.LOGGED;
		        List<ModificationStatement.Parsed> statements = new ArrayList<ModificationStatement.Parsed>();
		        Attributes.Raw attrs = new Attributes.Raw();
		    
		try {
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:475:5: ( K_BEGIN ( K_UNLOGGED | K_COUNTER )? K_BATCH ( usingClause[attrs] )? (s= batchStatementObjective ( ';' )? )* K_APPLY K_BATCH )
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:475:7: K_BEGIN ( K_UNLOGGED | K_COUNTER )? K_BATCH ( usingClause[attrs] )? (s= batchStatementObjective ( ';' )? )* K_APPLY K_BATCH
			{
			match(input,K_BEGIN,FOLLOW_K_BEGIN_in_batchStatement2059); 
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:476:7: ( K_UNLOGGED | K_COUNTER )?
			int alt38=3;
			int LA38_0 = input.LA(1);
			if ( (LA38_0==K_UNLOGGED) ) {
				alt38=1;
			}
			else if ( (LA38_0==K_COUNTER) ) {
				alt38=2;
			}
			switch (alt38) {
				case 1 :
					// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:476:9: K_UNLOGGED
					{
					match(input,K_UNLOGGED,FOLLOW_K_UNLOGGED_in_batchStatement2069); 
					 type = BatchStatement.Type.UNLOGGED; 
					}
					break;
				case 2 :
					// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:476:63: K_COUNTER
					{
					match(input,K_COUNTER,FOLLOW_K_COUNTER_in_batchStatement2075); 
					 type = BatchStatement.Type.COUNTER; 
					}
					break;

			}

			match(input,K_BATCH,FOLLOW_K_BATCH_in_batchStatement2088); 
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:477:15: ( usingClause[attrs] )?
			int alt39=2;
			int LA39_0 = input.LA(1);
			if ( (LA39_0==K_USING) ) {
				alt39=1;
			}
			switch (alt39) {
				case 1 :
					// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:477:17: usingClause[attrs]
					{
					pushFollow(FOLLOW_usingClause_in_batchStatement2092);
					usingClause(attrs);
					state._fsp--;

					}
					break;

			}

			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:478:11: (s= batchStatementObjective ( ';' )? )*
			loop41:
			while (true) {
				int alt41=2;
				int LA41_0 = input.LA(1);
				if ( (LA41_0==K_DELETE||LA41_0==K_INSERT||LA41_0==K_UPDATE) ) {
					alt41=1;
				}

				switch (alt41) {
				case 1 :
					// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:478:13: s= batchStatementObjective ( ';' )?
					{
					pushFollow(FOLLOW_batchStatementObjective_in_batchStatement2112);
					s=batchStatementObjective();
					state._fsp--;

					// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:478:39: ( ';' )?
					int alt40=2;
					int LA40_0 = input.LA(1);
					if ( (LA40_0==149) ) {
						alt40=1;
					}
					switch (alt40) {
						case 1 :
							// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:478:39: ';'
							{
							match(input,149,FOLLOW_149_in_batchStatement2114); 
							}
							break;

					}

					 statements.add(s); 
					}
					break;

				default :
					break loop41;
				}
			}

			match(input,K_APPLY,FOLLOW_K_APPLY_in_batchStatement2128); 
			match(input,K_BATCH,FOLLOW_K_BATCH_in_batchStatement2130); 

			          return new BatchStatement.Parsed(type, attrs, statements);
			      
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
	// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:485:1: batchStatementObjective returns [ModificationStatement.Parsed statement] : (i= insertStatement |u= updateStatement |d= deleteStatement );
	public final ModificationStatement.Parsed batchStatementObjective() throws RecognitionException {
		ModificationStatement.Parsed statement = null;


		UpdateStatement.ParsedInsert i =null;
		UpdateStatement.ParsedUpdate u =null;
		DeleteStatement.Parsed d =null;

		try {
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:486:5: (i= insertStatement |u= updateStatement |d= deleteStatement )
			int alt42=3;
			switch ( input.LA(1) ) {
			case K_INSERT:
				{
				alt42=1;
				}
				break;
			case K_UPDATE:
				{
				alt42=2;
				}
				break;
			case K_DELETE:
				{
				alt42=3;
				}
				break;
			default:
				NoViableAltException nvae =
					new NoViableAltException("", 42, 0, input);
				throw nvae;
			}
			switch (alt42) {
				case 1 :
					// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:486:7: i= insertStatement
					{
					pushFollow(FOLLOW_insertStatement_in_batchStatementObjective2161);
					i=insertStatement();
					state._fsp--;

					 statement = i; 
					}
					break;
				case 2 :
					// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:487:7: u= updateStatement
					{
					pushFollow(FOLLOW_updateStatement_in_batchStatementObjective2174);
					u=updateStatement();
					state._fsp--;

					 statement = u; 
					}
					break;
				case 3 :
					// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:488:7: d= deleteStatement
					{
					pushFollow(FOLLOW_deleteStatement_in_batchStatementObjective2187);
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



	// $ANTLR start "createKeyspaceStatement"
	// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:494:1: createKeyspaceStatement returns [CreateKeyspaceStatement expr] : K_CREATE K_KEYSPACE ( K_IF K_NOT K_EXISTS )? ks= keyspaceName K_WITH properties[attrs] ;
	public final CreateKeyspaceStatement createKeyspaceStatement() throws RecognitionException {
		CreateKeyspaceStatement expr = null;


		String ks =null;


		        KSPropDefs attrs = new KSPropDefs();
		        boolean ifNotExists = false;
		    
		try {
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:499:5: ( K_CREATE K_KEYSPACE ( K_IF K_NOT K_EXISTS )? ks= keyspaceName K_WITH properties[attrs] )
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:499:7: K_CREATE K_KEYSPACE ( K_IF K_NOT K_EXISTS )? ks= keyspaceName K_WITH properties[attrs]
			{
			match(input,K_CREATE,FOLLOW_K_CREATE_in_createKeyspaceStatement2222); 
			match(input,K_KEYSPACE,FOLLOW_K_KEYSPACE_in_createKeyspaceStatement2224); 
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:499:27: ( K_IF K_NOT K_EXISTS )?
			int alt43=2;
			int LA43_0 = input.LA(1);
			if ( (LA43_0==K_IF) ) {
				alt43=1;
			}
			switch (alt43) {
				case 1 :
					// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:499:28: K_IF K_NOT K_EXISTS
					{
					match(input,K_IF,FOLLOW_K_IF_in_createKeyspaceStatement2227); 
					match(input,K_NOT,FOLLOW_K_NOT_in_createKeyspaceStatement2229); 
					match(input,K_EXISTS,FOLLOW_K_EXISTS_in_createKeyspaceStatement2231); 
					 ifNotExists = true; 
					}
					break;

			}

			pushFollow(FOLLOW_keyspaceName_in_createKeyspaceStatement2240);
			ks=keyspaceName();
			state._fsp--;

			match(input,K_WITH,FOLLOW_K_WITH_in_createKeyspaceStatement2248); 
			pushFollow(FOLLOW_properties_in_createKeyspaceStatement2250);
			properties(attrs);
			state._fsp--;

			 expr = new CreateKeyspaceStatement(ks, attrs, ifNotExists); 
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



	// $ANTLR start "createTableStatement"
	// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:510:1: createTableStatement returns [CreateTableStatement.RawStatement expr] : K_CREATE K_COLUMNFAMILY ( K_IF K_NOT K_EXISTS )? cf= columnFamilyName cfamDefinition[expr] ;
	public final CreateTableStatement.RawStatement createTableStatement() throws RecognitionException {
		CreateTableStatement.RawStatement expr = null;


		CFName cf =null;

		 boolean ifNotExists = false; 
		try {
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:512:5: ( K_CREATE K_COLUMNFAMILY ( K_IF K_NOT K_EXISTS )? cf= columnFamilyName cfamDefinition[expr] )
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:512:7: K_CREATE K_COLUMNFAMILY ( K_IF K_NOT K_EXISTS )? cf= columnFamilyName cfamDefinition[expr]
			{
			match(input,K_CREATE,FOLLOW_K_CREATE_in_createTableStatement2285); 
			match(input,K_COLUMNFAMILY,FOLLOW_K_COLUMNFAMILY_in_createTableStatement2287); 
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:512:31: ( K_IF K_NOT K_EXISTS )?
			int alt44=2;
			int LA44_0 = input.LA(1);
			if ( (LA44_0==K_IF) ) {
				alt44=1;
			}
			switch (alt44) {
				case 1 :
					// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:512:32: K_IF K_NOT K_EXISTS
					{
					match(input,K_IF,FOLLOW_K_IF_in_createTableStatement2290); 
					match(input,K_NOT,FOLLOW_K_NOT_in_createTableStatement2292); 
					match(input,K_EXISTS,FOLLOW_K_EXISTS_in_createTableStatement2294); 
					 ifNotExists = true; 
					}
					break;

			}

			pushFollow(FOLLOW_columnFamilyName_in_createTableStatement2309);
			cf=columnFamilyName();
			state._fsp--;

			 expr = new CreateTableStatement.RawStatement(cf, ifNotExists); 
			pushFollow(FOLLOW_cfamDefinition_in_createTableStatement2319);
			cfamDefinition(expr);
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
	// $ANTLR end "createTableStatement"



	// $ANTLR start "cfamDefinition"
	// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:517:1: cfamDefinition[CreateTableStatement.RawStatement expr] : '(' cfamColumns[expr] ( ',' ( cfamColumns[expr] )? )* ')' ( K_WITH cfamProperty[expr] ( K_AND cfamProperty[expr] )* )? ;
	public final void cfamDefinition(CreateTableStatement.RawStatement expr) throws RecognitionException {
		try {
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:518:5: ( '(' cfamColumns[expr] ( ',' ( cfamColumns[expr] )? )* ')' ( K_WITH cfamProperty[expr] ( K_AND cfamProperty[expr] )* )? )
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:518:7: '(' cfamColumns[expr] ( ',' ( cfamColumns[expr] )? )* ')' ( K_WITH cfamProperty[expr] ( K_AND cfamProperty[expr] )* )?
			{
			match(input,142,FOLLOW_142_in_cfamDefinition2338); 
			pushFollow(FOLLOW_cfamColumns_in_cfamDefinition2340);
			cfamColumns(expr);
			state._fsp--;

			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:518:29: ( ',' ( cfamColumns[expr] )? )*
			loop46:
			while (true) {
				int alt46=2;
				int LA46_0 = input.LA(1);
				if ( (LA46_0==145) ) {
					alt46=1;
				}

				switch (alt46) {
				case 1 :
					// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:518:31: ',' ( cfamColumns[expr] )?
					{
					match(input,145,FOLLOW_145_in_cfamDefinition2345); 
					// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:518:35: ( cfamColumns[expr] )?
					int alt45=2;
					int LA45_0 = input.LA(1);
					if ( (LA45_0==IDENT||LA45_0==K_ALL||LA45_0==K_AS||LA45_0==K_ASCII||(LA45_0 >= K_BIGINT && LA45_0 <= K_BOOLEAN)||LA45_0==K_CLUSTERING||(LA45_0 >= K_COMPACT && LA45_0 <= K_COUNTER)||(LA45_0 >= K_CUSTOM && LA45_0 <= K_DECIMAL)||(LA45_0 >= K_DISTINCT && LA45_0 <= K_DOUBLE)||(LA45_0 >= K_EXISTS && LA45_0 <= K_FLOAT)||LA45_0==K_FROZEN||LA45_0==K_INET||LA45_0==K_INT||(LA45_0 >= K_KEY && LA45_0 <= K_KEYS)||LA45_0==K_KEYSPACES||(LA45_0 >= K_LIST && LA45_0 <= K_MAP)||LA45_0==K_NOSUPERUSER||(LA45_0 >= K_PASSWORD && LA45_0 <= K_PRIMARY)||(LA45_0 >= K_STATIC && LA45_0 <= K_TIMEUUID)||LA45_0==K_TRIGGER||(LA45_0 >= K_TTL && LA45_0 <= K_TYPE)||(LA45_0 >= K_USER && LA45_0 <= K_USERS)||(LA45_0 >= K_UUID && LA45_0 <= K_VARINT)||LA45_0==K_WRITETIME||LA45_0==QUOTED_NAME) ) {
						alt45=1;
					}
					switch (alt45) {
						case 1 :
							// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:518:35: cfamColumns[expr]
							{
							pushFollow(FOLLOW_cfamColumns_in_cfamDefinition2347);
							cfamColumns(expr);
							state._fsp--;

							}
							break;

					}

					}
					break;

				default :
					break loop46;
				}
			}

			match(input,143,FOLLOW_143_in_cfamDefinition2354); 
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:519:7: ( K_WITH cfamProperty[expr] ( K_AND cfamProperty[expr] )* )?
			int alt48=2;
			int LA48_0 = input.LA(1);
			if ( (LA48_0==K_WITH) ) {
				alt48=1;
			}
			switch (alt48) {
				case 1 :
					// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:519:9: K_WITH cfamProperty[expr] ( K_AND cfamProperty[expr] )*
					{
					match(input,K_WITH,FOLLOW_K_WITH_in_cfamDefinition2364); 
					pushFollow(FOLLOW_cfamProperty_in_cfamDefinition2366);
					cfamProperty(expr);
					state._fsp--;

					// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:519:35: ( K_AND cfamProperty[expr] )*
					loop47:
					while (true) {
						int alt47=2;
						int LA47_0 = input.LA(1);
						if ( (LA47_0==K_AND) ) {
							alt47=1;
						}

						switch (alt47) {
						case 1 :
							// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:519:37: K_AND cfamProperty[expr]
							{
							match(input,K_AND,FOLLOW_K_AND_in_cfamDefinition2371); 
							pushFollow(FOLLOW_cfamProperty_in_cfamDefinition2373);
							cfamProperty(expr);
							state._fsp--;

							}
							break;

						default :
							break loop47;
						}
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
	// $ANTLR end "cfamDefinition"



	// $ANTLR start "cfamColumns"
	// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:522:1: cfamColumns[CreateTableStatement.RawStatement expr] : (k= ident v= comparatorType ( K_STATIC )? ( K_PRIMARY K_KEY )? | K_PRIMARY K_KEY '(' pkDef[expr] ( ',' c= ident )* ')' );
	public final void cfamColumns(CreateTableStatement.RawStatement expr) throws RecognitionException {
		ColumnIdentifier k =null;
		CQL3Type.Raw v =null;
		ColumnIdentifier c =null;

		try {
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:523:5: (k= ident v= comparatorType ( K_STATIC )? ( K_PRIMARY K_KEY )? | K_PRIMARY K_KEY '(' pkDef[expr] ( ',' c= ident )* ')' )
			int alt52=2;
			int LA52_0 = input.LA(1);
			if ( (LA52_0==IDENT||LA52_0==K_ALL||LA52_0==K_AS||LA52_0==K_ASCII||(LA52_0 >= K_BIGINT && LA52_0 <= K_BOOLEAN)||LA52_0==K_CLUSTERING||(LA52_0 >= K_COMPACT && LA52_0 <= K_COUNTER)||(LA52_0 >= K_CUSTOM && LA52_0 <= K_DECIMAL)||(LA52_0 >= K_DISTINCT && LA52_0 <= K_DOUBLE)||(LA52_0 >= K_EXISTS && LA52_0 <= K_FLOAT)||LA52_0==K_FROZEN||LA52_0==K_INET||LA52_0==K_INT||(LA52_0 >= K_KEY && LA52_0 <= K_KEYS)||LA52_0==K_KEYSPACES||(LA52_0 >= K_LIST && LA52_0 <= K_MAP)||LA52_0==K_NOSUPERUSER||(LA52_0 >= K_PASSWORD && LA52_0 <= K_PERMISSIONS)||(LA52_0 >= K_STATIC && LA52_0 <= K_TIMEUUID)||LA52_0==K_TRIGGER||(LA52_0 >= K_TTL && LA52_0 <= K_TYPE)||(LA52_0 >= K_USER && LA52_0 <= K_USERS)||(LA52_0 >= K_UUID && LA52_0 <= K_VARINT)||LA52_0==K_WRITETIME||LA52_0==QUOTED_NAME) ) {
				alt52=1;
			}
			else if ( (LA52_0==K_PRIMARY) ) {
				alt52=2;
			}

			else {
				NoViableAltException nvae =
					new NoViableAltException("", 52, 0, input);
				throw nvae;
			}

			switch (alt52) {
				case 1 :
					// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:523:7: k= ident v= comparatorType ( K_STATIC )? ( K_PRIMARY K_KEY )?
					{
					pushFollow(FOLLOW_ident_in_cfamColumns2399);
					k=ident();
					state._fsp--;

					pushFollow(FOLLOW_comparatorType_in_cfamColumns2403);
					v=comparatorType();
					state._fsp--;

					 boolean isStatic=false; 
					// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:523:60: ( K_STATIC )?
					int alt49=2;
					int LA49_0 = input.LA(1);
					if ( (LA49_0==K_STATIC) ) {
						alt49=1;
					}
					switch (alt49) {
						case 1 :
							// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:523:61: K_STATIC
							{
							match(input,K_STATIC,FOLLOW_K_STATIC_in_cfamColumns2408); 
							isStatic = true;
							}
							break;

					}

					 expr.addDefinition(k, v, isStatic); 
					// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:524:9: ( K_PRIMARY K_KEY )?
					int alt50=2;
					int LA50_0 = input.LA(1);
					if ( (LA50_0==K_PRIMARY) ) {
						alt50=1;
					}
					switch (alt50) {
						case 1 :
							// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:524:10: K_PRIMARY K_KEY
							{
							match(input,K_PRIMARY,FOLLOW_K_PRIMARY_in_cfamColumns2425); 
							match(input,K_KEY,FOLLOW_K_KEY_in_cfamColumns2427); 
							 expr.addKeyAliases(Collections.singletonList(k)); 
							}
							break;

					}

					}
					break;
				case 2 :
					// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:525:7: K_PRIMARY K_KEY '(' pkDef[expr] ( ',' c= ident )* ')'
					{
					match(input,K_PRIMARY,FOLLOW_K_PRIMARY_in_cfamColumns2439); 
					match(input,K_KEY,FOLLOW_K_KEY_in_cfamColumns2441); 
					match(input,142,FOLLOW_142_in_cfamColumns2443); 
					pushFollow(FOLLOW_pkDef_in_cfamColumns2445);
					pkDef(expr);
					state._fsp--;

					// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:525:39: ( ',' c= ident )*
					loop51:
					while (true) {
						int alt51=2;
						int LA51_0 = input.LA(1);
						if ( (LA51_0==145) ) {
							alt51=1;
						}

						switch (alt51) {
						case 1 :
							// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:525:40: ',' c= ident
							{
							match(input,145,FOLLOW_145_in_cfamColumns2449); 
							pushFollow(FOLLOW_ident_in_cfamColumns2453);
							c=ident();
							state._fsp--;

							 expr.addColumnAlias(c); 
							}
							break;

						default :
							break loop51;
						}
					}

					match(input,143,FOLLOW_143_in_cfamColumns2460); 
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
	// $ANTLR end "cfamColumns"



	// $ANTLR start "pkDef"
	// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:528:1: pkDef[CreateTableStatement.RawStatement expr] : (k= ident | '(' k1= ident ( ',' kn= ident )* ')' );
	public final void pkDef(CreateTableStatement.RawStatement expr) throws RecognitionException {
		ColumnIdentifier k =null;
		ColumnIdentifier k1 =null;
		ColumnIdentifier kn =null;

		try {
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:529:5: (k= ident | '(' k1= ident ( ',' kn= ident )* ')' )
			int alt54=2;
			int LA54_0 = input.LA(1);
			if ( (LA54_0==IDENT||LA54_0==K_ALL||LA54_0==K_AS||LA54_0==K_ASCII||(LA54_0 >= K_BIGINT && LA54_0 <= K_BOOLEAN)||LA54_0==K_CLUSTERING||(LA54_0 >= K_COMPACT && LA54_0 <= K_COUNTER)||(LA54_0 >= K_CUSTOM && LA54_0 <= K_DECIMAL)||(LA54_0 >= K_DISTINCT && LA54_0 <= K_DOUBLE)||(LA54_0 >= K_EXISTS && LA54_0 <= K_FLOAT)||LA54_0==K_FROZEN||LA54_0==K_INET||LA54_0==K_INT||(LA54_0 >= K_KEY && LA54_0 <= K_KEYS)||LA54_0==K_KEYSPACES||(LA54_0 >= K_LIST && LA54_0 <= K_MAP)||LA54_0==K_NOSUPERUSER||(LA54_0 >= K_PASSWORD && LA54_0 <= K_PERMISSIONS)||(LA54_0 >= K_STATIC && LA54_0 <= K_TIMEUUID)||LA54_0==K_TRIGGER||(LA54_0 >= K_TTL && LA54_0 <= K_TYPE)||(LA54_0 >= K_USER && LA54_0 <= K_USERS)||(LA54_0 >= K_UUID && LA54_0 <= K_VARINT)||LA54_0==K_WRITETIME||LA54_0==QUOTED_NAME) ) {
				alt54=1;
			}
			else if ( (LA54_0==142) ) {
				alt54=2;
			}

			else {
				NoViableAltException nvae =
					new NoViableAltException("", 54, 0, input);
				throw nvae;
			}

			switch (alt54) {
				case 1 :
					// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:529:7: k= ident
					{
					pushFollow(FOLLOW_ident_in_pkDef2480);
					k=ident();
					state._fsp--;

					 expr.addKeyAliases(Collections.singletonList(k)); 
					}
					break;
				case 2 :
					// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:530:7: '(' k1= ident ( ',' kn= ident )* ')'
					{
					match(input,142,FOLLOW_142_in_pkDef2490); 
					 List<ColumnIdentifier> l = new ArrayList<ColumnIdentifier>(); 
					pushFollow(FOLLOW_ident_in_pkDef2496);
					k1=ident();
					state._fsp--;

					 l.add(k1); 
					// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:530:101: ( ',' kn= ident )*
					loop53:
					while (true) {
						int alt53=2;
						int LA53_0 = input.LA(1);
						if ( (LA53_0==145) ) {
							alt53=1;
						}

						switch (alt53) {
						case 1 :
							// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:530:103: ',' kn= ident
							{
							match(input,145,FOLLOW_145_in_pkDef2502); 
							pushFollow(FOLLOW_ident_in_pkDef2506);
							kn=ident();
							state._fsp--;

							 l.add(kn); 
							}
							break;

						default :
							break loop53;
						}
					}

					match(input,143,FOLLOW_143_in_pkDef2513); 
					 expr.addKeyAliases(l); 
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
	// $ANTLR end "pkDef"



	// $ANTLR start "cfamProperty"
	// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:533:1: cfamProperty[CreateTableStatement.RawStatement expr] : ( property[expr.properties] | K_COMPACT K_STORAGE | K_CLUSTERING K_ORDER K_BY '(' cfamOrdering[expr] ( ',' cfamOrdering[expr] )* ')' );
	public final void cfamProperty(CreateTableStatement.RawStatement expr) throws RecognitionException {
		try {
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:534:5: ( property[expr.properties] | K_COMPACT K_STORAGE | K_CLUSTERING K_ORDER K_BY '(' cfamOrdering[expr] ( ',' cfamOrdering[expr] )* ')' )
			int alt56=3;
			switch ( input.LA(1) ) {
			case IDENT:
			case K_ALL:
			case K_AS:
			case K_ASCII:
			case K_BIGINT:
			case K_BLOB:
			case K_BOOLEAN:
			case K_CONTAINS:
			case K_COUNT:
			case K_COUNTER:
			case K_CUSTOM:
			case K_DECIMAL:
			case K_DISTINCT:
			case K_DOUBLE:
			case K_EXISTS:
			case K_FILTERING:
			case K_FLOAT:
			case K_FROZEN:
			case K_INET:
			case K_INT:
			case K_KEY:
			case K_KEYS:
			case K_KEYSPACES:
			case K_LIST:
			case K_MAP:
			case K_NOSUPERUSER:
			case K_PASSWORD:
			case K_PERMISSION:
			case K_PERMISSIONS:
			case K_STATIC:
			case K_STORAGE:
			case K_SUPERUSER:
			case K_TEXT:
			case K_TIMESTAMP:
			case K_TIMEUUID:
			case K_TRIGGER:
			case K_TTL:
			case K_TUPLE:
			case K_TYPE:
			case K_USER:
			case K_USERS:
			case K_UUID:
			case K_VALUES:
			case K_VARCHAR:
			case K_VARINT:
			case K_WRITETIME:
			case QUOTED_NAME:
				{
				alt56=1;
				}
				break;
			case K_COMPACT:
				{
				int LA56_2 = input.LA(2);
				if ( (LA56_2==K_STORAGE) ) {
					alt56=2;
				}
				else if ( (LA56_2==152) ) {
					alt56=1;
				}

				else {
					int nvaeMark = input.mark();
					try {
						input.consume();
						NoViableAltException nvae =
							new NoViableAltException("", 56, 2, input);
						throw nvae;
					} finally {
						input.rewind(nvaeMark);
					}
				}

				}
				break;
			case K_CLUSTERING:
				{
				int LA56_3 = input.LA(2);
				if ( (LA56_3==K_ORDER) ) {
					alt56=3;
				}
				else if ( (LA56_3==152) ) {
					alt56=1;
				}

				else {
					int nvaeMark = input.mark();
					try {
						input.consume();
						NoViableAltException nvae =
							new NoViableAltException("", 56, 3, input);
						throw nvae;
					} finally {
						input.rewind(nvaeMark);
					}
				}

				}
				break;
			default:
				NoViableAltException nvae =
					new NoViableAltException("", 56, 0, input);
				throw nvae;
			}
			switch (alt56) {
				case 1 :
					// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:534:7: property[expr.properties]
					{
					pushFollow(FOLLOW_property_in_cfamProperty2533);
					property(expr.properties);
					state._fsp--;

					}
					break;
				case 2 :
					// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:535:7: K_COMPACT K_STORAGE
					{
					match(input,K_COMPACT,FOLLOW_K_COMPACT_in_cfamProperty2542); 
					match(input,K_STORAGE,FOLLOW_K_STORAGE_in_cfamProperty2544); 
					 expr.setCompactStorage(); 
					}
					break;
				case 3 :
					// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:536:7: K_CLUSTERING K_ORDER K_BY '(' cfamOrdering[expr] ( ',' cfamOrdering[expr] )* ')'
					{
					match(input,K_CLUSTERING,FOLLOW_K_CLUSTERING_in_cfamProperty2554); 
					match(input,K_ORDER,FOLLOW_K_ORDER_in_cfamProperty2556); 
					match(input,K_BY,FOLLOW_K_BY_in_cfamProperty2558); 
					match(input,142,FOLLOW_142_in_cfamProperty2560); 
					pushFollow(FOLLOW_cfamOrdering_in_cfamProperty2562);
					cfamOrdering(expr);
					state._fsp--;

					// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:536:56: ( ',' cfamOrdering[expr] )*
					loop55:
					while (true) {
						int alt55=2;
						int LA55_0 = input.LA(1);
						if ( (LA55_0==145) ) {
							alt55=1;
						}

						switch (alt55) {
						case 1 :
							// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:536:57: ',' cfamOrdering[expr]
							{
							match(input,145,FOLLOW_145_in_cfamProperty2566); 
							pushFollow(FOLLOW_cfamOrdering_in_cfamProperty2568);
							cfamOrdering(expr);
							state._fsp--;

							}
							break;

						default :
							break loop55;
						}
					}

					match(input,143,FOLLOW_143_in_cfamProperty2573); 
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
	// $ANTLR end "cfamProperty"



	// $ANTLR start "cfamOrdering"
	// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:539:1: cfamOrdering[CreateTableStatement.RawStatement expr] : k= ident ( K_ASC | K_DESC ) ;
	public final void cfamOrdering(CreateTableStatement.RawStatement expr) throws RecognitionException {
		ColumnIdentifier k =null;

		 boolean reversed=false; 
		try {
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:541:5: (k= ident ( K_ASC | K_DESC ) )
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:541:7: k= ident ( K_ASC | K_DESC )
			{
			pushFollow(FOLLOW_ident_in_cfamOrdering2601);
			k=ident();
			state._fsp--;

			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:541:15: ( K_ASC | K_DESC )
			int alt57=2;
			int LA57_0 = input.LA(1);
			if ( (LA57_0==K_ASC) ) {
				alt57=1;
			}
			else if ( (LA57_0==K_DESC) ) {
				alt57=2;
			}

			else {
				NoViableAltException nvae =
					new NoViableAltException("", 57, 0, input);
				throw nvae;
			}

			switch (alt57) {
				case 1 :
					// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:541:16: K_ASC
					{
					match(input,K_ASC,FOLLOW_K_ASC_in_cfamOrdering2604); 
					}
					break;
				case 2 :
					// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:541:24: K_DESC
					{
					match(input,K_DESC,FOLLOW_K_DESC_in_cfamOrdering2608); 
					 reversed=true;
					}
					break;

			}

			 expr.setOrdering(k, reversed); 
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
	// $ANTLR end "cfamOrdering"



	// $ANTLR start "createTypeStatement"
	// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:552:1: createTypeStatement returns [CreateTypeStatement expr] : K_CREATE K_TYPE ( K_IF K_NOT K_EXISTS )? tn= userTypeName '(' typeColumns[expr] ( ',' ( typeColumns[expr] )? )* ')' ;
	public final CreateTypeStatement createTypeStatement() throws RecognitionException {
		CreateTypeStatement expr = null;


		UTName tn =null;

		 boolean ifNotExists = false; 
		try {
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:554:5: ( K_CREATE K_TYPE ( K_IF K_NOT K_EXISTS )? tn= userTypeName '(' typeColumns[expr] ( ',' ( typeColumns[expr] )? )* ')' )
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:554:7: K_CREATE K_TYPE ( K_IF K_NOT K_EXISTS )? tn= userTypeName '(' typeColumns[expr] ( ',' ( typeColumns[expr] )? )* ')'
			{
			match(input,K_CREATE,FOLLOW_K_CREATE_in_createTypeStatement2647); 
			match(input,K_TYPE,FOLLOW_K_TYPE_in_createTypeStatement2649); 
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:554:23: ( K_IF K_NOT K_EXISTS )?
			int alt58=2;
			int LA58_0 = input.LA(1);
			if ( (LA58_0==K_IF) ) {
				alt58=1;
			}
			switch (alt58) {
				case 1 :
					// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:554:24: K_IF K_NOT K_EXISTS
					{
					match(input,K_IF,FOLLOW_K_IF_in_createTypeStatement2652); 
					match(input,K_NOT,FOLLOW_K_NOT_in_createTypeStatement2654); 
					match(input,K_EXISTS,FOLLOW_K_EXISTS_in_createTypeStatement2656); 
					 ifNotExists = true; 
					}
					break;

			}

			pushFollow(FOLLOW_userTypeName_in_createTypeStatement2674);
			tn=userTypeName();
			state._fsp--;

			 expr = new CreateTypeStatement(tn, ifNotExists); 
			match(input,142,FOLLOW_142_in_createTypeStatement2687); 
			pushFollow(FOLLOW_typeColumns_in_createTypeStatement2689);
			typeColumns(expr);
			state._fsp--;

			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:556:32: ( ',' ( typeColumns[expr] )? )*
			loop60:
			while (true) {
				int alt60=2;
				int LA60_0 = input.LA(1);
				if ( (LA60_0==145) ) {
					alt60=1;
				}

				switch (alt60) {
				case 1 :
					// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:556:34: ',' ( typeColumns[expr] )?
					{
					match(input,145,FOLLOW_145_in_createTypeStatement2694); 
					// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:556:38: ( typeColumns[expr] )?
					int alt59=2;
					int LA59_0 = input.LA(1);
					if ( (LA59_0==IDENT||LA59_0==K_ALL||LA59_0==K_AS||LA59_0==K_ASCII||(LA59_0 >= K_BIGINT && LA59_0 <= K_BOOLEAN)||LA59_0==K_CLUSTERING||(LA59_0 >= K_COMPACT && LA59_0 <= K_COUNTER)||(LA59_0 >= K_CUSTOM && LA59_0 <= K_DECIMAL)||(LA59_0 >= K_DISTINCT && LA59_0 <= K_DOUBLE)||(LA59_0 >= K_EXISTS && LA59_0 <= K_FLOAT)||LA59_0==K_FROZEN||LA59_0==K_INET||LA59_0==K_INT||(LA59_0 >= K_KEY && LA59_0 <= K_KEYS)||LA59_0==K_KEYSPACES||(LA59_0 >= K_LIST && LA59_0 <= K_MAP)||LA59_0==K_NOSUPERUSER||(LA59_0 >= K_PASSWORD && LA59_0 <= K_PERMISSIONS)||(LA59_0 >= K_STATIC && LA59_0 <= K_TIMEUUID)||LA59_0==K_TRIGGER||(LA59_0 >= K_TTL && LA59_0 <= K_TYPE)||(LA59_0 >= K_USER && LA59_0 <= K_USERS)||(LA59_0 >= K_UUID && LA59_0 <= K_VARINT)||LA59_0==K_WRITETIME||LA59_0==QUOTED_NAME) ) {
						alt59=1;
					}
					switch (alt59) {
						case 1 :
							// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:556:38: typeColumns[expr]
							{
							pushFollow(FOLLOW_typeColumns_in_createTypeStatement2696);
							typeColumns(expr);
							state._fsp--;

							}
							break;

					}

					}
					break;

				default :
					break loop60;
				}
			}

			match(input,143,FOLLOW_143_in_createTypeStatement2703); 
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
	// $ANTLR end "createTypeStatement"



	// $ANTLR start "typeColumns"
	// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:559:1: typeColumns[CreateTypeStatement expr] : k= ident v= comparatorType ;
	public final void typeColumns(CreateTypeStatement expr) throws RecognitionException {
		ColumnIdentifier k =null;
		CQL3Type.Raw v =null;

		try {
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:560:5: (k= ident v= comparatorType )
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:560:7: k= ident v= comparatorType
			{
			pushFollow(FOLLOW_ident_in_typeColumns2723);
			k=ident();
			state._fsp--;

			pushFollow(FOLLOW_comparatorType_in_typeColumns2727);
			v=comparatorType();
			state._fsp--;

			 expr.addDefinition(k, v); 
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
	// $ANTLR end "typeColumns"



	// $ANTLR start "createIndexStatement"
	// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:568:1: createIndexStatement returns [CreateIndexStatement expr] : K_CREATE ( K_CUSTOM )? K_INDEX ( K_IF K_NOT K_EXISTS )? ( idxName[name] )? K_ON cf= columnFamilyName '(' id= indexIdent ')' ( K_USING cls= STRING_LITERAL )? ( K_WITH properties[props] )? ;
	public final CreateIndexStatement createIndexStatement() throws RecognitionException {
		CreateIndexStatement expr = null;


		Token cls=null;
		CFName cf =null;
		IndexTarget.Raw id =null;


		        IndexPropDefs props = new IndexPropDefs();
		        boolean ifNotExists = false;
		        IndexName name = new IndexName();
		    
		try {
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:574:5: ( K_CREATE ( K_CUSTOM )? K_INDEX ( K_IF K_NOT K_EXISTS )? ( idxName[name] )? K_ON cf= columnFamilyName '(' id= indexIdent ')' ( K_USING cls= STRING_LITERAL )? ( K_WITH properties[props] )? )
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:574:7: K_CREATE ( K_CUSTOM )? K_INDEX ( K_IF K_NOT K_EXISTS )? ( idxName[name] )? K_ON cf= columnFamilyName '(' id= indexIdent ')' ( K_USING cls= STRING_LITERAL )? ( K_WITH properties[props] )?
			{
			match(input,K_CREATE,FOLLOW_K_CREATE_in_createIndexStatement2762); 
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:574:16: ( K_CUSTOM )?
			int alt61=2;
			int LA61_0 = input.LA(1);
			if ( (LA61_0==K_CUSTOM) ) {
				alt61=1;
			}
			switch (alt61) {
				case 1 :
					// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:574:17: K_CUSTOM
					{
					match(input,K_CUSTOM,FOLLOW_K_CUSTOM_in_createIndexStatement2765); 
					 props.isCustom = true; 
					}
					break;

			}

			match(input,K_INDEX,FOLLOW_K_INDEX_in_createIndexStatement2771); 
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:574:63: ( K_IF K_NOT K_EXISTS )?
			int alt62=2;
			int LA62_0 = input.LA(1);
			if ( (LA62_0==K_IF) ) {
				alt62=1;
			}
			switch (alt62) {
				case 1 :
					// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:574:64: K_IF K_NOT K_EXISTS
					{
					match(input,K_IF,FOLLOW_K_IF_in_createIndexStatement2774); 
					match(input,K_NOT,FOLLOW_K_NOT_in_createIndexStatement2776); 
					match(input,K_EXISTS,FOLLOW_K_EXISTS_in_createIndexStatement2778); 
					 ifNotExists = true; 
					}
					break;

			}

			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:575:9: ( idxName[name] )?
			int alt63=2;
			int LA63_0 = input.LA(1);
			if ( (LA63_0==IDENT||LA63_0==K_ALL||LA63_0==K_AS||LA63_0==K_ASCII||(LA63_0 >= K_BIGINT && LA63_0 <= K_BOOLEAN)||LA63_0==K_CLUSTERING||(LA63_0 >= K_COMPACT && LA63_0 <= K_COUNTER)||(LA63_0 >= K_CUSTOM && LA63_0 <= K_DECIMAL)||(LA63_0 >= K_DISTINCT && LA63_0 <= K_DOUBLE)||(LA63_0 >= K_EXISTS && LA63_0 <= K_FLOAT)||LA63_0==K_FROZEN||LA63_0==K_INET||LA63_0==K_INT||(LA63_0 >= K_KEY && LA63_0 <= K_KEYS)||LA63_0==K_KEYSPACES||(LA63_0 >= K_LIST && LA63_0 <= K_MAP)||LA63_0==K_NOSUPERUSER||(LA63_0 >= K_PASSWORD && LA63_0 <= K_PERMISSIONS)||(LA63_0 >= K_STATIC && LA63_0 <= K_TIMEUUID)||LA63_0==K_TRIGGER||(LA63_0 >= K_TTL && LA63_0 <= K_TYPE)||(LA63_0 >= K_USER && LA63_0 <= K_USERS)||(LA63_0 >= K_UUID && LA63_0 <= K_VARINT)||LA63_0==K_WRITETIME||(LA63_0 >= QMARK && LA63_0 <= QUOTED_NAME)) ) {
				alt63=1;
			}
			switch (alt63) {
				case 1 :
					// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:575:10: idxName[name]
					{
					pushFollow(FOLLOW_idxName_in_createIndexStatement2794);
					idxName(name);
					state._fsp--;

					}
					break;

			}

			match(input,K_ON,FOLLOW_K_ON_in_createIndexStatement2799); 
			pushFollow(FOLLOW_columnFamilyName_in_createIndexStatement2803);
			cf=columnFamilyName();
			state._fsp--;

			match(input,142,FOLLOW_142_in_createIndexStatement2805); 
			pushFollow(FOLLOW_indexIdent_in_createIndexStatement2809);
			id=indexIdent();
			state._fsp--;

			match(input,143,FOLLOW_143_in_createIndexStatement2811); 
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:576:9: ( K_USING cls= STRING_LITERAL )?
			int alt64=2;
			int LA64_0 = input.LA(1);
			if ( (LA64_0==K_USING) ) {
				alt64=1;
			}
			switch (alt64) {
				case 1 :
					// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:576:10: K_USING cls= STRING_LITERAL
					{
					match(input,K_USING,FOLLOW_K_USING_in_createIndexStatement2822); 
					cls=(Token)match(input,STRING_LITERAL,FOLLOW_STRING_LITERAL_in_createIndexStatement2826); 
					 props.customClass = (cls!=null?cls.getText():null); 
					}
					break;

			}

			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:577:9: ( K_WITH properties[props] )?
			int alt65=2;
			int LA65_0 = input.LA(1);
			if ( (LA65_0==K_WITH) ) {
				alt65=1;
			}
			switch (alt65) {
				case 1 :
					// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:577:10: K_WITH properties[props]
					{
					match(input,K_WITH,FOLLOW_K_WITH_in_createIndexStatement2841); 
					pushFollow(FOLLOW_properties_in_createIndexStatement2843);
					properties(props);
					state._fsp--;

					}
					break;

			}

			 expr = new CreateIndexStatement(cf, name, id, props, ifNotExists); 
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



	// $ANTLR start "indexIdent"
	// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:581:1: indexIdent returns [IndexTarget.Raw id] : (c= cident | K_KEYS '(' c= cident ')' | K_FULL '(' c= cident ')' );
	public final IndexTarget.Raw indexIdent() throws RecognitionException {
		IndexTarget.Raw id = null;


		ColumnIdentifier.Raw c =null;

		try {
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:582:5: (c= cident | K_KEYS '(' c= cident ')' | K_FULL '(' c= cident ')' )
			int alt66=3;
			switch ( input.LA(1) ) {
			case IDENT:
			case K_ALL:
			case K_AS:
			case K_ASCII:
			case K_BIGINT:
			case K_BLOB:
			case K_BOOLEAN:
			case K_CLUSTERING:
			case K_COMPACT:
			case K_CONTAINS:
			case K_COUNT:
			case K_COUNTER:
			case K_CUSTOM:
			case K_DECIMAL:
			case K_DISTINCT:
			case K_DOUBLE:
			case K_EXISTS:
			case K_FILTERING:
			case K_FLOAT:
			case K_FROZEN:
			case K_INET:
			case K_INT:
			case K_KEY:
			case K_KEYSPACES:
			case K_LIST:
			case K_MAP:
			case K_NOSUPERUSER:
			case K_PASSWORD:
			case K_PERMISSION:
			case K_PERMISSIONS:
			case K_STATIC:
			case K_STORAGE:
			case K_SUPERUSER:
			case K_TEXT:
			case K_TIMESTAMP:
			case K_TIMEUUID:
			case K_TRIGGER:
			case K_TTL:
			case K_TUPLE:
			case K_TYPE:
			case K_USER:
			case K_USERS:
			case K_UUID:
			case K_VALUES:
			case K_VARCHAR:
			case K_VARINT:
			case K_WRITETIME:
			case QUOTED_NAME:
				{
				alt66=1;
				}
				break;
			case K_KEYS:
				{
				int LA66_2 = input.LA(2);
				if ( (LA66_2==142) ) {
					alt66=2;
				}
				else if ( (LA66_2==143) ) {
					alt66=1;
				}

				else {
					int nvaeMark = input.mark();
					try {
						input.consume();
						NoViableAltException nvae =
							new NoViableAltException("", 66, 2, input);
						throw nvae;
					} finally {
						input.rewind(nvaeMark);
					}
				}

				}
				break;
			case K_FULL:
				{
				alt66=3;
				}
				break;
			default:
				NoViableAltException nvae =
					new NoViableAltException("", 66, 0, input);
				throw nvae;
			}
			switch (alt66) {
				case 1 :
					// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:582:7: c= cident
					{
					pushFollow(FOLLOW_cident_in_indexIdent2877);
					c=cident();
					state._fsp--;

					 id = IndexTarget.Raw.valuesOf(c); 
					}
					break;
				case 2 :
					// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:583:7: K_KEYS '(' c= cident ')'
					{
					match(input,K_KEYS,FOLLOW_K_KEYS_in_indexIdent2902); 
					match(input,142,FOLLOW_142_in_indexIdent2904); 
					pushFollow(FOLLOW_cident_in_indexIdent2908);
					c=cident();
					state._fsp--;

					match(input,143,FOLLOW_143_in_indexIdent2910); 
					 id = IndexTarget.Raw.keysOf(c); 
					}
					break;
				case 3 :
					// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:584:7: K_FULL '(' c= cident ')'
					{
					match(input,K_FULL,FOLLOW_K_FULL_in_indexIdent2920); 
					match(input,142,FOLLOW_142_in_indexIdent2922); 
					pushFollow(FOLLOW_cident_in_indexIdent2926);
					c=cident();
					state._fsp--;

					match(input,143,FOLLOW_143_in_indexIdent2928); 
					 id = IndexTarget.Raw.fullCollection(c); 
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
		return id;
	}
	// $ANTLR end "indexIdent"



	// $ANTLR start "createTriggerStatement"
	// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:591:1: createTriggerStatement returns [CreateTriggerStatement expr] : K_CREATE K_TRIGGER ( K_IF K_NOT K_EXISTS )? (name= cident ) K_ON cf= columnFamilyName K_USING cls= STRING_LITERAL ;
	public final CreateTriggerStatement createTriggerStatement() throws RecognitionException {
		CreateTriggerStatement expr = null;


		Token cls=null;
		ColumnIdentifier.Raw name =null;
		CFName cf =null;


		        boolean ifNotExists = false;
		    
		try {
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:595:5: ( K_CREATE K_TRIGGER ( K_IF K_NOT K_EXISTS )? (name= cident ) K_ON cf= columnFamilyName K_USING cls= STRING_LITERAL )
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:595:7: K_CREATE K_TRIGGER ( K_IF K_NOT K_EXISTS )? (name= cident ) K_ON cf= columnFamilyName K_USING cls= STRING_LITERAL
			{
			match(input,K_CREATE,FOLLOW_K_CREATE_in_createTriggerStatement2963); 
			match(input,K_TRIGGER,FOLLOW_K_TRIGGER_in_createTriggerStatement2965); 
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:595:26: ( K_IF K_NOT K_EXISTS )?
			int alt67=2;
			int LA67_0 = input.LA(1);
			if ( (LA67_0==K_IF) ) {
				alt67=1;
			}
			switch (alt67) {
				case 1 :
					// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:595:27: K_IF K_NOT K_EXISTS
					{
					match(input,K_IF,FOLLOW_K_IF_in_createTriggerStatement2968); 
					match(input,K_NOT,FOLLOW_K_NOT_in_createTriggerStatement2970); 
					match(input,K_EXISTS,FOLLOW_K_EXISTS_in_createTriggerStatement2972); 
					 ifNotExists = true; 
					}
					break;

			}

			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:595:74: (name= cident )
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:595:75: name= cident
			{
			pushFollow(FOLLOW_cident_in_createTriggerStatement2982);
			name=cident();
			state._fsp--;

			}

			match(input,K_ON,FOLLOW_K_ON_in_createTriggerStatement2993); 
			pushFollow(FOLLOW_columnFamilyName_in_createTriggerStatement2997);
			cf=columnFamilyName();
			state._fsp--;

			match(input,K_USING,FOLLOW_K_USING_in_createTriggerStatement2999); 
			cls=(Token)match(input,STRING_LITERAL,FOLLOW_STRING_LITERAL_in_createTriggerStatement3003); 
			 expr = new CreateTriggerStatement(cf, name.toString(), (cls!=null?cls.getText():null), ifNotExists); 
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
	// $ANTLR end "createTriggerStatement"



	// $ANTLR start "dropTriggerStatement"
	// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:603:1: dropTriggerStatement returns [DropTriggerStatement expr] : K_DROP K_TRIGGER ( K_IF K_EXISTS )? (name= cident ) K_ON cf= columnFamilyName ;
	public final DropTriggerStatement dropTriggerStatement() throws RecognitionException {
		DropTriggerStatement expr = null;


		ColumnIdentifier.Raw name =null;
		CFName cf =null;

		 boolean ifExists = false; 
		try {
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:605:5: ( K_DROP K_TRIGGER ( K_IF K_EXISTS )? (name= cident ) K_ON cf= columnFamilyName )
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:605:7: K_DROP K_TRIGGER ( K_IF K_EXISTS )? (name= cident ) K_ON cf= columnFamilyName
			{
			match(input,K_DROP,FOLLOW_K_DROP_in_dropTriggerStatement3044); 
			match(input,K_TRIGGER,FOLLOW_K_TRIGGER_in_dropTriggerStatement3046); 
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:605:24: ( K_IF K_EXISTS )?
			int alt68=2;
			int LA68_0 = input.LA(1);
			if ( (LA68_0==K_IF) ) {
				alt68=1;
			}
			switch (alt68) {
				case 1 :
					// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:605:25: K_IF K_EXISTS
					{
					match(input,K_IF,FOLLOW_K_IF_in_dropTriggerStatement3049); 
					match(input,K_EXISTS,FOLLOW_K_EXISTS_in_dropTriggerStatement3051); 
					 ifExists = true; 
					}
					break;

			}

			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:605:63: (name= cident )
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:605:64: name= cident
			{
			pushFollow(FOLLOW_cident_in_dropTriggerStatement3061);
			name=cident();
			state._fsp--;

			}

			match(input,K_ON,FOLLOW_K_ON_in_dropTriggerStatement3064); 
			pushFollow(FOLLOW_columnFamilyName_in_dropTriggerStatement3068);
			cf=columnFamilyName();
			state._fsp--;

			 expr = new DropTriggerStatement(cf, name.toString(), ifExists); 
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
	// $ANTLR end "dropTriggerStatement"



	// $ANTLR start "alterKeyspaceStatement"
	// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:612:1: alterKeyspaceStatement returns [AlterKeyspaceStatement expr] : K_ALTER K_KEYSPACE ks= keyspaceName K_WITH properties[attrs] ;
	public final AlterKeyspaceStatement alterKeyspaceStatement() throws RecognitionException {
		AlterKeyspaceStatement expr = null;


		String ks =null;

		 KSPropDefs attrs = new KSPropDefs(); 
		try {
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:614:5: ( K_ALTER K_KEYSPACE ks= keyspaceName K_WITH properties[attrs] )
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:614:7: K_ALTER K_KEYSPACE ks= keyspaceName K_WITH properties[attrs]
			{
			match(input,K_ALTER,FOLLOW_K_ALTER_in_alterKeyspaceStatement3108); 
			match(input,K_KEYSPACE,FOLLOW_K_KEYSPACE_in_alterKeyspaceStatement3110); 
			pushFollow(FOLLOW_keyspaceName_in_alterKeyspaceStatement3114);
			ks=keyspaceName();
			state._fsp--;

			match(input,K_WITH,FOLLOW_K_WITH_in_alterKeyspaceStatement3124); 
			pushFollow(FOLLOW_properties_in_alterKeyspaceStatement3126);
			properties(attrs);
			state._fsp--;

			 expr = new AlterKeyspaceStatement(ks, attrs); 
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
	// $ANTLR end "alterKeyspaceStatement"



	// $ANTLR start "alterTableStatement"
	// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:626:1: alterTableStatement returns [AlterTableStatement expr] : K_ALTER K_COLUMNFAMILY cf= columnFamilyName ( K_ALTER id= cident K_TYPE v= comparatorType | K_ADD id= cident v= comparatorType ( K_STATIC )? | K_DROP id= cident | K_WITH properties[props] | K_RENAME id1= cident K_TO toId1= cident ( K_AND idn= cident K_TO toIdn= cident )* ) ;
	public final AlterTableStatement alterTableStatement() throws RecognitionException {
		AlterTableStatement expr = null;


		CFName cf =null;
		ColumnIdentifier.Raw id =null;
		CQL3Type.Raw v =null;
		ColumnIdentifier.Raw id1 =null;
		ColumnIdentifier.Raw toId1 =null;
		ColumnIdentifier.Raw idn =null;
		ColumnIdentifier.Raw toIdn =null;


		        AlterTableStatement.Type type = null;
		        CFPropDefs props = new CFPropDefs();
		        Map<ColumnIdentifier.Raw, ColumnIdentifier.Raw> renames = new HashMap<ColumnIdentifier.Raw, ColumnIdentifier.Raw>();
		        boolean isStatic = false;
		    
		try {
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:633:5: ( K_ALTER K_COLUMNFAMILY cf= columnFamilyName ( K_ALTER id= cident K_TYPE v= comparatorType | K_ADD id= cident v= comparatorType ( K_STATIC )? | K_DROP id= cident | K_WITH properties[props] | K_RENAME id1= cident K_TO toId1= cident ( K_AND idn= cident K_TO toIdn= cident )* ) )
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:633:7: K_ALTER K_COLUMNFAMILY cf= columnFamilyName ( K_ALTER id= cident K_TYPE v= comparatorType | K_ADD id= cident v= comparatorType ( K_STATIC )? | K_DROP id= cident | K_WITH properties[props] | K_RENAME id1= cident K_TO toId1= cident ( K_AND idn= cident K_TO toIdn= cident )* )
			{
			match(input,K_ALTER,FOLLOW_K_ALTER_in_alterTableStatement3162); 
			match(input,K_COLUMNFAMILY,FOLLOW_K_COLUMNFAMILY_in_alterTableStatement3164); 
			pushFollow(FOLLOW_columnFamilyName_in_alterTableStatement3168);
			cf=columnFamilyName();
			state._fsp--;

			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:634:11: ( K_ALTER id= cident K_TYPE v= comparatorType | K_ADD id= cident v= comparatorType ( K_STATIC )? | K_DROP id= cident | K_WITH properties[props] | K_RENAME id1= cident K_TO toId1= cident ( K_AND idn= cident K_TO toIdn= cident )* )
			int alt71=5;
			switch ( input.LA(1) ) {
			case K_ALTER:
				{
				alt71=1;
				}
				break;
			case K_ADD:
				{
				alt71=2;
				}
				break;
			case K_DROP:
				{
				alt71=3;
				}
				break;
			case K_WITH:
				{
				alt71=4;
				}
				break;
			case K_RENAME:
				{
				alt71=5;
				}
				break;
			default:
				NoViableAltException nvae =
					new NoViableAltException("", 71, 0, input);
				throw nvae;
			}
			switch (alt71) {
				case 1 :
					// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:634:13: K_ALTER id= cident K_TYPE v= comparatorType
					{
					match(input,K_ALTER,FOLLOW_K_ALTER_in_alterTableStatement3182); 
					pushFollow(FOLLOW_cident_in_alterTableStatement3186);
					id=cident();
					state._fsp--;

					match(input,K_TYPE,FOLLOW_K_TYPE_in_alterTableStatement3188); 
					pushFollow(FOLLOW_comparatorType_in_alterTableStatement3192);
					v=comparatorType();
					state._fsp--;

					 type = AlterTableStatement.Type.ALTER; 
					}
					break;
				case 2 :
					// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:635:13: K_ADD id= cident v= comparatorType ( K_STATIC )?
					{
					match(input,K_ADD,FOLLOW_K_ADD_in_alterTableStatement3208); 
					pushFollow(FOLLOW_cident_in_alterTableStatement3214);
					id=cident();
					state._fsp--;

					pushFollow(FOLLOW_comparatorType_in_alterTableStatement3218);
					v=comparatorType();
					state._fsp--;

					// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:635:48: ( K_STATIC )?
					int alt69=2;
					int LA69_0 = input.LA(1);
					if ( (LA69_0==K_STATIC) ) {
						alt69=1;
					}
					switch (alt69) {
						case 1 :
							// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:635:49: K_STATIC
							{
							 isStatic=true; 
							match(input,K_STATIC,FOLLOW_K_STATIC_in_alterTableStatement3223); 
							}
							break;

					}

					 type = AlterTableStatement.Type.ADD; 
					}
					break;
				case 3 :
					// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:636:13: K_DROP id= cident
					{
					match(input,K_DROP,FOLLOW_K_DROP_in_alterTableStatement3241); 
					pushFollow(FOLLOW_cident_in_alterTableStatement3246);
					id=cident();
					state._fsp--;

					 type = AlterTableStatement.Type.DROP; 
					}
					break;
				case 4 :
					// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:637:13: K_WITH properties[props]
					{
					match(input,K_WITH,FOLLOW_K_WITH_in_alterTableStatement3286); 
					pushFollow(FOLLOW_properties_in_alterTableStatement3289);
					properties(props);
					state._fsp--;

					 type = AlterTableStatement.Type.OPTS; 
					}
					break;
				case 5 :
					// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:638:13: K_RENAME id1= cident K_TO toId1= cident ( K_AND idn= cident K_TO toIdn= cident )*
					{
					match(input,K_RENAME,FOLLOW_K_RENAME_in_alterTableStatement3322); 
					 type = AlterTableStatement.Type.RENAME; 
					pushFollow(FOLLOW_cident_in_alterTableStatement3376);
					id1=cident();
					state._fsp--;

					match(input,K_TO,FOLLOW_K_TO_in_alterTableStatement3378); 
					pushFollow(FOLLOW_cident_in_alterTableStatement3382);
					toId1=cident();
					state._fsp--;

					 renames.put(id1, toId1); 
					// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:640:16: ( K_AND idn= cident K_TO toIdn= cident )*
					loop70:
					while (true) {
						int alt70=2;
						int LA70_0 = input.LA(1);
						if ( (LA70_0==K_AND) ) {
							alt70=1;
						}

						switch (alt70) {
						case 1 :
							// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:640:18: K_AND idn= cident K_TO toIdn= cident
							{
							match(input,K_AND,FOLLOW_K_AND_in_alterTableStatement3403); 
							pushFollow(FOLLOW_cident_in_alterTableStatement3407);
							idn=cident();
							state._fsp--;

							match(input,K_TO,FOLLOW_K_TO_in_alterTableStatement3409); 
							pushFollow(FOLLOW_cident_in_alterTableStatement3413);
							toIdn=cident();
							state._fsp--;

							 renames.put(idn, toIdn); 
							}
							break;

						default :
							break loop70;
						}
					}

					}
					break;

			}


			        expr = new AlterTableStatement(cf, type, id, v, props, renames, isStatic);
			    
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



	// $ANTLR start "alterTypeStatement"
	// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:652:1: alterTypeStatement returns [AlterTypeStatement expr] : K_ALTER K_TYPE name= userTypeName ( K_ALTER f= ident K_TYPE v= comparatorType | K_ADD f= ident v= comparatorType | K_RENAME id1= ident K_TO toId1= ident ( K_AND idn= ident K_TO toIdn= ident )* ) ;
	public final AlterTypeStatement alterTypeStatement() throws RecognitionException {
		AlterTypeStatement expr = null;


		UTName name =null;
		ColumnIdentifier f =null;
		CQL3Type.Raw v =null;
		ColumnIdentifier id1 =null;
		ColumnIdentifier toId1 =null;
		ColumnIdentifier idn =null;
		ColumnIdentifier toIdn =null;

		try {
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:653:5: ( K_ALTER K_TYPE name= userTypeName ( K_ALTER f= ident K_TYPE v= comparatorType | K_ADD f= ident v= comparatorType | K_RENAME id1= ident K_TO toId1= ident ( K_AND idn= ident K_TO toIdn= ident )* ) )
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:653:7: K_ALTER K_TYPE name= userTypeName ( K_ALTER f= ident K_TYPE v= comparatorType | K_ADD f= ident v= comparatorType | K_RENAME id1= ident K_TO toId1= ident ( K_AND idn= ident K_TO toIdn= ident )* )
			{
			match(input,K_ALTER,FOLLOW_K_ALTER_in_alterTypeStatement3459); 
			match(input,K_TYPE,FOLLOW_K_TYPE_in_alterTypeStatement3461); 
			pushFollow(FOLLOW_userTypeName_in_alterTypeStatement3465);
			name=userTypeName();
			state._fsp--;

			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:654:11: ( K_ALTER f= ident K_TYPE v= comparatorType | K_ADD f= ident v= comparatorType | K_RENAME id1= ident K_TO toId1= ident ( K_AND idn= ident K_TO toIdn= ident )* )
			int alt73=3;
			switch ( input.LA(1) ) {
			case K_ALTER:
				{
				alt73=1;
				}
				break;
			case K_ADD:
				{
				alt73=2;
				}
				break;
			case K_RENAME:
				{
				alt73=3;
				}
				break;
			default:
				NoViableAltException nvae =
					new NoViableAltException("", 73, 0, input);
				throw nvae;
			}
			switch (alt73) {
				case 1 :
					// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:654:13: K_ALTER f= ident K_TYPE v= comparatorType
					{
					match(input,K_ALTER,FOLLOW_K_ALTER_in_alterTypeStatement3479); 
					pushFollow(FOLLOW_ident_in_alterTypeStatement3483);
					f=ident();
					state._fsp--;

					match(input,K_TYPE,FOLLOW_K_TYPE_in_alterTypeStatement3485); 
					pushFollow(FOLLOW_comparatorType_in_alterTypeStatement3489);
					v=comparatorType();
					state._fsp--;

					 expr = AlterTypeStatement.alter(name, f, v); 
					}
					break;
				case 2 :
					// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:655:13: K_ADD f= ident v= comparatorType
					{
					match(input,K_ADD,FOLLOW_K_ADD_in_alterTypeStatement3505); 
					pushFollow(FOLLOW_ident_in_alterTypeStatement3511);
					f=ident();
					state._fsp--;

					pushFollow(FOLLOW_comparatorType_in_alterTypeStatement3515);
					v=comparatorType();
					state._fsp--;

					 expr = AlterTypeStatement.addition(name, f, v); 
					}
					break;
				case 3 :
					// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:656:13: K_RENAME id1= ident K_TO toId1= ident ( K_AND idn= ident K_TO toIdn= ident )*
					{
					match(input,K_RENAME,FOLLOW_K_RENAME_in_alterTypeStatement3538); 
					 Map<ColumnIdentifier, ColumnIdentifier> renames = new HashMap<ColumnIdentifier, ColumnIdentifier>(); 
					pushFollow(FOLLOW_ident_in_alterTypeStatement3576);
					id1=ident();
					state._fsp--;

					match(input,K_TO,FOLLOW_K_TO_in_alterTypeStatement3578); 
					pushFollow(FOLLOW_ident_in_alterTypeStatement3582);
					toId1=ident();
					state._fsp--;

					 renames.put(id1, toId1); 
					// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:659:18: ( K_AND idn= ident K_TO toIdn= ident )*
					loop72:
					while (true) {
						int alt72=2;
						int LA72_0 = input.LA(1);
						if ( (LA72_0==K_AND) ) {
							alt72=1;
						}

						switch (alt72) {
						case 1 :
							// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:659:20: K_AND idn= ident K_TO toIdn= ident
							{
							match(input,K_AND,FOLLOW_K_AND_in_alterTypeStatement3605); 
							pushFollow(FOLLOW_ident_in_alterTypeStatement3609);
							idn=ident();
							state._fsp--;

							match(input,K_TO,FOLLOW_K_TO_in_alterTypeStatement3611); 
							pushFollow(FOLLOW_ident_in_alterTypeStatement3615);
							toIdn=ident();
							state._fsp--;

							 renames.put(idn, toIdn); 
							}
							break;

						default :
							break loop72;
						}
					}

					 expr = AlterTypeStatement.renames(name, renames); 
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
	// $ANTLR end "alterTypeStatement"



	// $ANTLR start "dropKeyspaceStatement"
	// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:668:1: dropKeyspaceStatement returns [DropKeyspaceStatement ksp] : K_DROP K_KEYSPACE ( K_IF K_EXISTS )? ks= keyspaceName ;
	public final DropKeyspaceStatement dropKeyspaceStatement() throws RecognitionException {
		DropKeyspaceStatement ksp = null;


		String ks =null;

		 boolean ifExists = false; 
		try {
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:670:5: ( K_DROP K_KEYSPACE ( K_IF K_EXISTS )? ks= keyspaceName )
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:670:7: K_DROP K_KEYSPACE ( K_IF K_EXISTS )? ks= keyspaceName
			{
			match(input,K_DROP,FOLLOW_K_DROP_in_dropKeyspaceStatement3682); 
			match(input,K_KEYSPACE,FOLLOW_K_KEYSPACE_in_dropKeyspaceStatement3684); 
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:670:25: ( K_IF K_EXISTS )?
			int alt74=2;
			int LA74_0 = input.LA(1);
			if ( (LA74_0==K_IF) ) {
				alt74=1;
			}
			switch (alt74) {
				case 1 :
					// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:670:26: K_IF K_EXISTS
					{
					match(input,K_IF,FOLLOW_K_IF_in_dropKeyspaceStatement3687); 
					match(input,K_EXISTS,FOLLOW_K_EXISTS_in_dropKeyspaceStatement3689); 
					 ifExists = true; 
					}
					break;

			}

			pushFollow(FOLLOW_keyspaceName_in_dropKeyspaceStatement3698);
			ks=keyspaceName();
			state._fsp--;

			 ksp = new DropKeyspaceStatement(ks, ifExists); 
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



	// $ANTLR start "dropTableStatement"
	// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:676:1: dropTableStatement returns [DropTableStatement stmt] : K_DROP K_COLUMNFAMILY ( K_IF K_EXISTS )? cf= columnFamilyName ;
	public final DropTableStatement dropTableStatement() throws RecognitionException {
		DropTableStatement stmt = null;


		CFName cf =null;

		 boolean ifExists = false; 
		try {
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:678:5: ( K_DROP K_COLUMNFAMILY ( K_IF K_EXISTS )? cf= columnFamilyName )
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:678:7: K_DROP K_COLUMNFAMILY ( K_IF K_EXISTS )? cf= columnFamilyName
			{
			match(input,K_DROP,FOLLOW_K_DROP_in_dropTableStatement3732); 
			match(input,K_COLUMNFAMILY,FOLLOW_K_COLUMNFAMILY_in_dropTableStatement3734); 
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:678:29: ( K_IF K_EXISTS )?
			int alt75=2;
			int LA75_0 = input.LA(1);
			if ( (LA75_0==K_IF) ) {
				alt75=1;
			}
			switch (alt75) {
				case 1 :
					// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:678:30: K_IF K_EXISTS
					{
					match(input,K_IF,FOLLOW_K_IF_in_dropTableStatement3737); 
					match(input,K_EXISTS,FOLLOW_K_EXISTS_in_dropTableStatement3739); 
					 ifExists = true; 
					}
					break;

			}

			pushFollow(FOLLOW_columnFamilyName_in_dropTableStatement3748);
			cf=columnFamilyName();
			state._fsp--;

			 stmt = new DropTableStatement(cf, ifExists); 
			}

		}
		catch (RecognitionException re) {
			reportError(re);
			recover(input,re);
		}
		finally {
			// do for sure before leaving
		}
		return stmt;
	}
	// $ANTLR end "dropTableStatement"



	// $ANTLR start "dropTypeStatement"
	// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:684:1: dropTypeStatement returns [DropTypeStatement stmt] : K_DROP K_TYPE ( K_IF K_EXISTS )? name= userTypeName ;
	public final DropTypeStatement dropTypeStatement() throws RecognitionException {
		DropTypeStatement stmt = null;


		UTName name =null;

		 boolean ifExists = false; 
		try {
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:686:5: ( K_DROP K_TYPE ( K_IF K_EXISTS )? name= userTypeName )
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:686:7: K_DROP K_TYPE ( K_IF K_EXISTS )? name= userTypeName
			{
			match(input,K_DROP,FOLLOW_K_DROP_in_dropTypeStatement3782); 
			match(input,K_TYPE,FOLLOW_K_TYPE_in_dropTypeStatement3784); 
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:686:21: ( K_IF K_EXISTS )?
			int alt76=2;
			int LA76_0 = input.LA(1);
			if ( (LA76_0==K_IF) ) {
				alt76=1;
			}
			switch (alt76) {
				case 1 :
					// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:686:22: K_IF K_EXISTS
					{
					match(input,K_IF,FOLLOW_K_IF_in_dropTypeStatement3787); 
					match(input,K_EXISTS,FOLLOW_K_EXISTS_in_dropTypeStatement3789); 
					 ifExists = true; 
					}
					break;

			}

			pushFollow(FOLLOW_userTypeName_in_dropTypeStatement3798);
			name=userTypeName();
			state._fsp--;

			 stmt = new DropTypeStatement(name, ifExists); 
			}

		}
		catch (RecognitionException re) {
			reportError(re);
			recover(input,re);
		}
		finally {
			// do for sure before leaving
		}
		return stmt;
	}
	// $ANTLR end "dropTypeStatement"



	// $ANTLR start "dropIndexStatement"
	// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:692:1: dropIndexStatement returns [DropIndexStatement expr] : K_DROP K_INDEX ( K_IF K_EXISTS )? index= indexName ;
	public final DropIndexStatement dropIndexStatement() throws RecognitionException {
		DropIndexStatement expr = null;


		IndexName index =null;

		 boolean ifExists = false; 
		try {
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:694:5: ( K_DROP K_INDEX ( K_IF K_EXISTS )? index= indexName )
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:694:7: K_DROP K_INDEX ( K_IF K_EXISTS )? index= indexName
			{
			match(input,K_DROP,FOLLOW_K_DROP_in_dropIndexStatement3832); 
			match(input,K_INDEX,FOLLOW_K_INDEX_in_dropIndexStatement3834); 
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:694:22: ( K_IF K_EXISTS )?
			int alt77=2;
			int LA77_0 = input.LA(1);
			if ( (LA77_0==K_IF) ) {
				alt77=1;
			}
			switch (alt77) {
				case 1 :
					// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:694:23: K_IF K_EXISTS
					{
					match(input,K_IF,FOLLOW_K_IF_in_dropIndexStatement3837); 
					match(input,K_EXISTS,FOLLOW_K_EXISTS_in_dropIndexStatement3839); 
					 ifExists = true; 
					}
					break;

			}

			pushFollow(FOLLOW_indexName_in_dropIndexStatement3848);
			index=indexName();
			state._fsp--;

			 expr = new DropIndexStatement(index, ifExists); 
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



	// $ANTLR start "truncateStatement"
	// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:701:1: truncateStatement returns [TruncateStatement stmt] : K_TRUNCATE cf= columnFamilyName ;
	public final TruncateStatement truncateStatement() throws RecognitionException {
		TruncateStatement stmt = null;


		CFName cf =null;

		try {
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:702:5: ( K_TRUNCATE cf= columnFamilyName )
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:702:7: K_TRUNCATE cf= columnFamilyName
			{
			match(input,K_TRUNCATE,FOLLOW_K_TRUNCATE_in_truncateStatement3879); 
			pushFollow(FOLLOW_columnFamilyName_in_truncateStatement3883);
			cf=columnFamilyName();
			state._fsp--;

			 stmt = new TruncateStatement(cf); 
			}

		}
		catch (RecognitionException re) {
			reportError(re);
			recover(input,re);
		}
		finally {
			// do for sure before leaving
		}
		return stmt;
	}
	// $ANTLR end "truncateStatement"



	// $ANTLR start "grantStatement"
	// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:708:1: grantStatement returns [GrantStatement stmt] : K_GRANT permissionOrAll K_ON resource K_TO username ;
	public final GrantStatement grantStatement() throws RecognitionException {
		GrantStatement stmt = null;


		Set<Permission> permissionOrAll1 =null;
		IResource resource2 =null;
		ParserRuleReturnScope username3 =null;

		try {
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:709:5: ( K_GRANT permissionOrAll K_ON resource K_TO username )
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:709:7: K_GRANT permissionOrAll K_ON resource K_TO username
			{
			match(input,K_GRANT,FOLLOW_K_GRANT_in_grantStatement3908); 
			pushFollow(FOLLOW_permissionOrAll_in_grantStatement3920);
			permissionOrAll1=permissionOrAll();
			state._fsp--;

			match(input,K_ON,FOLLOW_K_ON_in_grantStatement3928); 
			pushFollow(FOLLOW_resource_in_grantStatement3940);
			resource2=resource();
			state._fsp--;

			match(input,K_TO,FOLLOW_K_TO_in_grantStatement3948); 
			pushFollow(FOLLOW_username_in_grantStatement3960);
			username3=username();
			state._fsp--;

			 stmt = new GrantStatement(permissionOrAll1, resource2, (username3!=null?input.toString(username3.start,username3.stop):null)); 
			}

		}
		catch (RecognitionException re) {
			reportError(re);
			recover(input,re);
		}
		finally {
			// do for sure before leaving
		}
		return stmt;
	}
	// $ANTLR end "grantStatement"



	// $ANTLR start "revokeStatement"
	// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:721:1: revokeStatement returns [RevokeStatement stmt] : K_REVOKE permissionOrAll K_ON resource K_FROM username ;
	public final RevokeStatement revokeStatement() throws RecognitionException {
		RevokeStatement stmt = null;


		Set<Permission> permissionOrAll4 =null;
		IResource resource5 =null;
		ParserRuleReturnScope username6 =null;

		try {
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:722:5: ( K_REVOKE permissionOrAll K_ON resource K_FROM username )
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:722:7: K_REVOKE permissionOrAll K_ON resource K_FROM username
			{
			match(input,K_REVOKE,FOLLOW_K_REVOKE_in_revokeStatement3991); 
			pushFollow(FOLLOW_permissionOrAll_in_revokeStatement4003);
			permissionOrAll4=permissionOrAll();
			state._fsp--;

			match(input,K_ON,FOLLOW_K_ON_in_revokeStatement4011); 
			pushFollow(FOLLOW_resource_in_revokeStatement4023);
			resource5=resource();
			state._fsp--;

			match(input,K_FROM,FOLLOW_K_FROM_in_revokeStatement4031); 
			pushFollow(FOLLOW_username_in_revokeStatement4043);
			username6=username();
			state._fsp--;

			 stmt = new RevokeStatement(permissionOrAll4, resource5, (username6!=null?input.toString(username6.start,username6.stop):null)); 
			}

		}
		catch (RecognitionException re) {
			reportError(re);
			recover(input,re);
		}
		finally {
			// do for sure before leaving
		}
		return stmt;
	}
	// $ANTLR end "revokeStatement"



	// $ANTLR start "listPermissionsStatement"
	// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:731:1: listPermissionsStatement returns [ListPermissionsStatement stmt] : K_LIST permissionOrAll ( K_ON resource )? ( K_OF username )? ( K_NORECURSIVE )? ;
	public final ListPermissionsStatement listPermissionsStatement() throws RecognitionException {
		ListPermissionsStatement stmt = null;


		IResource resource7 =null;
		ParserRuleReturnScope username8 =null;
		Set<Permission> permissionOrAll9 =null;


		        IResource resource = null;
		        String username = null;
		        boolean recursive = true;
		    
		try {
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:737:5: ( K_LIST permissionOrAll ( K_ON resource )? ( K_OF username )? ( K_NORECURSIVE )? )
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:737:7: K_LIST permissionOrAll ( K_ON resource )? ( K_OF username )? ( K_NORECURSIVE )?
			{
			match(input,K_LIST,FOLLOW_K_LIST_in_listPermissionsStatement4081); 
			pushFollow(FOLLOW_permissionOrAll_in_listPermissionsStatement4093);
			permissionOrAll9=permissionOrAll();
			state._fsp--;

			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:739:7: ( K_ON resource )?
			int alt78=2;
			int LA78_0 = input.LA(1);
			if ( (LA78_0==K_ON) ) {
				alt78=1;
			}
			switch (alt78) {
				case 1 :
					// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:739:9: K_ON resource
					{
					match(input,K_ON,FOLLOW_K_ON_in_listPermissionsStatement4103); 
					pushFollow(FOLLOW_resource_in_listPermissionsStatement4105);
					resource7=resource();
					state._fsp--;

					 resource = resource7; 
					}
					break;

			}

			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:740:7: ( K_OF username )?
			int alt79=2;
			int LA79_0 = input.LA(1);
			if ( (LA79_0==K_OF) ) {
				alt79=1;
			}
			switch (alt79) {
				case 1 :
					// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:740:9: K_OF username
					{
					match(input,K_OF,FOLLOW_K_OF_in_listPermissionsStatement4120); 
					pushFollow(FOLLOW_username_in_listPermissionsStatement4122);
					username8=username();
					state._fsp--;

					 username = (username8!=null?input.toString(username8.start,username8.stop):null); 
					}
					break;

			}

			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:741:7: ( K_NORECURSIVE )?
			int alt80=2;
			int LA80_0 = input.LA(1);
			if ( (LA80_0==K_NORECURSIVE) ) {
				alt80=1;
			}
			switch (alt80) {
				case 1 :
					// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:741:9: K_NORECURSIVE
					{
					match(input,K_NORECURSIVE,FOLLOW_K_NORECURSIVE_in_listPermissionsStatement4137); 
					 recursive = false; 
					}
					break;

			}

			 stmt = new ListPermissionsStatement(permissionOrAll9, resource, username, recursive); 
			}

		}
		catch (RecognitionException re) {
			reportError(re);
			recover(input,re);
		}
		finally {
			// do for sure before leaving
		}
		return stmt;
	}
	// $ANTLR end "listPermissionsStatement"



	// $ANTLR start "permission"
	// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:745:1: permission returns [Permission perm] : p= ( K_CREATE | K_ALTER | K_DROP | K_SELECT | K_MODIFY | K_AUTHORIZE ) ;
	public final Permission permission() throws RecognitionException {
		Permission perm = null;


		Token p=null;

		try {
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:746:5: (p= ( K_CREATE | K_ALTER | K_DROP | K_SELECT | K_MODIFY | K_AUTHORIZE ) )
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:746:7: p= ( K_CREATE | K_ALTER | K_DROP | K_SELECT | K_MODIFY | K_AUTHORIZE )
			{
			p=input.LT(1);
			if ( input.LA(1)==K_ALTER||input.LA(1)==K_AUTHORIZE||input.LA(1)==K_CREATE||input.LA(1)==K_DROP||input.LA(1)==K_MODIFY||input.LA(1)==K_SELECT ) {
				input.consume();
				state.errorRecovery=false;
			}
			else {
				MismatchedSetException mse = new MismatchedSetException(null,input);
				throw mse;
			}
			 perm = Permission.valueOf((p!=null?p.getText():null).toUpperCase()); 
			}

		}
		catch (RecognitionException re) {
			reportError(re);
			recover(input,re);
		}
		finally {
			// do for sure before leaving
		}
		return perm;
	}
	// $ANTLR end "permission"



	// $ANTLR start "permissionOrAll"
	// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:750:1: permissionOrAll returns [Set<Permission> perms] : ( K_ALL ( K_PERMISSIONS )? |p= permission ( K_PERMISSION )? );
	public final Set<Permission> permissionOrAll() throws RecognitionException {
		Set<Permission> perms = null;


		Permission p =null;

		try {
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:751:5: ( K_ALL ( K_PERMISSIONS )? |p= permission ( K_PERMISSION )? )
			int alt83=2;
			int LA83_0 = input.LA(1);
			if ( (LA83_0==K_ALL) ) {
				alt83=1;
			}
			else if ( (LA83_0==K_ALTER||LA83_0==K_AUTHORIZE||LA83_0==K_CREATE||LA83_0==K_DROP||LA83_0==K_MODIFY||LA83_0==K_SELECT) ) {
				alt83=2;
			}

			else {
				NoViableAltException nvae =
					new NoViableAltException("", 83, 0, input);
				throw nvae;
			}

			switch (alt83) {
				case 1 :
					// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:751:7: K_ALL ( K_PERMISSIONS )?
					{
					match(input,K_ALL,FOLLOW_K_ALL_in_permissionOrAll4222); 
					// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:751:13: ( K_PERMISSIONS )?
					int alt81=2;
					int LA81_0 = input.LA(1);
					if ( (LA81_0==K_PERMISSIONS) ) {
						alt81=1;
					}
					switch (alt81) {
						case 1 :
							// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:751:15: K_PERMISSIONS
							{
							match(input,K_PERMISSIONS,FOLLOW_K_PERMISSIONS_in_permissionOrAll4226); 
							}
							break;

					}

					 perms = Permission.ALL_DATA; 
					}
					break;
				case 2 :
					// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:752:7: p= permission ( K_PERMISSION )?
					{
					pushFollow(FOLLOW_permission_in_permissionOrAll4247);
					p=permission();
					state._fsp--;

					// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:752:20: ( K_PERMISSION )?
					int alt82=2;
					int LA82_0 = input.LA(1);
					if ( (LA82_0==K_PERMISSION) ) {
						alt82=1;
					}
					switch (alt82) {
						case 1 :
							// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:752:22: K_PERMISSION
							{
							match(input,K_PERMISSION,FOLLOW_K_PERMISSION_in_permissionOrAll4251); 
							}
							break;

					}

					 perms = EnumSet.of(p); 
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
		return perms;
	}
	// $ANTLR end "permissionOrAll"



	// $ANTLR start "resource"
	// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:755:1: resource returns [IResource res] : r= dataResource ;
	public final IResource resource() throws RecognitionException {
		IResource res = null;


		DataResource r =null;

		try {
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:756:5: (r= dataResource )
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:756:7: r= dataResource
			{
			pushFollow(FOLLOW_dataResource_in_resource4279);
			r=dataResource();
			state._fsp--;

			 res = r; 
			}

		}
		catch (RecognitionException re) {
			reportError(re);
			recover(input,re);
		}
		finally {
			// do for sure before leaving
		}
		return res;
	}
	// $ANTLR end "resource"



	// $ANTLR start "dataResource"
	// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:759:1: dataResource returns [DataResource res] : ( K_ALL K_KEYSPACES | K_KEYSPACE ks= keyspaceName | ( K_COLUMNFAMILY )? cf= columnFamilyName );
	public final DataResource dataResource() throws RecognitionException {
		DataResource res = null;


		String ks =null;
		CFName cf =null;

		try {
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:760:5: ( K_ALL K_KEYSPACES | K_KEYSPACE ks= keyspaceName | ( K_COLUMNFAMILY )? cf= columnFamilyName )
			int alt85=3;
			switch ( input.LA(1) ) {
			case K_ALL:
				{
				int LA85_1 = input.LA(2);
				if ( (LA85_1==K_KEYSPACES) ) {
					alt85=1;
				}
				else if ( (LA85_1==EOF||LA85_1==K_FROM||LA85_1==K_NORECURSIVE||LA85_1==K_OF||LA85_1==K_TO||LA85_1==147||LA85_1==149) ) {
					alt85=3;
				}

				else {
					int nvaeMark = input.mark();
					try {
						input.consume();
						NoViableAltException nvae =
							new NoViableAltException("", 85, 1, input);
						throw nvae;
					} finally {
						input.rewind(nvaeMark);
					}
				}

				}
				break;
			case K_KEYSPACE:
				{
				alt85=2;
				}
				break;
			case IDENT:
			case K_AS:
			case K_ASCII:
			case K_BIGINT:
			case K_BLOB:
			case K_BOOLEAN:
			case K_CLUSTERING:
			case K_COLUMNFAMILY:
			case K_COMPACT:
			case K_CONTAINS:
			case K_COUNT:
			case K_COUNTER:
			case K_CUSTOM:
			case K_DECIMAL:
			case K_DISTINCT:
			case K_DOUBLE:
			case K_EXISTS:
			case K_FILTERING:
			case K_FLOAT:
			case K_FROZEN:
			case K_INET:
			case K_INT:
			case K_KEY:
			case K_KEYS:
			case K_KEYSPACES:
			case K_LIST:
			case K_MAP:
			case K_NOSUPERUSER:
			case K_PASSWORD:
			case K_PERMISSION:
			case K_PERMISSIONS:
			case K_STATIC:
			case K_STORAGE:
			case K_SUPERUSER:
			case K_TEXT:
			case K_TIMESTAMP:
			case K_TIMEUUID:
			case K_TRIGGER:
			case K_TTL:
			case K_TUPLE:
			case K_TYPE:
			case K_USER:
			case K_USERS:
			case K_UUID:
			case K_VALUES:
			case K_VARCHAR:
			case K_VARINT:
			case K_WRITETIME:
			case QMARK:
			case QUOTED_NAME:
				{
				alt85=3;
				}
				break;
			default:
				NoViableAltException nvae =
					new NoViableAltException("", 85, 0, input);
				throw nvae;
			}
			switch (alt85) {
				case 1 :
					// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:760:7: K_ALL K_KEYSPACES
					{
					match(input,K_ALL,FOLLOW_K_ALL_in_dataResource4302); 
					match(input,K_KEYSPACES,FOLLOW_K_KEYSPACES_in_dataResource4304); 
					 res = DataResource.root(); 
					}
					break;
				case 2 :
					// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:761:7: K_KEYSPACE ks= keyspaceName
					{
					match(input,K_KEYSPACE,FOLLOW_K_KEYSPACE_in_dataResource4314); 
					pushFollow(FOLLOW_keyspaceName_in_dataResource4320);
					ks=keyspaceName();
					state._fsp--;

					 res = DataResource.keyspace(ks); 
					}
					break;
				case 3 :
					// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:762:7: ( K_COLUMNFAMILY )? cf= columnFamilyName
					{
					// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:762:7: ( K_COLUMNFAMILY )?
					int alt84=2;
					int LA84_0 = input.LA(1);
					if ( (LA84_0==K_COLUMNFAMILY) ) {
						alt84=1;
					}
					switch (alt84) {
						case 1 :
							// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:762:9: K_COLUMNFAMILY
							{
							match(input,K_COLUMNFAMILY,FOLLOW_K_COLUMNFAMILY_in_dataResource4332); 
							}
							break;

					}

					pushFollow(FOLLOW_columnFamilyName_in_dataResource4341);
					cf=columnFamilyName();
					state._fsp--;

					 res = DataResource.columnFamily(cf.getKeyspace(), cf.getColumnFamily()); 
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
		return res;
	}
	// $ANTLR end "dataResource"



	// $ANTLR start "createUserStatement"
	// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:769:1: createUserStatement returns [CreateUserStatement stmt] : K_CREATE K_USER ( K_IF K_NOT K_EXISTS )? username ( K_WITH userOptions[opts] )? ( K_SUPERUSER | K_NOSUPERUSER )? ;
	public final CreateUserStatement createUserStatement() throws RecognitionException {
		CreateUserStatement stmt = null;


		ParserRuleReturnScope username10 =null;


		        UserOptions opts = new UserOptions();
		        boolean superuser = false;
		        boolean ifNotExists = false;
		    
		try {
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:775:5: ( K_CREATE K_USER ( K_IF K_NOT K_EXISTS )? username ( K_WITH userOptions[opts] )? ( K_SUPERUSER | K_NOSUPERUSER )? )
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:775:7: K_CREATE K_USER ( K_IF K_NOT K_EXISTS )? username ( K_WITH userOptions[opts] )? ( K_SUPERUSER | K_NOSUPERUSER )?
			{
			match(input,K_CREATE,FOLLOW_K_CREATE_in_createUserStatement4381); 
			match(input,K_USER,FOLLOW_K_USER_in_createUserStatement4383); 
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:775:23: ( K_IF K_NOT K_EXISTS )?
			int alt86=2;
			int LA86_0 = input.LA(1);
			if ( (LA86_0==K_IF) ) {
				alt86=1;
			}
			switch (alt86) {
				case 1 :
					// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:775:24: K_IF K_NOT K_EXISTS
					{
					match(input,K_IF,FOLLOW_K_IF_in_createUserStatement4386); 
					match(input,K_NOT,FOLLOW_K_NOT_in_createUserStatement4388); 
					match(input,K_EXISTS,FOLLOW_K_EXISTS_in_createUserStatement4390); 
					 ifNotExists = true; 
					}
					break;

			}

			pushFollow(FOLLOW_username_in_createUserStatement4396);
			username10=username();
			state._fsp--;

			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:776:7: ( K_WITH userOptions[opts] )?
			int alt87=2;
			int LA87_0 = input.LA(1);
			if ( (LA87_0==K_WITH) ) {
				alt87=1;
			}
			switch (alt87) {
				case 1 :
					// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:776:9: K_WITH userOptions[opts]
					{
					match(input,K_WITH,FOLLOW_K_WITH_in_createUserStatement4406); 
					pushFollow(FOLLOW_userOptions_in_createUserStatement4408);
					userOptions(opts);
					state._fsp--;

					}
					break;

			}

			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:777:7: ( K_SUPERUSER | K_NOSUPERUSER )?
			int alt88=3;
			int LA88_0 = input.LA(1);
			if ( (LA88_0==K_SUPERUSER) ) {
				alt88=1;
			}
			else if ( (LA88_0==K_NOSUPERUSER) ) {
				alt88=2;
			}
			switch (alt88) {
				case 1 :
					// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:777:9: K_SUPERUSER
					{
					match(input,K_SUPERUSER,FOLLOW_K_SUPERUSER_in_createUserStatement4422); 
					 superuser = true; 
					}
					break;
				case 2 :
					// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:777:45: K_NOSUPERUSER
					{
					match(input,K_NOSUPERUSER,FOLLOW_K_NOSUPERUSER_in_createUserStatement4428); 
					 superuser = false; 
					}
					break;

			}

			 stmt = new CreateUserStatement((username10!=null?input.toString(username10.start,username10.stop):null), opts, superuser, ifNotExists); 
			}

		}
		catch (RecognitionException re) {
			reportError(re);
			recover(input,re);
		}
		finally {
			// do for sure before leaving
		}
		return stmt;
	}
	// $ANTLR end "createUserStatement"



	// $ANTLR start "alterUserStatement"
	// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:784:1: alterUserStatement returns [AlterUserStatement stmt] : K_ALTER K_USER username ( K_WITH userOptions[opts] )? ( K_SUPERUSER | K_NOSUPERUSER )? ;
	public final AlterUserStatement alterUserStatement() throws RecognitionException {
		AlterUserStatement stmt = null;


		ParserRuleReturnScope username11 =null;


		        UserOptions opts = new UserOptions();
		        Boolean superuser = null;
		    
		try {
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:789:5: ( K_ALTER K_USER username ( K_WITH userOptions[opts] )? ( K_SUPERUSER | K_NOSUPERUSER )? )
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:789:7: K_ALTER K_USER username ( K_WITH userOptions[opts] )? ( K_SUPERUSER | K_NOSUPERUSER )?
			{
			match(input,K_ALTER,FOLLOW_K_ALTER_in_alterUserStatement4473); 
			match(input,K_USER,FOLLOW_K_USER_in_alterUserStatement4475); 
			pushFollow(FOLLOW_username_in_alterUserStatement4477);
			username11=username();
			state._fsp--;

			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:790:7: ( K_WITH userOptions[opts] )?
			int alt89=2;
			int LA89_0 = input.LA(1);
			if ( (LA89_0==K_WITH) ) {
				alt89=1;
			}
			switch (alt89) {
				case 1 :
					// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:790:9: K_WITH userOptions[opts]
					{
					match(input,K_WITH,FOLLOW_K_WITH_in_alterUserStatement4487); 
					pushFollow(FOLLOW_userOptions_in_alterUserStatement4489);
					userOptions(opts);
					state._fsp--;

					}
					break;

			}

			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:791:7: ( K_SUPERUSER | K_NOSUPERUSER )?
			int alt90=3;
			int LA90_0 = input.LA(1);
			if ( (LA90_0==K_SUPERUSER) ) {
				alt90=1;
			}
			else if ( (LA90_0==K_NOSUPERUSER) ) {
				alt90=2;
			}
			switch (alt90) {
				case 1 :
					// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:791:9: K_SUPERUSER
					{
					match(input,K_SUPERUSER,FOLLOW_K_SUPERUSER_in_alterUserStatement4503); 
					 superuser = true; 
					}
					break;
				case 2 :
					// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:791:45: K_NOSUPERUSER
					{
					match(input,K_NOSUPERUSER,FOLLOW_K_NOSUPERUSER_in_alterUserStatement4509); 
					 superuser = false; 
					}
					break;

			}

			 stmt = new AlterUserStatement((username11!=null?input.toString(username11.start,username11.stop):null), opts, superuser); 
			}

		}
		catch (RecognitionException re) {
			reportError(re);
			recover(input,re);
		}
		finally {
			// do for sure before leaving
		}
		return stmt;
	}
	// $ANTLR end "alterUserStatement"



	// $ANTLR start "dropUserStatement"
	// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:798:1: dropUserStatement returns [DropUserStatement stmt] : K_DROP K_USER ( K_IF K_EXISTS )? username ;
	public final DropUserStatement dropUserStatement() throws RecognitionException {
		DropUserStatement stmt = null;


		ParserRuleReturnScope username12 =null;

		 boolean ifExists = false; 
		try {
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:800:5: ( K_DROP K_USER ( K_IF K_EXISTS )? username )
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:800:7: K_DROP K_USER ( K_IF K_EXISTS )? username
			{
			match(input,K_DROP,FOLLOW_K_DROP_in_dropUserStatement4554); 
			match(input,K_USER,FOLLOW_K_USER_in_dropUserStatement4556); 
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:800:21: ( K_IF K_EXISTS )?
			int alt91=2;
			int LA91_0 = input.LA(1);
			if ( (LA91_0==K_IF) ) {
				alt91=1;
			}
			switch (alt91) {
				case 1 :
					// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:800:22: K_IF K_EXISTS
					{
					match(input,K_IF,FOLLOW_K_IF_in_dropUserStatement4559); 
					match(input,K_EXISTS,FOLLOW_K_EXISTS_in_dropUserStatement4561); 
					 ifExists = true; 
					}
					break;

			}

			pushFollow(FOLLOW_username_in_dropUserStatement4567);
			username12=username();
			state._fsp--;

			 stmt = new DropUserStatement((username12!=null?input.toString(username12.start,username12.stop):null), ifExists); 
			}

		}
		catch (RecognitionException re) {
			reportError(re);
			recover(input,re);
		}
		finally {
			// do for sure before leaving
		}
		return stmt;
	}
	// $ANTLR end "dropUserStatement"



	// $ANTLR start "listUsersStatement"
	// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:806:1: listUsersStatement returns [ListUsersStatement stmt] : K_LIST K_USERS ;
	public final ListUsersStatement listUsersStatement() throws RecognitionException {
		ListUsersStatement stmt = null;


		try {
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:807:5: ( K_LIST K_USERS )
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:807:7: K_LIST K_USERS
			{
			match(input,K_LIST,FOLLOW_K_LIST_in_listUsersStatement4592); 
			match(input,K_USERS,FOLLOW_K_USERS_in_listUsersStatement4594); 
			 stmt = new ListUsersStatement(); 
			}

		}
		catch (RecognitionException re) {
			reportError(re);
			recover(input,re);
		}
		finally {
			// do for sure before leaving
		}
		return stmt;
	}
	// $ANTLR end "listUsersStatement"



	// $ANTLR start "userOptions"
	// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:810:1: userOptions[UserOptions opts] : userOption[opts] ;
	public final void userOptions(UserOptions opts) throws RecognitionException {
		try {
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:811:5: ( userOption[opts] )
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:811:7: userOption[opts]
			{
			pushFollow(FOLLOW_userOption_in_userOptions4614);
			userOption(opts);
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
	}
	// $ANTLR end "userOptions"



	// $ANTLR start "userOption"
	// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:814:1: userOption[UserOptions opts] : k= K_PASSWORD v= STRING_LITERAL ;
	public final void userOption(UserOptions opts) throws RecognitionException {
		Token k=null;
		Token v=null;

		try {
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:815:5: (k= K_PASSWORD v= STRING_LITERAL )
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:815:7: k= K_PASSWORD v= STRING_LITERAL
			{
			k=(Token)match(input,K_PASSWORD,FOLLOW_K_PASSWORD_in_userOption4635); 
			v=(Token)match(input,STRING_LITERAL,FOLLOW_STRING_LITERAL_in_userOption4639); 
			 opts.put((k!=null?k.getText():null), (v!=null?v.getText():null)); 
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
	// $ANTLR end "userOption"



	// $ANTLR start "cident"
	// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:823:1: cident returns [ColumnIdentifier.Raw id] : (t= IDENT |t= QUOTED_NAME |k= unreserved_keyword );
	public final ColumnIdentifier.Raw cident() throws RecognitionException {
		ColumnIdentifier.Raw id = null;


		Token t=null;
		String k =null;

		try {
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:824:5: (t= IDENT |t= QUOTED_NAME |k= unreserved_keyword )
			int alt92=3;
			switch ( input.LA(1) ) {
			case IDENT:
				{
				alt92=1;
				}
				break;
			case QUOTED_NAME:
				{
				alt92=2;
				}
				break;
			case K_ALL:
			case K_AS:
			case K_ASCII:
			case K_BIGINT:
			case K_BLOB:
			case K_BOOLEAN:
			case K_CLUSTERING:
			case K_COMPACT:
			case K_CONTAINS:
			case K_COUNT:
			case K_COUNTER:
			case K_CUSTOM:
			case K_DECIMAL:
			case K_DISTINCT:
			case K_DOUBLE:
			case K_EXISTS:
			case K_FILTERING:
			case K_FLOAT:
			case K_FROZEN:
			case K_INET:
			case K_INT:
			case K_KEY:
			case K_KEYS:
			case K_KEYSPACES:
			case K_LIST:
			case K_MAP:
			case K_NOSUPERUSER:
			case K_PASSWORD:
			case K_PERMISSION:
			case K_PERMISSIONS:
			case K_STATIC:
			case K_STORAGE:
			case K_SUPERUSER:
			case K_TEXT:
			case K_TIMESTAMP:
			case K_TIMEUUID:
			case K_TRIGGER:
			case K_TTL:
			case K_TUPLE:
			case K_TYPE:
			case K_USER:
			case K_USERS:
			case K_UUID:
			case K_VALUES:
			case K_VARCHAR:
			case K_VARINT:
			case K_WRITETIME:
				{
				alt92=3;
				}
				break;
			default:
				NoViableAltException nvae =
					new NoViableAltException("", 92, 0, input);
				throw nvae;
			}
			switch (alt92) {
				case 1 :
					// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:824:7: t= IDENT
					{
					t=(Token)match(input,IDENT,FOLLOW_IDENT_in_cident4670); 
					 id = new ColumnIdentifier.Raw((t!=null?t.getText():null), false); 
					}
					break;
				case 2 :
					// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:825:7: t= QUOTED_NAME
					{
					t=(Token)match(input,QUOTED_NAME,FOLLOW_QUOTED_NAME_in_cident4695); 
					 id = new ColumnIdentifier.Raw((t!=null?t.getText():null), true); 
					}
					break;
				case 3 :
					// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:826:7: k= unreserved_keyword
					{
					pushFollow(FOLLOW_unreserved_keyword_in_cident4714);
					k=unreserved_keyword();
					state._fsp--;

					 id = new ColumnIdentifier.Raw(k, false); 
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
		return id;
	}
	// $ANTLR end "cident"



	// $ANTLR start "ident"
	// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:830:1: ident returns [ColumnIdentifier id] : (t= IDENT |t= QUOTED_NAME |k= unreserved_keyword );
	public final ColumnIdentifier ident() throws RecognitionException {
		ColumnIdentifier id = null;


		Token t=null;
		String k =null;

		try {
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:831:5: (t= IDENT |t= QUOTED_NAME |k= unreserved_keyword )
			int alt93=3;
			switch ( input.LA(1) ) {
			case IDENT:
				{
				alt93=1;
				}
				break;
			case QUOTED_NAME:
				{
				alt93=2;
				}
				break;
			case K_ALL:
			case K_AS:
			case K_ASCII:
			case K_BIGINT:
			case K_BLOB:
			case K_BOOLEAN:
			case K_CLUSTERING:
			case K_COMPACT:
			case K_CONTAINS:
			case K_COUNT:
			case K_COUNTER:
			case K_CUSTOM:
			case K_DECIMAL:
			case K_DISTINCT:
			case K_DOUBLE:
			case K_EXISTS:
			case K_FILTERING:
			case K_FLOAT:
			case K_FROZEN:
			case K_INET:
			case K_INT:
			case K_KEY:
			case K_KEYS:
			case K_KEYSPACES:
			case K_LIST:
			case K_MAP:
			case K_NOSUPERUSER:
			case K_PASSWORD:
			case K_PERMISSION:
			case K_PERMISSIONS:
			case K_STATIC:
			case K_STORAGE:
			case K_SUPERUSER:
			case K_TEXT:
			case K_TIMESTAMP:
			case K_TIMEUUID:
			case K_TRIGGER:
			case K_TTL:
			case K_TUPLE:
			case K_TYPE:
			case K_USER:
			case K_USERS:
			case K_UUID:
			case K_VALUES:
			case K_VARCHAR:
			case K_VARINT:
			case K_WRITETIME:
				{
				alt93=3;
				}
				break;
			default:
				NoViableAltException nvae =
					new NoViableAltException("", 93, 0, input);
				throw nvae;
			}
			switch (alt93) {
				case 1 :
					// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:831:7: t= IDENT
					{
					t=(Token)match(input,IDENT,FOLLOW_IDENT_in_ident4740); 
					 id = new ColumnIdentifier((t!=null?t.getText():null), false); 
					}
					break;
				case 2 :
					// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:832:7: t= QUOTED_NAME
					{
					t=(Token)match(input,QUOTED_NAME,FOLLOW_QUOTED_NAME_in_ident4765); 
					 id = new ColumnIdentifier((t!=null?t.getText():null), true); 
					}
					break;
				case 3 :
					// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:833:7: k= unreserved_keyword
					{
					pushFollow(FOLLOW_unreserved_keyword_in_ident4784);
					k=unreserved_keyword();
					state._fsp--;

					 id = new ColumnIdentifier(k, false); 
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
		return id;
	}
	// $ANTLR end "ident"



	// $ANTLR start "keyspaceName"
	// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:837:1: keyspaceName returns [String id] : ksName[name] ;
	public final String keyspaceName() throws RecognitionException {
		String id = null;


		 CFName name = new CFName(); 
		try {
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:839:5: ( ksName[name] )
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:839:7: ksName[name]
			{
			pushFollow(FOLLOW_ksName_in_keyspaceName4817);
			ksName(name);
			state._fsp--;

			 id = name.getKeyspace(); 
			}

		}
		catch (RecognitionException re) {
			reportError(re);
			recover(input,re);
		}
		finally {
			// do for sure before leaving
		}
		return id;
	}
	// $ANTLR end "keyspaceName"



	// $ANTLR start "indexName"
	// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:842:1: indexName returns [IndexName name] : ( ksName[name] '.' )? idxName[name] ;
	public final IndexName indexName() throws RecognitionException {
		IndexName name = null;


		 name = new IndexName(); 
		try {
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:844:5: ( ( ksName[name] '.' )? idxName[name] )
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:844:7: ( ksName[name] '.' )? idxName[name]
			{
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:844:7: ( ksName[name] '.' )?
			int alt94=2;
			switch ( input.LA(1) ) {
				case IDENT:
					{
					int LA94_1 = input.LA(2);
					if ( (LA94_1==147) ) {
						alt94=1;
					}
					}
					break;
				case QUOTED_NAME:
					{
					int LA94_2 = input.LA(2);
					if ( (LA94_2==147) ) {
						alt94=1;
					}
					}
					break;
				case K_ALL:
				case K_AS:
				case K_CLUSTERING:
				case K_COMPACT:
				case K_CONTAINS:
				case K_CUSTOM:
				case K_DISTINCT:
				case K_EXISTS:
				case K_FILTERING:
				case K_FROZEN:
				case K_KEYS:
				case K_KEYSPACES:
				case K_LIST:
				case K_MAP:
				case K_NOSUPERUSER:
				case K_PASSWORD:
				case K_PERMISSION:
				case K_PERMISSIONS:
				case K_STATIC:
				case K_STORAGE:
				case K_SUPERUSER:
				case K_TRIGGER:
				case K_TUPLE:
				case K_TYPE:
				case K_USER:
				case K_USERS:
				case K_VALUES:
					{
					int LA94_3 = input.LA(2);
					if ( (LA94_3==147) ) {
						alt94=1;
					}
					}
					break;
				case K_ASCII:
					{
					int LA94_4 = input.LA(2);
					if ( (LA94_4==147) ) {
						alt94=1;
					}
					}
					break;
				case K_BIGINT:
					{
					int LA94_5 = input.LA(2);
					if ( (LA94_5==147) ) {
						alt94=1;
					}
					}
					break;
				case K_BLOB:
					{
					int LA94_6 = input.LA(2);
					if ( (LA94_6==147) ) {
						alt94=1;
					}
					}
					break;
				case K_BOOLEAN:
					{
					int LA94_7 = input.LA(2);
					if ( (LA94_7==147) ) {
						alt94=1;
					}
					}
					break;
				case K_COUNTER:
					{
					int LA94_8 = input.LA(2);
					if ( (LA94_8==147) ) {
						alt94=1;
					}
					}
					break;
				case K_DECIMAL:
					{
					int LA94_9 = input.LA(2);
					if ( (LA94_9==147) ) {
						alt94=1;
					}
					}
					break;
				case K_DOUBLE:
					{
					int LA94_10 = input.LA(2);
					if ( (LA94_10==147) ) {
						alt94=1;
					}
					}
					break;
				case K_FLOAT:
					{
					int LA94_11 = input.LA(2);
					if ( (LA94_11==147) ) {
						alt94=1;
					}
					}
					break;
				case K_INET:
					{
					int LA94_12 = input.LA(2);
					if ( (LA94_12==147) ) {
						alt94=1;
					}
					}
					break;
				case K_INT:
					{
					int LA94_13 = input.LA(2);
					if ( (LA94_13==147) ) {
						alt94=1;
					}
					}
					break;
				case K_TEXT:
					{
					int LA94_14 = input.LA(2);
					if ( (LA94_14==147) ) {
						alt94=1;
					}
					}
					break;
				case K_TIMESTAMP:
					{
					int LA94_15 = input.LA(2);
					if ( (LA94_15==147) ) {
						alt94=1;
					}
					}
					break;
				case K_UUID:
					{
					int LA94_16 = input.LA(2);
					if ( (LA94_16==147) ) {
						alt94=1;
					}
					}
					break;
				case K_VARCHAR:
					{
					int LA94_17 = input.LA(2);
					if ( (LA94_17==147) ) {
						alt94=1;
					}
					}
					break;
				case K_VARINT:
					{
					int LA94_18 = input.LA(2);
					if ( (LA94_18==147) ) {
						alt94=1;
					}
					}
					break;
				case K_TIMEUUID:
					{
					int LA94_19 = input.LA(2);
					if ( (LA94_19==147) ) {
						alt94=1;
					}
					}
					break;
				case K_COUNT:
				case K_KEY:
				case K_TTL:
				case K_WRITETIME:
					{
					int LA94_20 = input.LA(2);
					if ( (LA94_20==147) ) {
						alt94=1;
					}
					}
					break;
				case QMARK:
					{
					int LA94_21 = input.LA(2);
					if ( (LA94_21==147) ) {
						alt94=1;
					}
					}
					break;
			}
			switch (alt94) {
				case 1 :
					// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:844:8: ksName[name] '.'
					{
					pushFollow(FOLLOW_ksName_in_indexName4851);
					ksName(name);
					state._fsp--;

					match(input,147,FOLLOW_147_in_indexName4854); 
					}
					break;

			}

			pushFollow(FOLLOW_idxName_in_indexName4858);
			idxName(name);
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
		return name;
	}
	// $ANTLR end "indexName"



	// $ANTLR start "columnFamilyName"
	// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:847:1: columnFamilyName returns [CFName name] : ( ksName[name] '.' )? cfName[name] ;
	public final CFName columnFamilyName() throws RecognitionException {
		CFName name = null;


		 name = new CFName(); 
		try {
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:849:5: ( ( ksName[name] '.' )? cfName[name] )
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:849:7: ( ksName[name] '.' )? cfName[name]
			{
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:849:7: ( ksName[name] '.' )?
			int alt95=2;
			switch ( input.LA(1) ) {
				case IDENT:
					{
					int LA95_1 = input.LA(2);
					if ( (LA95_1==147) ) {
						alt95=1;
					}
					}
					break;
				case QUOTED_NAME:
					{
					int LA95_2 = input.LA(2);
					if ( (LA95_2==147) ) {
						alt95=1;
					}
					}
					break;
				case K_ALL:
				case K_AS:
				case K_CLUSTERING:
				case K_COMPACT:
				case K_CONTAINS:
				case K_CUSTOM:
				case K_DISTINCT:
				case K_EXISTS:
				case K_FILTERING:
				case K_FROZEN:
				case K_KEYS:
				case K_KEYSPACES:
				case K_LIST:
				case K_MAP:
				case K_NOSUPERUSER:
				case K_PASSWORD:
				case K_PERMISSION:
				case K_PERMISSIONS:
				case K_STATIC:
				case K_STORAGE:
				case K_SUPERUSER:
				case K_TRIGGER:
				case K_TUPLE:
				case K_TYPE:
				case K_USER:
				case K_USERS:
				case K_VALUES:
					{
					int LA95_3 = input.LA(2);
					if ( (LA95_3==147) ) {
						alt95=1;
					}
					}
					break;
				case K_ASCII:
					{
					int LA95_4 = input.LA(2);
					if ( (LA95_4==147) ) {
						alt95=1;
					}
					}
					break;
				case K_BIGINT:
					{
					int LA95_5 = input.LA(2);
					if ( (LA95_5==147) ) {
						alt95=1;
					}
					}
					break;
				case K_BLOB:
					{
					int LA95_6 = input.LA(2);
					if ( (LA95_6==147) ) {
						alt95=1;
					}
					}
					break;
				case K_BOOLEAN:
					{
					int LA95_7 = input.LA(2);
					if ( (LA95_7==147) ) {
						alt95=1;
					}
					}
					break;
				case K_COUNTER:
					{
					int LA95_8 = input.LA(2);
					if ( (LA95_8==147) ) {
						alt95=1;
					}
					}
					break;
				case K_DECIMAL:
					{
					int LA95_9 = input.LA(2);
					if ( (LA95_9==147) ) {
						alt95=1;
					}
					}
					break;
				case K_DOUBLE:
					{
					int LA95_10 = input.LA(2);
					if ( (LA95_10==147) ) {
						alt95=1;
					}
					}
					break;
				case K_FLOAT:
					{
					int LA95_11 = input.LA(2);
					if ( (LA95_11==147) ) {
						alt95=1;
					}
					}
					break;
				case K_INET:
					{
					int LA95_12 = input.LA(2);
					if ( (LA95_12==147) ) {
						alt95=1;
					}
					}
					break;
				case K_INT:
					{
					int LA95_13 = input.LA(2);
					if ( (LA95_13==147) ) {
						alt95=1;
					}
					}
					break;
				case K_TEXT:
					{
					int LA95_14 = input.LA(2);
					if ( (LA95_14==147) ) {
						alt95=1;
					}
					}
					break;
				case K_TIMESTAMP:
					{
					int LA95_15 = input.LA(2);
					if ( (LA95_15==147) ) {
						alt95=1;
					}
					}
					break;
				case K_UUID:
					{
					int LA95_16 = input.LA(2);
					if ( (LA95_16==147) ) {
						alt95=1;
					}
					}
					break;
				case K_VARCHAR:
					{
					int LA95_17 = input.LA(2);
					if ( (LA95_17==147) ) {
						alt95=1;
					}
					}
					break;
				case K_VARINT:
					{
					int LA95_18 = input.LA(2);
					if ( (LA95_18==147) ) {
						alt95=1;
					}
					}
					break;
				case K_TIMEUUID:
					{
					int LA95_19 = input.LA(2);
					if ( (LA95_19==147) ) {
						alt95=1;
					}
					}
					break;
				case K_COUNT:
				case K_KEY:
				case K_TTL:
				case K_WRITETIME:
					{
					int LA95_20 = input.LA(2);
					if ( (LA95_20==147) ) {
						alt95=1;
					}
					}
					break;
				case QMARK:
					{
					int LA95_21 = input.LA(2);
					if ( (LA95_21==147) ) {
						alt95=1;
					}
					}
					break;
			}
			switch (alt95) {
				case 1 :
					// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:849:8: ksName[name] '.'
					{
					pushFollow(FOLLOW_ksName_in_columnFamilyName4890);
					ksName(name);
					state._fsp--;

					match(input,147,FOLLOW_147_in_columnFamilyName4893); 
					}
					break;

			}

			pushFollow(FOLLOW_cfName_in_columnFamilyName4897);
			cfName(name);
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
		return name;
	}
	// $ANTLR end "columnFamilyName"



	// $ANTLR start "userTypeName"
	// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:852:1: userTypeName returns [UTName name] : (ks= ident '.' )? ut= non_type_ident ;
	public final UTName userTypeName() throws RecognitionException {
		UTName name = null;


		ColumnIdentifier ks =null;
		ColumnIdentifier ut =null;

		try {
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:853:5: ( (ks= ident '.' )? ut= non_type_ident )
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:853:7: (ks= ident '.' )? ut= non_type_ident
			{
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:853:7: (ks= ident '.' )?
			int alt96=2;
			switch ( input.LA(1) ) {
				case IDENT:
					{
					int LA96_1 = input.LA(2);
					if ( (LA96_1==147) ) {
						alt96=1;
					}
					}
					break;
				case QUOTED_NAME:
					{
					int LA96_2 = input.LA(2);
					if ( (LA96_2==147) ) {
						alt96=1;
					}
					}
					break;
				case K_ALL:
				case K_AS:
				case K_CLUSTERING:
				case K_COMPACT:
				case K_CONTAINS:
				case K_CUSTOM:
				case K_DISTINCT:
				case K_EXISTS:
				case K_FILTERING:
				case K_FROZEN:
				case K_KEYS:
				case K_KEYSPACES:
				case K_LIST:
				case K_MAP:
				case K_NOSUPERUSER:
				case K_PASSWORD:
				case K_PERMISSION:
				case K_PERMISSIONS:
				case K_STATIC:
				case K_STORAGE:
				case K_SUPERUSER:
				case K_TRIGGER:
				case K_TUPLE:
				case K_TYPE:
				case K_USER:
				case K_USERS:
				case K_VALUES:
					{
					int LA96_3 = input.LA(2);
					if ( (LA96_3==147) ) {
						alt96=1;
					}
					}
					break;
				case K_ASCII:
				case K_BIGINT:
				case K_BLOB:
				case K_BOOLEAN:
				case K_COUNT:
				case K_COUNTER:
				case K_DECIMAL:
				case K_DOUBLE:
				case K_FLOAT:
				case K_INET:
				case K_INT:
				case K_TEXT:
				case K_TIMESTAMP:
				case K_TIMEUUID:
				case K_TTL:
				case K_UUID:
				case K_VARCHAR:
				case K_VARINT:
				case K_WRITETIME:
					{
					alt96=1;
					}
					break;
				case K_KEY:
					{
					int LA96_5 = input.LA(2);
					if ( (LA96_5==147) ) {
						alt96=1;
					}
					}
					break;
			}
			switch (alt96) {
				case 1 :
					// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:853:8: ks= ident '.'
					{
					pushFollow(FOLLOW_ident_in_userTypeName4922);
					ks=ident();
					state._fsp--;

					match(input,147,FOLLOW_147_in_userTypeName4924); 
					}
					break;

			}

			pushFollow(FOLLOW_non_type_ident_in_userTypeName4930);
			ut=non_type_ident();
			state._fsp--;

			 return new UTName(ks, ut); 
			}

		}
		catch (RecognitionException re) {
			reportError(re);
			recover(input,re);
		}
		finally {
			// do for sure before leaving
		}
		return name;
	}
	// $ANTLR end "userTypeName"



	// $ANTLR start "ksName"
	// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:856:1: ksName[KeyspaceElementName name] : (t= IDENT |t= QUOTED_NAME |k= unreserved_keyword | QMARK );
	public final void ksName(KeyspaceElementName name) throws RecognitionException {
		Token t=null;
		String k =null;

		try {
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:857:5: (t= IDENT |t= QUOTED_NAME |k= unreserved_keyword | QMARK )
			int alt97=4;
			switch ( input.LA(1) ) {
			case IDENT:
				{
				alt97=1;
				}
				break;
			case QUOTED_NAME:
				{
				alt97=2;
				}
				break;
			case K_ALL:
			case K_AS:
			case K_ASCII:
			case K_BIGINT:
			case K_BLOB:
			case K_BOOLEAN:
			case K_CLUSTERING:
			case K_COMPACT:
			case K_CONTAINS:
			case K_COUNT:
			case K_COUNTER:
			case K_CUSTOM:
			case K_DECIMAL:
			case K_DISTINCT:
			case K_DOUBLE:
			case K_EXISTS:
			case K_FILTERING:
			case K_FLOAT:
			case K_FROZEN:
			case K_INET:
			case K_INT:
			case K_KEY:
			case K_KEYS:
			case K_KEYSPACES:
			case K_LIST:
			case K_MAP:
			case K_NOSUPERUSER:
			case K_PASSWORD:
			case K_PERMISSION:
			case K_PERMISSIONS:
			case K_STATIC:
			case K_STORAGE:
			case K_SUPERUSER:
			case K_TEXT:
			case K_TIMESTAMP:
			case K_TIMEUUID:
			case K_TRIGGER:
			case K_TTL:
			case K_TUPLE:
			case K_TYPE:
			case K_USER:
			case K_USERS:
			case K_UUID:
			case K_VALUES:
			case K_VARCHAR:
			case K_VARINT:
			case K_WRITETIME:
				{
				alt97=3;
				}
				break;
			case QMARK:
				{
				alt97=4;
				}
				break;
			default:
				NoViableAltException nvae =
					new NoViableAltException("", 97, 0, input);
				throw nvae;
			}
			switch (alt97) {
				case 1 :
					// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:857:7: t= IDENT
					{
					t=(Token)match(input,IDENT,FOLLOW_IDENT_in_ksName4952); 
					 name.setKeyspace((t!=null?t.getText():null), false);
					}
					break;
				case 2 :
					// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:858:7: t= QUOTED_NAME
					{
					t=(Token)match(input,QUOTED_NAME,FOLLOW_QUOTED_NAME_in_ksName4977); 
					 name.setKeyspace((t!=null?t.getText():null), true);
					}
					break;
				case 3 :
					// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:859:7: k= unreserved_keyword
					{
					pushFollow(FOLLOW_unreserved_keyword_in_ksName4996);
					k=unreserved_keyword();
					state._fsp--;

					 name.setKeyspace(k, false);
					}
					break;
				case 4 :
					// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:860:7: QMARK
					{
					match(input,QMARK,FOLLOW_QMARK_in_ksName5006); 
					addRecognitionError("Bind variables cannot be used for keyspace names");
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
	// $ANTLR end "ksName"



	// $ANTLR start "cfName"
	// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:863:1: cfName[CFName name] : (t= IDENT |t= QUOTED_NAME |k= unreserved_keyword | QMARK );
	public final void cfName(CFName name) throws RecognitionException {
		Token t=null;
		String k =null;

		try {
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:864:5: (t= IDENT |t= QUOTED_NAME |k= unreserved_keyword | QMARK )
			int alt98=4;
			switch ( input.LA(1) ) {
			case IDENT:
				{
				alt98=1;
				}
				break;
			case QUOTED_NAME:
				{
				alt98=2;
				}
				break;
			case K_ALL:
			case K_AS:
			case K_ASCII:
			case K_BIGINT:
			case K_BLOB:
			case K_BOOLEAN:
			case K_CLUSTERING:
			case K_COMPACT:
			case K_CONTAINS:
			case K_COUNT:
			case K_COUNTER:
			case K_CUSTOM:
			case K_DECIMAL:
			case K_DISTINCT:
			case K_DOUBLE:
			case K_EXISTS:
			case K_FILTERING:
			case K_FLOAT:
			case K_FROZEN:
			case K_INET:
			case K_INT:
			case K_KEY:
			case K_KEYS:
			case K_KEYSPACES:
			case K_LIST:
			case K_MAP:
			case K_NOSUPERUSER:
			case K_PASSWORD:
			case K_PERMISSION:
			case K_PERMISSIONS:
			case K_STATIC:
			case K_STORAGE:
			case K_SUPERUSER:
			case K_TEXT:
			case K_TIMESTAMP:
			case K_TIMEUUID:
			case K_TRIGGER:
			case K_TTL:
			case K_TUPLE:
			case K_TYPE:
			case K_USER:
			case K_USERS:
			case K_UUID:
			case K_VALUES:
			case K_VARCHAR:
			case K_VARINT:
			case K_WRITETIME:
				{
				alt98=3;
				}
				break;
			case QMARK:
				{
				alt98=4;
				}
				break;
			default:
				NoViableAltException nvae =
					new NoViableAltException("", 98, 0, input);
				throw nvae;
			}
			switch (alt98) {
				case 1 :
					// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:864:7: t= IDENT
					{
					t=(Token)match(input,IDENT,FOLLOW_IDENT_in_cfName5028); 
					 name.setColumnFamily((t!=null?t.getText():null), false); 
					}
					break;
				case 2 :
					// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:865:7: t= QUOTED_NAME
					{
					t=(Token)match(input,QUOTED_NAME,FOLLOW_QUOTED_NAME_in_cfName5053); 
					 name.setColumnFamily((t!=null?t.getText():null), true); 
					}
					break;
				case 3 :
					// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:866:7: k= unreserved_keyword
					{
					pushFollow(FOLLOW_unreserved_keyword_in_cfName5072);
					k=unreserved_keyword();
					state._fsp--;

					 name.setColumnFamily(k, false); 
					}
					break;
				case 4 :
					// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:867:7: QMARK
					{
					match(input,QMARK,FOLLOW_QMARK_in_cfName5082); 
					addRecognitionError("Bind variables cannot be used for table names");
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
	// $ANTLR end "cfName"



	// $ANTLR start "idxName"
	// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:870:1: idxName[IndexName name] : (t= IDENT |t= QUOTED_NAME |k= unreserved_keyword | QMARK );
	public final void idxName(IndexName name) throws RecognitionException {
		Token t=null;
		String k =null;

		try {
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:871:5: (t= IDENT |t= QUOTED_NAME |k= unreserved_keyword | QMARK )
			int alt99=4;
			switch ( input.LA(1) ) {
			case IDENT:
				{
				alt99=1;
				}
				break;
			case QUOTED_NAME:
				{
				alt99=2;
				}
				break;
			case K_ALL:
			case K_AS:
			case K_ASCII:
			case K_BIGINT:
			case K_BLOB:
			case K_BOOLEAN:
			case K_CLUSTERING:
			case K_COMPACT:
			case K_CONTAINS:
			case K_COUNT:
			case K_COUNTER:
			case K_CUSTOM:
			case K_DECIMAL:
			case K_DISTINCT:
			case K_DOUBLE:
			case K_EXISTS:
			case K_FILTERING:
			case K_FLOAT:
			case K_FROZEN:
			case K_INET:
			case K_INT:
			case K_KEY:
			case K_KEYS:
			case K_KEYSPACES:
			case K_LIST:
			case K_MAP:
			case K_NOSUPERUSER:
			case K_PASSWORD:
			case K_PERMISSION:
			case K_PERMISSIONS:
			case K_STATIC:
			case K_STORAGE:
			case K_SUPERUSER:
			case K_TEXT:
			case K_TIMESTAMP:
			case K_TIMEUUID:
			case K_TRIGGER:
			case K_TTL:
			case K_TUPLE:
			case K_TYPE:
			case K_USER:
			case K_USERS:
			case K_UUID:
			case K_VALUES:
			case K_VARCHAR:
			case K_VARINT:
			case K_WRITETIME:
				{
				alt99=3;
				}
				break;
			case QMARK:
				{
				alt99=4;
				}
				break;
			default:
				NoViableAltException nvae =
					new NoViableAltException("", 99, 0, input);
				throw nvae;
			}
			switch (alt99) {
				case 1 :
					// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:871:7: t= IDENT
					{
					t=(Token)match(input,IDENT,FOLLOW_IDENT_in_idxName5104); 
					 name.setIndex((t!=null?t.getText():null), false); 
					}
					break;
				case 2 :
					// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:872:7: t= QUOTED_NAME
					{
					t=(Token)match(input,QUOTED_NAME,FOLLOW_QUOTED_NAME_in_idxName5129); 
					 name.setIndex((t!=null?t.getText():null), true);
					}
					break;
				case 3 :
					// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:873:7: k= unreserved_keyword
					{
					pushFollow(FOLLOW_unreserved_keyword_in_idxName5148);
					k=unreserved_keyword();
					state._fsp--;

					 name.setIndex(k, false); 
					}
					break;
				case 4 :
					// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:874:7: QMARK
					{
					match(input,QMARK,FOLLOW_QMARK_in_idxName5158); 
					addRecognitionError("Bind variables cannot be used for index names");
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
	// $ANTLR end "idxName"



	// $ANTLR start "constant"
	// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:877:1: constant returns [Constants.Literal constant] : (t= STRING_LITERAL |t= INTEGER |t= FLOAT |t= BOOLEAN |t= UUID |t= HEXNUMBER | ( '-' )? t= ( K_NAN | K_INFINITY ) );
	public final Constants.Literal constant() throws RecognitionException {
		Constants.Literal constant = null;


		Token t=null;

		try {
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:878:5: (t= STRING_LITERAL |t= INTEGER |t= FLOAT |t= BOOLEAN |t= UUID |t= HEXNUMBER | ( '-' )? t= ( K_NAN | K_INFINITY ) )
			int alt101=7;
			switch ( input.LA(1) ) {
			case STRING_LITERAL:
				{
				alt101=1;
				}
				break;
			case INTEGER:
				{
				alt101=2;
				}
				break;
			case FLOAT:
				{
				alt101=3;
				}
				break;
			case BOOLEAN:
				{
				alt101=4;
				}
				break;
			case UUID:
				{
				alt101=5;
				}
				break;
			case HEXNUMBER:
				{
				alt101=6;
				}
				break;
			case K_INFINITY:
			case K_NAN:
			case 146:
				{
				alt101=7;
				}
				break;
			default:
				NoViableAltException nvae =
					new NoViableAltException("", 101, 0, input);
				throw nvae;
			}
			switch (alt101) {
				case 1 :
					// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:878:7: t= STRING_LITERAL
					{
					t=(Token)match(input,STRING_LITERAL,FOLLOW_STRING_LITERAL_in_constant5183); 
					 constant = Constants.Literal.string((t!=null?t.getText():null)); 
					}
					break;
				case 2 :
					// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:879:7: t= INTEGER
					{
					t=(Token)match(input,INTEGER,FOLLOW_INTEGER_in_constant5195); 
					 constant = Constants.Literal.integer((t!=null?t.getText():null)); 
					}
					break;
				case 3 :
					// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:880:7: t= FLOAT
					{
					t=(Token)match(input,FLOAT,FOLLOW_FLOAT_in_constant5214); 
					 constant = Constants.Literal.floatingPoint((t!=null?t.getText():null)); 
					}
					break;
				case 4 :
					// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:881:7: t= BOOLEAN
					{
					t=(Token)match(input,BOOLEAN,FOLLOW_BOOLEAN_in_constant5235); 
					 constant = Constants.Literal.bool((t!=null?t.getText():null)); 
					}
					break;
				case 5 :
					// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:882:7: t= UUID
					{
					t=(Token)match(input,UUID,FOLLOW_UUID_in_constant5254); 
					 constant = Constants.Literal.uuid((t!=null?t.getText():null)); 
					}
					break;
				case 6 :
					// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:883:7: t= HEXNUMBER
					{
					t=(Token)match(input,HEXNUMBER,FOLLOW_HEXNUMBER_in_constant5276); 
					 constant = Constants.Literal.hex((t!=null?t.getText():null)); 
					}
					break;
				case 7 :
					// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:884:7: ( '-' )? t= ( K_NAN | K_INFINITY )
					{
					 String sign=""; 
					// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:884:27: ( '-' )?
					int alt100=2;
					int LA100_0 = input.LA(1);
					if ( (LA100_0==146) ) {
						alt100=1;
					}
					switch (alt100) {
						case 1 :
							// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:884:28: '-'
							{
							match(input,146,FOLLOW_146_in_constant5294); 
							sign = "-"; 
							}
							break;

					}

					t=input.LT(1);
					if ( input.LA(1)==K_INFINITY||input.LA(1)==K_NAN ) {
						input.consume();
						state.errorRecovery=false;
					}
					else {
						MismatchedSetException mse = new MismatchedSetException(null,input);
						throw mse;
					}
					 constant = Constants.Literal.floatingPoint(sign + (t!=null?t.getText():null)); 
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
		return constant;
	}
	// $ANTLR end "constant"



	// $ANTLR start "mapLiteral"
	// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:887:1: mapLiteral returns [Maps.Literal map] : '{' (k1= term ':' v1= term ( ',' kn= term ':' vn= term )* )? '}' ;
	public final Maps.Literal mapLiteral() throws RecognitionException {
		Maps.Literal map = null;


		Term.Raw k1 =null;
		Term.Raw v1 =null;
		Term.Raw kn =null;
		Term.Raw vn =null;

		try {
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:888:5: ( '{' (k1= term ':' v1= term ( ',' kn= term ':' vn= term )* )? '}' )
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:888:7: '{' (k1= term ':' v1= term ( ',' kn= term ':' vn= term )* )? '}'
			{
			match(input,158,FOLLOW_158_in_mapLiteral5332); 
			 List<Pair<Term.Raw, Term.Raw>> m = new ArrayList<Pair<Term.Raw, Term.Raw>>(); 
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:889:11: (k1= term ':' v1= term ( ',' kn= term ':' vn= term )* )?
			int alt103=2;
			int LA103_0 = input.LA(1);
			if ( (LA103_0==BOOLEAN||LA103_0==FLOAT||LA103_0==HEXNUMBER||(LA103_0 >= IDENT && LA103_0 <= INTEGER)||LA103_0==K_ALL||LA103_0==K_AS||LA103_0==K_ASCII||(LA103_0 >= K_BIGINT && LA103_0 <= K_BOOLEAN)||LA103_0==K_CLUSTERING||(LA103_0 >= K_COMPACT && LA103_0 <= K_CONTAINS)||LA103_0==K_COUNTER||(LA103_0 >= K_CUSTOM && LA103_0 <= K_DECIMAL)||(LA103_0 >= K_DISTINCT && LA103_0 <= K_DOUBLE)||(LA103_0 >= K_EXISTS && LA103_0 <= K_FLOAT)||LA103_0==K_FROZEN||(LA103_0 >= K_INET && LA103_0 <= K_INFINITY)||LA103_0==K_INT||LA103_0==K_KEYS||LA103_0==K_KEYSPACES||(LA103_0 >= K_LIST && LA103_0 <= K_MAP)||LA103_0==K_NAN||LA103_0==K_NOSUPERUSER||LA103_0==K_NULL||(LA103_0 >= K_PASSWORD && LA103_0 <= K_PERMISSIONS)||(LA103_0 >= K_STATIC && LA103_0 <= K_TIMEUUID)||(LA103_0 >= K_TOKEN && LA103_0 <= K_TRIGGER)||(LA103_0 >= K_TUPLE && LA103_0 <= K_TYPE)||(LA103_0 >= K_USER && LA103_0 <= K_USERS)||(LA103_0 >= K_UUID && LA103_0 <= K_VARINT)||LA103_0==QMARK||LA103_0==STRING_LITERAL||LA103_0==UUID||LA103_0==142||LA103_0==146||LA103_0==148||LA103_0==155||LA103_0==158) ) {
				alt103=1;
			}
			switch (alt103) {
				case 1 :
					// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:889:13: k1= term ':' v1= term ( ',' kn= term ':' vn= term )*
					{
					pushFollow(FOLLOW_term_in_mapLiteral5350);
					k1=term();
					state._fsp--;

					match(input,148,FOLLOW_148_in_mapLiteral5352); 
					pushFollow(FOLLOW_term_in_mapLiteral5356);
					v1=term();
					state._fsp--;

					 m.add(Pair.create(k1, v1)); 
					// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:889:65: ( ',' kn= term ':' vn= term )*
					loop102:
					while (true) {
						int alt102=2;
						int LA102_0 = input.LA(1);
						if ( (LA102_0==145) ) {
							alt102=1;
						}

						switch (alt102) {
						case 1 :
							// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:889:67: ',' kn= term ':' vn= term
							{
							match(input,145,FOLLOW_145_in_mapLiteral5362); 
							pushFollow(FOLLOW_term_in_mapLiteral5366);
							kn=term();
							state._fsp--;

							match(input,148,FOLLOW_148_in_mapLiteral5368); 
							pushFollow(FOLLOW_term_in_mapLiteral5372);
							vn=term();
							state._fsp--;

							 m.add(Pair.create(kn, vn)); 
							}
							break;

						default :
							break loop102;
						}
					}

					}
					break;

			}

			match(input,159,FOLLOW_159_in_mapLiteral5388); 
			 map = new Maps.Literal(m); 
			}

		}
		catch (RecognitionException re) {
			reportError(re);
			recover(input,re);
		}
		finally {
			// do for sure before leaving
		}
		return map;
	}
	// $ANTLR end "mapLiteral"



	// $ANTLR start "setOrMapLiteral"
	// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:893:1: setOrMapLiteral[Term.Raw t] returns [Term.Raw value] : ( ':' v= term ( ',' kn= term ':' vn= term )* | ( ',' tn= term )* );
	public final Term.Raw setOrMapLiteral(Term.Raw t) throws RecognitionException {
		Term.Raw value = null;


		Term.Raw v =null;
		Term.Raw kn =null;
		Term.Raw vn =null;
		Term.Raw tn =null;

		try {
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:894:5: ( ':' v= term ( ',' kn= term ':' vn= term )* | ( ',' tn= term )* )
			int alt106=2;
			int LA106_0 = input.LA(1);
			if ( (LA106_0==148) ) {
				alt106=1;
			}
			else if ( (LA106_0==145||LA106_0==159) ) {
				alt106=2;
			}

			else {
				NoViableAltException nvae =
					new NoViableAltException("", 106, 0, input);
				throw nvae;
			}

			switch (alt106) {
				case 1 :
					// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:894:7: ':' v= term ( ',' kn= term ':' vn= term )*
					{
					match(input,148,FOLLOW_148_in_setOrMapLiteral5412); 
					pushFollow(FOLLOW_term_in_setOrMapLiteral5416);
					v=term();
					state._fsp--;

					 List<Pair<Term.Raw, Term.Raw>> m = new ArrayList<Pair<Term.Raw, Term.Raw>>(); m.add(Pair.create(t, v)); 
					// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:895:11: ( ',' kn= term ':' vn= term )*
					loop104:
					while (true) {
						int alt104=2;
						int LA104_0 = input.LA(1);
						if ( (LA104_0==145) ) {
							alt104=1;
						}

						switch (alt104) {
						case 1 :
							// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:895:13: ',' kn= term ':' vn= term
							{
							match(input,145,FOLLOW_145_in_setOrMapLiteral5432); 
							pushFollow(FOLLOW_term_in_setOrMapLiteral5436);
							kn=term();
							state._fsp--;

							match(input,148,FOLLOW_148_in_setOrMapLiteral5438); 
							pushFollow(FOLLOW_term_in_setOrMapLiteral5442);
							vn=term();
							state._fsp--;

							 m.add(Pair.create(kn, vn)); 
							}
							break;

						default :
							break loop104;
						}
					}

					 value = new Maps.Literal(m); 
					}
					break;
				case 2 :
					// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:897:7: ( ',' tn= term )*
					{
					 List<Term.Raw> s = new ArrayList<Term.Raw>(); s.add(t); 
					// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:898:11: ( ',' tn= term )*
					loop105:
					while (true) {
						int alt105=2;
						int LA105_0 = input.LA(1);
						if ( (LA105_0==145) ) {
							alt105=1;
						}

						switch (alt105) {
						case 1 :
							// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:898:13: ',' tn= term
							{
							match(input,145,FOLLOW_145_in_setOrMapLiteral5477); 
							pushFollow(FOLLOW_term_in_setOrMapLiteral5481);
							tn=term();
							state._fsp--;

							 s.add(tn); 
							}
							break;

						default :
							break loop105;
						}
					}

					 value = new Sets.Literal(s); 
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
		return value;
	}
	// $ANTLR end "setOrMapLiteral"



	// $ANTLR start "collectionLiteral"
	// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:902:1: collectionLiteral returns [Term.Raw value] : ( '[' (t1= term ( ',' tn= term )* )? ']' | '{' t= term v= setOrMapLiteral[t] '}' | '{' '}' );
	public final Term.Raw collectionLiteral() throws RecognitionException {
		Term.Raw value = null;


		Term.Raw t1 =null;
		Term.Raw tn =null;
		Term.Raw t =null;
		Term.Raw v =null;

		try {
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:903:5: ( '[' (t1= term ( ',' tn= term )* )? ']' | '{' t= term v= setOrMapLiteral[t] '}' | '{' '}' )
			int alt109=3;
			int LA109_0 = input.LA(1);
			if ( (LA109_0==155) ) {
				alt109=1;
			}
			else if ( (LA109_0==158) ) {
				int LA109_2 = input.LA(2);
				if ( (LA109_2==159) ) {
					alt109=3;
				}
				else if ( (LA109_2==BOOLEAN||LA109_2==FLOAT||LA109_2==HEXNUMBER||(LA109_2 >= IDENT && LA109_2 <= INTEGER)||LA109_2==K_ALL||LA109_2==K_AS||LA109_2==K_ASCII||(LA109_2 >= K_BIGINT && LA109_2 <= K_BOOLEAN)||LA109_2==K_CLUSTERING||(LA109_2 >= K_COMPACT && LA109_2 <= K_CONTAINS)||LA109_2==K_COUNTER||(LA109_2 >= K_CUSTOM && LA109_2 <= K_DECIMAL)||(LA109_2 >= K_DISTINCT && LA109_2 <= K_DOUBLE)||(LA109_2 >= K_EXISTS && LA109_2 <= K_FLOAT)||LA109_2==K_FROZEN||(LA109_2 >= K_INET && LA109_2 <= K_INFINITY)||LA109_2==K_INT||LA109_2==K_KEYS||LA109_2==K_KEYSPACES||(LA109_2 >= K_LIST && LA109_2 <= K_MAP)||LA109_2==K_NAN||LA109_2==K_NOSUPERUSER||LA109_2==K_NULL||(LA109_2 >= K_PASSWORD && LA109_2 <= K_PERMISSIONS)||(LA109_2 >= K_STATIC && LA109_2 <= K_TIMEUUID)||(LA109_2 >= K_TOKEN && LA109_2 <= K_TRIGGER)||(LA109_2 >= K_TUPLE && LA109_2 <= K_TYPE)||(LA109_2 >= K_USER && LA109_2 <= K_USERS)||(LA109_2 >= K_UUID && LA109_2 <= K_VARINT)||LA109_2==QMARK||LA109_2==STRING_LITERAL||LA109_2==UUID||LA109_2==142||LA109_2==146||LA109_2==148||LA109_2==155||LA109_2==158) ) {
					alt109=2;
				}

				else {
					int nvaeMark = input.mark();
					try {
						input.consume();
						NoViableAltException nvae =
							new NoViableAltException("", 109, 2, input);
						throw nvae;
					} finally {
						input.rewind(nvaeMark);
					}
				}

			}

			else {
				NoViableAltException nvae =
					new NoViableAltException("", 109, 0, input);
				throw nvae;
			}

			switch (alt109) {
				case 1 :
					// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:903:7: '[' (t1= term ( ',' tn= term )* )? ']'
					{
					match(input,155,FOLLOW_155_in_collectionLiteral5515); 
					 List<Term.Raw> l = new ArrayList<Term.Raw>(); 
					// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:904:11: (t1= term ( ',' tn= term )* )?
					int alt108=2;
					int LA108_0 = input.LA(1);
					if ( (LA108_0==BOOLEAN||LA108_0==FLOAT||LA108_0==HEXNUMBER||(LA108_0 >= IDENT && LA108_0 <= INTEGER)||LA108_0==K_ALL||LA108_0==K_AS||LA108_0==K_ASCII||(LA108_0 >= K_BIGINT && LA108_0 <= K_BOOLEAN)||LA108_0==K_CLUSTERING||(LA108_0 >= K_COMPACT && LA108_0 <= K_CONTAINS)||LA108_0==K_COUNTER||(LA108_0 >= K_CUSTOM && LA108_0 <= K_DECIMAL)||(LA108_0 >= K_DISTINCT && LA108_0 <= K_DOUBLE)||(LA108_0 >= K_EXISTS && LA108_0 <= K_FLOAT)||LA108_0==K_FROZEN||(LA108_0 >= K_INET && LA108_0 <= K_INFINITY)||LA108_0==K_INT||LA108_0==K_KEYS||LA108_0==K_KEYSPACES||(LA108_0 >= K_LIST && LA108_0 <= K_MAP)||LA108_0==K_NAN||LA108_0==K_NOSUPERUSER||LA108_0==K_NULL||(LA108_0 >= K_PASSWORD && LA108_0 <= K_PERMISSIONS)||(LA108_0 >= K_STATIC && LA108_0 <= K_TIMEUUID)||(LA108_0 >= K_TOKEN && LA108_0 <= K_TRIGGER)||(LA108_0 >= K_TUPLE && LA108_0 <= K_TYPE)||(LA108_0 >= K_USER && LA108_0 <= K_USERS)||(LA108_0 >= K_UUID && LA108_0 <= K_VARINT)||LA108_0==QMARK||LA108_0==STRING_LITERAL||LA108_0==UUID||LA108_0==142||LA108_0==146||LA108_0==148||LA108_0==155||LA108_0==158) ) {
						alt108=1;
					}
					switch (alt108) {
						case 1 :
							// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:904:13: t1= term ( ',' tn= term )*
							{
							pushFollow(FOLLOW_term_in_collectionLiteral5533);
							t1=term();
							state._fsp--;

							 l.add(t1); 
							// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:904:36: ( ',' tn= term )*
							loop107:
							while (true) {
								int alt107=2;
								int LA107_0 = input.LA(1);
								if ( (LA107_0==145) ) {
									alt107=1;
								}

								switch (alt107) {
								case 1 :
									// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:904:38: ',' tn= term
									{
									match(input,145,FOLLOW_145_in_collectionLiteral5539); 
									pushFollow(FOLLOW_term_in_collectionLiteral5543);
									tn=term();
									state._fsp--;

									 l.add(tn); 
									}
									break;

								default :
									break loop107;
								}
							}

							}
							break;

					}

					match(input,157,FOLLOW_157_in_collectionLiteral5559); 
					 value = new Lists.Literal(l); 
					}
					break;
				case 2 :
					// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:906:7: '{' t= term v= setOrMapLiteral[t] '}'
					{
					match(input,158,FOLLOW_158_in_collectionLiteral5569); 
					pushFollow(FOLLOW_term_in_collectionLiteral5573);
					t=term();
					state._fsp--;

					pushFollow(FOLLOW_setOrMapLiteral_in_collectionLiteral5577);
					v=setOrMapLiteral(t);
					state._fsp--;

					 value = v; 
					match(input,159,FOLLOW_159_in_collectionLiteral5582); 
					}
					break;
				case 3 :
					// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:909:7: '{' '}'
					{
					match(input,158,FOLLOW_158_in_collectionLiteral5600); 
					match(input,159,FOLLOW_159_in_collectionLiteral5602); 
					 value = new Sets.Literal(Collections.<Term.Raw>emptyList()); 
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
		return value;
	}
	// $ANTLR end "collectionLiteral"



	// $ANTLR start "usertypeLiteral"
	// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:912:1: usertypeLiteral returns [UserTypes.Literal ut] : '{' k1= ident ':' v1= term ( ',' kn= ident ':' vn= term )* '}' ;
	public final UserTypes.Literal usertypeLiteral() throws RecognitionException {
		UserTypes.Literal ut = null;


		ColumnIdentifier k1 =null;
		Term.Raw v1 =null;
		ColumnIdentifier kn =null;
		Term.Raw vn =null;

		 Map<ColumnIdentifier, Term.Raw> m = new HashMap<ColumnIdentifier, Term.Raw>(); 
		try {
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:916:5: ( '{' k1= ident ':' v1= term ( ',' kn= ident ':' vn= term )* '}' )
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:916:7: '{' k1= ident ':' v1= term ( ',' kn= ident ':' vn= term )* '}'
			{
			match(input,158,FOLLOW_158_in_usertypeLiteral5646); 
			pushFollow(FOLLOW_ident_in_usertypeLiteral5650);
			k1=ident();
			state._fsp--;

			match(input,148,FOLLOW_148_in_usertypeLiteral5652); 
			pushFollow(FOLLOW_term_in_usertypeLiteral5656);
			v1=term();
			state._fsp--;

			 m.put(k1, v1); 
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:916:51: ( ',' kn= ident ':' vn= term )*
			loop110:
			while (true) {
				int alt110=2;
				int LA110_0 = input.LA(1);
				if ( (LA110_0==145) ) {
					alt110=1;
				}

				switch (alt110) {
				case 1 :
					// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:916:53: ',' kn= ident ':' vn= term
					{
					match(input,145,FOLLOW_145_in_usertypeLiteral5662); 
					pushFollow(FOLLOW_ident_in_usertypeLiteral5666);
					kn=ident();
					state._fsp--;

					match(input,148,FOLLOW_148_in_usertypeLiteral5668); 
					pushFollow(FOLLOW_term_in_usertypeLiteral5672);
					vn=term();
					state._fsp--;

					 m.put(kn, vn); 
					}
					break;

				default :
					break loop110;
				}
			}

			match(input,159,FOLLOW_159_in_usertypeLiteral5679); 
			}

			 ut = new UserTypes.Literal(m); 
		}
		catch (RecognitionException re) {
			reportError(re);
			recover(input,re);
		}
		finally {
			// do for sure before leaving
		}
		return ut;
	}
	// $ANTLR end "usertypeLiteral"



	// $ANTLR start "tupleLiteral"
	// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:919:1: tupleLiteral returns [Tuples.Literal tt] : '(' t1= term ( ',' tn= term )* ')' ;
	public final Tuples.Literal tupleLiteral() throws RecognitionException {
		Tuples.Literal tt = null;


		Term.Raw t1 =null;
		Term.Raw tn =null;

		 List<Term.Raw> l = new ArrayList<Term.Raw>(); 
		try {
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:922:5: ( '(' t1= term ( ',' tn= term )* ')' )
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:922:7: '(' t1= term ( ',' tn= term )* ')'
			{
			match(input,142,FOLLOW_142_in_tupleLiteral5716); 
			pushFollow(FOLLOW_term_in_tupleLiteral5720);
			t1=term();
			state._fsp--;

			 l.add(t1); 
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:922:34: ( ',' tn= term )*
			loop111:
			while (true) {
				int alt111=2;
				int LA111_0 = input.LA(1);
				if ( (LA111_0==145) ) {
					alt111=1;
				}

				switch (alt111) {
				case 1 :
					// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:922:36: ',' tn= term
					{
					match(input,145,FOLLOW_145_in_tupleLiteral5726); 
					pushFollow(FOLLOW_term_in_tupleLiteral5730);
					tn=term();
					state._fsp--;

					 l.add(tn); 
					}
					break;

				default :
					break loop111;
				}
			}

			match(input,143,FOLLOW_143_in_tupleLiteral5737); 
			}

			 tt = new Tuples.Literal(l); 
		}
		catch (RecognitionException re) {
			reportError(re);
			recover(input,re);
		}
		finally {
			// do for sure before leaving
		}
		return tt;
	}
	// $ANTLR end "tupleLiteral"



	// $ANTLR start "value"
	// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:925:1: value returns [Term.Raw value] : (c= constant |l= collectionLiteral |u= usertypeLiteral |t= tupleLiteral | K_NULL | ':' id= ident | QMARK );
	public final Term.Raw value() throws RecognitionException {
		Term.Raw value = null;


		Constants.Literal c =null;
		Term.Raw l =null;
		UserTypes.Literal u =null;
		Tuples.Literal t =null;
		ColumnIdentifier id =null;

		try {
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:926:5: (c= constant |l= collectionLiteral |u= usertypeLiteral |t= tupleLiteral | K_NULL | ':' id= ident | QMARK )
			int alt112=7;
			switch ( input.LA(1) ) {
			case BOOLEAN:
			case FLOAT:
			case HEXNUMBER:
			case INTEGER:
			case K_INFINITY:
			case K_NAN:
			case STRING_LITERAL:
			case UUID:
			case 146:
				{
				alt112=1;
				}
				break;
			case 155:
				{
				alt112=2;
				}
				break;
			case 158:
				{
				switch ( input.LA(2) ) {
				case BOOLEAN:
				case FLOAT:
				case HEXNUMBER:
				case INTEGER:
				case K_INFINITY:
				case K_NAN:
				case K_NULL:
				case K_TOKEN:
				case QMARK:
				case STRING_LITERAL:
				case UUID:
				case 142:
				case 146:
				case 148:
				case 155:
				case 158:
				case 159:
					{
					alt112=2;
					}
					break;
				case IDENT:
					{
					int LA112_8 = input.LA(3);
					if ( (LA112_8==142) ) {
						alt112=2;
					}
					else if ( (LA112_8==148) ) {
						alt112=3;
					}

					else {
						int nvaeMark = input.mark();
						try {
							for (int nvaeConsume = 0; nvaeConsume < 3 - 1; nvaeConsume++) {
								input.consume();
							}
							NoViableAltException nvae =
								new NoViableAltException("", 112, 8, input);
							throw nvae;
						} finally {
							input.rewind(nvaeMark);
						}
					}

					}
					break;
				case K_ALL:
				case K_AS:
				case K_CLUSTERING:
				case K_COMPACT:
				case K_CONTAINS:
				case K_CUSTOM:
				case K_DISTINCT:
				case K_EXISTS:
				case K_FILTERING:
				case K_FROZEN:
				case K_KEYS:
				case K_KEYSPACES:
				case K_LIST:
				case K_MAP:
				case K_NOSUPERUSER:
				case K_PASSWORD:
				case K_PERMISSION:
				case K_PERMISSIONS:
				case K_STATIC:
				case K_STORAGE:
				case K_SUPERUSER:
				case K_TRIGGER:
				case K_TUPLE:
				case K_TYPE:
				case K_USER:
				case K_USERS:
				case K_VALUES:
					{
					int LA112_9 = input.LA(3);
					if ( (LA112_9==142) ) {
						alt112=2;
					}
					else if ( (LA112_9==148) ) {
						alt112=3;
					}

					else {
						int nvaeMark = input.mark();
						try {
							for (int nvaeConsume = 0; nvaeConsume < 3 - 1; nvaeConsume++) {
								input.consume();
							}
							NoViableAltException nvae =
								new NoViableAltException("", 112, 9, input);
							throw nvae;
						} finally {
							input.rewind(nvaeMark);
						}
					}

					}
					break;
				case K_ASCII:
					{
					int LA112_10 = input.LA(3);
					if ( (LA112_10==142) ) {
						alt112=2;
					}
					else if ( (LA112_10==148) ) {
						alt112=3;
					}

					else {
						int nvaeMark = input.mark();
						try {
							for (int nvaeConsume = 0; nvaeConsume < 3 - 1; nvaeConsume++) {
								input.consume();
							}
							NoViableAltException nvae =
								new NoViableAltException("", 112, 10, input);
							throw nvae;
						} finally {
							input.rewind(nvaeMark);
						}
					}

					}
					break;
				case K_BIGINT:
					{
					int LA112_11 = input.LA(3);
					if ( (LA112_11==142) ) {
						alt112=2;
					}
					else if ( (LA112_11==148) ) {
						alt112=3;
					}

					else {
						int nvaeMark = input.mark();
						try {
							for (int nvaeConsume = 0; nvaeConsume < 3 - 1; nvaeConsume++) {
								input.consume();
							}
							NoViableAltException nvae =
								new NoViableAltException("", 112, 11, input);
							throw nvae;
						} finally {
							input.rewind(nvaeMark);
						}
					}

					}
					break;
				case K_BLOB:
					{
					int LA112_12 = input.LA(3);
					if ( (LA112_12==142) ) {
						alt112=2;
					}
					else if ( (LA112_12==148) ) {
						alt112=3;
					}

					else {
						int nvaeMark = input.mark();
						try {
							for (int nvaeConsume = 0; nvaeConsume < 3 - 1; nvaeConsume++) {
								input.consume();
							}
							NoViableAltException nvae =
								new NoViableAltException("", 112, 12, input);
							throw nvae;
						} finally {
							input.rewind(nvaeMark);
						}
					}

					}
					break;
				case K_BOOLEAN:
					{
					int LA112_13 = input.LA(3);
					if ( (LA112_13==142) ) {
						alt112=2;
					}
					else if ( (LA112_13==148) ) {
						alt112=3;
					}

					else {
						int nvaeMark = input.mark();
						try {
							for (int nvaeConsume = 0; nvaeConsume < 3 - 1; nvaeConsume++) {
								input.consume();
							}
							NoViableAltException nvae =
								new NoViableAltException("", 112, 13, input);
							throw nvae;
						} finally {
							input.rewind(nvaeMark);
						}
					}

					}
					break;
				case K_COUNTER:
					{
					int LA112_14 = input.LA(3);
					if ( (LA112_14==142) ) {
						alt112=2;
					}
					else if ( (LA112_14==148) ) {
						alt112=3;
					}

					else {
						int nvaeMark = input.mark();
						try {
							for (int nvaeConsume = 0; nvaeConsume < 3 - 1; nvaeConsume++) {
								input.consume();
							}
							NoViableAltException nvae =
								new NoViableAltException("", 112, 14, input);
							throw nvae;
						} finally {
							input.rewind(nvaeMark);
						}
					}

					}
					break;
				case K_DECIMAL:
					{
					int LA112_15 = input.LA(3);
					if ( (LA112_15==142) ) {
						alt112=2;
					}
					else if ( (LA112_15==148) ) {
						alt112=3;
					}

					else {
						int nvaeMark = input.mark();
						try {
							for (int nvaeConsume = 0; nvaeConsume < 3 - 1; nvaeConsume++) {
								input.consume();
							}
							NoViableAltException nvae =
								new NoViableAltException("", 112, 15, input);
							throw nvae;
						} finally {
							input.rewind(nvaeMark);
						}
					}

					}
					break;
				case K_DOUBLE:
					{
					int LA112_16 = input.LA(3);
					if ( (LA112_16==142) ) {
						alt112=2;
					}
					else if ( (LA112_16==148) ) {
						alt112=3;
					}

					else {
						int nvaeMark = input.mark();
						try {
							for (int nvaeConsume = 0; nvaeConsume < 3 - 1; nvaeConsume++) {
								input.consume();
							}
							NoViableAltException nvae =
								new NoViableAltException("", 112, 16, input);
							throw nvae;
						} finally {
							input.rewind(nvaeMark);
						}
					}

					}
					break;
				case K_FLOAT:
					{
					int LA112_17 = input.LA(3);
					if ( (LA112_17==142) ) {
						alt112=2;
					}
					else if ( (LA112_17==148) ) {
						alt112=3;
					}

					else {
						int nvaeMark = input.mark();
						try {
							for (int nvaeConsume = 0; nvaeConsume < 3 - 1; nvaeConsume++) {
								input.consume();
							}
							NoViableAltException nvae =
								new NoViableAltException("", 112, 17, input);
							throw nvae;
						} finally {
							input.rewind(nvaeMark);
						}
					}

					}
					break;
				case K_INET:
					{
					int LA112_18 = input.LA(3);
					if ( (LA112_18==142) ) {
						alt112=2;
					}
					else if ( (LA112_18==148) ) {
						alt112=3;
					}

					else {
						int nvaeMark = input.mark();
						try {
							for (int nvaeConsume = 0; nvaeConsume < 3 - 1; nvaeConsume++) {
								input.consume();
							}
							NoViableAltException nvae =
								new NoViableAltException("", 112, 18, input);
							throw nvae;
						} finally {
							input.rewind(nvaeMark);
						}
					}

					}
					break;
				case K_INT:
					{
					int LA112_19 = input.LA(3);
					if ( (LA112_19==142) ) {
						alt112=2;
					}
					else if ( (LA112_19==148) ) {
						alt112=3;
					}

					else {
						int nvaeMark = input.mark();
						try {
							for (int nvaeConsume = 0; nvaeConsume < 3 - 1; nvaeConsume++) {
								input.consume();
							}
							NoViableAltException nvae =
								new NoViableAltException("", 112, 19, input);
							throw nvae;
						} finally {
							input.rewind(nvaeMark);
						}
					}

					}
					break;
				case K_TEXT:
					{
					int LA112_20 = input.LA(3);
					if ( (LA112_20==142) ) {
						alt112=2;
					}
					else if ( (LA112_20==148) ) {
						alt112=3;
					}

					else {
						int nvaeMark = input.mark();
						try {
							for (int nvaeConsume = 0; nvaeConsume < 3 - 1; nvaeConsume++) {
								input.consume();
							}
							NoViableAltException nvae =
								new NoViableAltException("", 112, 20, input);
							throw nvae;
						} finally {
							input.rewind(nvaeMark);
						}
					}

					}
					break;
				case K_TIMESTAMP:
					{
					int LA112_21 = input.LA(3);
					if ( (LA112_21==142) ) {
						alt112=2;
					}
					else if ( (LA112_21==148) ) {
						alt112=3;
					}

					else {
						int nvaeMark = input.mark();
						try {
							for (int nvaeConsume = 0; nvaeConsume < 3 - 1; nvaeConsume++) {
								input.consume();
							}
							NoViableAltException nvae =
								new NoViableAltException("", 112, 21, input);
							throw nvae;
						} finally {
							input.rewind(nvaeMark);
						}
					}

					}
					break;
				case K_UUID:
					{
					int LA112_22 = input.LA(3);
					if ( (LA112_22==142) ) {
						alt112=2;
					}
					else if ( (LA112_22==148) ) {
						alt112=3;
					}

					else {
						int nvaeMark = input.mark();
						try {
							for (int nvaeConsume = 0; nvaeConsume < 3 - 1; nvaeConsume++) {
								input.consume();
							}
							NoViableAltException nvae =
								new NoViableAltException("", 112, 22, input);
							throw nvae;
						} finally {
							input.rewind(nvaeMark);
						}
					}

					}
					break;
				case K_VARCHAR:
					{
					int LA112_23 = input.LA(3);
					if ( (LA112_23==142) ) {
						alt112=2;
					}
					else if ( (LA112_23==148) ) {
						alt112=3;
					}

					else {
						int nvaeMark = input.mark();
						try {
							for (int nvaeConsume = 0; nvaeConsume < 3 - 1; nvaeConsume++) {
								input.consume();
							}
							NoViableAltException nvae =
								new NoViableAltException("", 112, 23, input);
							throw nvae;
						} finally {
							input.rewind(nvaeMark);
						}
					}

					}
					break;
				case K_VARINT:
					{
					int LA112_24 = input.LA(3);
					if ( (LA112_24==142) ) {
						alt112=2;
					}
					else if ( (LA112_24==148) ) {
						alt112=3;
					}

					else {
						int nvaeMark = input.mark();
						try {
							for (int nvaeConsume = 0; nvaeConsume < 3 - 1; nvaeConsume++) {
								input.consume();
							}
							NoViableAltException nvae =
								new NoViableAltException("", 112, 24, input);
							throw nvae;
						} finally {
							input.rewind(nvaeMark);
						}
					}

					}
					break;
				case K_TIMEUUID:
					{
					int LA112_25 = input.LA(3);
					if ( (LA112_25==142) ) {
						alt112=2;
					}
					else if ( (LA112_25==148) ) {
						alt112=3;
					}

					else {
						int nvaeMark = input.mark();
						try {
							for (int nvaeConsume = 0; nvaeConsume < 3 - 1; nvaeConsume++) {
								input.consume();
							}
							NoViableAltException nvae =
								new NoViableAltException("", 112, 25, input);
							throw nvae;
						} finally {
							input.rewind(nvaeMark);
						}
					}

					}
					break;
				case K_COUNT:
				case K_KEY:
				case K_TTL:
				case K_WRITETIME:
				case QUOTED_NAME:
					{
					alt112=3;
					}
					break;
				default:
					int nvaeMark = input.mark();
					try {
						input.consume();
						NoViableAltException nvae =
							new NoViableAltException("", 112, 3, input);
						throw nvae;
					} finally {
						input.rewind(nvaeMark);
					}
				}
				}
				break;
			case 142:
				{
				alt112=4;
				}
				break;
			case K_NULL:
				{
				alt112=5;
				}
				break;
			case 148:
				{
				alt112=6;
				}
				break;
			case QMARK:
				{
				alt112=7;
				}
				break;
			default:
				NoViableAltException nvae =
					new NoViableAltException("", 112, 0, input);
				throw nvae;
			}
			switch (alt112) {
				case 1 :
					// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:926:7: c= constant
					{
					pushFollow(FOLLOW_constant_in_value5760);
					c=constant();
					state._fsp--;

					 value = c; 
					}
					break;
				case 2 :
					// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:927:7: l= collectionLiteral
					{
					pushFollow(FOLLOW_collectionLiteral_in_value5782);
					l=collectionLiteral();
					state._fsp--;

					 value = l; 
					}
					break;
				case 3 :
					// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:928:7: u= usertypeLiteral
					{
					pushFollow(FOLLOW_usertypeLiteral_in_value5795);
					u=usertypeLiteral();
					state._fsp--;

					 value = u; 
					}
					break;
				case 4 :
					// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:929:7: t= tupleLiteral
					{
					pushFollow(FOLLOW_tupleLiteral_in_value5810);
					t=tupleLiteral();
					state._fsp--;

					 value = t; 
					}
					break;
				case 5 :
					// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:930:7: K_NULL
					{
					match(input,K_NULL,FOLLOW_K_NULL_in_value5826); 
					 value = Constants.NULL_LITERAL; 
					}
					break;
				case 6 :
					// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:931:7: ':' id= ident
					{
					match(input,148,FOLLOW_148_in_value5850); 
					pushFollow(FOLLOW_ident_in_value5854);
					id=ident();
					state._fsp--;

					 value = newBindVariables(id); 
					}
					break;
				case 7 :
					// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:932:7: QMARK
					{
					match(input,QMARK,FOLLOW_QMARK_in_value5872); 
					 value = newBindVariables(null); 
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
		return value;
	}
	// $ANTLR end "value"



	// $ANTLR start "intValue"
	// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:935:1: intValue returns [Term.Raw value] : (|t= INTEGER | ':' id= ident | QMARK );
	public final Term.Raw intValue() throws RecognitionException {
		Term.Raw value = null;


		Token t=null;
		ColumnIdentifier id =null;

		try {
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:936:5: (|t= INTEGER | ':' id= ident | QMARK )
			int alt113=4;
			switch ( input.LA(1) ) {
			case EOF:
			case K_ALLOW:
			case K_AND:
			case K_APPLY:
			case K_DELETE:
			case K_INSERT:
			case K_SET:
			case K_UPDATE:
			case K_WHERE:
			case 149:
				{
				alt113=1;
				}
				break;
			case INTEGER:
				{
				alt113=2;
				}
				break;
			case 148:
				{
				alt113=3;
				}
				break;
			case QMARK:
				{
				alt113=4;
				}
				break;
			default:
				NoViableAltException nvae =
					new NoViableAltException("", 113, 0, input);
				throw nvae;
			}
			switch (alt113) {
				case 1 :
					// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:937:5: 
					{
					}
					break;
				case 2 :
					// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:937:7: t= INTEGER
					{
					t=(Token)match(input,INTEGER,FOLLOW_INTEGER_in_intValue5918); 
					 value = Constants.Literal.integer((t!=null?t.getText():null)); 
					}
					break;
				case 3 :
					// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:938:7: ':' id= ident
					{
					match(input,148,FOLLOW_148_in_intValue5932); 
					pushFollow(FOLLOW_ident_in_intValue5936);
					id=ident();
					state._fsp--;

					 value = newBindVariables(id); 
					}
					break;
				case 4 :
					// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:939:7: QMARK
					{
					match(input,QMARK,FOLLOW_QMARK_in_intValue5947); 
					 value = newBindVariables(null); 
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
		return value;
	}
	// $ANTLR end "intValue"



	// $ANTLR start "functionName"
	// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:942:1: functionName returns [String s] : (f= IDENT |u= unreserved_function_keyword | K_TOKEN );
	public final String functionName() throws RecognitionException {
		String s = null;


		Token f=null;
		String u =null;

		try {
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:943:5: (f= IDENT |u= unreserved_function_keyword | K_TOKEN )
			int alt114=3;
			switch ( input.LA(1) ) {
			case IDENT:
				{
				alt114=1;
				}
				break;
			case K_ALL:
			case K_AS:
			case K_ASCII:
			case K_BIGINT:
			case K_BLOB:
			case K_BOOLEAN:
			case K_CLUSTERING:
			case K_COMPACT:
			case K_CONTAINS:
			case K_COUNTER:
			case K_CUSTOM:
			case K_DECIMAL:
			case K_DISTINCT:
			case K_DOUBLE:
			case K_EXISTS:
			case K_FILTERING:
			case K_FLOAT:
			case K_FROZEN:
			case K_INET:
			case K_INT:
			case K_KEYS:
			case K_KEYSPACES:
			case K_LIST:
			case K_MAP:
			case K_NOSUPERUSER:
			case K_PASSWORD:
			case K_PERMISSION:
			case K_PERMISSIONS:
			case K_STATIC:
			case K_STORAGE:
			case K_SUPERUSER:
			case K_TEXT:
			case K_TIMESTAMP:
			case K_TIMEUUID:
			case K_TRIGGER:
			case K_TUPLE:
			case K_TYPE:
			case K_USER:
			case K_USERS:
			case K_UUID:
			case K_VALUES:
			case K_VARCHAR:
			case K_VARINT:
				{
				alt114=2;
				}
				break;
			case K_TOKEN:
				{
				alt114=3;
				}
				break;
			default:
				NoViableAltException nvae =
					new NoViableAltException("", 114, 0, input);
				throw nvae;
			}
			switch (alt114) {
				case 1 :
					// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:943:7: f= IDENT
					{
					f=(Token)match(input,IDENT,FOLLOW_IDENT_in_functionName5980); 
					 s = (f!=null?f.getText():null); 
					}
					break;
				case 2 :
					// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:944:7: u= unreserved_function_keyword
					{
					pushFollow(FOLLOW_unreserved_function_keyword_in_functionName6014);
					u=unreserved_function_keyword();
					state._fsp--;

					 s = u; 
					}
					break;
				case 3 :
					// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:945:7: K_TOKEN
					{
					match(input,K_TOKEN,FOLLOW_K_TOKEN_in_functionName6024); 
					 s = "token"; 
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
		return s;
	}
	// $ANTLR end "functionName"



	// $ANTLR start "function"
	// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:948:1: function returns [Term.Raw t] : (f= functionName '(' ')' |f= functionName '(' args= functionArgs ')' );
	public final Term.Raw function() throws RecognitionException {
		Term.Raw t = null;


		String f =null;
		List<Term.Raw> args =null;

		try {
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:949:5: (f= functionName '(' ')' |f= functionName '(' args= functionArgs ')' )
			int alt115=2;
			switch ( input.LA(1) ) {
			case IDENT:
				{
				int LA115_1 = input.LA(2);
				if ( (LA115_1==142) ) {
					int LA115_20 = input.LA(3);
					if ( (LA115_20==143) ) {
						alt115=1;
					}
					else if ( (LA115_20==BOOLEAN||LA115_20==FLOAT||LA115_20==HEXNUMBER||(LA115_20 >= IDENT && LA115_20 <= INTEGER)||LA115_20==K_ALL||LA115_20==K_AS||LA115_20==K_ASCII||(LA115_20 >= K_BIGINT && LA115_20 <= K_BOOLEAN)||LA115_20==K_CLUSTERING||(LA115_20 >= K_COMPACT && LA115_20 <= K_CONTAINS)||LA115_20==K_COUNTER||(LA115_20 >= K_CUSTOM && LA115_20 <= K_DECIMAL)||(LA115_20 >= K_DISTINCT && LA115_20 <= K_DOUBLE)||(LA115_20 >= K_EXISTS && LA115_20 <= K_FLOAT)||LA115_20==K_FROZEN||(LA115_20 >= K_INET && LA115_20 <= K_INFINITY)||LA115_20==K_INT||LA115_20==K_KEYS||LA115_20==K_KEYSPACES||(LA115_20 >= K_LIST && LA115_20 <= K_MAP)||LA115_20==K_NAN||LA115_20==K_NOSUPERUSER||LA115_20==K_NULL||(LA115_20 >= K_PASSWORD && LA115_20 <= K_PERMISSIONS)||(LA115_20 >= K_STATIC && LA115_20 <= K_TIMEUUID)||(LA115_20 >= K_TOKEN && LA115_20 <= K_TRIGGER)||(LA115_20 >= K_TUPLE && LA115_20 <= K_TYPE)||(LA115_20 >= K_USER && LA115_20 <= K_USERS)||(LA115_20 >= K_UUID && LA115_20 <= K_VARINT)||LA115_20==QMARK||LA115_20==STRING_LITERAL||LA115_20==UUID||LA115_20==142||LA115_20==146||LA115_20==148||LA115_20==155||LA115_20==158) ) {
						alt115=2;
					}

					else {
						int nvaeMark = input.mark();
						try {
							for (int nvaeConsume = 0; nvaeConsume < 3 - 1; nvaeConsume++) {
								input.consume();
							}
							NoViableAltException nvae =
								new NoViableAltException("", 115, 20, input);
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
							new NoViableAltException("", 115, 1, input);
						throw nvae;
					} finally {
						input.rewind(nvaeMark);
					}
				}

				}
				break;
			case K_ALL:
			case K_AS:
			case K_CLUSTERING:
			case K_COMPACT:
			case K_CONTAINS:
			case K_CUSTOM:
			case K_DISTINCT:
			case K_EXISTS:
			case K_FILTERING:
			case K_FROZEN:
			case K_KEYS:
			case K_KEYSPACES:
			case K_LIST:
			case K_MAP:
			case K_NOSUPERUSER:
			case K_PASSWORD:
			case K_PERMISSION:
			case K_PERMISSIONS:
			case K_STATIC:
			case K_STORAGE:
			case K_SUPERUSER:
			case K_TRIGGER:
			case K_TUPLE:
			case K_TYPE:
			case K_USER:
			case K_USERS:
			case K_VALUES:
				{
				int LA115_2 = input.LA(2);
				if ( (LA115_2==142) ) {
					int LA115_20 = input.LA(3);
					if ( (LA115_20==143) ) {
						alt115=1;
					}
					else if ( (LA115_20==BOOLEAN||LA115_20==FLOAT||LA115_20==HEXNUMBER||(LA115_20 >= IDENT && LA115_20 <= INTEGER)||LA115_20==K_ALL||LA115_20==K_AS||LA115_20==K_ASCII||(LA115_20 >= K_BIGINT && LA115_20 <= K_BOOLEAN)||LA115_20==K_CLUSTERING||(LA115_20 >= K_COMPACT && LA115_20 <= K_CONTAINS)||LA115_20==K_COUNTER||(LA115_20 >= K_CUSTOM && LA115_20 <= K_DECIMAL)||(LA115_20 >= K_DISTINCT && LA115_20 <= K_DOUBLE)||(LA115_20 >= K_EXISTS && LA115_20 <= K_FLOAT)||LA115_20==K_FROZEN||(LA115_20 >= K_INET && LA115_20 <= K_INFINITY)||LA115_20==K_INT||LA115_20==K_KEYS||LA115_20==K_KEYSPACES||(LA115_20 >= K_LIST && LA115_20 <= K_MAP)||LA115_20==K_NAN||LA115_20==K_NOSUPERUSER||LA115_20==K_NULL||(LA115_20 >= K_PASSWORD && LA115_20 <= K_PERMISSIONS)||(LA115_20 >= K_STATIC && LA115_20 <= K_TIMEUUID)||(LA115_20 >= K_TOKEN && LA115_20 <= K_TRIGGER)||(LA115_20 >= K_TUPLE && LA115_20 <= K_TYPE)||(LA115_20 >= K_USER && LA115_20 <= K_USERS)||(LA115_20 >= K_UUID && LA115_20 <= K_VARINT)||LA115_20==QMARK||LA115_20==STRING_LITERAL||LA115_20==UUID||LA115_20==142||LA115_20==146||LA115_20==148||LA115_20==155||LA115_20==158) ) {
						alt115=2;
					}

					else {
						int nvaeMark = input.mark();
						try {
							for (int nvaeConsume = 0; nvaeConsume < 3 - 1; nvaeConsume++) {
								input.consume();
							}
							NoViableAltException nvae =
								new NoViableAltException("", 115, 20, input);
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
							new NoViableAltException("", 115, 2, input);
						throw nvae;
					} finally {
						input.rewind(nvaeMark);
					}
				}

				}
				break;
			case K_ASCII:
				{
				int LA115_3 = input.LA(2);
				if ( (LA115_3==142) ) {
					int LA115_20 = input.LA(3);
					if ( (LA115_20==143) ) {
						alt115=1;
					}
					else if ( (LA115_20==BOOLEAN||LA115_20==FLOAT||LA115_20==HEXNUMBER||(LA115_20 >= IDENT && LA115_20 <= INTEGER)||LA115_20==K_ALL||LA115_20==K_AS||LA115_20==K_ASCII||(LA115_20 >= K_BIGINT && LA115_20 <= K_BOOLEAN)||LA115_20==K_CLUSTERING||(LA115_20 >= K_COMPACT && LA115_20 <= K_CONTAINS)||LA115_20==K_COUNTER||(LA115_20 >= K_CUSTOM && LA115_20 <= K_DECIMAL)||(LA115_20 >= K_DISTINCT && LA115_20 <= K_DOUBLE)||(LA115_20 >= K_EXISTS && LA115_20 <= K_FLOAT)||LA115_20==K_FROZEN||(LA115_20 >= K_INET && LA115_20 <= K_INFINITY)||LA115_20==K_INT||LA115_20==K_KEYS||LA115_20==K_KEYSPACES||(LA115_20 >= K_LIST && LA115_20 <= K_MAP)||LA115_20==K_NAN||LA115_20==K_NOSUPERUSER||LA115_20==K_NULL||(LA115_20 >= K_PASSWORD && LA115_20 <= K_PERMISSIONS)||(LA115_20 >= K_STATIC && LA115_20 <= K_TIMEUUID)||(LA115_20 >= K_TOKEN && LA115_20 <= K_TRIGGER)||(LA115_20 >= K_TUPLE && LA115_20 <= K_TYPE)||(LA115_20 >= K_USER && LA115_20 <= K_USERS)||(LA115_20 >= K_UUID && LA115_20 <= K_VARINT)||LA115_20==QMARK||LA115_20==STRING_LITERAL||LA115_20==UUID||LA115_20==142||LA115_20==146||LA115_20==148||LA115_20==155||LA115_20==158) ) {
						alt115=2;
					}

					else {
						int nvaeMark = input.mark();
						try {
							for (int nvaeConsume = 0; nvaeConsume < 3 - 1; nvaeConsume++) {
								input.consume();
							}
							NoViableAltException nvae =
								new NoViableAltException("", 115, 20, input);
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
							new NoViableAltException("", 115, 3, input);
						throw nvae;
					} finally {
						input.rewind(nvaeMark);
					}
				}

				}
				break;
			case K_BIGINT:
				{
				int LA115_4 = input.LA(2);
				if ( (LA115_4==142) ) {
					int LA115_20 = input.LA(3);
					if ( (LA115_20==143) ) {
						alt115=1;
					}
					else if ( (LA115_20==BOOLEAN||LA115_20==FLOAT||LA115_20==HEXNUMBER||(LA115_20 >= IDENT && LA115_20 <= INTEGER)||LA115_20==K_ALL||LA115_20==K_AS||LA115_20==K_ASCII||(LA115_20 >= K_BIGINT && LA115_20 <= K_BOOLEAN)||LA115_20==K_CLUSTERING||(LA115_20 >= K_COMPACT && LA115_20 <= K_CONTAINS)||LA115_20==K_COUNTER||(LA115_20 >= K_CUSTOM && LA115_20 <= K_DECIMAL)||(LA115_20 >= K_DISTINCT && LA115_20 <= K_DOUBLE)||(LA115_20 >= K_EXISTS && LA115_20 <= K_FLOAT)||LA115_20==K_FROZEN||(LA115_20 >= K_INET && LA115_20 <= K_INFINITY)||LA115_20==K_INT||LA115_20==K_KEYS||LA115_20==K_KEYSPACES||(LA115_20 >= K_LIST && LA115_20 <= K_MAP)||LA115_20==K_NAN||LA115_20==K_NOSUPERUSER||LA115_20==K_NULL||(LA115_20 >= K_PASSWORD && LA115_20 <= K_PERMISSIONS)||(LA115_20 >= K_STATIC && LA115_20 <= K_TIMEUUID)||(LA115_20 >= K_TOKEN && LA115_20 <= K_TRIGGER)||(LA115_20 >= K_TUPLE && LA115_20 <= K_TYPE)||(LA115_20 >= K_USER && LA115_20 <= K_USERS)||(LA115_20 >= K_UUID && LA115_20 <= K_VARINT)||LA115_20==QMARK||LA115_20==STRING_LITERAL||LA115_20==UUID||LA115_20==142||LA115_20==146||LA115_20==148||LA115_20==155||LA115_20==158) ) {
						alt115=2;
					}

					else {
						int nvaeMark = input.mark();
						try {
							for (int nvaeConsume = 0; nvaeConsume < 3 - 1; nvaeConsume++) {
								input.consume();
							}
							NoViableAltException nvae =
								new NoViableAltException("", 115, 20, input);
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
							new NoViableAltException("", 115, 4, input);
						throw nvae;
					} finally {
						input.rewind(nvaeMark);
					}
				}

				}
				break;
			case K_BLOB:
				{
				int LA115_5 = input.LA(2);
				if ( (LA115_5==142) ) {
					int LA115_20 = input.LA(3);
					if ( (LA115_20==143) ) {
						alt115=1;
					}
					else if ( (LA115_20==BOOLEAN||LA115_20==FLOAT||LA115_20==HEXNUMBER||(LA115_20 >= IDENT && LA115_20 <= INTEGER)||LA115_20==K_ALL||LA115_20==K_AS||LA115_20==K_ASCII||(LA115_20 >= K_BIGINT && LA115_20 <= K_BOOLEAN)||LA115_20==K_CLUSTERING||(LA115_20 >= K_COMPACT && LA115_20 <= K_CONTAINS)||LA115_20==K_COUNTER||(LA115_20 >= K_CUSTOM && LA115_20 <= K_DECIMAL)||(LA115_20 >= K_DISTINCT && LA115_20 <= K_DOUBLE)||(LA115_20 >= K_EXISTS && LA115_20 <= K_FLOAT)||LA115_20==K_FROZEN||(LA115_20 >= K_INET && LA115_20 <= K_INFINITY)||LA115_20==K_INT||LA115_20==K_KEYS||LA115_20==K_KEYSPACES||(LA115_20 >= K_LIST && LA115_20 <= K_MAP)||LA115_20==K_NAN||LA115_20==K_NOSUPERUSER||LA115_20==K_NULL||(LA115_20 >= K_PASSWORD && LA115_20 <= K_PERMISSIONS)||(LA115_20 >= K_STATIC && LA115_20 <= K_TIMEUUID)||(LA115_20 >= K_TOKEN && LA115_20 <= K_TRIGGER)||(LA115_20 >= K_TUPLE && LA115_20 <= K_TYPE)||(LA115_20 >= K_USER && LA115_20 <= K_USERS)||(LA115_20 >= K_UUID && LA115_20 <= K_VARINT)||LA115_20==QMARK||LA115_20==STRING_LITERAL||LA115_20==UUID||LA115_20==142||LA115_20==146||LA115_20==148||LA115_20==155||LA115_20==158) ) {
						alt115=2;
					}

					else {
						int nvaeMark = input.mark();
						try {
							for (int nvaeConsume = 0; nvaeConsume < 3 - 1; nvaeConsume++) {
								input.consume();
							}
							NoViableAltException nvae =
								new NoViableAltException("", 115, 20, input);
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
							new NoViableAltException("", 115, 5, input);
						throw nvae;
					} finally {
						input.rewind(nvaeMark);
					}
				}

				}
				break;
			case K_BOOLEAN:
				{
				int LA115_6 = input.LA(2);
				if ( (LA115_6==142) ) {
					int LA115_20 = input.LA(3);
					if ( (LA115_20==143) ) {
						alt115=1;
					}
					else if ( (LA115_20==BOOLEAN||LA115_20==FLOAT||LA115_20==HEXNUMBER||(LA115_20 >= IDENT && LA115_20 <= INTEGER)||LA115_20==K_ALL||LA115_20==K_AS||LA115_20==K_ASCII||(LA115_20 >= K_BIGINT && LA115_20 <= K_BOOLEAN)||LA115_20==K_CLUSTERING||(LA115_20 >= K_COMPACT && LA115_20 <= K_CONTAINS)||LA115_20==K_COUNTER||(LA115_20 >= K_CUSTOM && LA115_20 <= K_DECIMAL)||(LA115_20 >= K_DISTINCT && LA115_20 <= K_DOUBLE)||(LA115_20 >= K_EXISTS && LA115_20 <= K_FLOAT)||LA115_20==K_FROZEN||(LA115_20 >= K_INET && LA115_20 <= K_INFINITY)||LA115_20==K_INT||LA115_20==K_KEYS||LA115_20==K_KEYSPACES||(LA115_20 >= K_LIST && LA115_20 <= K_MAP)||LA115_20==K_NAN||LA115_20==K_NOSUPERUSER||LA115_20==K_NULL||(LA115_20 >= K_PASSWORD && LA115_20 <= K_PERMISSIONS)||(LA115_20 >= K_STATIC && LA115_20 <= K_TIMEUUID)||(LA115_20 >= K_TOKEN && LA115_20 <= K_TRIGGER)||(LA115_20 >= K_TUPLE && LA115_20 <= K_TYPE)||(LA115_20 >= K_USER && LA115_20 <= K_USERS)||(LA115_20 >= K_UUID && LA115_20 <= K_VARINT)||LA115_20==QMARK||LA115_20==STRING_LITERAL||LA115_20==UUID||LA115_20==142||LA115_20==146||LA115_20==148||LA115_20==155||LA115_20==158) ) {
						alt115=2;
					}

					else {
						int nvaeMark = input.mark();
						try {
							for (int nvaeConsume = 0; nvaeConsume < 3 - 1; nvaeConsume++) {
								input.consume();
							}
							NoViableAltException nvae =
								new NoViableAltException("", 115, 20, input);
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
							new NoViableAltException("", 115, 6, input);
						throw nvae;
					} finally {
						input.rewind(nvaeMark);
					}
				}

				}
				break;
			case K_COUNTER:
				{
				int LA115_7 = input.LA(2);
				if ( (LA115_7==142) ) {
					int LA115_20 = input.LA(3);
					if ( (LA115_20==143) ) {
						alt115=1;
					}
					else if ( (LA115_20==BOOLEAN||LA115_20==FLOAT||LA115_20==HEXNUMBER||(LA115_20 >= IDENT && LA115_20 <= INTEGER)||LA115_20==K_ALL||LA115_20==K_AS||LA115_20==K_ASCII||(LA115_20 >= K_BIGINT && LA115_20 <= K_BOOLEAN)||LA115_20==K_CLUSTERING||(LA115_20 >= K_COMPACT && LA115_20 <= K_CONTAINS)||LA115_20==K_COUNTER||(LA115_20 >= K_CUSTOM && LA115_20 <= K_DECIMAL)||(LA115_20 >= K_DISTINCT && LA115_20 <= K_DOUBLE)||(LA115_20 >= K_EXISTS && LA115_20 <= K_FLOAT)||LA115_20==K_FROZEN||(LA115_20 >= K_INET && LA115_20 <= K_INFINITY)||LA115_20==K_INT||LA115_20==K_KEYS||LA115_20==K_KEYSPACES||(LA115_20 >= K_LIST && LA115_20 <= K_MAP)||LA115_20==K_NAN||LA115_20==K_NOSUPERUSER||LA115_20==K_NULL||(LA115_20 >= K_PASSWORD && LA115_20 <= K_PERMISSIONS)||(LA115_20 >= K_STATIC && LA115_20 <= K_TIMEUUID)||(LA115_20 >= K_TOKEN && LA115_20 <= K_TRIGGER)||(LA115_20 >= K_TUPLE && LA115_20 <= K_TYPE)||(LA115_20 >= K_USER && LA115_20 <= K_USERS)||(LA115_20 >= K_UUID && LA115_20 <= K_VARINT)||LA115_20==QMARK||LA115_20==STRING_LITERAL||LA115_20==UUID||LA115_20==142||LA115_20==146||LA115_20==148||LA115_20==155||LA115_20==158) ) {
						alt115=2;
					}

					else {
						int nvaeMark = input.mark();
						try {
							for (int nvaeConsume = 0; nvaeConsume < 3 - 1; nvaeConsume++) {
								input.consume();
							}
							NoViableAltException nvae =
								new NoViableAltException("", 115, 20, input);
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
							new NoViableAltException("", 115, 7, input);
						throw nvae;
					} finally {
						input.rewind(nvaeMark);
					}
				}

				}
				break;
			case K_DECIMAL:
				{
				int LA115_8 = input.LA(2);
				if ( (LA115_8==142) ) {
					int LA115_20 = input.LA(3);
					if ( (LA115_20==143) ) {
						alt115=1;
					}
					else if ( (LA115_20==BOOLEAN||LA115_20==FLOAT||LA115_20==HEXNUMBER||(LA115_20 >= IDENT && LA115_20 <= INTEGER)||LA115_20==K_ALL||LA115_20==K_AS||LA115_20==K_ASCII||(LA115_20 >= K_BIGINT && LA115_20 <= K_BOOLEAN)||LA115_20==K_CLUSTERING||(LA115_20 >= K_COMPACT && LA115_20 <= K_CONTAINS)||LA115_20==K_COUNTER||(LA115_20 >= K_CUSTOM && LA115_20 <= K_DECIMAL)||(LA115_20 >= K_DISTINCT && LA115_20 <= K_DOUBLE)||(LA115_20 >= K_EXISTS && LA115_20 <= K_FLOAT)||LA115_20==K_FROZEN||(LA115_20 >= K_INET && LA115_20 <= K_INFINITY)||LA115_20==K_INT||LA115_20==K_KEYS||LA115_20==K_KEYSPACES||(LA115_20 >= K_LIST && LA115_20 <= K_MAP)||LA115_20==K_NAN||LA115_20==K_NOSUPERUSER||LA115_20==K_NULL||(LA115_20 >= K_PASSWORD && LA115_20 <= K_PERMISSIONS)||(LA115_20 >= K_STATIC && LA115_20 <= K_TIMEUUID)||(LA115_20 >= K_TOKEN && LA115_20 <= K_TRIGGER)||(LA115_20 >= K_TUPLE && LA115_20 <= K_TYPE)||(LA115_20 >= K_USER && LA115_20 <= K_USERS)||(LA115_20 >= K_UUID && LA115_20 <= K_VARINT)||LA115_20==QMARK||LA115_20==STRING_LITERAL||LA115_20==UUID||LA115_20==142||LA115_20==146||LA115_20==148||LA115_20==155||LA115_20==158) ) {
						alt115=2;
					}

					else {
						int nvaeMark = input.mark();
						try {
							for (int nvaeConsume = 0; nvaeConsume < 3 - 1; nvaeConsume++) {
								input.consume();
							}
							NoViableAltException nvae =
								new NoViableAltException("", 115, 20, input);
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
							new NoViableAltException("", 115, 8, input);
						throw nvae;
					} finally {
						input.rewind(nvaeMark);
					}
				}

				}
				break;
			case K_DOUBLE:
				{
				int LA115_9 = input.LA(2);
				if ( (LA115_9==142) ) {
					int LA115_20 = input.LA(3);
					if ( (LA115_20==143) ) {
						alt115=1;
					}
					else if ( (LA115_20==BOOLEAN||LA115_20==FLOAT||LA115_20==HEXNUMBER||(LA115_20 >= IDENT && LA115_20 <= INTEGER)||LA115_20==K_ALL||LA115_20==K_AS||LA115_20==K_ASCII||(LA115_20 >= K_BIGINT && LA115_20 <= K_BOOLEAN)||LA115_20==K_CLUSTERING||(LA115_20 >= K_COMPACT && LA115_20 <= K_CONTAINS)||LA115_20==K_COUNTER||(LA115_20 >= K_CUSTOM && LA115_20 <= K_DECIMAL)||(LA115_20 >= K_DISTINCT && LA115_20 <= K_DOUBLE)||(LA115_20 >= K_EXISTS && LA115_20 <= K_FLOAT)||LA115_20==K_FROZEN||(LA115_20 >= K_INET && LA115_20 <= K_INFINITY)||LA115_20==K_INT||LA115_20==K_KEYS||LA115_20==K_KEYSPACES||(LA115_20 >= K_LIST && LA115_20 <= K_MAP)||LA115_20==K_NAN||LA115_20==K_NOSUPERUSER||LA115_20==K_NULL||(LA115_20 >= K_PASSWORD && LA115_20 <= K_PERMISSIONS)||(LA115_20 >= K_STATIC && LA115_20 <= K_TIMEUUID)||(LA115_20 >= K_TOKEN && LA115_20 <= K_TRIGGER)||(LA115_20 >= K_TUPLE && LA115_20 <= K_TYPE)||(LA115_20 >= K_USER && LA115_20 <= K_USERS)||(LA115_20 >= K_UUID && LA115_20 <= K_VARINT)||LA115_20==QMARK||LA115_20==STRING_LITERAL||LA115_20==UUID||LA115_20==142||LA115_20==146||LA115_20==148||LA115_20==155||LA115_20==158) ) {
						alt115=2;
					}

					else {
						int nvaeMark = input.mark();
						try {
							for (int nvaeConsume = 0; nvaeConsume < 3 - 1; nvaeConsume++) {
								input.consume();
							}
							NoViableAltException nvae =
								new NoViableAltException("", 115, 20, input);
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
							new NoViableAltException("", 115, 9, input);
						throw nvae;
					} finally {
						input.rewind(nvaeMark);
					}
				}

				}
				break;
			case K_FLOAT:
				{
				int LA115_10 = input.LA(2);
				if ( (LA115_10==142) ) {
					int LA115_20 = input.LA(3);
					if ( (LA115_20==143) ) {
						alt115=1;
					}
					else if ( (LA115_20==BOOLEAN||LA115_20==FLOAT||LA115_20==HEXNUMBER||(LA115_20 >= IDENT && LA115_20 <= INTEGER)||LA115_20==K_ALL||LA115_20==K_AS||LA115_20==K_ASCII||(LA115_20 >= K_BIGINT && LA115_20 <= K_BOOLEAN)||LA115_20==K_CLUSTERING||(LA115_20 >= K_COMPACT && LA115_20 <= K_CONTAINS)||LA115_20==K_COUNTER||(LA115_20 >= K_CUSTOM && LA115_20 <= K_DECIMAL)||(LA115_20 >= K_DISTINCT && LA115_20 <= K_DOUBLE)||(LA115_20 >= K_EXISTS && LA115_20 <= K_FLOAT)||LA115_20==K_FROZEN||(LA115_20 >= K_INET && LA115_20 <= K_INFINITY)||LA115_20==K_INT||LA115_20==K_KEYS||LA115_20==K_KEYSPACES||(LA115_20 >= K_LIST && LA115_20 <= K_MAP)||LA115_20==K_NAN||LA115_20==K_NOSUPERUSER||LA115_20==K_NULL||(LA115_20 >= K_PASSWORD && LA115_20 <= K_PERMISSIONS)||(LA115_20 >= K_STATIC && LA115_20 <= K_TIMEUUID)||(LA115_20 >= K_TOKEN && LA115_20 <= K_TRIGGER)||(LA115_20 >= K_TUPLE && LA115_20 <= K_TYPE)||(LA115_20 >= K_USER && LA115_20 <= K_USERS)||(LA115_20 >= K_UUID && LA115_20 <= K_VARINT)||LA115_20==QMARK||LA115_20==STRING_LITERAL||LA115_20==UUID||LA115_20==142||LA115_20==146||LA115_20==148||LA115_20==155||LA115_20==158) ) {
						alt115=2;
					}

					else {
						int nvaeMark = input.mark();
						try {
							for (int nvaeConsume = 0; nvaeConsume < 3 - 1; nvaeConsume++) {
								input.consume();
							}
							NoViableAltException nvae =
								new NoViableAltException("", 115, 20, input);
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
							new NoViableAltException("", 115, 10, input);
						throw nvae;
					} finally {
						input.rewind(nvaeMark);
					}
				}

				}
				break;
			case K_INET:
				{
				int LA115_11 = input.LA(2);
				if ( (LA115_11==142) ) {
					int LA115_20 = input.LA(3);
					if ( (LA115_20==143) ) {
						alt115=1;
					}
					else if ( (LA115_20==BOOLEAN||LA115_20==FLOAT||LA115_20==HEXNUMBER||(LA115_20 >= IDENT && LA115_20 <= INTEGER)||LA115_20==K_ALL||LA115_20==K_AS||LA115_20==K_ASCII||(LA115_20 >= K_BIGINT && LA115_20 <= K_BOOLEAN)||LA115_20==K_CLUSTERING||(LA115_20 >= K_COMPACT && LA115_20 <= K_CONTAINS)||LA115_20==K_COUNTER||(LA115_20 >= K_CUSTOM && LA115_20 <= K_DECIMAL)||(LA115_20 >= K_DISTINCT && LA115_20 <= K_DOUBLE)||(LA115_20 >= K_EXISTS && LA115_20 <= K_FLOAT)||LA115_20==K_FROZEN||(LA115_20 >= K_INET && LA115_20 <= K_INFINITY)||LA115_20==K_INT||LA115_20==K_KEYS||LA115_20==K_KEYSPACES||(LA115_20 >= K_LIST && LA115_20 <= K_MAP)||LA115_20==K_NAN||LA115_20==K_NOSUPERUSER||LA115_20==K_NULL||(LA115_20 >= K_PASSWORD && LA115_20 <= K_PERMISSIONS)||(LA115_20 >= K_STATIC && LA115_20 <= K_TIMEUUID)||(LA115_20 >= K_TOKEN && LA115_20 <= K_TRIGGER)||(LA115_20 >= K_TUPLE && LA115_20 <= K_TYPE)||(LA115_20 >= K_USER && LA115_20 <= K_USERS)||(LA115_20 >= K_UUID && LA115_20 <= K_VARINT)||LA115_20==QMARK||LA115_20==STRING_LITERAL||LA115_20==UUID||LA115_20==142||LA115_20==146||LA115_20==148||LA115_20==155||LA115_20==158) ) {
						alt115=2;
					}

					else {
						int nvaeMark = input.mark();
						try {
							for (int nvaeConsume = 0; nvaeConsume < 3 - 1; nvaeConsume++) {
								input.consume();
							}
							NoViableAltException nvae =
								new NoViableAltException("", 115, 20, input);
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
							new NoViableAltException("", 115, 11, input);
						throw nvae;
					} finally {
						input.rewind(nvaeMark);
					}
				}

				}
				break;
			case K_INT:
				{
				int LA115_12 = input.LA(2);
				if ( (LA115_12==142) ) {
					int LA115_20 = input.LA(3);
					if ( (LA115_20==143) ) {
						alt115=1;
					}
					else if ( (LA115_20==BOOLEAN||LA115_20==FLOAT||LA115_20==HEXNUMBER||(LA115_20 >= IDENT && LA115_20 <= INTEGER)||LA115_20==K_ALL||LA115_20==K_AS||LA115_20==K_ASCII||(LA115_20 >= K_BIGINT && LA115_20 <= K_BOOLEAN)||LA115_20==K_CLUSTERING||(LA115_20 >= K_COMPACT && LA115_20 <= K_CONTAINS)||LA115_20==K_COUNTER||(LA115_20 >= K_CUSTOM && LA115_20 <= K_DECIMAL)||(LA115_20 >= K_DISTINCT && LA115_20 <= K_DOUBLE)||(LA115_20 >= K_EXISTS && LA115_20 <= K_FLOAT)||LA115_20==K_FROZEN||(LA115_20 >= K_INET && LA115_20 <= K_INFINITY)||LA115_20==K_INT||LA115_20==K_KEYS||LA115_20==K_KEYSPACES||(LA115_20 >= K_LIST && LA115_20 <= K_MAP)||LA115_20==K_NAN||LA115_20==K_NOSUPERUSER||LA115_20==K_NULL||(LA115_20 >= K_PASSWORD && LA115_20 <= K_PERMISSIONS)||(LA115_20 >= K_STATIC && LA115_20 <= K_TIMEUUID)||(LA115_20 >= K_TOKEN && LA115_20 <= K_TRIGGER)||(LA115_20 >= K_TUPLE && LA115_20 <= K_TYPE)||(LA115_20 >= K_USER && LA115_20 <= K_USERS)||(LA115_20 >= K_UUID && LA115_20 <= K_VARINT)||LA115_20==QMARK||LA115_20==STRING_LITERAL||LA115_20==UUID||LA115_20==142||LA115_20==146||LA115_20==148||LA115_20==155||LA115_20==158) ) {
						alt115=2;
					}

					else {
						int nvaeMark = input.mark();
						try {
							for (int nvaeConsume = 0; nvaeConsume < 3 - 1; nvaeConsume++) {
								input.consume();
							}
							NoViableAltException nvae =
								new NoViableAltException("", 115, 20, input);
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
							new NoViableAltException("", 115, 12, input);
						throw nvae;
					} finally {
						input.rewind(nvaeMark);
					}
				}

				}
				break;
			case K_TEXT:
				{
				int LA115_13 = input.LA(2);
				if ( (LA115_13==142) ) {
					int LA115_20 = input.LA(3);
					if ( (LA115_20==143) ) {
						alt115=1;
					}
					else if ( (LA115_20==BOOLEAN||LA115_20==FLOAT||LA115_20==HEXNUMBER||(LA115_20 >= IDENT && LA115_20 <= INTEGER)||LA115_20==K_ALL||LA115_20==K_AS||LA115_20==K_ASCII||(LA115_20 >= K_BIGINT && LA115_20 <= K_BOOLEAN)||LA115_20==K_CLUSTERING||(LA115_20 >= K_COMPACT && LA115_20 <= K_CONTAINS)||LA115_20==K_COUNTER||(LA115_20 >= K_CUSTOM && LA115_20 <= K_DECIMAL)||(LA115_20 >= K_DISTINCT && LA115_20 <= K_DOUBLE)||(LA115_20 >= K_EXISTS && LA115_20 <= K_FLOAT)||LA115_20==K_FROZEN||(LA115_20 >= K_INET && LA115_20 <= K_INFINITY)||LA115_20==K_INT||LA115_20==K_KEYS||LA115_20==K_KEYSPACES||(LA115_20 >= K_LIST && LA115_20 <= K_MAP)||LA115_20==K_NAN||LA115_20==K_NOSUPERUSER||LA115_20==K_NULL||(LA115_20 >= K_PASSWORD && LA115_20 <= K_PERMISSIONS)||(LA115_20 >= K_STATIC && LA115_20 <= K_TIMEUUID)||(LA115_20 >= K_TOKEN && LA115_20 <= K_TRIGGER)||(LA115_20 >= K_TUPLE && LA115_20 <= K_TYPE)||(LA115_20 >= K_USER && LA115_20 <= K_USERS)||(LA115_20 >= K_UUID && LA115_20 <= K_VARINT)||LA115_20==QMARK||LA115_20==STRING_LITERAL||LA115_20==UUID||LA115_20==142||LA115_20==146||LA115_20==148||LA115_20==155||LA115_20==158) ) {
						alt115=2;
					}

					else {
						int nvaeMark = input.mark();
						try {
							for (int nvaeConsume = 0; nvaeConsume < 3 - 1; nvaeConsume++) {
								input.consume();
							}
							NoViableAltException nvae =
								new NoViableAltException("", 115, 20, input);
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
							new NoViableAltException("", 115, 13, input);
						throw nvae;
					} finally {
						input.rewind(nvaeMark);
					}
				}

				}
				break;
			case K_TIMESTAMP:
				{
				int LA115_14 = input.LA(2);
				if ( (LA115_14==142) ) {
					int LA115_20 = input.LA(3);
					if ( (LA115_20==143) ) {
						alt115=1;
					}
					else if ( (LA115_20==BOOLEAN||LA115_20==FLOAT||LA115_20==HEXNUMBER||(LA115_20 >= IDENT && LA115_20 <= INTEGER)||LA115_20==K_ALL||LA115_20==K_AS||LA115_20==K_ASCII||(LA115_20 >= K_BIGINT && LA115_20 <= K_BOOLEAN)||LA115_20==K_CLUSTERING||(LA115_20 >= K_COMPACT && LA115_20 <= K_CONTAINS)||LA115_20==K_COUNTER||(LA115_20 >= K_CUSTOM && LA115_20 <= K_DECIMAL)||(LA115_20 >= K_DISTINCT && LA115_20 <= K_DOUBLE)||(LA115_20 >= K_EXISTS && LA115_20 <= K_FLOAT)||LA115_20==K_FROZEN||(LA115_20 >= K_INET && LA115_20 <= K_INFINITY)||LA115_20==K_INT||LA115_20==K_KEYS||LA115_20==K_KEYSPACES||(LA115_20 >= K_LIST && LA115_20 <= K_MAP)||LA115_20==K_NAN||LA115_20==K_NOSUPERUSER||LA115_20==K_NULL||(LA115_20 >= K_PASSWORD && LA115_20 <= K_PERMISSIONS)||(LA115_20 >= K_STATIC && LA115_20 <= K_TIMEUUID)||(LA115_20 >= K_TOKEN && LA115_20 <= K_TRIGGER)||(LA115_20 >= K_TUPLE && LA115_20 <= K_TYPE)||(LA115_20 >= K_USER && LA115_20 <= K_USERS)||(LA115_20 >= K_UUID && LA115_20 <= K_VARINT)||LA115_20==QMARK||LA115_20==STRING_LITERAL||LA115_20==UUID||LA115_20==142||LA115_20==146||LA115_20==148||LA115_20==155||LA115_20==158) ) {
						alt115=2;
					}

					else {
						int nvaeMark = input.mark();
						try {
							for (int nvaeConsume = 0; nvaeConsume < 3 - 1; nvaeConsume++) {
								input.consume();
							}
							NoViableAltException nvae =
								new NoViableAltException("", 115, 20, input);
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
							new NoViableAltException("", 115, 14, input);
						throw nvae;
					} finally {
						input.rewind(nvaeMark);
					}
				}

				}
				break;
			case K_UUID:
				{
				int LA115_15 = input.LA(2);
				if ( (LA115_15==142) ) {
					int LA115_20 = input.LA(3);
					if ( (LA115_20==143) ) {
						alt115=1;
					}
					else if ( (LA115_20==BOOLEAN||LA115_20==FLOAT||LA115_20==HEXNUMBER||(LA115_20 >= IDENT && LA115_20 <= INTEGER)||LA115_20==K_ALL||LA115_20==K_AS||LA115_20==K_ASCII||(LA115_20 >= K_BIGINT && LA115_20 <= K_BOOLEAN)||LA115_20==K_CLUSTERING||(LA115_20 >= K_COMPACT && LA115_20 <= K_CONTAINS)||LA115_20==K_COUNTER||(LA115_20 >= K_CUSTOM && LA115_20 <= K_DECIMAL)||(LA115_20 >= K_DISTINCT && LA115_20 <= K_DOUBLE)||(LA115_20 >= K_EXISTS && LA115_20 <= K_FLOAT)||LA115_20==K_FROZEN||(LA115_20 >= K_INET && LA115_20 <= K_INFINITY)||LA115_20==K_INT||LA115_20==K_KEYS||LA115_20==K_KEYSPACES||(LA115_20 >= K_LIST && LA115_20 <= K_MAP)||LA115_20==K_NAN||LA115_20==K_NOSUPERUSER||LA115_20==K_NULL||(LA115_20 >= K_PASSWORD && LA115_20 <= K_PERMISSIONS)||(LA115_20 >= K_STATIC && LA115_20 <= K_TIMEUUID)||(LA115_20 >= K_TOKEN && LA115_20 <= K_TRIGGER)||(LA115_20 >= K_TUPLE && LA115_20 <= K_TYPE)||(LA115_20 >= K_USER && LA115_20 <= K_USERS)||(LA115_20 >= K_UUID && LA115_20 <= K_VARINT)||LA115_20==QMARK||LA115_20==STRING_LITERAL||LA115_20==UUID||LA115_20==142||LA115_20==146||LA115_20==148||LA115_20==155||LA115_20==158) ) {
						alt115=2;
					}

					else {
						int nvaeMark = input.mark();
						try {
							for (int nvaeConsume = 0; nvaeConsume < 3 - 1; nvaeConsume++) {
								input.consume();
							}
							NoViableAltException nvae =
								new NoViableAltException("", 115, 20, input);
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
							new NoViableAltException("", 115, 15, input);
						throw nvae;
					} finally {
						input.rewind(nvaeMark);
					}
				}

				}
				break;
			case K_VARCHAR:
				{
				int LA115_16 = input.LA(2);
				if ( (LA115_16==142) ) {
					int LA115_20 = input.LA(3);
					if ( (LA115_20==143) ) {
						alt115=1;
					}
					else if ( (LA115_20==BOOLEAN||LA115_20==FLOAT||LA115_20==HEXNUMBER||(LA115_20 >= IDENT && LA115_20 <= INTEGER)||LA115_20==K_ALL||LA115_20==K_AS||LA115_20==K_ASCII||(LA115_20 >= K_BIGINT && LA115_20 <= K_BOOLEAN)||LA115_20==K_CLUSTERING||(LA115_20 >= K_COMPACT && LA115_20 <= K_CONTAINS)||LA115_20==K_COUNTER||(LA115_20 >= K_CUSTOM && LA115_20 <= K_DECIMAL)||(LA115_20 >= K_DISTINCT && LA115_20 <= K_DOUBLE)||(LA115_20 >= K_EXISTS && LA115_20 <= K_FLOAT)||LA115_20==K_FROZEN||(LA115_20 >= K_INET && LA115_20 <= K_INFINITY)||LA115_20==K_INT||LA115_20==K_KEYS||LA115_20==K_KEYSPACES||(LA115_20 >= K_LIST && LA115_20 <= K_MAP)||LA115_20==K_NAN||LA115_20==K_NOSUPERUSER||LA115_20==K_NULL||(LA115_20 >= K_PASSWORD && LA115_20 <= K_PERMISSIONS)||(LA115_20 >= K_STATIC && LA115_20 <= K_TIMEUUID)||(LA115_20 >= K_TOKEN && LA115_20 <= K_TRIGGER)||(LA115_20 >= K_TUPLE && LA115_20 <= K_TYPE)||(LA115_20 >= K_USER && LA115_20 <= K_USERS)||(LA115_20 >= K_UUID && LA115_20 <= K_VARINT)||LA115_20==QMARK||LA115_20==STRING_LITERAL||LA115_20==UUID||LA115_20==142||LA115_20==146||LA115_20==148||LA115_20==155||LA115_20==158) ) {
						alt115=2;
					}

					else {
						int nvaeMark = input.mark();
						try {
							for (int nvaeConsume = 0; nvaeConsume < 3 - 1; nvaeConsume++) {
								input.consume();
							}
							NoViableAltException nvae =
								new NoViableAltException("", 115, 20, input);
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
							new NoViableAltException("", 115, 16, input);
						throw nvae;
					} finally {
						input.rewind(nvaeMark);
					}
				}

				}
				break;
			case K_VARINT:
				{
				int LA115_17 = input.LA(2);
				if ( (LA115_17==142) ) {
					int LA115_20 = input.LA(3);
					if ( (LA115_20==143) ) {
						alt115=1;
					}
					else if ( (LA115_20==BOOLEAN||LA115_20==FLOAT||LA115_20==HEXNUMBER||(LA115_20 >= IDENT && LA115_20 <= INTEGER)||LA115_20==K_ALL||LA115_20==K_AS||LA115_20==K_ASCII||(LA115_20 >= K_BIGINT && LA115_20 <= K_BOOLEAN)||LA115_20==K_CLUSTERING||(LA115_20 >= K_COMPACT && LA115_20 <= K_CONTAINS)||LA115_20==K_COUNTER||(LA115_20 >= K_CUSTOM && LA115_20 <= K_DECIMAL)||(LA115_20 >= K_DISTINCT && LA115_20 <= K_DOUBLE)||(LA115_20 >= K_EXISTS && LA115_20 <= K_FLOAT)||LA115_20==K_FROZEN||(LA115_20 >= K_INET && LA115_20 <= K_INFINITY)||LA115_20==K_INT||LA115_20==K_KEYS||LA115_20==K_KEYSPACES||(LA115_20 >= K_LIST && LA115_20 <= K_MAP)||LA115_20==K_NAN||LA115_20==K_NOSUPERUSER||LA115_20==K_NULL||(LA115_20 >= K_PASSWORD && LA115_20 <= K_PERMISSIONS)||(LA115_20 >= K_STATIC && LA115_20 <= K_TIMEUUID)||(LA115_20 >= K_TOKEN && LA115_20 <= K_TRIGGER)||(LA115_20 >= K_TUPLE && LA115_20 <= K_TYPE)||(LA115_20 >= K_USER && LA115_20 <= K_USERS)||(LA115_20 >= K_UUID && LA115_20 <= K_VARINT)||LA115_20==QMARK||LA115_20==STRING_LITERAL||LA115_20==UUID||LA115_20==142||LA115_20==146||LA115_20==148||LA115_20==155||LA115_20==158) ) {
						alt115=2;
					}

					else {
						int nvaeMark = input.mark();
						try {
							for (int nvaeConsume = 0; nvaeConsume < 3 - 1; nvaeConsume++) {
								input.consume();
							}
							NoViableAltException nvae =
								new NoViableAltException("", 115, 20, input);
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
							new NoViableAltException("", 115, 17, input);
						throw nvae;
					} finally {
						input.rewind(nvaeMark);
					}
				}

				}
				break;
			case K_TIMEUUID:
				{
				int LA115_18 = input.LA(2);
				if ( (LA115_18==142) ) {
					int LA115_20 = input.LA(3);
					if ( (LA115_20==143) ) {
						alt115=1;
					}
					else if ( (LA115_20==BOOLEAN||LA115_20==FLOAT||LA115_20==HEXNUMBER||(LA115_20 >= IDENT && LA115_20 <= INTEGER)||LA115_20==K_ALL||LA115_20==K_AS||LA115_20==K_ASCII||(LA115_20 >= K_BIGINT && LA115_20 <= K_BOOLEAN)||LA115_20==K_CLUSTERING||(LA115_20 >= K_COMPACT && LA115_20 <= K_CONTAINS)||LA115_20==K_COUNTER||(LA115_20 >= K_CUSTOM && LA115_20 <= K_DECIMAL)||(LA115_20 >= K_DISTINCT && LA115_20 <= K_DOUBLE)||(LA115_20 >= K_EXISTS && LA115_20 <= K_FLOAT)||LA115_20==K_FROZEN||(LA115_20 >= K_INET && LA115_20 <= K_INFINITY)||LA115_20==K_INT||LA115_20==K_KEYS||LA115_20==K_KEYSPACES||(LA115_20 >= K_LIST && LA115_20 <= K_MAP)||LA115_20==K_NAN||LA115_20==K_NOSUPERUSER||LA115_20==K_NULL||(LA115_20 >= K_PASSWORD && LA115_20 <= K_PERMISSIONS)||(LA115_20 >= K_STATIC && LA115_20 <= K_TIMEUUID)||(LA115_20 >= K_TOKEN && LA115_20 <= K_TRIGGER)||(LA115_20 >= K_TUPLE && LA115_20 <= K_TYPE)||(LA115_20 >= K_USER && LA115_20 <= K_USERS)||(LA115_20 >= K_UUID && LA115_20 <= K_VARINT)||LA115_20==QMARK||LA115_20==STRING_LITERAL||LA115_20==UUID||LA115_20==142||LA115_20==146||LA115_20==148||LA115_20==155||LA115_20==158) ) {
						alt115=2;
					}

					else {
						int nvaeMark = input.mark();
						try {
							for (int nvaeConsume = 0; nvaeConsume < 3 - 1; nvaeConsume++) {
								input.consume();
							}
							NoViableAltException nvae =
								new NoViableAltException("", 115, 20, input);
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
							new NoViableAltException("", 115, 18, input);
						throw nvae;
					} finally {
						input.rewind(nvaeMark);
					}
				}

				}
				break;
			case K_TOKEN:
				{
				int LA115_19 = input.LA(2);
				if ( (LA115_19==142) ) {
					int LA115_20 = input.LA(3);
					if ( (LA115_20==143) ) {
						alt115=1;
					}
					else if ( (LA115_20==BOOLEAN||LA115_20==FLOAT||LA115_20==HEXNUMBER||(LA115_20 >= IDENT && LA115_20 <= INTEGER)||LA115_20==K_ALL||LA115_20==K_AS||LA115_20==K_ASCII||(LA115_20 >= K_BIGINT && LA115_20 <= K_BOOLEAN)||LA115_20==K_CLUSTERING||(LA115_20 >= K_COMPACT && LA115_20 <= K_CONTAINS)||LA115_20==K_COUNTER||(LA115_20 >= K_CUSTOM && LA115_20 <= K_DECIMAL)||(LA115_20 >= K_DISTINCT && LA115_20 <= K_DOUBLE)||(LA115_20 >= K_EXISTS && LA115_20 <= K_FLOAT)||LA115_20==K_FROZEN||(LA115_20 >= K_INET && LA115_20 <= K_INFINITY)||LA115_20==K_INT||LA115_20==K_KEYS||LA115_20==K_KEYSPACES||(LA115_20 >= K_LIST && LA115_20 <= K_MAP)||LA115_20==K_NAN||LA115_20==K_NOSUPERUSER||LA115_20==K_NULL||(LA115_20 >= K_PASSWORD && LA115_20 <= K_PERMISSIONS)||(LA115_20 >= K_STATIC && LA115_20 <= K_TIMEUUID)||(LA115_20 >= K_TOKEN && LA115_20 <= K_TRIGGER)||(LA115_20 >= K_TUPLE && LA115_20 <= K_TYPE)||(LA115_20 >= K_USER && LA115_20 <= K_USERS)||(LA115_20 >= K_UUID && LA115_20 <= K_VARINT)||LA115_20==QMARK||LA115_20==STRING_LITERAL||LA115_20==UUID||LA115_20==142||LA115_20==146||LA115_20==148||LA115_20==155||LA115_20==158) ) {
						alt115=2;
					}

					else {
						int nvaeMark = input.mark();
						try {
							for (int nvaeConsume = 0; nvaeConsume < 3 - 1; nvaeConsume++) {
								input.consume();
							}
							NoViableAltException nvae =
								new NoViableAltException("", 115, 20, input);
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
							new NoViableAltException("", 115, 19, input);
						throw nvae;
					} finally {
						input.rewind(nvaeMark);
					}
				}

				}
				break;
			default:
				NoViableAltException nvae =
					new NoViableAltException("", 115, 0, input);
				throw nvae;
			}
			switch (alt115) {
				case 1 :
					// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:949:7: f= functionName '(' ')'
					{
					pushFollow(FOLLOW_functionName_in_function6071);
					f=functionName();
					state._fsp--;

					match(input,142,FOLLOW_142_in_function6073); 
					match(input,143,FOLLOW_143_in_function6075); 
					 t = new FunctionCall.Raw(f, Collections.<Term.Raw>emptyList()); 
					}
					break;
				case 2 :
					// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:950:7: f= functionName '(' args= functionArgs ')'
					{
					pushFollow(FOLLOW_functionName_in_function6105);
					f=functionName();
					state._fsp--;

					match(input,142,FOLLOW_142_in_function6107); 
					pushFollow(FOLLOW_functionArgs_in_function6111);
					args=functionArgs();
					state._fsp--;

					match(input,143,FOLLOW_143_in_function6113); 
					 t = new FunctionCall.Raw(f, args); 
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
		return t;
	}
	// $ANTLR end "function"



	// $ANTLR start "functionArgs"
	// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:953:1: functionArgs returns [List<Term.Raw> args] : t1= term ( ',' tn= term )* ;
	public final List<Term.Raw> functionArgs() throws RecognitionException {
		List<Term.Raw> args = null;


		Term.Raw t1 =null;
		Term.Raw tn =null;

		 args = new ArrayList<Term.Raw>(); 
		try {
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:955:5: (t1= term ( ',' tn= term )* )
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:955:7: t1= term ( ',' tn= term )*
			{
			pushFollow(FOLLOW_term_in_functionArgs6146);
			t1=term();
			state._fsp--;

			args.add(t1); 
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:955:32: ( ',' tn= term )*
			loop116:
			while (true) {
				int alt116=2;
				int LA116_0 = input.LA(1);
				if ( (LA116_0==145) ) {
					alt116=1;
				}

				switch (alt116) {
				case 1 :
					// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:955:34: ',' tn= term
					{
					match(input,145,FOLLOW_145_in_functionArgs6152); 
					pushFollow(FOLLOW_term_in_functionArgs6156);
					tn=term();
					state._fsp--;

					 args.add(tn); 
					}
					break;

				default :
					break loop116;
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
		return args;
	}
	// $ANTLR end "functionArgs"



	// $ANTLR start "term"
	// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:958:1: term returns [Term.Raw term] : (v= value |f= function | '(' c= comparatorType ')' t= term );
	public final Term.Raw term() throws RecognitionException {
		Term.Raw term = null;


		Term.Raw v =null;
		Term.Raw f =null;
		CQL3Type.Raw c =null;
		Term.Raw t =null;

		try {
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:959:5: (v= value |f= function | '(' c= comparatorType ')' t= term )
			int alt117=3;
			switch ( input.LA(1) ) {
			case BOOLEAN:
			case FLOAT:
			case HEXNUMBER:
			case INTEGER:
			case K_INFINITY:
			case K_NAN:
			case K_NULL:
			case QMARK:
			case STRING_LITERAL:
			case UUID:
			case 146:
			case 148:
			case 155:
			case 158:
				{
				alt117=1;
				}
				break;
			case 142:
				{
				switch ( input.LA(2) ) {
				case STRING_LITERAL:
					{
					int LA117_4 = input.LA(3);
					if ( (LA117_4==145) ) {
						alt117=1;
					}
					else if ( (LA117_4==143) ) {
						switch ( input.LA(4) ) {
						case EOF:
						case K_ALLOW:
						case K_AND:
						case K_APPLY:
						case K_DELETE:
						case K_IF:
						case K_INSERT:
						case K_LIMIT:
						case K_ORDER:
						case K_UPDATE:
						case K_WHERE:
						case 143:
						case 144:
						case 145:
						case 149:
						case 157:
						case 159:
							{
							alt117=1;
							}
							break;
						case 148:
							{
							switch ( input.LA(5) ) {
							case BOOLEAN:
							case FLOAT:
							case HEXNUMBER:
							case INTEGER:
							case K_INFINITY:
							case K_NAN:
							case K_NULL:
							case K_TOKEN:
							case QMARK:
							case STRING_LITERAL:
							case UUID:
							case 142:
							case 146:
							case 148:
							case 155:
							case 158:
								{
								alt117=1;
								}
								break;
							case IDENT:
								{
								int LA117_30 = input.LA(6);
								if ( (LA117_30==142) ) {
									alt117=1;
								}
								else if ( (LA117_30==EOF||LA117_30==K_ALLOW||(LA117_30 >= K_AND && LA117_30 <= K_APPLY)||LA117_30==K_DELETE||LA117_30==K_IF||LA117_30==K_INSERT||LA117_30==K_LIMIT||LA117_30==K_ORDER||LA117_30==K_UPDATE||LA117_30==K_WHERE||(LA117_30 >= 143 && LA117_30 <= 145)||(LA117_30 >= 148 && LA117_30 <= 149)||LA117_30==157||LA117_30==159) ) {
									alt117=3;
								}

								else {
									int nvaeMark = input.mark();
									try {
										for (int nvaeConsume = 0; nvaeConsume < 6 - 1; nvaeConsume++) {
											input.consume();
										}
										NoViableAltException nvae =
											new NoViableAltException("", 117, 30, input);
										throw nvae;
									} finally {
										input.rewind(nvaeMark);
									}
								}

								}
								break;
							case K_ALL:
							case K_AS:
							case K_CLUSTERING:
							case K_COMPACT:
							case K_CONTAINS:
							case K_CUSTOM:
							case K_DISTINCT:
							case K_EXISTS:
							case K_FILTERING:
							case K_FROZEN:
							case K_KEYS:
							case K_KEYSPACES:
							case K_LIST:
							case K_MAP:
							case K_NOSUPERUSER:
							case K_PASSWORD:
							case K_PERMISSION:
							case K_PERMISSIONS:
							case K_STATIC:
							case K_STORAGE:
							case K_SUPERUSER:
							case K_TRIGGER:
							case K_TUPLE:
							case K_TYPE:
							case K_USER:
							case K_USERS:
							case K_VALUES:
								{
								int LA117_31 = input.LA(6);
								if ( (LA117_31==142) ) {
									alt117=1;
								}
								else if ( (LA117_31==EOF||LA117_31==K_ALLOW||(LA117_31 >= K_AND && LA117_31 <= K_APPLY)||LA117_31==K_DELETE||LA117_31==K_IF||LA117_31==K_INSERT||LA117_31==K_LIMIT||LA117_31==K_ORDER||LA117_31==K_UPDATE||LA117_31==K_WHERE||(LA117_31 >= 143 && LA117_31 <= 145)||(LA117_31 >= 148 && LA117_31 <= 149)||LA117_31==157||LA117_31==159) ) {
									alt117=3;
								}

								else {
									int nvaeMark = input.mark();
									try {
										for (int nvaeConsume = 0; nvaeConsume < 6 - 1; nvaeConsume++) {
											input.consume();
										}
										NoViableAltException nvae =
											new NoViableAltException("", 117, 31, input);
										throw nvae;
									} finally {
										input.rewind(nvaeMark);
									}
								}

								}
								break;
							case K_ASCII:
								{
								int LA117_32 = input.LA(6);
								if ( (LA117_32==142) ) {
									alt117=1;
								}
								else if ( (LA117_32==EOF||LA117_32==K_ALLOW||(LA117_32 >= K_AND && LA117_32 <= K_APPLY)||LA117_32==K_DELETE||LA117_32==K_IF||LA117_32==K_INSERT||LA117_32==K_LIMIT||LA117_32==K_ORDER||LA117_32==K_UPDATE||LA117_32==K_WHERE||(LA117_32 >= 143 && LA117_32 <= 145)||(LA117_32 >= 148 && LA117_32 <= 149)||LA117_32==157||LA117_32==159) ) {
									alt117=3;
								}

								else {
									int nvaeMark = input.mark();
									try {
										for (int nvaeConsume = 0; nvaeConsume < 6 - 1; nvaeConsume++) {
											input.consume();
										}
										NoViableAltException nvae =
											new NoViableAltException("", 117, 32, input);
										throw nvae;
									} finally {
										input.rewind(nvaeMark);
									}
								}

								}
								break;
							case K_BIGINT:
								{
								int LA117_33 = input.LA(6);
								if ( (LA117_33==142) ) {
									alt117=1;
								}
								else if ( (LA117_33==EOF||LA117_33==K_ALLOW||(LA117_33 >= K_AND && LA117_33 <= K_APPLY)||LA117_33==K_DELETE||LA117_33==K_IF||LA117_33==K_INSERT||LA117_33==K_LIMIT||LA117_33==K_ORDER||LA117_33==K_UPDATE||LA117_33==K_WHERE||(LA117_33 >= 143 && LA117_33 <= 145)||(LA117_33 >= 148 && LA117_33 <= 149)||LA117_33==157||LA117_33==159) ) {
									alt117=3;
								}

								else {
									int nvaeMark = input.mark();
									try {
										for (int nvaeConsume = 0; nvaeConsume < 6 - 1; nvaeConsume++) {
											input.consume();
										}
										NoViableAltException nvae =
											new NoViableAltException("", 117, 33, input);
										throw nvae;
									} finally {
										input.rewind(nvaeMark);
									}
								}

								}
								break;
							case K_BLOB:
								{
								int LA117_34 = input.LA(6);
								if ( (LA117_34==142) ) {
									alt117=1;
								}
								else if ( (LA117_34==EOF||LA117_34==K_ALLOW||(LA117_34 >= K_AND && LA117_34 <= K_APPLY)||LA117_34==K_DELETE||LA117_34==K_IF||LA117_34==K_INSERT||LA117_34==K_LIMIT||LA117_34==K_ORDER||LA117_34==K_UPDATE||LA117_34==K_WHERE||(LA117_34 >= 143 && LA117_34 <= 145)||(LA117_34 >= 148 && LA117_34 <= 149)||LA117_34==157||LA117_34==159) ) {
									alt117=3;
								}

								else {
									int nvaeMark = input.mark();
									try {
										for (int nvaeConsume = 0; nvaeConsume < 6 - 1; nvaeConsume++) {
											input.consume();
										}
										NoViableAltException nvae =
											new NoViableAltException("", 117, 34, input);
										throw nvae;
									} finally {
										input.rewind(nvaeMark);
									}
								}

								}
								break;
							case K_BOOLEAN:
								{
								int LA117_35 = input.LA(6);
								if ( (LA117_35==142) ) {
									alt117=1;
								}
								else if ( (LA117_35==EOF||LA117_35==K_ALLOW||(LA117_35 >= K_AND && LA117_35 <= K_APPLY)||LA117_35==K_DELETE||LA117_35==K_IF||LA117_35==K_INSERT||LA117_35==K_LIMIT||LA117_35==K_ORDER||LA117_35==K_UPDATE||LA117_35==K_WHERE||(LA117_35 >= 143 && LA117_35 <= 145)||(LA117_35 >= 148 && LA117_35 <= 149)||LA117_35==157||LA117_35==159) ) {
									alt117=3;
								}

								else {
									int nvaeMark = input.mark();
									try {
										for (int nvaeConsume = 0; nvaeConsume < 6 - 1; nvaeConsume++) {
											input.consume();
										}
										NoViableAltException nvae =
											new NoViableAltException("", 117, 35, input);
										throw nvae;
									} finally {
										input.rewind(nvaeMark);
									}
								}

								}
								break;
							case K_COUNTER:
								{
								int LA117_36 = input.LA(6);
								if ( (LA117_36==142) ) {
									alt117=1;
								}
								else if ( (LA117_36==EOF||LA117_36==K_ALLOW||(LA117_36 >= K_AND && LA117_36 <= K_APPLY)||LA117_36==K_DELETE||LA117_36==K_IF||LA117_36==K_INSERT||LA117_36==K_LIMIT||LA117_36==K_ORDER||LA117_36==K_UPDATE||LA117_36==K_WHERE||(LA117_36 >= 143 && LA117_36 <= 145)||(LA117_36 >= 148 && LA117_36 <= 149)||LA117_36==157||LA117_36==159) ) {
									alt117=3;
								}

								else {
									int nvaeMark = input.mark();
									try {
										for (int nvaeConsume = 0; nvaeConsume < 6 - 1; nvaeConsume++) {
											input.consume();
										}
										NoViableAltException nvae =
											new NoViableAltException("", 117, 36, input);
										throw nvae;
									} finally {
										input.rewind(nvaeMark);
									}
								}

								}
								break;
							case K_DECIMAL:
								{
								int LA117_37 = input.LA(6);
								if ( (LA117_37==142) ) {
									alt117=1;
								}
								else if ( (LA117_37==EOF||LA117_37==K_ALLOW||(LA117_37 >= K_AND && LA117_37 <= K_APPLY)||LA117_37==K_DELETE||LA117_37==K_IF||LA117_37==K_INSERT||LA117_37==K_LIMIT||LA117_37==K_ORDER||LA117_37==K_UPDATE||LA117_37==K_WHERE||(LA117_37 >= 143 && LA117_37 <= 145)||(LA117_37 >= 148 && LA117_37 <= 149)||LA117_37==157||LA117_37==159) ) {
									alt117=3;
								}

								else {
									int nvaeMark = input.mark();
									try {
										for (int nvaeConsume = 0; nvaeConsume < 6 - 1; nvaeConsume++) {
											input.consume();
										}
										NoViableAltException nvae =
											new NoViableAltException("", 117, 37, input);
										throw nvae;
									} finally {
										input.rewind(nvaeMark);
									}
								}

								}
								break;
							case K_DOUBLE:
								{
								int LA117_38 = input.LA(6);
								if ( (LA117_38==142) ) {
									alt117=1;
								}
								else if ( (LA117_38==EOF||LA117_38==K_ALLOW||(LA117_38 >= K_AND && LA117_38 <= K_APPLY)||LA117_38==K_DELETE||LA117_38==K_IF||LA117_38==K_INSERT||LA117_38==K_LIMIT||LA117_38==K_ORDER||LA117_38==K_UPDATE||LA117_38==K_WHERE||(LA117_38 >= 143 && LA117_38 <= 145)||(LA117_38 >= 148 && LA117_38 <= 149)||LA117_38==157||LA117_38==159) ) {
									alt117=3;
								}

								else {
									int nvaeMark = input.mark();
									try {
										for (int nvaeConsume = 0; nvaeConsume < 6 - 1; nvaeConsume++) {
											input.consume();
										}
										NoViableAltException nvae =
											new NoViableAltException("", 117, 38, input);
										throw nvae;
									} finally {
										input.rewind(nvaeMark);
									}
								}

								}
								break;
							case K_FLOAT:
								{
								int LA117_39 = input.LA(6);
								if ( (LA117_39==142) ) {
									alt117=1;
								}
								else if ( (LA117_39==EOF||LA117_39==K_ALLOW||(LA117_39 >= K_AND && LA117_39 <= K_APPLY)||LA117_39==K_DELETE||LA117_39==K_IF||LA117_39==K_INSERT||LA117_39==K_LIMIT||LA117_39==K_ORDER||LA117_39==K_UPDATE||LA117_39==K_WHERE||(LA117_39 >= 143 && LA117_39 <= 145)||(LA117_39 >= 148 && LA117_39 <= 149)||LA117_39==157||LA117_39==159) ) {
									alt117=3;
								}

								else {
									int nvaeMark = input.mark();
									try {
										for (int nvaeConsume = 0; nvaeConsume < 6 - 1; nvaeConsume++) {
											input.consume();
										}
										NoViableAltException nvae =
											new NoViableAltException("", 117, 39, input);
										throw nvae;
									} finally {
										input.rewind(nvaeMark);
									}
								}

								}
								break;
							case K_INET:
								{
								int LA117_40 = input.LA(6);
								if ( (LA117_40==142) ) {
									alt117=1;
								}
								else if ( (LA117_40==EOF||LA117_40==K_ALLOW||(LA117_40 >= K_AND && LA117_40 <= K_APPLY)||LA117_40==K_DELETE||LA117_40==K_IF||LA117_40==K_INSERT||LA117_40==K_LIMIT||LA117_40==K_ORDER||LA117_40==K_UPDATE||LA117_40==K_WHERE||(LA117_40 >= 143 && LA117_40 <= 145)||(LA117_40 >= 148 && LA117_40 <= 149)||LA117_40==157||LA117_40==159) ) {
									alt117=3;
								}

								else {
									int nvaeMark = input.mark();
									try {
										for (int nvaeConsume = 0; nvaeConsume < 6 - 1; nvaeConsume++) {
											input.consume();
										}
										NoViableAltException nvae =
											new NoViableAltException("", 117, 40, input);
										throw nvae;
									} finally {
										input.rewind(nvaeMark);
									}
								}

								}
								break;
							case K_INT:
								{
								int LA117_41 = input.LA(6);
								if ( (LA117_41==142) ) {
									alt117=1;
								}
								else if ( (LA117_41==EOF||LA117_41==K_ALLOW||(LA117_41 >= K_AND && LA117_41 <= K_APPLY)||LA117_41==K_DELETE||LA117_41==K_IF||LA117_41==K_INSERT||LA117_41==K_LIMIT||LA117_41==K_ORDER||LA117_41==K_UPDATE||LA117_41==K_WHERE||(LA117_41 >= 143 && LA117_41 <= 145)||(LA117_41 >= 148 && LA117_41 <= 149)||LA117_41==157||LA117_41==159) ) {
									alt117=3;
								}

								else {
									int nvaeMark = input.mark();
									try {
										for (int nvaeConsume = 0; nvaeConsume < 6 - 1; nvaeConsume++) {
											input.consume();
										}
										NoViableAltException nvae =
											new NoViableAltException("", 117, 41, input);
										throw nvae;
									} finally {
										input.rewind(nvaeMark);
									}
								}

								}
								break;
							case K_TEXT:
								{
								int LA117_42 = input.LA(6);
								if ( (LA117_42==142) ) {
									alt117=1;
								}
								else if ( (LA117_42==EOF||LA117_42==K_ALLOW||(LA117_42 >= K_AND && LA117_42 <= K_APPLY)||LA117_42==K_DELETE||LA117_42==K_IF||LA117_42==K_INSERT||LA117_42==K_LIMIT||LA117_42==K_ORDER||LA117_42==K_UPDATE||LA117_42==K_WHERE||(LA117_42 >= 143 && LA117_42 <= 145)||(LA117_42 >= 148 && LA117_42 <= 149)||LA117_42==157||LA117_42==159) ) {
									alt117=3;
								}

								else {
									int nvaeMark = input.mark();
									try {
										for (int nvaeConsume = 0; nvaeConsume < 6 - 1; nvaeConsume++) {
											input.consume();
										}
										NoViableAltException nvae =
											new NoViableAltException("", 117, 42, input);
										throw nvae;
									} finally {
										input.rewind(nvaeMark);
									}
								}

								}
								break;
							case K_TIMESTAMP:
								{
								int LA117_43 = input.LA(6);
								if ( (LA117_43==142) ) {
									alt117=1;
								}
								else if ( (LA117_43==EOF||LA117_43==K_ALLOW||(LA117_43 >= K_AND && LA117_43 <= K_APPLY)||LA117_43==K_DELETE||LA117_43==K_IF||LA117_43==K_INSERT||LA117_43==K_LIMIT||LA117_43==K_ORDER||LA117_43==K_UPDATE||LA117_43==K_WHERE||(LA117_43 >= 143 && LA117_43 <= 145)||(LA117_43 >= 148 && LA117_43 <= 149)||LA117_43==157||LA117_43==159) ) {
									alt117=3;
								}

								else {
									int nvaeMark = input.mark();
									try {
										for (int nvaeConsume = 0; nvaeConsume < 6 - 1; nvaeConsume++) {
											input.consume();
										}
										NoViableAltException nvae =
											new NoViableAltException("", 117, 43, input);
										throw nvae;
									} finally {
										input.rewind(nvaeMark);
									}
								}

								}
								break;
							case K_UUID:
								{
								int LA117_44 = input.LA(6);
								if ( (LA117_44==142) ) {
									alt117=1;
								}
								else if ( (LA117_44==EOF||LA117_44==K_ALLOW||(LA117_44 >= K_AND && LA117_44 <= K_APPLY)||LA117_44==K_DELETE||LA117_44==K_IF||LA117_44==K_INSERT||LA117_44==K_LIMIT||LA117_44==K_ORDER||LA117_44==K_UPDATE||LA117_44==K_WHERE||(LA117_44 >= 143 && LA117_44 <= 145)||(LA117_44 >= 148 && LA117_44 <= 149)||LA117_44==157||LA117_44==159) ) {
									alt117=3;
								}

								else {
									int nvaeMark = input.mark();
									try {
										for (int nvaeConsume = 0; nvaeConsume < 6 - 1; nvaeConsume++) {
											input.consume();
										}
										NoViableAltException nvae =
											new NoViableAltException("", 117, 44, input);
										throw nvae;
									} finally {
										input.rewind(nvaeMark);
									}
								}

								}
								break;
							case K_VARCHAR:
								{
								int LA117_45 = input.LA(6);
								if ( (LA117_45==142) ) {
									alt117=1;
								}
								else if ( (LA117_45==EOF||LA117_45==K_ALLOW||(LA117_45 >= K_AND && LA117_45 <= K_APPLY)||LA117_45==K_DELETE||LA117_45==K_IF||LA117_45==K_INSERT||LA117_45==K_LIMIT||LA117_45==K_ORDER||LA117_45==K_UPDATE||LA117_45==K_WHERE||(LA117_45 >= 143 && LA117_45 <= 145)||(LA117_45 >= 148 && LA117_45 <= 149)||LA117_45==157||LA117_45==159) ) {
									alt117=3;
								}

								else {
									int nvaeMark = input.mark();
									try {
										for (int nvaeConsume = 0; nvaeConsume < 6 - 1; nvaeConsume++) {
											input.consume();
										}
										NoViableAltException nvae =
											new NoViableAltException("", 117, 45, input);
										throw nvae;
									} finally {
										input.rewind(nvaeMark);
									}
								}

								}
								break;
							case K_VARINT:
								{
								int LA117_46 = input.LA(6);
								if ( (LA117_46==142) ) {
									alt117=1;
								}
								else if ( (LA117_46==EOF||LA117_46==K_ALLOW||(LA117_46 >= K_AND && LA117_46 <= K_APPLY)||LA117_46==K_DELETE||LA117_46==K_IF||LA117_46==K_INSERT||LA117_46==K_LIMIT||LA117_46==K_ORDER||LA117_46==K_UPDATE||LA117_46==K_WHERE||(LA117_46 >= 143 && LA117_46 <= 145)||(LA117_46 >= 148 && LA117_46 <= 149)||LA117_46==157||LA117_46==159) ) {
									alt117=3;
								}

								else {
									int nvaeMark = input.mark();
									try {
										for (int nvaeConsume = 0; nvaeConsume < 6 - 1; nvaeConsume++) {
											input.consume();
										}
										NoViableAltException nvae =
											new NoViableAltException("", 117, 46, input);
										throw nvae;
									} finally {
										input.rewind(nvaeMark);
									}
								}

								}
								break;
							case K_TIMEUUID:
								{
								int LA117_47 = input.LA(6);
								if ( (LA117_47==142) ) {
									alt117=1;
								}
								else if ( (LA117_47==EOF||LA117_47==K_ALLOW||(LA117_47 >= K_AND && LA117_47 <= K_APPLY)||LA117_47==K_DELETE||LA117_47==K_IF||LA117_47==K_INSERT||LA117_47==K_LIMIT||LA117_47==K_ORDER||LA117_47==K_UPDATE||LA117_47==K_WHERE||(LA117_47 >= 143 && LA117_47 <= 145)||(LA117_47 >= 148 && LA117_47 <= 149)||LA117_47==157||LA117_47==159) ) {
									alt117=3;
								}

								else {
									int nvaeMark = input.mark();
									try {
										for (int nvaeConsume = 0; nvaeConsume < 6 - 1; nvaeConsume++) {
											input.consume();
										}
										NoViableAltException nvae =
											new NoViableAltException("", 117, 47, input);
										throw nvae;
									} finally {
										input.rewind(nvaeMark);
									}
								}

								}
								break;
							case K_COUNT:
							case K_KEY:
							case K_TTL:
							case K_WRITETIME:
							case QUOTED_NAME:
								{
								alt117=3;
								}
								break;
							default:
								int nvaeMark = input.mark();
								try {
									for (int nvaeConsume = 0; nvaeConsume < 5 - 1; nvaeConsume++) {
										input.consume();
									}
									NoViableAltException nvae =
										new NoViableAltException("", 117, 29, input);
									throw nvae;
								} finally {
									input.rewind(nvaeMark);
								}
							}
							}
							break;
						case BOOLEAN:
						case FLOAT:
						case HEXNUMBER:
						case IDENT:
						case INTEGER:
						case K_ALL:
						case K_AS:
						case K_ASCII:
						case K_BIGINT:
						case K_BLOB:
						case K_BOOLEAN:
						case K_CLUSTERING:
						case K_COMPACT:
						case K_CONTAINS:
						case K_COUNTER:
						case K_CUSTOM:
						case K_DECIMAL:
						case K_DISTINCT:
						case K_DOUBLE:
						case K_EXISTS:
						case K_FILTERING:
						case K_FLOAT:
						case K_FROZEN:
						case K_INET:
						case K_INFINITY:
						case K_INT:
						case K_KEYS:
						case K_KEYSPACES:
						case K_LIST:
						case K_MAP:
						case K_NAN:
						case K_NOSUPERUSER:
						case K_NULL:
						case K_PASSWORD:
						case K_PERMISSION:
						case K_PERMISSIONS:
						case K_STATIC:
						case K_STORAGE:
						case K_SUPERUSER:
						case K_TEXT:
						case K_TIMESTAMP:
						case K_TIMEUUID:
						case K_TOKEN:
						case K_TRIGGER:
						case K_TUPLE:
						case K_TYPE:
						case K_USER:
						case K_USERS:
						case K_UUID:
						case K_VALUES:
						case K_VARCHAR:
						case K_VARINT:
						case QMARK:
						case STRING_LITERAL:
						case UUID:
						case 142:
						case 146:
						case 155:
						case 158:
							{
							alt117=3;
							}
							break;
						default:
							int nvaeMark = input.mark();
							try {
								for (int nvaeConsume = 0; nvaeConsume < 4 - 1; nvaeConsume++) {
									input.consume();
								}
								NoViableAltException nvae =
									new NoViableAltException("", 117, 28, input);
								throw nvae;
							} finally {
								input.rewind(nvaeMark);
							}
						}
					}

					else {
						int nvaeMark = input.mark();
						try {
							for (int nvaeConsume = 0; nvaeConsume < 3 - 1; nvaeConsume++) {
								input.consume();
							}
							NoViableAltException nvae =
								new NoViableAltException("", 117, 4, input);
							throw nvae;
						} finally {
							input.rewind(nvaeMark);
						}
					}

					}
					break;
				case BOOLEAN:
				case FLOAT:
				case HEXNUMBER:
				case INTEGER:
				case K_INFINITY:
				case K_NAN:
				case K_NULL:
				case K_TOKEN:
				case QMARK:
				case UUID:
				case 142:
				case 146:
				case 148:
				case 155:
				case 158:
					{
					alt117=1;
					}
					break;
				case IDENT:
					{
					int LA117_5 = input.LA(3);
					if ( (LA117_5==142) ) {
						alt117=1;
					}
					else if ( (LA117_5==143||LA117_5==147) ) {
						alt117=3;
					}

					else {
						int nvaeMark = input.mark();
						try {
							for (int nvaeConsume = 0; nvaeConsume < 3 - 1; nvaeConsume++) {
								input.consume();
							}
							NoViableAltException nvae =
								new NoViableAltException("", 117, 5, input);
							throw nvae;
						} finally {
							input.rewind(nvaeMark);
						}
					}

					}
					break;
				case K_MAP:
					{
					int LA117_6 = input.LA(3);
					if ( (LA117_6==143||LA117_6==147||LA117_6==150) ) {
						alt117=3;
					}
					else if ( (LA117_6==142) ) {
						alt117=1;
					}

					else {
						int nvaeMark = input.mark();
						try {
							for (int nvaeConsume = 0; nvaeConsume < 3 - 1; nvaeConsume++) {
								input.consume();
							}
							NoViableAltException nvae =
								new NoViableAltException("", 117, 6, input);
							throw nvae;
						} finally {
							input.rewind(nvaeMark);
						}
					}

					}
					break;
				case K_ASCII:
					{
					int LA117_7 = input.LA(3);
					if ( (LA117_7==142) ) {
						alt117=1;
					}
					else if ( (LA117_7==143||LA117_7==147) ) {
						alt117=3;
					}

					else {
						int nvaeMark = input.mark();
						try {
							for (int nvaeConsume = 0; nvaeConsume < 3 - 1; nvaeConsume++) {
								input.consume();
							}
							NoViableAltException nvae =
								new NoViableAltException("", 117, 7, input);
							throw nvae;
						} finally {
							input.rewind(nvaeMark);
						}
					}

					}
					break;
				case K_BIGINT:
					{
					int LA117_8 = input.LA(3);
					if ( (LA117_8==142) ) {
						alt117=1;
					}
					else if ( (LA117_8==143||LA117_8==147) ) {
						alt117=3;
					}

					else {
						int nvaeMark = input.mark();
						try {
							for (int nvaeConsume = 0; nvaeConsume < 3 - 1; nvaeConsume++) {
								input.consume();
							}
							NoViableAltException nvae =
								new NoViableAltException("", 117, 8, input);
							throw nvae;
						} finally {
							input.rewind(nvaeMark);
						}
					}

					}
					break;
				case K_BLOB:
					{
					int LA117_9 = input.LA(3);
					if ( (LA117_9==142) ) {
						alt117=1;
					}
					else if ( (LA117_9==143||LA117_9==147) ) {
						alt117=3;
					}

					else {
						int nvaeMark = input.mark();
						try {
							for (int nvaeConsume = 0; nvaeConsume < 3 - 1; nvaeConsume++) {
								input.consume();
							}
							NoViableAltException nvae =
								new NoViableAltException("", 117, 9, input);
							throw nvae;
						} finally {
							input.rewind(nvaeMark);
						}
					}

					}
					break;
				case K_BOOLEAN:
					{
					int LA117_10 = input.LA(3);
					if ( (LA117_10==142) ) {
						alt117=1;
					}
					else if ( (LA117_10==143||LA117_10==147) ) {
						alt117=3;
					}

					else {
						int nvaeMark = input.mark();
						try {
							for (int nvaeConsume = 0; nvaeConsume < 3 - 1; nvaeConsume++) {
								input.consume();
							}
							NoViableAltException nvae =
								new NoViableAltException("", 117, 10, input);
							throw nvae;
						} finally {
							input.rewind(nvaeMark);
						}
					}

					}
					break;
				case K_COUNTER:
					{
					int LA117_11 = input.LA(3);
					if ( (LA117_11==142) ) {
						alt117=1;
					}
					else if ( (LA117_11==143||LA117_11==147) ) {
						alt117=3;
					}

					else {
						int nvaeMark = input.mark();
						try {
							for (int nvaeConsume = 0; nvaeConsume < 3 - 1; nvaeConsume++) {
								input.consume();
							}
							NoViableAltException nvae =
								new NoViableAltException("", 117, 11, input);
							throw nvae;
						} finally {
							input.rewind(nvaeMark);
						}
					}

					}
					break;
				case K_DECIMAL:
					{
					int LA117_12 = input.LA(3);
					if ( (LA117_12==142) ) {
						alt117=1;
					}
					else if ( (LA117_12==143||LA117_12==147) ) {
						alt117=3;
					}

					else {
						int nvaeMark = input.mark();
						try {
							for (int nvaeConsume = 0; nvaeConsume < 3 - 1; nvaeConsume++) {
								input.consume();
							}
							NoViableAltException nvae =
								new NoViableAltException("", 117, 12, input);
							throw nvae;
						} finally {
							input.rewind(nvaeMark);
						}
					}

					}
					break;
				case K_DOUBLE:
					{
					int LA117_13 = input.LA(3);
					if ( (LA117_13==142) ) {
						alt117=1;
					}
					else if ( (LA117_13==143||LA117_13==147) ) {
						alt117=3;
					}

					else {
						int nvaeMark = input.mark();
						try {
							for (int nvaeConsume = 0; nvaeConsume < 3 - 1; nvaeConsume++) {
								input.consume();
							}
							NoViableAltException nvae =
								new NoViableAltException("", 117, 13, input);
							throw nvae;
						} finally {
							input.rewind(nvaeMark);
						}
					}

					}
					break;
				case K_FLOAT:
					{
					int LA117_14 = input.LA(3);
					if ( (LA117_14==142) ) {
						alt117=1;
					}
					else if ( (LA117_14==143||LA117_14==147) ) {
						alt117=3;
					}

					else {
						int nvaeMark = input.mark();
						try {
							for (int nvaeConsume = 0; nvaeConsume < 3 - 1; nvaeConsume++) {
								input.consume();
							}
							NoViableAltException nvae =
								new NoViableAltException("", 117, 14, input);
							throw nvae;
						} finally {
							input.rewind(nvaeMark);
						}
					}

					}
					break;
				case K_INET:
					{
					int LA117_15 = input.LA(3);
					if ( (LA117_15==142) ) {
						alt117=1;
					}
					else if ( (LA117_15==143||LA117_15==147) ) {
						alt117=3;
					}

					else {
						int nvaeMark = input.mark();
						try {
							for (int nvaeConsume = 0; nvaeConsume < 3 - 1; nvaeConsume++) {
								input.consume();
							}
							NoViableAltException nvae =
								new NoViableAltException("", 117, 15, input);
							throw nvae;
						} finally {
							input.rewind(nvaeMark);
						}
					}

					}
					break;
				case K_INT:
					{
					int LA117_16 = input.LA(3);
					if ( (LA117_16==142) ) {
						alt117=1;
					}
					else if ( (LA117_16==143||LA117_16==147) ) {
						alt117=3;
					}

					else {
						int nvaeMark = input.mark();
						try {
							for (int nvaeConsume = 0; nvaeConsume < 3 - 1; nvaeConsume++) {
								input.consume();
							}
							NoViableAltException nvae =
								new NoViableAltException("", 117, 16, input);
							throw nvae;
						} finally {
							input.rewind(nvaeMark);
						}
					}

					}
					break;
				case K_TEXT:
					{
					int LA117_17 = input.LA(3);
					if ( (LA117_17==142) ) {
						alt117=1;
					}
					else if ( (LA117_17==143||LA117_17==147) ) {
						alt117=3;
					}

					else {
						int nvaeMark = input.mark();
						try {
							for (int nvaeConsume = 0; nvaeConsume < 3 - 1; nvaeConsume++) {
								input.consume();
							}
							NoViableAltException nvae =
								new NoViableAltException("", 117, 17, input);
							throw nvae;
						} finally {
							input.rewind(nvaeMark);
						}
					}

					}
					break;
				case K_TIMESTAMP:
					{
					int LA117_18 = input.LA(3);
					if ( (LA117_18==142) ) {
						alt117=1;
					}
					else if ( (LA117_18==143||LA117_18==147) ) {
						alt117=3;
					}

					else {
						int nvaeMark = input.mark();
						try {
							for (int nvaeConsume = 0; nvaeConsume < 3 - 1; nvaeConsume++) {
								input.consume();
							}
							NoViableAltException nvae =
								new NoViableAltException("", 117, 18, input);
							throw nvae;
						} finally {
							input.rewind(nvaeMark);
						}
					}

					}
					break;
				case K_UUID:
					{
					int LA117_19 = input.LA(3);
					if ( (LA117_19==142) ) {
						alt117=1;
					}
					else if ( (LA117_19==143||LA117_19==147) ) {
						alt117=3;
					}

					else {
						int nvaeMark = input.mark();
						try {
							for (int nvaeConsume = 0; nvaeConsume < 3 - 1; nvaeConsume++) {
								input.consume();
							}
							NoViableAltException nvae =
								new NoViableAltException("", 117, 19, input);
							throw nvae;
						} finally {
							input.rewind(nvaeMark);
						}
					}

					}
					break;
				case K_VARCHAR:
					{
					int LA117_20 = input.LA(3);
					if ( (LA117_20==142) ) {
						alt117=1;
					}
					else if ( (LA117_20==143||LA117_20==147) ) {
						alt117=3;
					}

					else {
						int nvaeMark = input.mark();
						try {
							for (int nvaeConsume = 0; nvaeConsume < 3 - 1; nvaeConsume++) {
								input.consume();
							}
							NoViableAltException nvae =
								new NoViableAltException("", 117, 20, input);
							throw nvae;
						} finally {
							input.rewind(nvaeMark);
						}
					}

					}
					break;
				case K_VARINT:
					{
					int LA117_21 = input.LA(3);
					if ( (LA117_21==142) ) {
						alt117=1;
					}
					else if ( (LA117_21==143||LA117_21==147) ) {
						alt117=3;
					}

					else {
						int nvaeMark = input.mark();
						try {
							for (int nvaeConsume = 0; nvaeConsume < 3 - 1; nvaeConsume++) {
								input.consume();
							}
							NoViableAltException nvae =
								new NoViableAltException("", 117, 21, input);
							throw nvae;
						} finally {
							input.rewind(nvaeMark);
						}
					}

					}
					break;
				case K_TIMEUUID:
					{
					int LA117_22 = input.LA(3);
					if ( (LA117_22==142) ) {
						alt117=1;
					}
					else if ( (LA117_22==143||LA117_22==147) ) {
						alt117=3;
					}

					else {
						int nvaeMark = input.mark();
						try {
							for (int nvaeConsume = 0; nvaeConsume < 3 - 1; nvaeConsume++) {
								input.consume();
							}
							NoViableAltException nvae =
								new NoViableAltException("", 117, 22, input);
							throw nvae;
						} finally {
							input.rewind(nvaeMark);
						}
					}

					}
					break;
				case K_LIST:
					{
					int LA117_23 = input.LA(3);
					if ( (LA117_23==143||LA117_23==147||LA117_23==150) ) {
						alt117=3;
					}
					else if ( (LA117_23==142) ) {
						alt117=1;
					}

					else {
						int nvaeMark = input.mark();
						try {
							for (int nvaeConsume = 0; nvaeConsume < 3 - 1; nvaeConsume++) {
								input.consume();
							}
							NoViableAltException nvae =
								new NoViableAltException("", 117, 23, input);
							throw nvae;
						} finally {
							input.rewind(nvaeMark);
						}
					}

					}
					break;
				case K_TUPLE:
					{
					int LA117_24 = input.LA(3);
					if ( (LA117_24==143||LA117_24==147||LA117_24==150) ) {
						alt117=3;
					}
					else if ( (LA117_24==142) ) {
						alt117=1;
					}

					else {
						int nvaeMark = input.mark();
						try {
							for (int nvaeConsume = 0; nvaeConsume < 3 - 1; nvaeConsume++) {
								input.consume();
							}
							NoViableAltException nvae =
								new NoViableAltException("", 117, 24, input);
							throw nvae;
						} finally {
							input.rewind(nvaeMark);
						}
					}

					}
					break;
				case K_COUNT:
				case K_KEY:
				case K_SET:
				case K_TTL:
				case K_WRITETIME:
				case QUOTED_NAME:
					{
					alt117=3;
					}
					break;
				case K_FROZEN:
					{
					int LA117_26 = input.LA(3);
					if ( (LA117_26==143||LA117_26==147||LA117_26==150) ) {
						alt117=3;
					}
					else if ( (LA117_26==142) ) {
						alt117=1;
					}

					else {
						int nvaeMark = input.mark();
						try {
							for (int nvaeConsume = 0; nvaeConsume < 3 - 1; nvaeConsume++) {
								input.consume();
							}
							NoViableAltException nvae =
								new NoViableAltException("", 117, 26, input);
							throw nvae;
						} finally {
							input.rewind(nvaeMark);
						}
					}

					}
					break;
				case K_ALL:
				case K_AS:
				case K_CLUSTERING:
				case K_COMPACT:
				case K_CONTAINS:
				case K_CUSTOM:
				case K_DISTINCT:
				case K_EXISTS:
				case K_FILTERING:
				case K_KEYS:
				case K_KEYSPACES:
				case K_NOSUPERUSER:
				case K_PASSWORD:
				case K_PERMISSION:
				case K_PERMISSIONS:
				case K_STATIC:
				case K_STORAGE:
				case K_SUPERUSER:
				case K_TRIGGER:
				case K_TYPE:
				case K_USER:
				case K_USERS:
				case K_VALUES:
					{
					int LA117_27 = input.LA(3);
					if ( (LA117_27==142) ) {
						alt117=1;
					}
					else if ( (LA117_27==143||LA117_27==147) ) {
						alt117=3;
					}

					else {
						int nvaeMark = input.mark();
						try {
							for (int nvaeConsume = 0; nvaeConsume < 3 - 1; nvaeConsume++) {
								input.consume();
							}
							NoViableAltException nvae =
								new NoViableAltException("", 117, 27, input);
							throw nvae;
						} finally {
							input.rewind(nvaeMark);
						}
					}

					}
					break;
				default:
					int nvaeMark = input.mark();
					try {
						input.consume();
						NoViableAltException nvae =
							new NoViableAltException("", 117, 2, input);
						throw nvae;
					} finally {
						input.rewind(nvaeMark);
					}
				}
				}
				break;
			case IDENT:
			case K_ALL:
			case K_AS:
			case K_ASCII:
			case K_BIGINT:
			case K_BLOB:
			case K_BOOLEAN:
			case K_CLUSTERING:
			case K_COMPACT:
			case K_CONTAINS:
			case K_COUNTER:
			case K_CUSTOM:
			case K_DECIMAL:
			case K_DISTINCT:
			case K_DOUBLE:
			case K_EXISTS:
			case K_FILTERING:
			case K_FLOAT:
			case K_FROZEN:
			case K_INET:
			case K_INT:
			case K_KEYS:
			case K_KEYSPACES:
			case K_LIST:
			case K_MAP:
			case K_NOSUPERUSER:
			case K_PASSWORD:
			case K_PERMISSION:
			case K_PERMISSIONS:
			case K_STATIC:
			case K_STORAGE:
			case K_SUPERUSER:
			case K_TEXT:
			case K_TIMESTAMP:
			case K_TIMEUUID:
			case K_TOKEN:
			case K_TRIGGER:
			case K_TUPLE:
			case K_TYPE:
			case K_USER:
			case K_USERS:
			case K_UUID:
			case K_VALUES:
			case K_VARCHAR:
			case K_VARINT:
				{
				alt117=2;
				}
				break;
			default:
				NoViableAltException nvae =
					new NoViableAltException("", 117, 0, input);
				throw nvae;
			}
			switch (alt117) {
				case 1 :
					// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:959:7: v= value
					{
					pushFollow(FOLLOW_value_in_term6184);
					v=value();
					state._fsp--;

					 term = v; 
					}
					break;
				case 2 :
					// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:960:7: f= function
					{
					pushFollow(FOLLOW_function_in_term6221);
					f=function();
					state._fsp--;

					 term = f; 
					}
					break;
				case 3 :
					// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:961:7: '(' c= comparatorType ')' t= term
					{
					match(input,142,FOLLOW_142_in_term6253); 
					pushFollow(FOLLOW_comparatorType_in_term6257);
					c=comparatorType();
					state._fsp--;

					match(input,143,FOLLOW_143_in_term6259); 
					pushFollow(FOLLOW_term_in_term6263);
					t=term();
					state._fsp--;

					 term = new TypeCast(c, t); 
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
		return term;
	}
	// $ANTLR end "term"



	// $ANTLR start "columnOperation"
	// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:964:1: columnOperation[List<Pair<ColumnIdentifier.Raw, Operation.RawUpdate>> operations] : key= cident columnOperationDifferentiator[operations, key] ;
	public final void columnOperation(List<Pair<ColumnIdentifier.Raw, Operation.RawUpdate>> operations) throws RecognitionException {
		ColumnIdentifier.Raw key =null;

		try {
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:965:5: (key= cident columnOperationDifferentiator[operations, key] )
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:965:7: key= cident columnOperationDifferentiator[operations, key]
			{
			pushFollow(FOLLOW_cident_in_columnOperation6286);
			key=cident();
			state._fsp--;

			pushFollow(FOLLOW_columnOperationDifferentiator_in_columnOperation6288);
			columnOperationDifferentiator(operations, key);
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
	}
	// $ANTLR end "columnOperation"



	// $ANTLR start "columnOperationDifferentiator"
	// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:968:1: columnOperationDifferentiator[List<Pair<ColumnIdentifier.Raw, Operation.RawUpdate>> operations, ColumnIdentifier.Raw key] : ( '=' normalColumnOperation[operations, key] | '[' k= term ']' specializedColumnOperation[operations, key, k] );
	public final void columnOperationDifferentiator(List<Pair<ColumnIdentifier.Raw, Operation.RawUpdate>> operations, ColumnIdentifier.Raw key) throws RecognitionException {
		Term.Raw k =null;

		try {
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:969:5: ( '=' normalColumnOperation[operations, key] | '[' k= term ']' specializedColumnOperation[operations, key, k] )
			int alt118=2;
			int LA118_0 = input.LA(1);
			if ( (LA118_0==152) ) {
				alt118=1;
			}
			else if ( (LA118_0==155) ) {
				alt118=2;
			}

			else {
				NoViableAltException nvae =
					new NoViableAltException("", 118, 0, input);
				throw nvae;
			}

			switch (alt118) {
				case 1 :
					// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:969:7: '=' normalColumnOperation[operations, key]
					{
					match(input,152,FOLLOW_152_in_columnOperationDifferentiator6307); 
					pushFollow(FOLLOW_normalColumnOperation_in_columnOperationDifferentiator6309);
					normalColumnOperation(operations, key);
					state._fsp--;

					}
					break;
				case 2 :
					// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:970:7: '[' k= term ']' specializedColumnOperation[operations, key, k]
					{
					match(input,155,FOLLOW_155_in_columnOperationDifferentiator6318); 
					pushFollow(FOLLOW_term_in_columnOperationDifferentiator6322);
					k=term();
					state._fsp--;

					match(input,157,FOLLOW_157_in_columnOperationDifferentiator6324); 
					pushFollow(FOLLOW_specializedColumnOperation_in_columnOperationDifferentiator6326);
					specializedColumnOperation(operations, key, k);
					state._fsp--;

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
	// $ANTLR end "columnOperationDifferentiator"



	// $ANTLR start "normalColumnOperation"
	// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:973:1: normalColumnOperation[List<Pair<ColumnIdentifier.Raw, Operation.RawUpdate>> operations, ColumnIdentifier.Raw key] : (t= term ( '+' c= cident )? |c= cident sig= ( '+' | '-' ) t= term |c= cident i= INTEGER );
	public final void normalColumnOperation(List<Pair<ColumnIdentifier.Raw, Operation.RawUpdate>> operations, ColumnIdentifier.Raw key) throws RecognitionException {
		Token sig=null;
		Token i=null;
		Term.Raw t =null;
		ColumnIdentifier.Raw c =null;

		try {
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:974:5: (t= term ( '+' c= cident )? |c= cident sig= ( '+' | '-' ) t= term |c= cident i= INTEGER )
			int alt120=3;
			switch ( input.LA(1) ) {
			case BOOLEAN:
			case FLOAT:
			case HEXNUMBER:
			case INTEGER:
			case K_INFINITY:
			case K_NAN:
			case K_NULL:
			case K_TOKEN:
			case QMARK:
			case STRING_LITERAL:
			case UUID:
			case 142:
			case 146:
			case 148:
			case 155:
			case 158:
				{
				alt120=1;
				}
				break;
			case IDENT:
				{
				switch ( input.LA(2) ) {
				case 142:
					{
					alt120=1;
					}
					break;
				case 144:
				case 146:
					{
					alt120=2;
					}
					break;
				case INTEGER:
					{
					alt120=3;
					}
					break;
				default:
					int nvaeMark = input.mark();
					try {
						input.consume();
						NoViableAltException nvae =
							new NoViableAltException("", 120, 2, input);
						throw nvae;
					} finally {
						input.rewind(nvaeMark);
					}
				}
				}
				break;
			case K_ALL:
			case K_AS:
			case K_CLUSTERING:
			case K_COMPACT:
			case K_CONTAINS:
			case K_CUSTOM:
			case K_DISTINCT:
			case K_EXISTS:
			case K_FILTERING:
			case K_FROZEN:
			case K_KEYS:
			case K_KEYSPACES:
			case K_LIST:
			case K_MAP:
			case K_NOSUPERUSER:
			case K_PASSWORD:
			case K_PERMISSION:
			case K_PERMISSIONS:
			case K_STATIC:
			case K_STORAGE:
			case K_SUPERUSER:
			case K_TRIGGER:
			case K_TUPLE:
			case K_TYPE:
			case K_USER:
			case K_USERS:
			case K_VALUES:
				{
				switch ( input.LA(2) ) {
				case 142:
					{
					alt120=1;
					}
					break;
				case 144:
				case 146:
					{
					alt120=2;
					}
					break;
				case INTEGER:
					{
					alt120=3;
					}
					break;
				default:
					int nvaeMark = input.mark();
					try {
						input.consume();
						NoViableAltException nvae =
							new NoViableAltException("", 120, 3, input);
						throw nvae;
					} finally {
						input.rewind(nvaeMark);
					}
				}
				}
				break;
			case K_ASCII:
				{
				switch ( input.LA(2) ) {
				case 142:
					{
					alt120=1;
					}
					break;
				case 144:
				case 146:
					{
					alt120=2;
					}
					break;
				case INTEGER:
					{
					alt120=3;
					}
					break;
				default:
					int nvaeMark = input.mark();
					try {
						input.consume();
						NoViableAltException nvae =
							new NoViableAltException("", 120, 4, input);
						throw nvae;
					} finally {
						input.rewind(nvaeMark);
					}
				}
				}
				break;
			case K_BIGINT:
				{
				switch ( input.LA(2) ) {
				case 142:
					{
					alt120=1;
					}
					break;
				case 144:
				case 146:
					{
					alt120=2;
					}
					break;
				case INTEGER:
					{
					alt120=3;
					}
					break;
				default:
					int nvaeMark = input.mark();
					try {
						input.consume();
						NoViableAltException nvae =
							new NoViableAltException("", 120, 5, input);
						throw nvae;
					} finally {
						input.rewind(nvaeMark);
					}
				}
				}
				break;
			case K_BLOB:
				{
				switch ( input.LA(2) ) {
				case 142:
					{
					alt120=1;
					}
					break;
				case 144:
				case 146:
					{
					alt120=2;
					}
					break;
				case INTEGER:
					{
					alt120=3;
					}
					break;
				default:
					int nvaeMark = input.mark();
					try {
						input.consume();
						NoViableAltException nvae =
							new NoViableAltException("", 120, 6, input);
						throw nvae;
					} finally {
						input.rewind(nvaeMark);
					}
				}
				}
				break;
			case K_BOOLEAN:
				{
				switch ( input.LA(2) ) {
				case 142:
					{
					alt120=1;
					}
					break;
				case 144:
				case 146:
					{
					alt120=2;
					}
					break;
				case INTEGER:
					{
					alt120=3;
					}
					break;
				default:
					int nvaeMark = input.mark();
					try {
						input.consume();
						NoViableAltException nvae =
							new NoViableAltException("", 120, 7, input);
						throw nvae;
					} finally {
						input.rewind(nvaeMark);
					}
				}
				}
				break;
			case K_COUNTER:
				{
				switch ( input.LA(2) ) {
				case 142:
					{
					alt120=1;
					}
					break;
				case 144:
				case 146:
					{
					alt120=2;
					}
					break;
				case INTEGER:
					{
					alt120=3;
					}
					break;
				default:
					int nvaeMark = input.mark();
					try {
						input.consume();
						NoViableAltException nvae =
							new NoViableAltException("", 120, 8, input);
						throw nvae;
					} finally {
						input.rewind(nvaeMark);
					}
				}
				}
				break;
			case K_DECIMAL:
				{
				switch ( input.LA(2) ) {
				case 142:
					{
					alt120=1;
					}
					break;
				case 144:
				case 146:
					{
					alt120=2;
					}
					break;
				case INTEGER:
					{
					alt120=3;
					}
					break;
				default:
					int nvaeMark = input.mark();
					try {
						input.consume();
						NoViableAltException nvae =
							new NoViableAltException("", 120, 9, input);
						throw nvae;
					} finally {
						input.rewind(nvaeMark);
					}
				}
				}
				break;
			case K_DOUBLE:
				{
				switch ( input.LA(2) ) {
				case 142:
					{
					alt120=1;
					}
					break;
				case 144:
				case 146:
					{
					alt120=2;
					}
					break;
				case INTEGER:
					{
					alt120=3;
					}
					break;
				default:
					int nvaeMark = input.mark();
					try {
						input.consume();
						NoViableAltException nvae =
							new NoViableAltException("", 120, 10, input);
						throw nvae;
					} finally {
						input.rewind(nvaeMark);
					}
				}
				}
				break;
			case K_FLOAT:
				{
				switch ( input.LA(2) ) {
				case 142:
					{
					alt120=1;
					}
					break;
				case 144:
				case 146:
					{
					alt120=2;
					}
					break;
				case INTEGER:
					{
					alt120=3;
					}
					break;
				default:
					int nvaeMark = input.mark();
					try {
						input.consume();
						NoViableAltException nvae =
							new NoViableAltException("", 120, 11, input);
						throw nvae;
					} finally {
						input.rewind(nvaeMark);
					}
				}
				}
				break;
			case K_INET:
				{
				switch ( input.LA(2) ) {
				case 142:
					{
					alt120=1;
					}
					break;
				case 144:
				case 146:
					{
					alt120=2;
					}
					break;
				case INTEGER:
					{
					alt120=3;
					}
					break;
				default:
					int nvaeMark = input.mark();
					try {
						input.consume();
						NoViableAltException nvae =
							new NoViableAltException("", 120, 12, input);
						throw nvae;
					} finally {
						input.rewind(nvaeMark);
					}
				}
				}
				break;
			case K_INT:
				{
				switch ( input.LA(2) ) {
				case 142:
					{
					alt120=1;
					}
					break;
				case 144:
				case 146:
					{
					alt120=2;
					}
					break;
				case INTEGER:
					{
					alt120=3;
					}
					break;
				default:
					int nvaeMark = input.mark();
					try {
						input.consume();
						NoViableAltException nvae =
							new NoViableAltException("", 120, 13, input);
						throw nvae;
					} finally {
						input.rewind(nvaeMark);
					}
				}
				}
				break;
			case K_TEXT:
				{
				switch ( input.LA(2) ) {
				case 142:
					{
					alt120=1;
					}
					break;
				case 144:
				case 146:
					{
					alt120=2;
					}
					break;
				case INTEGER:
					{
					alt120=3;
					}
					break;
				default:
					int nvaeMark = input.mark();
					try {
						input.consume();
						NoViableAltException nvae =
							new NoViableAltException("", 120, 14, input);
						throw nvae;
					} finally {
						input.rewind(nvaeMark);
					}
				}
				}
				break;
			case K_TIMESTAMP:
				{
				switch ( input.LA(2) ) {
				case 142:
					{
					alt120=1;
					}
					break;
				case 144:
				case 146:
					{
					alt120=2;
					}
					break;
				case INTEGER:
					{
					alt120=3;
					}
					break;
				default:
					int nvaeMark = input.mark();
					try {
						input.consume();
						NoViableAltException nvae =
							new NoViableAltException("", 120, 15, input);
						throw nvae;
					} finally {
						input.rewind(nvaeMark);
					}
				}
				}
				break;
			case K_UUID:
				{
				switch ( input.LA(2) ) {
				case 142:
					{
					alt120=1;
					}
					break;
				case 144:
				case 146:
					{
					alt120=2;
					}
					break;
				case INTEGER:
					{
					alt120=3;
					}
					break;
				default:
					int nvaeMark = input.mark();
					try {
						input.consume();
						NoViableAltException nvae =
							new NoViableAltException("", 120, 16, input);
						throw nvae;
					} finally {
						input.rewind(nvaeMark);
					}
				}
				}
				break;
			case K_VARCHAR:
				{
				switch ( input.LA(2) ) {
				case 142:
					{
					alt120=1;
					}
					break;
				case 144:
				case 146:
					{
					alt120=2;
					}
					break;
				case INTEGER:
					{
					alt120=3;
					}
					break;
				default:
					int nvaeMark = input.mark();
					try {
						input.consume();
						NoViableAltException nvae =
							new NoViableAltException("", 120, 17, input);
						throw nvae;
					} finally {
						input.rewind(nvaeMark);
					}
				}
				}
				break;
			case K_VARINT:
				{
				switch ( input.LA(2) ) {
				case 142:
					{
					alt120=1;
					}
					break;
				case 144:
				case 146:
					{
					alt120=2;
					}
					break;
				case INTEGER:
					{
					alt120=3;
					}
					break;
				default:
					int nvaeMark = input.mark();
					try {
						input.consume();
						NoViableAltException nvae =
							new NoViableAltException("", 120, 18, input);
						throw nvae;
					} finally {
						input.rewind(nvaeMark);
					}
				}
				}
				break;
			case K_TIMEUUID:
				{
				switch ( input.LA(2) ) {
				case 142:
					{
					alt120=1;
					}
					break;
				case 144:
				case 146:
					{
					alt120=2;
					}
					break;
				case INTEGER:
					{
					alt120=3;
					}
					break;
				default:
					int nvaeMark = input.mark();
					try {
						input.consume();
						NoViableAltException nvae =
							new NoViableAltException("", 120, 19, input);
						throw nvae;
					} finally {
						input.rewind(nvaeMark);
					}
				}
				}
				break;
			case QUOTED_NAME:
				{
				int LA120_20 = input.LA(2);
				if ( (LA120_20==144||LA120_20==146) ) {
					alt120=2;
				}
				else if ( (LA120_20==INTEGER) ) {
					alt120=3;
				}

				else {
					int nvaeMark = input.mark();
					try {
						input.consume();
						NoViableAltException nvae =
							new NoViableAltException("", 120, 20, input);
						throw nvae;
					} finally {
						input.rewind(nvaeMark);
					}
				}

				}
				break;
			case K_COUNT:
			case K_KEY:
			case K_TTL:
			case K_WRITETIME:
				{
				int LA120_21 = input.LA(2);
				if ( (LA120_21==144||LA120_21==146) ) {
					alt120=2;
				}
				else if ( (LA120_21==INTEGER) ) {
					alt120=3;
				}

				else {
					int nvaeMark = input.mark();
					try {
						input.consume();
						NoViableAltException nvae =
							new NoViableAltException("", 120, 21, input);
						throw nvae;
					} finally {
						input.rewind(nvaeMark);
					}
				}

				}
				break;
			default:
				NoViableAltException nvae =
					new NoViableAltException("", 120, 0, input);
				throw nvae;
			}
			switch (alt120) {
				case 1 :
					// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:974:7: t= term ( '+' c= cident )?
					{
					pushFollow(FOLLOW_term_in_normalColumnOperation6347);
					t=term();
					state._fsp--;

					// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:974:14: ( '+' c= cident )?
					int alt119=2;
					int LA119_0 = input.LA(1);
					if ( (LA119_0==144) ) {
						alt119=1;
					}
					switch (alt119) {
						case 1 :
							// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:974:15: '+' c= cident
							{
							match(input,144,FOLLOW_144_in_normalColumnOperation6350); 
							pushFollow(FOLLOW_cident_in_normalColumnOperation6354);
							c=cident();
							state._fsp--;

							}
							break;

					}


					          if (c == null)
					          {
					              addRawUpdate(operations, key, new Operation.SetValue(t));
					          }
					          else
					          {
					              if (!key.equals(c))
					                  addRecognitionError("Only expressions of the form X = <value> + X are supported.");
					              addRawUpdate(operations, key, new Operation.Prepend(t));
					          }
					      
					}
					break;
				case 2 :
					// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:987:7: c= cident sig= ( '+' | '-' ) t= term
					{
					pushFollow(FOLLOW_cident_in_normalColumnOperation6375);
					c=cident();
					state._fsp--;

					sig=input.LT(1);
					if ( input.LA(1)==144||input.LA(1)==146 ) {
						input.consume();
						state.errorRecovery=false;
					}
					else {
						MismatchedSetException mse = new MismatchedSetException(null,input);
						throw mse;
					}
					pushFollow(FOLLOW_term_in_normalColumnOperation6389);
					t=term();
					state._fsp--;


					          if (!key.equals(c))
					              addRecognitionError("Only expressions of the form X = X " + (sig!=null?sig.getText():null) + "<value> are supported.");
					          addRawUpdate(operations, key, (sig!=null?sig.getText():null).equals("+") ? new Operation.Addition(t) : new Operation.Substraction(t));
					      
					}
					break;
				case 3 :
					// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:993:7: c= cident i= INTEGER
					{
					pushFollow(FOLLOW_cident_in_normalColumnOperation6407);
					c=cident();
					state._fsp--;

					i=(Token)match(input,INTEGER,FOLLOW_INTEGER_in_normalColumnOperation6411); 

					          // Note that this production *is* necessary because X = X - 3 will in fact be lexed as [ X, '=', X, INTEGER].
					          if (!key.equals(c))
					              // We don't yet allow a '+' in front of an integer, but we could in the future really, so let's be future-proof in our error message
					              addRecognitionError("Only expressions of the form X = X " + ((i!=null?i.getText():null).charAt(0) == '-' ? '-' : '+') + " <value> are supported.");
					          addRawUpdate(operations, key, new Operation.Addition(Constants.Literal.integer((i!=null?i.getText():null))));
					      
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
	// $ANTLR end "normalColumnOperation"



	// $ANTLR start "specializedColumnOperation"
	// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1003:1: specializedColumnOperation[List<Pair<ColumnIdentifier.Raw, Operation.RawUpdate>> operations, ColumnIdentifier.Raw key, Term.Raw k] : '=' t= term ;
	public final void specializedColumnOperation(List<Pair<ColumnIdentifier.Raw, Operation.RawUpdate>> operations, ColumnIdentifier.Raw key, Term.Raw k) throws RecognitionException {
		Term.Raw t =null;

		try {
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1004:5: ( '=' t= term )
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1004:7: '=' t= term
			{
			match(input,152,FOLLOW_152_in_specializedColumnOperation6437); 
			pushFollow(FOLLOW_term_in_specializedColumnOperation6441);
			t=term();
			state._fsp--;


			          addRawUpdate(operations, key, new Operation.SetElement(k, t));
			      
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
	// $ANTLR end "specializedColumnOperation"



	// $ANTLR start "columnCondition"
	// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1010:1: columnCondition[List<Pair<ColumnIdentifier.Raw, ColumnCondition.Raw>> conditions] : key= cident (op= relationType t= term | K_IN (values= singleColumnInValues |marker= inMarker ) | '[' element= term ']' (op= relationType t= term | K_IN (values= singleColumnInValues |marker= inMarker ) ) ) ;
	public final void columnCondition(List<Pair<ColumnIdentifier.Raw, ColumnCondition.Raw>> conditions) throws RecognitionException {
		ColumnIdentifier.Raw key =null;
		Operator op =null;
		Term.Raw t =null;
		List<Term.Raw> values =null;
		AbstractMarker.INRaw marker =null;
		Term.Raw element =null;

		try {
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1012:5: (key= cident (op= relationType t= term | K_IN (values= singleColumnInValues |marker= inMarker ) | '[' element= term ']' (op= relationType t= term | K_IN (values= singleColumnInValues |marker= inMarker ) ) ) )
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1012:7: key= cident (op= relationType t= term | K_IN (values= singleColumnInValues |marker= inMarker ) | '[' element= term ']' (op= relationType t= term | K_IN (values= singleColumnInValues |marker= inMarker ) ) )
			{
			pushFollow(FOLLOW_cident_in_columnCondition6474);
			key=cident();
			state._fsp--;

			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1013:9: (op= relationType t= term | K_IN (values= singleColumnInValues |marker= inMarker ) | '[' element= term ']' (op= relationType t= term | K_IN (values= singleColumnInValues |marker= inMarker ) ) )
			int alt124=3;
			switch ( input.LA(1) ) {
			case 141:
			case 150:
			case 151:
			case 152:
			case 153:
			case 154:
				{
				alt124=1;
				}
				break;
			case K_IN:
				{
				alt124=2;
				}
				break;
			case 155:
				{
				alt124=3;
				}
				break;
			default:
				NoViableAltException nvae =
					new NoViableAltException("", 124, 0, input);
				throw nvae;
			}
			switch (alt124) {
				case 1 :
					// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1013:11: op= relationType t= term
					{
					pushFollow(FOLLOW_relationType_in_columnCondition6488);
					op=relationType();
					state._fsp--;

					pushFollow(FOLLOW_term_in_columnCondition6492);
					t=term();
					state._fsp--;

					 conditions.add(Pair.create(key, ColumnCondition.Raw.simpleCondition(t, op))); 
					}
					break;
				case 2 :
					// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1014:11: K_IN (values= singleColumnInValues |marker= inMarker )
					{
					match(input,K_IN,FOLLOW_K_IN_in_columnCondition6506); 
					// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1015:13: (values= singleColumnInValues |marker= inMarker )
					int alt121=2;
					int LA121_0 = input.LA(1);
					if ( (LA121_0==142) ) {
						alt121=1;
					}
					else if ( (LA121_0==QMARK||LA121_0==148) ) {
						alt121=2;
					}

					else {
						NoViableAltException nvae =
							new NoViableAltException("", 121, 0, input);
						throw nvae;
					}

					switch (alt121) {
						case 1 :
							// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1015:15: values= singleColumnInValues
							{
							pushFollow(FOLLOW_singleColumnInValues_in_columnCondition6524);
							values=singleColumnInValues();
							state._fsp--;

							 conditions.add(Pair.create(key, ColumnCondition.Raw.simpleInCondition(values))); 
							}
							break;
						case 2 :
							// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1016:15: marker= inMarker
							{
							pushFollow(FOLLOW_inMarker_in_columnCondition6544);
							marker=inMarker();
							state._fsp--;

							 conditions.add(Pair.create(key, ColumnCondition.Raw.simpleInCondition(marker))); 
							}
							break;

					}

					}
					break;
				case 3 :
					// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1018:11: '[' element= term ']' (op= relationType t= term | K_IN (values= singleColumnInValues |marker= inMarker ) )
					{
					match(input,155,FOLLOW_155_in_columnCondition6572); 
					pushFollow(FOLLOW_term_in_columnCondition6576);
					element=term();
					state._fsp--;

					match(input,157,FOLLOW_157_in_columnCondition6578); 
					// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1019:13: (op= relationType t= term | K_IN (values= singleColumnInValues |marker= inMarker ) )
					int alt123=2;
					int LA123_0 = input.LA(1);
					if ( (LA123_0==141||(LA123_0 >= 150 && LA123_0 <= 154)) ) {
						alt123=1;
					}
					else if ( (LA123_0==K_IN) ) {
						alt123=2;
					}

					else {
						NoViableAltException nvae =
							new NoViableAltException("", 123, 0, input);
						throw nvae;
					}

					switch (alt123) {
						case 1 :
							// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1019:15: op= relationType t= term
							{
							pushFollow(FOLLOW_relationType_in_columnCondition6596);
							op=relationType();
							state._fsp--;

							pushFollow(FOLLOW_term_in_columnCondition6600);
							t=term();
							state._fsp--;

							 conditions.add(Pair.create(key, ColumnCondition.Raw.collectionCondition(t, element, op))); 
							}
							break;
						case 2 :
							// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1020:15: K_IN (values= singleColumnInValues |marker= inMarker )
							{
							match(input,K_IN,FOLLOW_K_IN_in_columnCondition6618); 
							// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1021:17: (values= singleColumnInValues |marker= inMarker )
							int alt122=2;
							int LA122_0 = input.LA(1);
							if ( (LA122_0==142) ) {
								alt122=1;
							}
							else if ( (LA122_0==QMARK||LA122_0==148) ) {
								alt122=2;
							}

							else {
								NoViableAltException nvae =
									new NoViableAltException("", 122, 0, input);
								throw nvae;
							}

							switch (alt122) {
								case 1 :
									// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1021:19: values= singleColumnInValues
									{
									pushFollow(FOLLOW_singleColumnInValues_in_columnCondition6640);
									values=singleColumnInValues();
									state._fsp--;

									 conditions.add(Pair.create(key, ColumnCondition.Raw.collectionInCondition(element, values))); 
									}
									break;
								case 2 :
									// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1022:19: marker= inMarker
									{
									pushFollow(FOLLOW_inMarker_in_columnCondition6664);
									marker=inMarker();
									state._fsp--;

									 conditions.add(Pair.create(key, ColumnCondition.Raw.collectionInCondition(element, marker))); 
									}
									break;

							}

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
	// $ANTLR end "columnCondition"



	// $ANTLR start "properties"
	// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1028:1: properties[PropertyDefinitions props] : property[props] ( K_AND property[props] )* ;
	public final void properties(PropertyDefinitions props) throws RecognitionException {
		try {
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1029:5: ( property[props] ( K_AND property[props] )* )
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1029:7: property[props] ( K_AND property[props] )*
			{
			pushFollow(FOLLOW_property_in_properties6726);
			property(props);
			state._fsp--;

			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1029:23: ( K_AND property[props] )*
			loop125:
			while (true) {
				int alt125=2;
				int LA125_0 = input.LA(1);
				if ( (LA125_0==K_AND) ) {
					alt125=1;
				}

				switch (alt125) {
				case 1 :
					// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1029:24: K_AND property[props]
					{
					match(input,K_AND,FOLLOW_K_AND_in_properties6730); 
					pushFollow(FOLLOW_property_in_properties6732);
					property(props);
					state._fsp--;

					}
					break;

				default :
					break loop125;
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
	// $ANTLR end "properties"



	// $ANTLR start "property"
	// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1032:1: property[PropertyDefinitions props] : k= ident '=' (simple= propertyValue |map= mapLiteral ) ;
	public final void property(PropertyDefinitions props) throws RecognitionException {
		ColumnIdentifier k =null;
		String simple =null;
		Maps.Literal map =null;

		try {
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1033:5: (k= ident '=' (simple= propertyValue |map= mapLiteral ) )
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1033:7: k= ident '=' (simple= propertyValue |map= mapLiteral )
			{
			pushFollow(FOLLOW_ident_in_property6755);
			k=ident();
			state._fsp--;

			match(input,152,FOLLOW_152_in_property6757); 
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1033:19: (simple= propertyValue |map= mapLiteral )
			int alt126=2;
			int LA126_0 = input.LA(1);
			if ( (LA126_0==BOOLEAN||LA126_0==FLOAT||LA126_0==HEXNUMBER||LA126_0==INTEGER||LA126_0==K_ALL||LA126_0==K_AS||LA126_0==K_ASCII||(LA126_0 >= K_BIGINT && LA126_0 <= K_BOOLEAN)||LA126_0==K_CLUSTERING||(LA126_0 >= K_COMPACT && LA126_0 <= K_COUNTER)||(LA126_0 >= K_CUSTOM && LA126_0 <= K_DECIMAL)||(LA126_0 >= K_DISTINCT && LA126_0 <= K_DOUBLE)||(LA126_0 >= K_EXISTS && LA126_0 <= K_FLOAT)||LA126_0==K_FROZEN||(LA126_0 >= K_INET && LA126_0 <= K_INFINITY)||LA126_0==K_INT||(LA126_0 >= K_KEY && LA126_0 <= K_KEYS)||LA126_0==K_KEYSPACES||(LA126_0 >= K_LIST && LA126_0 <= K_MAP)||LA126_0==K_NAN||LA126_0==K_NOSUPERUSER||(LA126_0 >= K_PASSWORD && LA126_0 <= K_PERMISSIONS)||(LA126_0 >= K_STATIC && LA126_0 <= K_TIMEUUID)||LA126_0==K_TRIGGER||(LA126_0 >= K_TTL && LA126_0 <= K_TYPE)||(LA126_0 >= K_USER && LA126_0 <= K_USERS)||(LA126_0 >= K_UUID && LA126_0 <= K_VARINT)||LA126_0==K_WRITETIME||LA126_0==STRING_LITERAL||LA126_0==UUID||LA126_0==146) ) {
				alt126=1;
			}
			else if ( (LA126_0==158) ) {
				alt126=2;
			}

			else {
				NoViableAltException nvae =
					new NoViableAltException("", 126, 0, input);
				throw nvae;
			}

			switch (alt126) {
				case 1 :
					// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1033:20: simple= propertyValue
					{
					pushFollow(FOLLOW_propertyValue_in_property6762);
					simple=propertyValue();
					state._fsp--;

					 try { props.addProperty(k.toString(), simple); } catch (SyntaxException e) { addRecognitionError(e.getMessage()); } 
					}
					break;
				case 2 :
					// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1034:23: map= mapLiteral
					{
					pushFollow(FOLLOW_mapLiteral_in_property6790);
					map=mapLiteral();
					state._fsp--;

					 try { props.addProperty(k.toString(), convertPropertyMap(map)); } catch (SyntaxException e) { addRecognitionError(e.getMessage()); } 
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
	// $ANTLR end "property"



	// $ANTLR start "propertyValue"
	// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1037:1: propertyValue returns [String str] : (c= constant |u= unreserved_keyword );
	public final String propertyValue() throws RecognitionException {
		String str = null;


		Constants.Literal c =null;
		String u =null;

		try {
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1038:5: (c= constant |u= unreserved_keyword )
			int alt127=2;
			int LA127_0 = input.LA(1);
			if ( (LA127_0==BOOLEAN||LA127_0==FLOAT||LA127_0==HEXNUMBER||LA127_0==INTEGER||LA127_0==K_INFINITY||LA127_0==K_NAN||LA127_0==STRING_LITERAL||LA127_0==UUID||LA127_0==146) ) {
				alt127=1;
			}
			else if ( (LA127_0==K_ALL||LA127_0==K_AS||LA127_0==K_ASCII||(LA127_0 >= K_BIGINT && LA127_0 <= K_BOOLEAN)||LA127_0==K_CLUSTERING||(LA127_0 >= K_COMPACT && LA127_0 <= K_COUNTER)||(LA127_0 >= K_CUSTOM && LA127_0 <= K_DECIMAL)||(LA127_0 >= K_DISTINCT && LA127_0 <= K_DOUBLE)||(LA127_0 >= K_EXISTS && LA127_0 <= K_FLOAT)||LA127_0==K_FROZEN||LA127_0==K_INET||LA127_0==K_INT||(LA127_0 >= K_KEY && LA127_0 <= K_KEYS)||LA127_0==K_KEYSPACES||(LA127_0 >= K_LIST && LA127_0 <= K_MAP)||LA127_0==K_NOSUPERUSER||(LA127_0 >= K_PASSWORD && LA127_0 <= K_PERMISSIONS)||(LA127_0 >= K_STATIC && LA127_0 <= K_TIMEUUID)||LA127_0==K_TRIGGER||(LA127_0 >= K_TTL && LA127_0 <= K_TYPE)||(LA127_0 >= K_USER && LA127_0 <= K_USERS)||(LA127_0 >= K_UUID && LA127_0 <= K_VARINT)||LA127_0==K_WRITETIME) ) {
				alt127=2;
			}

			else {
				NoViableAltException nvae =
					new NoViableAltException("", 127, 0, input);
				throw nvae;
			}

			switch (alt127) {
				case 1 :
					// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1038:7: c= constant
					{
					pushFollow(FOLLOW_constant_in_propertyValue6819);
					c=constant();
					state._fsp--;

					 str = c.getRawText(); 
					}
					break;
				case 2 :
					// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1039:7: u= unreserved_keyword
					{
					pushFollow(FOLLOW_unreserved_keyword_in_propertyValue6841);
					u=unreserved_keyword();
					state._fsp--;

					 str = u; 
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
		return str;
	}
	// $ANTLR end "propertyValue"



	// $ANTLR start "relationType"
	// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1042:1: relationType returns [Operator op] : ( '=' | '<' | '<=' | '>' | '>=' | '!=' );
	public final Operator relationType() throws RecognitionException {
		Operator op = null;


		try {
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1043:5: ( '=' | '<' | '<=' | '>' | '>=' | '!=' )
			int alt128=6;
			switch ( input.LA(1) ) {
			case 152:
				{
				alt128=1;
				}
				break;
			case 150:
				{
				alt128=2;
				}
				break;
			case 151:
				{
				alt128=3;
				}
				break;
			case 153:
				{
				alt128=4;
				}
				break;
			case 154:
				{
				alt128=5;
				}
				break;
			case 141:
				{
				alt128=6;
				}
				break;
			default:
				NoViableAltException nvae =
					new NoViableAltException("", 128, 0, input);
				throw nvae;
			}
			switch (alt128) {
				case 1 :
					// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1043:7: '='
					{
					match(input,152,FOLLOW_152_in_relationType6864); 
					 op = Operator.EQ; 
					}
					break;
				case 2 :
					// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1044:7: '<'
					{
					match(input,150,FOLLOW_150_in_relationType6875); 
					 op = Operator.LT; 
					}
					break;
				case 3 :
					// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1045:7: '<='
					{
					match(input,151,FOLLOW_151_in_relationType6886); 
					 op = Operator.LTE; 
					}
					break;
				case 4 :
					// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1046:7: '>'
					{
					match(input,153,FOLLOW_153_in_relationType6896); 
					 op = Operator.GT; 
					}
					break;
				case 5 :
					// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1047:7: '>='
					{
					match(input,154,FOLLOW_154_in_relationType6907); 
					 op = Operator.GTE; 
					}
					break;
				case 6 :
					// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1048:7: '!='
					{
					match(input,141,FOLLOW_141_in_relationType6917); 
					 op = Operator.NEQ; 
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
		return op;
	}
	// $ANTLR end "relationType"



	// $ANTLR start "relation"
	// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1051:1: relation[List<Relation> clauses] : (name= cident type= relationType t= term | K_TOKEN l= tupleOfIdentifiers type= relationType t= term |name= cident K_IN marker= inMarker |name= cident K_IN inValues= singleColumnInValues |name= cident K_CONTAINS ( K_KEY )? t= term |ids= tupleOfIdentifiers ( K_IN ( '(' ')' |tupleInMarker= inMarkerForTuple |literals= tupleOfTupleLiterals |markers= tupleOfMarkersForTuples ) |type= relationType literal= tupleLiteral |type= relationType tupleMarker= markerForTuple ) | '(' relation[$clauses] ')' );
	public final void relation(List<Relation> clauses) throws RecognitionException {
		ColumnIdentifier.Raw name =null;
		Operator type =null;
		Term.Raw t =null;
		List<ColumnIdentifier.Raw> l =null;
		AbstractMarker.INRaw marker =null;
		List<Term.Raw> inValues =null;
		List<ColumnIdentifier.Raw> ids =null;
		Tuples.INRaw tupleInMarker =null;
		List<Tuples.Literal> literals =null;
		List<Tuples.Raw> markers =null;
		Tuples.Literal literal =null;
		Tuples.Raw tupleMarker =null;

		try {
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1052:5: (name= cident type= relationType t= term | K_TOKEN l= tupleOfIdentifiers type= relationType t= term |name= cident K_IN marker= inMarker |name= cident K_IN inValues= singleColumnInValues |name= cident K_CONTAINS ( K_KEY )? t= term |ids= tupleOfIdentifiers ( K_IN ( '(' ')' |tupleInMarker= inMarkerForTuple |literals= tupleOfTupleLiterals |markers= tupleOfMarkersForTuples ) |type= relationType literal= tupleLiteral |type= relationType tupleMarker= markerForTuple ) | '(' relation[$clauses] ')' )
			int alt132=7;
			switch ( input.LA(1) ) {
			case IDENT:
				{
				switch ( input.LA(2) ) {
				case 141:
				case 150:
				case 151:
				case 152:
				case 153:
				case 154:
					{
					alt132=1;
					}
					break;
				case K_IN:
					{
					int LA132_24 = input.LA(3);
					if ( (LA132_24==QMARK||LA132_24==148) ) {
						alt132=3;
					}
					else if ( (LA132_24==142) ) {
						alt132=4;
					}

					else {
						int nvaeMark = input.mark();
						try {
							for (int nvaeConsume = 0; nvaeConsume < 3 - 1; nvaeConsume++) {
								input.consume();
							}
							NoViableAltException nvae =
								new NoViableAltException("", 132, 24, input);
							throw nvae;
						} finally {
							input.rewind(nvaeMark);
						}
					}

					}
					break;
				case K_CONTAINS:
					{
					alt132=5;
					}
					break;
				default:
					int nvaeMark = input.mark();
					try {
						input.consume();
						NoViableAltException nvae =
							new NoViableAltException("", 132, 1, input);
						throw nvae;
					} finally {
						input.rewind(nvaeMark);
					}
				}
				}
				break;
			case QUOTED_NAME:
				{
				switch ( input.LA(2) ) {
				case 141:
				case 150:
				case 151:
				case 152:
				case 153:
				case 154:
					{
					alt132=1;
					}
					break;
				case K_IN:
					{
					int LA132_24 = input.LA(3);
					if ( (LA132_24==QMARK||LA132_24==148) ) {
						alt132=3;
					}
					else if ( (LA132_24==142) ) {
						alt132=4;
					}

					else {
						int nvaeMark = input.mark();
						try {
							for (int nvaeConsume = 0; nvaeConsume < 3 - 1; nvaeConsume++) {
								input.consume();
							}
							NoViableAltException nvae =
								new NoViableAltException("", 132, 24, input);
							throw nvae;
						} finally {
							input.rewind(nvaeMark);
						}
					}

					}
					break;
				case K_CONTAINS:
					{
					alt132=5;
					}
					break;
				default:
					int nvaeMark = input.mark();
					try {
						input.consume();
						NoViableAltException nvae =
							new NoViableAltException("", 132, 2, input);
						throw nvae;
					} finally {
						input.rewind(nvaeMark);
					}
				}
				}
				break;
			case K_ALL:
			case K_AS:
			case K_CLUSTERING:
			case K_COMPACT:
			case K_CONTAINS:
			case K_CUSTOM:
			case K_DISTINCT:
			case K_EXISTS:
			case K_FILTERING:
			case K_FROZEN:
			case K_KEYS:
			case K_KEYSPACES:
			case K_LIST:
			case K_MAP:
			case K_NOSUPERUSER:
			case K_PASSWORD:
			case K_PERMISSION:
			case K_PERMISSIONS:
			case K_STATIC:
			case K_STORAGE:
			case K_SUPERUSER:
			case K_TRIGGER:
			case K_TUPLE:
			case K_TYPE:
			case K_USER:
			case K_USERS:
			case K_VALUES:
				{
				switch ( input.LA(2) ) {
				case 141:
				case 150:
				case 151:
				case 152:
				case 153:
				case 154:
					{
					alt132=1;
					}
					break;
				case K_IN:
					{
					int LA132_24 = input.LA(3);
					if ( (LA132_24==QMARK||LA132_24==148) ) {
						alt132=3;
					}
					else if ( (LA132_24==142) ) {
						alt132=4;
					}

					else {
						int nvaeMark = input.mark();
						try {
							for (int nvaeConsume = 0; nvaeConsume < 3 - 1; nvaeConsume++) {
								input.consume();
							}
							NoViableAltException nvae =
								new NoViableAltException("", 132, 24, input);
							throw nvae;
						} finally {
							input.rewind(nvaeMark);
						}
					}

					}
					break;
				case K_CONTAINS:
					{
					alt132=5;
					}
					break;
				default:
					int nvaeMark = input.mark();
					try {
						input.consume();
						NoViableAltException nvae =
							new NoViableAltException("", 132, 3, input);
						throw nvae;
					} finally {
						input.rewind(nvaeMark);
					}
				}
				}
				break;
			case K_ASCII:
				{
				switch ( input.LA(2) ) {
				case 141:
				case 150:
				case 151:
				case 152:
				case 153:
				case 154:
					{
					alt132=1;
					}
					break;
				case K_IN:
					{
					int LA132_24 = input.LA(3);
					if ( (LA132_24==QMARK||LA132_24==148) ) {
						alt132=3;
					}
					else if ( (LA132_24==142) ) {
						alt132=4;
					}

					else {
						int nvaeMark = input.mark();
						try {
							for (int nvaeConsume = 0; nvaeConsume < 3 - 1; nvaeConsume++) {
								input.consume();
							}
							NoViableAltException nvae =
								new NoViableAltException("", 132, 24, input);
							throw nvae;
						} finally {
							input.rewind(nvaeMark);
						}
					}

					}
					break;
				case K_CONTAINS:
					{
					alt132=5;
					}
					break;
				default:
					int nvaeMark = input.mark();
					try {
						input.consume();
						NoViableAltException nvae =
							new NoViableAltException("", 132, 4, input);
						throw nvae;
					} finally {
						input.rewind(nvaeMark);
					}
				}
				}
				break;
			case K_BIGINT:
				{
				switch ( input.LA(2) ) {
				case 141:
				case 150:
				case 151:
				case 152:
				case 153:
				case 154:
					{
					alt132=1;
					}
					break;
				case K_IN:
					{
					int LA132_24 = input.LA(3);
					if ( (LA132_24==QMARK||LA132_24==148) ) {
						alt132=3;
					}
					else if ( (LA132_24==142) ) {
						alt132=4;
					}

					else {
						int nvaeMark = input.mark();
						try {
							for (int nvaeConsume = 0; nvaeConsume < 3 - 1; nvaeConsume++) {
								input.consume();
							}
							NoViableAltException nvae =
								new NoViableAltException("", 132, 24, input);
							throw nvae;
						} finally {
							input.rewind(nvaeMark);
						}
					}

					}
					break;
				case K_CONTAINS:
					{
					alt132=5;
					}
					break;
				default:
					int nvaeMark = input.mark();
					try {
						input.consume();
						NoViableAltException nvae =
							new NoViableAltException("", 132, 5, input);
						throw nvae;
					} finally {
						input.rewind(nvaeMark);
					}
				}
				}
				break;
			case K_BLOB:
				{
				switch ( input.LA(2) ) {
				case 141:
				case 150:
				case 151:
				case 152:
				case 153:
				case 154:
					{
					alt132=1;
					}
					break;
				case K_IN:
					{
					int LA132_24 = input.LA(3);
					if ( (LA132_24==QMARK||LA132_24==148) ) {
						alt132=3;
					}
					else if ( (LA132_24==142) ) {
						alt132=4;
					}

					else {
						int nvaeMark = input.mark();
						try {
							for (int nvaeConsume = 0; nvaeConsume < 3 - 1; nvaeConsume++) {
								input.consume();
							}
							NoViableAltException nvae =
								new NoViableAltException("", 132, 24, input);
							throw nvae;
						} finally {
							input.rewind(nvaeMark);
						}
					}

					}
					break;
				case K_CONTAINS:
					{
					alt132=5;
					}
					break;
				default:
					int nvaeMark = input.mark();
					try {
						input.consume();
						NoViableAltException nvae =
							new NoViableAltException("", 132, 6, input);
						throw nvae;
					} finally {
						input.rewind(nvaeMark);
					}
				}
				}
				break;
			case K_BOOLEAN:
				{
				switch ( input.LA(2) ) {
				case 141:
				case 150:
				case 151:
				case 152:
				case 153:
				case 154:
					{
					alt132=1;
					}
					break;
				case K_IN:
					{
					int LA132_24 = input.LA(3);
					if ( (LA132_24==QMARK||LA132_24==148) ) {
						alt132=3;
					}
					else if ( (LA132_24==142) ) {
						alt132=4;
					}

					else {
						int nvaeMark = input.mark();
						try {
							for (int nvaeConsume = 0; nvaeConsume < 3 - 1; nvaeConsume++) {
								input.consume();
							}
							NoViableAltException nvae =
								new NoViableAltException("", 132, 24, input);
							throw nvae;
						} finally {
							input.rewind(nvaeMark);
						}
					}

					}
					break;
				case K_CONTAINS:
					{
					alt132=5;
					}
					break;
				default:
					int nvaeMark = input.mark();
					try {
						input.consume();
						NoViableAltException nvae =
							new NoViableAltException("", 132, 7, input);
						throw nvae;
					} finally {
						input.rewind(nvaeMark);
					}
				}
				}
				break;
			case K_COUNTER:
				{
				switch ( input.LA(2) ) {
				case 141:
				case 150:
				case 151:
				case 152:
				case 153:
				case 154:
					{
					alt132=1;
					}
					break;
				case K_IN:
					{
					int LA132_24 = input.LA(3);
					if ( (LA132_24==QMARK||LA132_24==148) ) {
						alt132=3;
					}
					else if ( (LA132_24==142) ) {
						alt132=4;
					}

					else {
						int nvaeMark = input.mark();
						try {
							for (int nvaeConsume = 0; nvaeConsume < 3 - 1; nvaeConsume++) {
								input.consume();
							}
							NoViableAltException nvae =
								new NoViableAltException("", 132, 24, input);
							throw nvae;
						} finally {
							input.rewind(nvaeMark);
						}
					}

					}
					break;
				case K_CONTAINS:
					{
					alt132=5;
					}
					break;
				default:
					int nvaeMark = input.mark();
					try {
						input.consume();
						NoViableAltException nvae =
							new NoViableAltException("", 132, 8, input);
						throw nvae;
					} finally {
						input.rewind(nvaeMark);
					}
				}
				}
				break;
			case K_DECIMAL:
				{
				switch ( input.LA(2) ) {
				case 141:
				case 150:
				case 151:
				case 152:
				case 153:
				case 154:
					{
					alt132=1;
					}
					break;
				case K_IN:
					{
					int LA132_24 = input.LA(3);
					if ( (LA132_24==QMARK||LA132_24==148) ) {
						alt132=3;
					}
					else if ( (LA132_24==142) ) {
						alt132=4;
					}

					else {
						int nvaeMark = input.mark();
						try {
							for (int nvaeConsume = 0; nvaeConsume < 3 - 1; nvaeConsume++) {
								input.consume();
							}
							NoViableAltException nvae =
								new NoViableAltException("", 132, 24, input);
							throw nvae;
						} finally {
							input.rewind(nvaeMark);
						}
					}

					}
					break;
				case K_CONTAINS:
					{
					alt132=5;
					}
					break;
				default:
					int nvaeMark = input.mark();
					try {
						input.consume();
						NoViableAltException nvae =
							new NoViableAltException("", 132, 9, input);
						throw nvae;
					} finally {
						input.rewind(nvaeMark);
					}
				}
				}
				break;
			case K_DOUBLE:
				{
				switch ( input.LA(2) ) {
				case 141:
				case 150:
				case 151:
				case 152:
				case 153:
				case 154:
					{
					alt132=1;
					}
					break;
				case K_IN:
					{
					int LA132_24 = input.LA(3);
					if ( (LA132_24==QMARK||LA132_24==148) ) {
						alt132=3;
					}
					else if ( (LA132_24==142) ) {
						alt132=4;
					}

					else {
						int nvaeMark = input.mark();
						try {
							for (int nvaeConsume = 0; nvaeConsume < 3 - 1; nvaeConsume++) {
								input.consume();
							}
							NoViableAltException nvae =
								new NoViableAltException("", 132, 24, input);
							throw nvae;
						} finally {
							input.rewind(nvaeMark);
						}
					}

					}
					break;
				case K_CONTAINS:
					{
					alt132=5;
					}
					break;
				default:
					int nvaeMark = input.mark();
					try {
						input.consume();
						NoViableAltException nvae =
							new NoViableAltException("", 132, 10, input);
						throw nvae;
					} finally {
						input.rewind(nvaeMark);
					}
				}
				}
				break;
			case K_FLOAT:
				{
				switch ( input.LA(2) ) {
				case 141:
				case 150:
				case 151:
				case 152:
				case 153:
				case 154:
					{
					alt132=1;
					}
					break;
				case K_IN:
					{
					int LA132_24 = input.LA(3);
					if ( (LA132_24==QMARK||LA132_24==148) ) {
						alt132=3;
					}
					else if ( (LA132_24==142) ) {
						alt132=4;
					}

					else {
						int nvaeMark = input.mark();
						try {
							for (int nvaeConsume = 0; nvaeConsume < 3 - 1; nvaeConsume++) {
								input.consume();
							}
							NoViableAltException nvae =
								new NoViableAltException("", 132, 24, input);
							throw nvae;
						} finally {
							input.rewind(nvaeMark);
						}
					}

					}
					break;
				case K_CONTAINS:
					{
					alt132=5;
					}
					break;
				default:
					int nvaeMark = input.mark();
					try {
						input.consume();
						NoViableAltException nvae =
							new NoViableAltException("", 132, 11, input);
						throw nvae;
					} finally {
						input.rewind(nvaeMark);
					}
				}
				}
				break;
			case K_INET:
				{
				switch ( input.LA(2) ) {
				case 141:
				case 150:
				case 151:
				case 152:
				case 153:
				case 154:
					{
					alt132=1;
					}
					break;
				case K_IN:
					{
					int LA132_24 = input.LA(3);
					if ( (LA132_24==QMARK||LA132_24==148) ) {
						alt132=3;
					}
					else if ( (LA132_24==142) ) {
						alt132=4;
					}

					else {
						int nvaeMark = input.mark();
						try {
							for (int nvaeConsume = 0; nvaeConsume < 3 - 1; nvaeConsume++) {
								input.consume();
							}
							NoViableAltException nvae =
								new NoViableAltException("", 132, 24, input);
							throw nvae;
						} finally {
							input.rewind(nvaeMark);
						}
					}

					}
					break;
				case K_CONTAINS:
					{
					alt132=5;
					}
					break;
				default:
					int nvaeMark = input.mark();
					try {
						input.consume();
						NoViableAltException nvae =
							new NoViableAltException("", 132, 12, input);
						throw nvae;
					} finally {
						input.rewind(nvaeMark);
					}
				}
				}
				break;
			case K_INT:
				{
				switch ( input.LA(2) ) {
				case 141:
				case 150:
				case 151:
				case 152:
				case 153:
				case 154:
					{
					alt132=1;
					}
					break;
				case K_IN:
					{
					int LA132_24 = input.LA(3);
					if ( (LA132_24==QMARK||LA132_24==148) ) {
						alt132=3;
					}
					else if ( (LA132_24==142) ) {
						alt132=4;
					}

					else {
						int nvaeMark = input.mark();
						try {
							for (int nvaeConsume = 0; nvaeConsume < 3 - 1; nvaeConsume++) {
								input.consume();
							}
							NoViableAltException nvae =
								new NoViableAltException("", 132, 24, input);
							throw nvae;
						} finally {
							input.rewind(nvaeMark);
						}
					}

					}
					break;
				case K_CONTAINS:
					{
					alt132=5;
					}
					break;
				default:
					int nvaeMark = input.mark();
					try {
						input.consume();
						NoViableAltException nvae =
							new NoViableAltException("", 132, 13, input);
						throw nvae;
					} finally {
						input.rewind(nvaeMark);
					}
				}
				}
				break;
			case K_TEXT:
				{
				switch ( input.LA(2) ) {
				case 141:
				case 150:
				case 151:
				case 152:
				case 153:
				case 154:
					{
					alt132=1;
					}
					break;
				case K_IN:
					{
					int LA132_24 = input.LA(3);
					if ( (LA132_24==QMARK||LA132_24==148) ) {
						alt132=3;
					}
					else if ( (LA132_24==142) ) {
						alt132=4;
					}

					else {
						int nvaeMark = input.mark();
						try {
							for (int nvaeConsume = 0; nvaeConsume < 3 - 1; nvaeConsume++) {
								input.consume();
							}
							NoViableAltException nvae =
								new NoViableAltException("", 132, 24, input);
							throw nvae;
						} finally {
							input.rewind(nvaeMark);
						}
					}

					}
					break;
				case K_CONTAINS:
					{
					alt132=5;
					}
					break;
				default:
					int nvaeMark = input.mark();
					try {
						input.consume();
						NoViableAltException nvae =
							new NoViableAltException("", 132, 14, input);
						throw nvae;
					} finally {
						input.rewind(nvaeMark);
					}
				}
				}
				break;
			case K_TIMESTAMP:
				{
				switch ( input.LA(2) ) {
				case 141:
				case 150:
				case 151:
				case 152:
				case 153:
				case 154:
					{
					alt132=1;
					}
					break;
				case K_IN:
					{
					int LA132_24 = input.LA(3);
					if ( (LA132_24==QMARK||LA132_24==148) ) {
						alt132=3;
					}
					else if ( (LA132_24==142) ) {
						alt132=4;
					}

					else {
						int nvaeMark = input.mark();
						try {
							for (int nvaeConsume = 0; nvaeConsume < 3 - 1; nvaeConsume++) {
								input.consume();
							}
							NoViableAltException nvae =
								new NoViableAltException("", 132, 24, input);
							throw nvae;
						} finally {
							input.rewind(nvaeMark);
						}
					}

					}
					break;
				case K_CONTAINS:
					{
					alt132=5;
					}
					break;
				default:
					int nvaeMark = input.mark();
					try {
						input.consume();
						NoViableAltException nvae =
							new NoViableAltException("", 132, 15, input);
						throw nvae;
					} finally {
						input.rewind(nvaeMark);
					}
				}
				}
				break;
			case K_UUID:
				{
				switch ( input.LA(2) ) {
				case 141:
				case 150:
				case 151:
				case 152:
				case 153:
				case 154:
					{
					alt132=1;
					}
					break;
				case K_IN:
					{
					int LA132_24 = input.LA(3);
					if ( (LA132_24==QMARK||LA132_24==148) ) {
						alt132=3;
					}
					else if ( (LA132_24==142) ) {
						alt132=4;
					}

					else {
						int nvaeMark = input.mark();
						try {
							for (int nvaeConsume = 0; nvaeConsume < 3 - 1; nvaeConsume++) {
								input.consume();
							}
							NoViableAltException nvae =
								new NoViableAltException("", 132, 24, input);
							throw nvae;
						} finally {
							input.rewind(nvaeMark);
						}
					}

					}
					break;
				case K_CONTAINS:
					{
					alt132=5;
					}
					break;
				default:
					int nvaeMark = input.mark();
					try {
						input.consume();
						NoViableAltException nvae =
							new NoViableAltException("", 132, 16, input);
						throw nvae;
					} finally {
						input.rewind(nvaeMark);
					}
				}
				}
				break;
			case K_VARCHAR:
				{
				switch ( input.LA(2) ) {
				case 141:
				case 150:
				case 151:
				case 152:
				case 153:
				case 154:
					{
					alt132=1;
					}
					break;
				case K_IN:
					{
					int LA132_24 = input.LA(3);
					if ( (LA132_24==QMARK||LA132_24==148) ) {
						alt132=3;
					}
					else if ( (LA132_24==142) ) {
						alt132=4;
					}

					else {
						int nvaeMark = input.mark();
						try {
							for (int nvaeConsume = 0; nvaeConsume < 3 - 1; nvaeConsume++) {
								input.consume();
							}
							NoViableAltException nvae =
								new NoViableAltException("", 132, 24, input);
							throw nvae;
						} finally {
							input.rewind(nvaeMark);
						}
					}

					}
					break;
				case K_CONTAINS:
					{
					alt132=5;
					}
					break;
				default:
					int nvaeMark = input.mark();
					try {
						input.consume();
						NoViableAltException nvae =
							new NoViableAltException("", 132, 17, input);
						throw nvae;
					} finally {
						input.rewind(nvaeMark);
					}
				}
				}
				break;
			case K_VARINT:
				{
				switch ( input.LA(2) ) {
				case 141:
				case 150:
				case 151:
				case 152:
				case 153:
				case 154:
					{
					alt132=1;
					}
					break;
				case K_IN:
					{
					int LA132_24 = input.LA(3);
					if ( (LA132_24==QMARK||LA132_24==148) ) {
						alt132=3;
					}
					else if ( (LA132_24==142) ) {
						alt132=4;
					}

					else {
						int nvaeMark = input.mark();
						try {
							for (int nvaeConsume = 0; nvaeConsume < 3 - 1; nvaeConsume++) {
								input.consume();
							}
							NoViableAltException nvae =
								new NoViableAltException("", 132, 24, input);
							throw nvae;
						} finally {
							input.rewind(nvaeMark);
						}
					}

					}
					break;
				case K_CONTAINS:
					{
					alt132=5;
					}
					break;
				default:
					int nvaeMark = input.mark();
					try {
						input.consume();
						NoViableAltException nvae =
							new NoViableAltException("", 132, 18, input);
						throw nvae;
					} finally {
						input.rewind(nvaeMark);
					}
				}
				}
				break;
			case K_TIMEUUID:
				{
				switch ( input.LA(2) ) {
				case 141:
				case 150:
				case 151:
				case 152:
				case 153:
				case 154:
					{
					alt132=1;
					}
					break;
				case K_IN:
					{
					int LA132_24 = input.LA(3);
					if ( (LA132_24==QMARK||LA132_24==148) ) {
						alt132=3;
					}
					else if ( (LA132_24==142) ) {
						alt132=4;
					}

					else {
						int nvaeMark = input.mark();
						try {
							for (int nvaeConsume = 0; nvaeConsume < 3 - 1; nvaeConsume++) {
								input.consume();
							}
							NoViableAltException nvae =
								new NoViableAltException("", 132, 24, input);
							throw nvae;
						} finally {
							input.rewind(nvaeMark);
						}
					}

					}
					break;
				case K_CONTAINS:
					{
					alt132=5;
					}
					break;
				default:
					int nvaeMark = input.mark();
					try {
						input.consume();
						NoViableAltException nvae =
							new NoViableAltException("", 132, 19, input);
						throw nvae;
					} finally {
						input.rewind(nvaeMark);
					}
				}
				}
				break;
			case K_COUNT:
			case K_KEY:
			case K_TTL:
			case K_WRITETIME:
				{
				switch ( input.LA(2) ) {
				case 141:
				case 150:
				case 151:
				case 152:
				case 153:
				case 154:
					{
					alt132=1;
					}
					break;
				case K_IN:
					{
					int LA132_24 = input.LA(3);
					if ( (LA132_24==QMARK||LA132_24==148) ) {
						alt132=3;
					}
					else if ( (LA132_24==142) ) {
						alt132=4;
					}

					else {
						int nvaeMark = input.mark();
						try {
							for (int nvaeConsume = 0; nvaeConsume < 3 - 1; nvaeConsume++) {
								input.consume();
							}
							NoViableAltException nvae =
								new NoViableAltException("", 132, 24, input);
							throw nvae;
						} finally {
							input.rewind(nvaeMark);
						}
					}

					}
					break;
				case K_CONTAINS:
					{
					alt132=5;
					}
					break;
				default:
					int nvaeMark = input.mark();
					try {
						input.consume();
						NoViableAltException nvae =
							new NoViableAltException("", 132, 20, input);
						throw nvae;
					} finally {
						input.rewind(nvaeMark);
					}
				}
				}
				break;
			case K_TOKEN:
				{
				alt132=2;
				}
				break;
			case 142:
				{
				switch ( input.LA(2) ) {
				case IDENT:
					{
					int LA132_26 = input.LA(3);
					if ( (LA132_26==143||LA132_26==145) ) {
						alt132=6;
					}
					else if ( (LA132_26==K_CONTAINS||LA132_26==K_IN||LA132_26==141||(LA132_26 >= 150 && LA132_26 <= 154)) ) {
						alt132=7;
					}

					else {
						int nvaeMark = input.mark();
						try {
							for (int nvaeConsume = 0; nvaeConsume < 3 - 1; nvaeConsume++) {
								input.consume();
							}
							NoViableAltException nvae =
								new NoViableAltException("", 132, 26, input);
							throw nvae;
						} finally {
							input.rewind(nvaeMark);
						}
					}

					}
					break;
				case QUOTED_NAME:
					{
					int LA132_27 = input.LA(3);
					if ( (LA132_27==143||LA132_27==145) ) {
						alt132=6;
					}
					else if ( (LA132_27==K_CONTAINS||LA132_27==K_IN||LA132_27==141||(LA132_27 >= 150 && LA132_27 <= 154)) ) {
						alt132=7;
					}

					else {
						int nvaeMark = input.mark();
						try {
							for (int nvaeConsume = 0; nvaeConsume < 3 - 1; nvaeConsume++) {
								input.consume();
							}
							NoViableAltException nvae =
								new NoViableAltException("", 132, 27, input);
							throw nvae;
						} finally {
							input.rewind(nvaeMark);
						}
					}

					}
					break;
				case K_ALL:
				case K_AS:
				case K_CLUSTERING:
				case K_COMPACT:
				case K_CONTAINS:
				case K_CUSTOM:
				case K_DISTINCT:
				case K_EXISTS:
				case K_FILTERING:
				case K_FROZEN:
				case K_KEYS:
				case K_KEYSPACES:
				case K_LIST:
				case K_MAP:
				case K_NOSUPERUSER:
				case K_PASSWORD:
				case K_PERMISSION:
				case K_PERMISSIONS:
				case K_STATIC:
				case K_STORAGE:
				case K_SUPERUSER:
				case K_TRIGGER:
				case K_TUPLE:
				case K_TYPE:
				case K_USER:
				case K_USERS:
				case K_VALUES:
					{
					int LA132_28 = input.LA(3);
					if ( (LA132_28==143||LA132_28==145) ) {
						alt132=6;
					}
					else if ( (LA132_28==K_CONTAINS||LA132_28==K_IN||LA132_28==141||(LA132_28 >= 150 && LA132_28 <= 154)) ) {
						alt132=7;
					}

					else {
						int nvaeMark = input.mark();
						try {
							for (int nvaeConsume = 0; nvaeConsume < 3 - 1; nvaeConsume++) {
								input.consume();
							}
							NoViableAltException nvae =
								new NoViableAltException("", 132, 28, input);
							throw nvae;
						} finally {
							input.rewind(nvaeMark);
						}
					}

					}
					break;
				case K_ASCII:
					{
					int LA132_29 = input.LA(3);
					if ( (LA132_29==143||LA132_29==145) ) {
						alt132=6;
					}
					else if ( (LA132_29==K_CONTAINS||LA132_29==K_IN||LA132_29==141||(LA132_29 >= 150 && LA132_29 <= 154)) ) {
						alt132=7;
					}

					else {
						int nvaeMark = input.mark();
						try {
							for (int nvaeConsume = 0; nvaeConsume < 3 - 1; nvaeConsume++) {
								input.consume();
							}
							NoViableAltException nvae =
								new NoViableAltException("", 132, 29, input);
							throw nvae;
						} finally {
							input.rewind(nvaeMark);
						}
					}

					}
					break;
				case K_BIGINT:
					{
					int LA132_30 = input.LA(3);
					if ( (LA132_30==143||LA132_30==145) ) {
						alt132=6;
					}
					else if ( (LA132_30==K_CONTAINS||LA132_30==K_IN||LA132_30==141||(LA132_30 >= 150 && LA132_30 <= 154)) ) {
						alt132=7;
					}

					else {
						int nvaeMark = input.mark();
						try {
							for (int nvaeConsume = 0; nvaeConsume < 3 - 1; nvaeConsume++) {
								input.consume();
							}
							NoViableAltException nvae =
								new NoViableAltException("", 132, 30, input);
							throw nvae;
						} finally {
							input.rewind(nvaeMark);
						}
					}

					}
					break;
				case K_BLOB:
					{
					int LA132_31 = input.LA(3);
					if ( (LA132_31==143||LA132_31==145) ) {
						alt132=6;
					}
					else if ( (LA132_31==K_CONTAINS||LA132_31==K_IN||LA132_31==141||(LA132_31 >= 150 && LA132_31 <= 154)) ) {
						alt132=7;
					}

					else {
						int nvaeMark = input.mark();
						try {
							for (int nvaeConsume = 0; nvaeConsume < 3 - 1; nvaeConsume++) {
								input.consume();
							}
							NoViableAltException nvae =
								new NoViableAltException("", 132, 31, input);
							throw nvae;
						} finally {
							input.rewind(nvaeMark);
						}
					}

					}
					break;
				case K_BOOLEAN:
					{
					int LA132_32 = input.LA(3);
					if ( (LA132_32==143||LA132_32==145) ) {
						alt132=6;
					}
					else if ( (LA132_32==K_CONTAINS||LA132_32==K_IN||LA132_32==141||(LA132_32 >= 150 && LA132_32 <= 154)) ) {
						alt132=7;
					}

					else {
						int nvaeMark = input.mark();
						try {
							for (int nvaeConsume = 0; nvaeConsume < 3 - 1; nvaeConsume++) {
								input.consume();
							}
							NoViableAltException nvae =
								new NoViableAltException("", 132, 32, input);
							throw nvae;
						} finally {
							input.rewind(nvaeMark);
						}
					}

					}
					break;
				case K_COUNTER:
					{
					int LA132_33 = input.LA(3);
					if ( (LA132_33==143||LA132_33==145) ) {
						alt132=6;
					}
					else if ( (LA132_33==K_CONTAINS||LA132_33==K_IN||LA132_33==141||(LA132_33 >= 150 && LA132_33 <= 154)) ) {
						alt132=7;
					}

					else {
						int nvaeMark = input.mark();
						try {
							for (int nvaeConsume = 0; nvaeConsume < 3 - 1; nvaeConsume++) {
								input.consume();
							}
							NoViableAltException nvae =
								new NoViableAltException("", 132, 33, input);
							throw nvae;
						} finally {
							input.rewind(nvaeMark);
						}
					}

					}
					break;
				case K_DECIMAL:
					{
					int LA132_34 = input.LA(3);
					if ( (LA132_34==143||LA132_34==145) ) {
						alt132=6;
					}
					else if ( (LA132_34==K_CONTAINS||LA132_34==K_IN||LA132_34==141||(LA132_34 >= 150 && LA132_34 <= 154)) ) {
						alt132=7;
					}

					else {
						int nvaeMark = input.mark();
						try {
							for (int nvaeConsume = 0; nvaeConsume < 3 - 1; nvaeConsume++) {
								input.consume();
							}
							NoViableAltException nvae =
								new NoViableAltException("", 132, 34, input);
							throw nvae;
						} finally {
							input.rewind(nvaeMark);
						}
					}

					}
					break;
				case K_DOUBLE:
					{
					int LA132_35 = input.LA(3);
					if ( (LA132_35==143||LA132_35==145) ) {
						alt132=6;
					}
					else if ( (LA132_35==K_CONTAINS||LA132_35==K_IN||LA132_35==141||(LA132_35 >= 150 && LA132_35 <= 154)) ) {
						alt132=7;
					}

					else {
						int nvaeMark = input.mark();
						try {
							for (int nvaeConsume = 0; nvaeConsume < 3 - 1; nvaeConsume++) {
								input.consume();
							}
							NoViableAltException nvae =
								new NoViableAltException("", 132, 35, input);
							throw nvae;
						} finally {
							input.rewind(nvaeMark);
						}
					}

					}
					break;
				case K_FLOAT:
					{
					int LA132_36 = input.LA(3);
					if ( (LA132_36==143||LA132_36==145) ) {
						alt132=6;
					}
					else if ( (LA132_36==K_CONTAINS||LA132_36==K_IN||LA132_36==141||(LA132_36 >= 150 && LA132_36 <= 154)) ) {
						alt132=7;
					}

					else {
						int nvaeMark = input.mark();
						try {
							for (int nvaeConsume = 0; nvaeConsume < 3 - 1; nvaeConsume++) {
								input.consume();
							}
							NoViableAltException nvae =
								new NoViableAltException("", 132, 36, input);
							throw nvae;
						} finally {
							input.rewind(nvaeMark);
						}
					}

					}
					break;
				case K_INET:
					{
					int LA132_37 = input.LA(3);
					if ( (LA132_37==143||LA132_37==145) ) {
						alt132=6;
					}
					else if ( (LA132_37==K_CONTAINS||LA132_37==K_IN||LA132_37==141||(LA132_37 >= 150 && LA132_37 <= 154)) ) {
						alt132=7;
					}

					else {
						int nvaeMark = input.mark();
						try {
							for (int nvaeConsume = 0; nvaeConsume < 3 - 1; nvaeConsume++) {
								input.consume();
							}
							NoViableAltException nvae =
								new NoViableAltException("", 132, 37, input);
							throw nvae;
						} finally {
							input.rewind(nvaeMark);
						}
					}

					}
					break;
				case K_INT:
					{
					int LA132_38 = input.LA(3);
					if ( (LA132_38==143||LA132_38==145) ) {
						alt132=6;
					}
					else if ( (LA132_38==K_CONTAINS||LA132_38==K_IN||LA132_38==141||(LA132_38 >= 150 && LA132_38 <= 154)) ) {
						alt132=7;
					}

					else {
						int nvaeMark = input.mark();
						try {
							for (int nvaeConsume = 0; nvaeConsume < 3 - 1; nvaeConsume++) {
								input.consume();
							}
							NoViableAltException nvae =
								new NoViableAltException("", 132, 38, input);
							throw nvae;
						} finally {
							input.rewind(nvaeMark);
						}
					}

					}
					break;
				case K_TEXT:
					{
					int LA132_39 = input.LA(3);
					if ( (LA132_39==143||LA132_39==145) ) {
						alt132=6;
					}
					else if ( (LA132_39==K_CONTAINS||LA132_39==K_IN||LA132_39==141||(LA132_39 >= 150 && LA132_39 <= 154)) ) {
						alt132=7;
					}

					else {
						int nvaeMark = input.mark();
						try {
							for (int nvaeConsume = 0; nvaeConsume < 3 - 1; nvaeConsume++) {
								input.consume();
							}
							NoViableAltException nvae =
								new NoViableAltException("", 132, 39, input);
							throw nvae;
						} finally {
							input.rewind(nvaeMark);
						}
					}

					}
					break;
				case K_TIMESTAMP:
					{
					int LA132_40 = input.LA(3);
					if ( (LA132_40==143||LA132_40==145) ) {
						alt132=6;
					}
					else if ( (LA132_40==K_CONTAINS||LA132_40==K_IN||LA132_40==141||(LA132_40 >= 150 && LA132_40 <= 154)) ) {
						alt132=7;
					}

					else {
						int nvaeMark = input.mark();
						try {
							for (int nvaeConsume = 0; nvaeConsume < 3 - 1; nvaeConsume++) {
								input.consume();
							}
							NoViableAltException nvae =
								new NoViableAltException("", 132, 40, input);
							throw nvae;
						} finally {
							input.rewind(nvaeMark);
						}
					}

					}
					break;
				case K_UUID:
					{
					int LA132_41 = input.LA(3);
					if ( (LA132_41==143||LA132_41==145) ) {
						alt132=6;
					}
					else if ( (LA132_41==K_CONTAINS||LA132_41==K_IN||LA132_41==141||(LA132_41 >= 150 && LA132_41 <= 154)) ) {
						alt132=7;
					}

					else {
						int nvaeMark = input.mark();
						try {
							for (int nvaeConsume = 0; nvaeConsume < 3 - 1; nvaeConsume++) {
								input.consume();
							}
							NoViableAltException nvae =
								new NoViableAltException("", 132, 41, input);
							throw nvae;
						} finally {
							input.rewind(nvaeMark);
						}
					}

					}
					break;
				case K_VARCHAR:
					{
					int LA132_42 = input.LA(3);
					if ( (LA132_42==143||LA132_42==145) ) {
						alt132=6;
					}
					else if ( (LA132_42==K_CONTAINS||LA132_42==K_IN||LA132_42==141||(LA132_42 >= 150 && LA132_42 <= 154)) ) {
						alt132=7;
					}

					else {
						int nvaeMark = input.mark();
						try {
							for (int nvaeConsume = 0; nvaeConsume < 3 - 1; nvaeConsume++) {
								input.consume();
							}
							NoViableAltException nvae =
								new NoViableAltException("", 132, 42, input);
							throw nvae;
						} finally {
							input.rewind(nvaeMark);
						}
					}

					}
					break;
				case K_VARINT:
					{
					int LA132_43 = input.LA(3);
					if ( (LA132_43==143||LA132_43==145) ) {
						alt132=6;
					}
					else if ( (LA132_43==K_CONTAINS||LA132_43==K_IN||LA132_43==141||(LA132_43 >= 150 && LA132_43 <= 154)) ) {
						alt132=7;
					}

					else {
						int nvaeMark = input.mark();
						try {
							for (int nvaeConsume = 0; nvaeConsume < 3 - 1; nvaeConsume++) {
								input.consume();
							}
							NoViableAltException nvae =
								new NoViableAltException("", 132, 43, input);
							throw nvae;
						} finally {
							input.rewind(nvaeMark);
						}
					}

					}
					break;
				case K_TIMEUUID:
					{
					int LA132_44 = input.LA(3);
					if ( (LA132_44==143||LA132_44==145) ) {
						alt132=6;
					}
					else if ( (LA132_44==K_CONTAINS||LA132_44==K_IN||LA132_44==141||(LA132_44 >= 150 && LA132_44 <= 154)) ) {
						alt132=7;
					}

					else {
						int nvaeMark = input.mark();
						try {
							for (int nvaeConsume = 0; nvaeConsume < 3 - 1; nvaeConsume++) {
								input.consume();
							}
							NoViableAltException nvae =
								new NoViableAltException("", 132, 44, input);
							throw nvae;
						} finally {
							input.rewind(nvaeMark);
						}
					}

					}
					break;
				case K_COUNT:
				case K_KEY:
				case K_TTL:
				case K_WRITETIME:
					{
					int LA132_45 = input.LA(3);
					if ( (LA132_45==143||LA132_45==145) ) {
						alt132=6;
					}
					else if ( (LA132_45==K_CONTAINS||LA132_45==K_IN||LA132_45==141||(LA132_45 >= 150 && LA132_45 <= 154)) ) {
						alt132=7;
					}

					else {
						int nvaeMark = input.mark();
						try {
							for (int nvaeConsume = 0; nvaeConsume < 3 - 1; nvaeConsume++) {
								input.consume();
							}
							NoViableAltException nvae =
								new NoViableAltException("", 132, 45, input);
							throw nvae;
						} finally {
							input.rewind(nvaeMark);
						}
					}

					}
					break;
				case K_TOKEN:
				case 142:
					{
					alt132=7;
					}
					break;
				default:
					int nvaeMark = input.mark();
					try {
						input.consume();
						NoViableAltException nvae =
							new NoViableAltException("", 132, 22, input);
						throw nvae;
					} finally {
						input.rewind(nvaeMark);
					}
				}
				}
				break;
			default:
				NoViableAltException nvae =
					new NoViableAltException("", 132, 0, input);
				throw nvae;
			}
			switch (alt132) {
				case 1 :
					// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1052:7: name= cident type= relationType t= term
					{
					pushFollow(FOLLOW_cident_in_relation6939);
					name=cident();
					state._fsp--;

					pushFollow(FOLLOW_relationType_in_relation6943);
					type=relationType();
					state._fsp--;

					pushFollow(FOLLOW_term_in_relation6947);
					t=term();
					state._fsp--;

					 clauses.add(new SingleColumnRelation(name, type, t)); 
					}
					break;
				case 2 :
					// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1053:7: K_TOKEN l= tupleOfIdentifiers type= relationType t= term
					{
					match(input,K_TOKEN,FOLLOW_K_TOKEN_in_relation6957); 
					pushFollow(FOLLOW_tupleOfIdentifiers_in_relation6961);
					l=tupleOfIdentifiers();
					state._fsp--;

					pushFollow(FOLLOW_relationType_in_relation6965);
					type=relationType();
					state._fsp--;

					pushFollow(FOLLOW_term_in_relation6969);
					t=term();
					state._fsp--;


					            for (ColumnIdentifier.Raw id : l)
					                clauses.add(new SingleColumnRelation(id, type, t, true));
					        
					}
					break;
				case 3 :
					// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1058:7: name= cident K_IN marker= inMarker
					{
					pushFollow(FOLLOW_cident_in_relation6989);
					name=cident();
					state._fsp--;

					match(input,K_IN,FOLLOW_K_IN_in_relation6991); 
					pushFollow(FOLLOW_inMarker_in_relation6995);
					marker=inMarker();
					state._fsp--;

					 clauses.add(new SingleColumnRelation(name, Operator.IN, marker)); 
					}
					break;
				case 4 :
					// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1060:7: name= cident K_IN inValues= singleColumnInValues
					{
					pushFollow(FOLLOW_cident_in_relation7015);
					name=cident();
					state._fsp--;

					match(input,K_IN,FOLLOW_K_IN_in_relation7017); 
					pushFollow(FOLLOW_singleColumnInValues_in_relation7021);
					inValues=singleColumnInValues();
					state._fsp--;

					 clauses.add(SingleColumnRelation.createInRelation(name, inValues)); 
					}
					break;
				case 5 :
					// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1062:7: name= cident K_CONTAINS ( K_KEY )? t= term
					{
					pushFollow(FOLLOW_cident_in_relation7041);
					name=cident();
					state._fsp--;

					match(input,K_CONTAINS,FOLLOW_K_CONTAINS_in_relation7043); 
					 Operator rt = Operator.CONTAINS; 
					// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1062:67: ( K_KEY )?
					int alt129=2;
					int LA129_0 = input.LA(1);
					if ( (LA129_0==K_KEY) ) {
						alt129=1;
					}
					switch (alt129) {
						case 1 :
							// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1062:68: K_KEY
							{
							match(input,K_KEY,FOLLOW_K_KEY_in_relation7048); 
							 rt = Operator.CONTAINS_KEY; 
							}
							break;

					}

					pushFollow(FOLLOW_term_in_relation7064);
					t=term();
					state._fsp--;

					 clauses.add(new SingleColumnRelation(name, rt, t)); 
					}
					break;
				case 6 :
					// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1064:7: ids= tupleOfIdentifiers ( K_IN ( '(' ')' |tupleInMarker= inMarkerForTuple |literals= tupleOfTupleLiterals |markers= tupleOfMarkersForTuples ) |type= relationType literal= tupleLiteral |type= relationType tupleMarker= markerForTuple )
					{
					pushFollow(FOLLOW_tupleOfIdentifiers_in_relation7076);
					ids=tupleOfIdentifiers();
					state._fsp--;

					// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1065:7: ( K_IN ( '(' ')' |tupleInMarker= inMarkerForTuple |literals= tupleOfTupleLiterals |markers= tupleOfMarkersForTuples ) |type= relationType literal= tupleLiteral |type= relationType tupleMarker= markerForTuple )
					int alt131=3;
					switch ( input.LA(1) ) {
					case K_IN:
						{
						alt131=1;
						}
						break;
					case 152:
						{
						int LA131_2 = input.LA(2);
						if ( (LA131_2==142) ) {
							alt131=2;
						}
						else if ( (LA131_2==QMARK||LA131_2==148) ) {
							alt131=3;
						}

						else {
							int nvaeMark = input.mark();
							try {
								input.consume();
								NoViableAltException nvae =
									new NoViableAltException("", 131, 2, input);
								throw nvae;
							} finally {
								input.rewind(nvaeMark);
							}
						}

						}
						break;
					case 150:
						{
						int LA131_3 = input.LA(2);
						if ( (LA131_3==142) ) {
							alt131=2;
						}
						else if ( (LA131_3==QMARK||LA131_3==148) ) {
							alt131=3;
						}

						else {
							int nvaeMark = input.mark();
							try {
								input.consume();
								NoViableAltException nvae =
									new NoViableAltException("", 131, 3, input);
								throw nvae;
							} finally {
								input.rewind(nvaeMark);
							}
						}

						}
						break;
					case 151:
						{
						int LA131_4 = input.LA(2);
						if ( (LA131_4==142) ) {
							alt131=2;
						}
						else if ( (LA131_4==QMARK||LA131_4==148) ) {
							alt131=3;
						}

						else {
							int nvaeMark = input.mark();
							try {
								input.consume();
								NoViableAltException nvae =
									new NoViableAltException("", 131, 4, input);
								throw nvae;
							} finally {
								input.rewind(nvaeMark);
							}
						}

						}
						break;
					case 153:
						{
						int LA131_5 = input.LA(2);
						if ( (LA131_5==142) ) {
							alt131=2;
						}
						else if ( (LA131_5==QMARK||LA131_5==148) ) {
							alt131=3;
						}

						else {
							int nvaeMark = input.mark();
							try {
								input.consume();
								NoViableAltException nvae =
									new NoViableAltException("", 131, 5, input);
								throw nvae;
							} finally {
								input.rewind(nvaeMark);
							}
						}

						}
						break;
					case 154:
						{
						int LA131_6 = input.LA(2);
						if ( (LA131_6==142) ) {
							alt131=2;
						}
						else if ( (LA131_6==QMARK||LA131_6==148) ) {
							alt131=3;
						}

						else {
							int nvaeMark = input.mark();
							try {
								input.consume();
								NoViableAltException nvae =
									new NoViableAltException("", 131, 6, input);
								throw nvae;
							} finally {
								input.rewind(nvaeMark);
							}
						}

						}
						break;
					case 141:
						{
						int LA131_7 = input.LA(2);
						if ( (LA131_7==142) ) {
							alt131=2;
						}
						else if ( (LA131_7==QMARK||LA131_7==148) ) {
							alt131=3;
						}

						else {
							int nvaeMark = input.mark();
							try {
								input.consume();
								NoViableAltException nvae =
									new NoViableAltException("", 131, 7, input);
								throw nvae;
							} finally {
								input.rewind(nvaeMark);
							}
						}

						}
						break;
					default:
						NoViableAltException nvae =
							new NoViableAltException("", 131, 0, input);
						throw nvae;
					}
					switch (alt131) {
						case 1 :
							// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1065:9: K_IN ( '(' ')' |tupleInMarker= inMarkerForTuple |literals= tupleOfTupleLiterals |markers= tupleOfMarkersForTuples )
							{
							match(input,K_IN,FOLLOW_K_IN_in_relation7086); 
							// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1066:11: ( '(' ')' |tupleInMarker= inMarkerForTuple |literals= tupleOfTupleLiterals |markers= tupleOfMarkersForTuples )
							int alt130=4;
							int LA130_0 = input.LA(1);
							if ( (LA130_0==142) ) {
								switch ( input.LA(2) ) {
								case 143:
									{
									alt130=1;
									}
									break;
								case 142:
									{
									alt130=3;
									}
									break;
								case QMARK:
								case 148:
									{
									alt130=4;
									}
									break;
								default:
									int nvaeMark = input.mark();
									try {
										input.consume();
										NoViableAltException nvae =
											new NoViableAltException("", 130, 1, input);
										throw nvae;
									} finally {
										input.rewind(nvaeMark);
									}
								}
							}
							else if ( (LA130_0==QMARK||LA130_0==148) ) {
								alt130=2;
							}

							else {
								NoViableAltException nvae =
									new NoViableAltException("", 130, 0, input);
								throw nvae;
							}

							switch (alt130) {
								case 1 :
									// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1066:13: '(' ')'
									{
									match(input,142,FOLLOW_142_in_relation7100); 
									match(input,143,FOLLOW_143_in_relation7102); 
									 clauses.add(MultiColumnRelation.createInRelation(ids, new ArrayList<Tuples.Literal>())); 
									}
									break;
								case 2 :
									// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1068:13: tupleInMarker= inMarkerForTuple
									{
									pushFollow(FOLLOW_inMarkerForTuple_in_relation7134);
									tupleInMarker=inMarkerForTuple();
									state._fsp--;

									 clauses.add(MultiColumnRelation.createSingleMarkerInRelation(ids, tupleInMarker)); 
									}
									break;
								case 3 :
									// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1070:13: literals= tupleOfTupleLiterals
									{
									pushFollow(FOLLOW_tupleOfTupleLiterals_in_relation7168);
									literals=tupleOfTupleLiterals();
									state._fsp--;


									                  clauses.add(MultiColumnRelation.createInRelation(ids, literals));
									              
									}
									break;
								case 4 :
									// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1074:13: markers= tupleOfMarkersForTuples
									{
									pushFollow(FOLLOW_tupleOfMarkersForTuples_in_relation7202);
									markers=tupleOfMarkersForTuples();
									state._fsp--;

									 clauses.add(MultiColumnRelation.createInRelation(ids, markers)); 
									}
									break;

							}

							}
							break;
						case 2 :
							// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1077:9: type= relationType literal= tupleLiteral
							{
							pushFollow(FOLLOW_relationType_in_relation7244);
							type=relationType();
							state._fsp--;

							pushFollow(FOLLOW_tupleLiteral_in_relation7248);
							literal=tupleLiteral();
							state._fsp--;


							              clauses.add(MultiColumnRelation.createNonInRelation(ids, type, literal));
							          
							}
							break;
						case 3 :
							// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1081:9: type= relationType tupleMarker= markerForTuple
							{
							pushFollow(FOLLOW_relationType_in_relation7274);
							type=relationType();
							state._fsp--;

							pushFollow(FOLLOW_markerForTuple_in_relation7278);
							tupleMarker=markerForTuple();
							state._fsp--;

							 clauses.add(MultiColumnRelation.createNonInRelation(ids, type, tupleMarker)); 
							}
							break;

					}

					}
					break;
				case 7 :
					// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1084:7: '(' relation[$clauses] ')'
					{
					match(input,142,FOLLOW_142_in_relation7308); 
					pushFollow(FOLLOW_relation_in_relation7310);
					relation(clauses);
					state._fsp--;

					match(input,143,FOLLOW_143_in_relation7313); 
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
	// $ANTLR end "relation"



	// $ANTLR start "inMarker"
	// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1087:1: inMarker returns [AbstractMarker.INRaw marker] : ( QMARK | ':' name= ident );
	public final AbstractMarker.INRaw inMarker() throws RecognitionException {
		AbstractMarker.INRaw marker = null;


		ColumnIdentifier name =null;

		try {
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1088:5: ( QMARK | ':' name= ident )
			int alt133=2;
			int LA133_0 = input.LA(1);
			if ( (LA133_0==QMARK) ) {
				alt133=1;
			}
			else if ( (LA133_0==148) ) {
				alt133=2;
			}

			else {
				NoViableAltException nvae =
					new NoViableAltException("", 133, 0, input);
				throw nvae;
			}

			switch (alt133) {
				case 1 :
					// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1088:7: QMARK
					{
					match(input,QMARK,FOLLOW_QMARK_in_inMarker7334); 
					 marker = newINBindVariables(null); 
					}
					break;
				case 2 :
					// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1089:7: ':' name= ident
					{
					match(input,148,FOLLOW_148_in_inMarker7344); 
					pushFollow(FOLLOW_ident_in_inMarker7348);
					name=ident();
					state._fsp--;

					 marker = newINBindVariables(name); 
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
		return marker;
	}
	// $ANTLR end "inMarker"



	// $ANTLR start "tupleOfIdentifiers"
	// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1092:1: tupleOfIdentifiers returns [List<ColumnIdentifier.Raw> ids] : '(' n1= cident ( ',' ni= cident )* ')' ;
	public final List<ColumnIdentifier.Raw> tupleOfIdentifiers() throws RecognitionException {
		List<ColumnIdentifier.Raw> ids = null;


		ColumnIdentifier.Raw n1 =null;
		ColumnIdentifier.Raw ni =null;

		 ids = new ArrayList<ColumnIdentifier.Raw>(); 
		try {
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1094:5: ( '(' n1= cident ( ',' ni= cident )* ')' )
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1094:7: '(' n1= cident ( ',' ni= cident )* ')'
			{
			match(input,142,FOLLOW_142_in_tupleOfIdentifiers7380); 
			pushFollow(FOLLOW_cident_in_tupleOfIdentifiers7384);
			n1=cident();
			state._fsp--;

			 ids.add(n1); 
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1094:39: ( ',' ni= cident )*
			loop134:
			while (true) {
				int alt134=2;
				int LA134_0 = input.LA(1);
				if ( (LA134_0==145) ) {
					alt134=1;
				}

				switch (alt134) {
				case 1 :
					// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1094:40: ',' ni= cident
					{
					match(input,145,FOLLOW_145_in_tupleOfIdentifiers7389); 
					pushFollow(FOLLOW_cident_in_tupleOfIdentifiers7393);
					ni=cident();
					state._fsp--;

					 ids.add(ni); 
					}
					break;

				default :
					break loop134;
				}
			}

			match(input,143,FOLLOW_143_in_tupleOfIdentifiers7399); 
			}

		}
		catch (RecognitionException re) {
			reportError(re);
			recover(input,re);
		}
		finally {
			// do for sure before leaving
		}
		return ids;
	}
	// $ANTLR end "tupleOfIdentifiers"



	// $ANTLR start "singleColumnInValues"
	// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1097:1: singleColumnInValues returns [List<Term.Raw> terms] : '(' (t1= term ( ',' ti= term )* )? ')' ;
	public final List<Term.Raw> singleColumnInValues() throws RecognitionException {
		List<Term.Raw> terms = null;


		Term.Raw t1 =null;
		Term.Raw ti =null;

		 terms = new ArrayList<Term.Raw>(); 
		try {
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1099:5: ( '(' (t1= term ( ',' ti= term )* )? ')' )
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1099:7: '(' (t1= term ( ',' ti= term )* )? ')'
			{
			match(input,142,FOLLOW_142_in_singleColumnInValues7429); 
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1099:11: (t1= term ( ',' ti= term )* )?
			int alt136=2;
			int LA136_0 = input.LA(1);
			if ( (LA136_0==BOOLEAN||LA136_0==FLOAT||LA136_0==HEXNUMBER||(LA136_0 >= IDENT && LA136_0 <= INTEGER)||LA136_0==K_ALL||LA136_0==K_AS||LA136_0==K_ASCII||(LA136_0 >= K_BIGINT && LA136_0 <= K_BOOLEAN)||LA136_0==K_CLUSTERING||(LA136_0 >= K_COMPACT && LA136_0 <= K_CONTAINS)||LA136_0==K_COUNTER||(LA136_0 >= K_CUSTOM && LA136_0 <= K_DECIMAL)||(LA136_0 >= K_DISTINCT && LA136_0 <= K_DOUBLE)||(LA136_0 >= K_EXISTS && LA136_0 <= K_FLOAT)||LA136_0==K_FROZEN||(LA136_0 >= K_INET && LA136_0 <= K_INFINITY)||LA136_0==K_INT||LA136_0==K_KEYS||LA136_0==K_KEYSPACES||(LA136_0 >= K_LIST && LA136_0 <= K_MAP)||LA136_0==K_NAN||LA136_0==K_NOSUPERUSER||LA136_0==K_NULL||(LA136_0 >= K_PASSWORD && LA136_0 <= K_PERMISSIONS)||(LA136_0 >= K_STATIC && LA136_0 <= K_TIMEUUID)||(LA136_0 >= K_TOKEN && LA136_0 <= K_TRIGGER)||(LA136_0 >= K_TUPLE && LA136_0 <= K_TYPE)||(LA136_0 >= K_USER && LA136_0 <= K_USERS)||(LA136_0 >= K_UUID && LA136_0 <= K_VARINT)||LA136_0==QMARK||LA136_0==STRING_LITERAL||LA136_0==UUID||LA136_0==142||LA136_0==146||LA136_0==148||LA136_0==155||LA136_0==158) ) {
				alt136=1;
			}
			switch (alt136) {
				case 1 :
					// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1099:13: t1= term ( ',' ti= term )*
					{
					pushFollow(FOLLOW_term_in_singleColumnInValues7437);
					t1=term();
					state._fsp--;

					 terms.add(t1); 
					// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1099:43: ( ',' ti= term )*
					loop135:
					while (true) {
						int alt135=2;
						int LA135_0 = input.LA(1);
						if ( (LA135_0==145) ) {
							alt135=1;
						}

						switch (alt135) {
						case 1 :
							// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1099:44: ',' ti= term
							{
							match(input,145,FOLLOW_145_in_singleColumnInValues7442); 
							pushFollow(FOLLOW_term_in_singleColumnInValues7446);
							ti=term();
							state._fsp--;

							 terms.add(ti); 
							}
							break;

						default :
							break loop135;
						}
					}

					}
					break;

			}

			match(input,143,FOLLOW_143_in_singleColumnInValues7455); 
			}

		}
		catch (RecognitionException re) {
			reportError(re);
			recover(input,re);
		}
		finally {
			// do for sure before leaving
		}
		return terms;
	}
	// $ANTLR end "singleColumnInValues"



	// $ANTLR start "tupleOfTupleLiterals"
	// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1102:1: tupleOfTupleLiterals returns [List<Tuples.Literal> literals] : '(' t1= tupleLiteral ( ',' ti= tupleLiteral )* ')' ;
	public final List<Tuples.Literal> tupleOfTupleLiterals() throws RecognitionException {
		List<Tuples.Literal> literals = null;


		Tuples.Literal t1 =null;
		Tuples.Literal ti =null;

		 literals = new ArrayList<>(); 
		try {
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1104:5: ( '(' t1= tupleLiteral ( ',' ti= tupleLiteral )* ')' )
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1104:7: '(' t1= tupleLiteral ( ',' ti= tupleLiteral )* ')'
			{
			match(input,142,FOLLOW_142_in_tupleOfTupleLiterals7485); 
			pushFollow(FOLLOW_tupleLiteral_in_tupleOfTupleLiterals7489);
			t1=tupleLiteral();
			state._fsp--;

			 literals.add(t1); 
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1104:50: ( ',' ti= tupleLiteral )*
			loop137:
			while (true) {
				int alt137=2;
				int LA137_0 = input.LA(1);
				if ( (LA137_0==145) ) {
					alt137=1;
				}

				switch (alt137) {
				case 1 :
					// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1104:51: ',' ti= tupleLiteral
					{
					match(input,145,FOLLOW_145_in_tupleOfTupleLiterals7494); 
					pushFollow(FOLLOW_tupleLiteral_in_tupleOfTupleLiterals7498);
					ti=tupleLiteral();
					state._fsp--;

					 literals.add(ti); 
					}
					break;

				default :
					break loop137;
				}
			}

			match(input,143,FOLLOW_143_in_tupleOfTupleLiterals7504); 
			}

		}
		catch (RecognitionException re) {
			reportError(re);
			recover(input,re);
		}
		finally {
			// do for sure before leaving
		}
		return literals;
	}
	// $ANTLR end "tupleOfTupleLiterals"



	// $ANTLR start "markerForTuple"
	// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1107:1: markerForTuple returns [Tuples.Raw marker] : ( QMARK | ':' name= ident );
	public final Tuples.Raw markerForTuple() throws RecognitionException {
		Tuples.Raw marker = null;


		ColumnIdentifier name =null;

		try {
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1108:5: ( QMARK | ':' name= ident )
			int alt138=2;
			int LA138_0 = input.LA(1);
			if ( (LA138_0==QMARK) ) {
				alt138=1;
			}
			else if ( (LA138_0==148) ) {
				alt138=2;
			}

			else {
				NoViableAltException nvae =
					new NoViableAltException("", 138, 0, input);
				throw nvae;
			}

			switch (alt138) {
				case 1 :
					// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1108:7: QMARK
					{
					match(input,QMARK,FOLLOW_QMARK_in_markerForTuple7525); 
					 marker = newTupleBindVariables(null); 
					}
					break;
				case 2 :
					// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1109:7: ':' name= ident
					{
					match(input,148,FOLLOW_148_in_markerForTuple7535); 
					pushFollow(FOLLOW_ident_in_markerForTuple7539);
					name=ident();
					state._fsp--;

					 marker = newTupleBindVariables(name); 
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
		return marker;
	}
	// $ANTLR end "markerForTuple"



	// $ANTLR start "tupleOfMarkersForTuples"
	// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1112:1: tupleOfMarkersForTuples returns [List<Tuples.Raw> markers] : '(' m1= markerForTuple ( ',' mi= markerForTuple )* ')' ;
	public final List<Tuples.Raw> tupleOfMarkersForTuples() throws RecognitionException {
		List<Tuples.Raw> markers = null;


		Tuples.Raw m1 =null;
		Tuples.Raw mi =null;

		 markers = new ArrayList<Tuples.Raw>(); 
		try {
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1114:5: ( '(' m1= markerForTuple ( ',' mi= markerForTuple )* ')' )
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1114:7: '(' m1= markerForTuple ( ',' mi= markerForTuple )* ')'
			{
			match(input,142,FOLLOW_142_in_tupleOfMarkersForTuples7571); 
			pushFollow(FOLLOW_markerForTuple_in_tupleOfMarkersForTuples7575);
			m1=markerForTuple();
			state._fsp--;

			 markers.add(m1); 
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1114:51: ( ',' mi= markerForTuple )*
			loop139:
			while (true) {
				int alt139=2;
				int LA139_0 = input.LA(1);
				if ( (LA139_0==145) ) {
					alt139=1;
				}

				switch (alt139) {
				case 1 :
					// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1114:52: ',' mi= markerForTuple
					{
					match(input,145,FOLLOW_145_in_tupleOfMarkersForTuples7580); 
					pushFollow(FOLLOW_markerForTuple_in_tupleOfMarkersForTuples7584);
					mi=markerForTuple();
					state._fsp--;

					 markers.add(mi); 
					}
					break;

				default :
					break loop139;
				}
			}

			match(input,143,FOLLOW_143_in_tupleOfMarkersForTuples7590); 
			}

		}
		catch (RecognitionException re) {
			reportError(re);
			recover(input,re);
		}
		finally {
			// do for sure before leaving
		}
		return markers;
	}
	// $ANTLR end "tupleOfMarkersForTuples"



	// $ANTLR start "inMarkerForTuple"
	// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1117:1: inMarkerForTuple returns [Tuples.INRaw marker] : ( QMARK | ':' name= ident );
	public final Tuples.INRaw inMarkerForTuple() throws RecognitionException {
		Tuples.INRaw marker = null;


		ColumnIdentifier name =null;

		try {
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1118:5: ( QMARK | ':' name= ident )
			int alt140=2;
			int LA140_0 = input.LA(1);
			if ( (LA140_0==QMARK) ) {
				alt140=1;
			}
			else if ( (LA140_0==148) ) {
				alt140=2;
			}

			else {
				NoViableAltException nvae =
					new NoViableAltException("", 140, 0, input);
				throw nvae;
			}

			switch (alt140) {
				case 1 :
					// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1118:7: QMARK
					{
					match(input,QMARK,FOLLOW_QMARK_in_inMarkerForTuple7611); 
					 marker = newTupleINBindVariables(null); 
					}
					break;
				case 2 :
					// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1119:7: ':' name= ident
					{
					match(input,148,FOLLOW_148_in_inMarkerForTuple7621); 
					pushFollow(FOLLOW_ident_in_inMarkerForTuple7625);
					name=ident();
					state._fsp--;

					 marker = newTupleINBindVariables(name); 
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
		return marker;
	}
	// $ANTLR end "inMarkerForTuple"



	// $ANTLR start "comparatorType"
	// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1122:1: comparatorType returns [CQL3Type.Raw t] : (n= native_type |c= collection_type |tt= tuple_type |id= userTypeName | K_FROZEN '<' f= comparatorType '>' |s= STRING_LITERAL );
	public final CQL3Type.Raw comparatorType() throws RecognitionException {
		CQL3Type.Raw t = null;


		Token s=null;
		CQL3Type n =null;
		CQL3Type.Raw c =null;
		CQL3Type.Raw tt =null;
		UTName id =null;
		CQL3Type.Raw f =null;

		try {
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1123:5: (n= native_type |c= collection_type |tt= tuple_type |id= userTypeName | K_FROZEN '<' f= comparatorType '>' |s= STRING_LITERAL )
			int alt141=6;
			switch ( input.LA(1) ) {
			case K_ASCII:
				{
				int LA141_1 = input.LA(2);
				if ( (LA141_1==EOF||LA141_1==K_PRIMARY||LA141_1==K_STATIC||LA141_1==143||LA141_1==145||LA141_1==149||LA141_1==153) ) {
					alt141=1;
				}
				else if ( (LA141_1==147) ) {
					alt141=4;
				}

				else {
					int nvaeMark = input.mark();
					try {
						input.consume();
						NoViableAltException nvae =
							new NoViableAltException("", 141, 1, input);
						throw nvae;
					} finally {
						input.rewind(nvaeMark);
					}
				}

				}
				break;
			case K_BIGINT:
				{
				int LA141_2 = input.LA(2);
				if ( (LA141_2==EOF||LA141_2==K_PRIMARY||LA141_2==K_STATIC||LA141_2==143||LA141_2==145||LA141_2==149||LA141_2==153) ) {
					alt141=1;
				}
				else if ( (LA141_2==147) ) {
					alt141=4;
				}

				else {
					int nvaeMark = input.mark();
					try {
						input.consume();
						NoViableAltException nvae =
							new NoViableAltException("", 141, 2, input);
						throw nvae;
					} finally {
						input.rewind(nvaeMark);
					}
				}

				}
				break;
			case K_BLOB:
				{
				int LA141_3 = input.LA(2);
				if ( (LA141_3==EOF||LA141_3==K_PRIMARY||LA141_3==K_STATIC||LA141_3==143||LA141_3==145||LA141_3==149||LA141_3==153) ) {
					alt141=1;
				}
				else if ( (LA141_3==147) ) {
					alt141=4;
				}

				else {
					int nvaeMark = input.mark();
					try {
						input.consume();
						NoViableAltException nvae =
							new NoViableAltException("", 141, 3, input);
						throw nvae;
					} finally {
						input.rewind(nvaeMark);
					}
				}

				}
				break;
			case K_BOOLEAN:
				{
				int LA141_4 = input.LA(2);
				if ( (LA141_4==EOF||LA141_4==K_PRIMARY||LA141_4==K_STATIC||LA141_4==143||LA141_4==145||LA141_4==149||LA141_4==153) ) {
					alt141=1;
				}
				else if ( (LA141_4==147) ) {
					alt141=4;
				}

				else {
					int nvaeMark = input.mark();
					try {
						input.consume();
						NoViableAltException nvae =
							new NoViableAltException("", 141, 4, input);
						throw nvae;
					} finally {
						input.rewind(nvaeMark);
					}
				}

				}
				break;
			case K_COUNTER:
				{
				int LA141_5 = input.LA(2);
				if ( (LA141_5==EOF||LA141_5==K_PRIMARY||LA141_5==K_STATIC||LA141_5==143||LA141_5==145||LA141_5==149||LA141_5==153) ) {
					alt141=1;
				}
				else if ( (LA141_5==147) ) {
					alt141=4;
				}

				else {
					int nvaeMark = input.mark();
					try {
						input.consume();
						NoViableAltException nvae =
							new NoViableAltException("", 141, 5, input);
						throw nvae;
					} finally {
						input.rewind(nvaeMark);
					}
				}

				}
				break;
			case K_DECIMAL:
				{
				int LA141_6 = input.LA(2);
				if ( (LA141_6==EOF||LA141_6==K_PRIMARY||LA141_6==K_STATIC||LA141_6==143||LA141_6==145||LA141_6==149||LA141_6==153) ) {
					alt141=1;
				}
				else if ( (LA141_6==147) ) {
					alt141=4;
				}

				else {
					int nvaeMark = input.mark();
					try {
						input.consume();
						NoViableAltException nvae =
							new NoViableAltException("", 141, 6, input);
						throw nvae;
					} finally {
						input.rewind(nvaeMark);
					}
				}

				}
				break;
			case K_DOUBLE:
				{
				int LA141_7 = input.LA(2);
				if ( (LA141_7==EOF||LA141_7==K_PRIMARY||LA141_7==K_STATIC||LA141_7==143||LA141_7==145||LA141_7==149||LA141_7==153) ) {
					alt141=1;
				}
				else if ( (LA141_7==147) ) {
					alt141=4;
				}

				else {
					int nvaeMark = input.mark();
					try {
						input.consume();
						NoViableAltException nvae =
							new NoViableAltException("", 141, 7, input);
						throw nvae;
					} finally {
						input.rewind(nvaeMark);
					}
				}

				}
				break;
			case K_FLOAT:
				{
				int LA141_8 = input.LA(2);
				if ( (LA141_8==EOF||LA141_8==K_PRIMARY||LA141_8==K_STATIC||LA141_8==143||LA141_8==145||LA141_8==149||LA141_8==153) ) {
					alt141=1;
				}
				else if ( (LA141_8==147) ) {
					alt141=4;
				}

				else {
					int nvaeMark = input.mark();
					try {
						input.consume();
						NoViableAltException nvae =
							new NoViableAltException("", 141, 8, input);
						throw nvae;
					} finally {
						input.rewind(nvaeMark);
					}
				}

				}
				break;
			case K_INET:
				{
				int LA141_9 = input.LA(2);
				if ( (LA141_9==EOF||LA141_9==K_PRIMARY||LA141_9==K_STATIC||LA141_9==143||LA141_9==145||LA141_9==149||LA141_9==153) ) {
					alt141=1;
				}
				else if ( (LA141_9==147) ) {
					alt141=4;
				}

				else {
					int nvaeMark = input.mark();
					try {
						input.consume();
						NoViableAltException nvae =
							new NoViableAltException("", 141, 9, input);
						throw nvae;
					} finally {
						input.rewind(nvaeMark);
					}
				}

				}
				break;
			case K_INT:
				{
				int LA141_10 = input.LA(2);
				if ( (LA141_10==EOF||LA141_10==K_PRIMARY||LA141_10==K_STATIC||LA141_10==143||LA141_10==145||LA141_10==149||LA141_10==153) ) {
					alt141=1;
				}
				else if ( (LA141_10==147) ) {
					alt141=4;
				}

				else {
					int nvaeMark = input.mark();
					try {
						input.consume();
						NoViableAltException nvae =
							new NoViableAltException("", 141, 10, input);
						throw nvae;
					} finally {
						input.rewind(nvaeMark);
					}
				}

				}
				break;
			case K_TEXT:
				{
				int LA141_11 = input.LA(2);
				if ( (LA141_11==EOF||LA141_11==K_PRIMARY||LA141_11==K_STATIC||LA141_11==143||LA141_11==145||LA141_11==149||LA141_11==153) ) {
					alt141=1;
				}
				else if ( (LA141_11==147) ) {
					alt141=4;
				}

				else {
					int nvaeMark = input.mark();
					try {
						input.consume();
						NoViableAltException nvae =
							new NoViableAltException("", 141, 11, input);
						throw nvae;
					} finally {
						input.rewind(nvaeMark);
					}
				}

				}
				break;
			case K_TIMESTAMP:
				{
				int LA141_12 = input.LA(2);
				if ( (LA141_12==EOF||LA141_12==K_PRIMARY||LA141_12==K_STATIC||LA141_12==143||LA141_12==145||LA141_12==149||LA141_12==153) ) {
					alt141=1;
				}
				else if ( (LA141_12==147) ) {
					alt141=4;
				}

				else {
					int nvaeMark = input.mark();
					try {
						input.consume();
						NoViableAltException nvae =
							new NoViableAltException("", 141, 12, input);
						throw nvae;
					} finally {
						input.rewind(nvaeMark);
					}
				}

				}
				break;
			case K_UUID:
				{
				int LA141_13 = input.LA(2);
				if ( (LA141_13==EOF||LA141_13==K_PRIMARY||LA141_13==K_STATIC||LA141_13==143||LA141_13==145||LA141_13==149||LA141_13==153) ) {
					alt141=1;
				}
				else if ( (LA141_13==147) ) {
					alt141=4;
				}

				else {
					int nvaeMark = input.mark();
					try {
						input.consume();
						NoViableAltException nvae =
							new NoViableAltException("", 141, 13, input);
						throw nvae;
					} finally {
						input.rewind(nvaeMark);
					}
				}

				}
				break;
			case K_VARCHAR:
				{
				int LA141_14 = input.LA(2);
				if ( (LA141_14==EOF||LA141_14==K_PRIMARY||LA141_14==K_STATIC||LA141_14==143||LA141_14==145||LA141_14==149||LA141_14==153) ) {
					alt141=1;
				}
				else if ( (LA141_14==147) ) {
					alt141=4;
				}

				else {
					int nvaeMark = input.mark();
					try {
						input.consume();
						NoViableAltException nvae =
							new NoViableAltException("", 141, 14, input);
						throw nvae;
					} finally {
						input.rewind(nvaeMark);
					}
				}

				}
				break;
			case K_VARINT:
				{
				int LA141_15 = input.LA(2);
				if ( (LA141_15==EOF||LA141_15==K_PRIMARY||LA141_15==K_STATIC||LA141_15==143||LA141_15==145||LA141_15==149||LA141_15==153) ) {
					alt141=1;
				}
				else if ( (LA141_15==147) ) {
					alt141=4;
				}

				else {
					int nvaeMark = input.mark();
					try {
						input.consume();
						NoViableAltException nvae =
							new NoViableAltException("", 141, 15, input);
						throw nvae;
					} finally {
						input.rewind(nvaeMark);
					}
				}

				}
				break;
			case K_TIMEUUID:
				{
				int LA141_16 = input.LA(2);
				if ( (LA141_16==EOF||LA141_16==K_PRIMARY||LA141_16==K_STATIC||LA141_16==143||LA141_16==145||LA141_16==149||LA141_16==153) ) {
					alt141=1;
				}
				else if ( (LA141_16==147) ) {
					alt141=4;
				}

				else {
					int nvaeMark = input.mark();
					try {
						input.consume();
						NoViableAltException nvae =
							new NoViableAltException("", 141, 16, input);
						throw nvae;
					} finally {
						input.rewind(nvaeMark);
					}
				}

				}
				break;
			case K_MAP:
				{
				int LA141_17 = input.LA(2);
				if ( (LA141_17==150) ) {
					alt141=2;
				}
				else if ( (LA141_17==EOF||LA141_17==K_PRIMARY||LA141_17==K_STATIC||LA141_17==143||LA141_17==145||LA141_17==147||LA141_17==149||LA141_17==153) ) {
					alt141=4;
				}

				else {
					int nvaeMark = input.mark();
					try {
						input.consume();
						NoViableAltException nvae =
							new NoViableAltException("", 141, 17, input);
						throw nvae;
					} finally {
						input.rewind(nvaeMark);
					}
				}

				}
				break;
			case K_LIST:
				{
				int LA141_18 = input.LA(2);
				if ( (LA141_18==150) ) {
					alt141=2;
				}
				else if ( (LA141_18==EOF||LA141_18==K_PRIMARY||LA141_18==K_STATIC||LA141_18==143||LA141_18==145||LA141_18==147||LA141_18==149||LA141_18==153) ) {
					alt141=4;
				}

				else {
					int nvaeMark = input.mark();
					try {
						input.consume();
						NoViableAltException nvae =
							new NoViableAltException("", 141, 18, input);
						throw nvae;
					} finally {
						input.rewind(nvaeMark);
					}
				}

				}
				break;
			case K_SET:
				{
				alt141=2;
				}
				break;
			case K_TUPLE:
				{
				int LA141_20 = input.LA(2);
				if ( (LA141_20==150) ) {
					alt141=3;
				}
				else if ( (LA141_20==EOF||LA141_20==K_PRIMARY||LA141_20==K_STATIC||LA141_20==143||LA141_20==145||LA141_20==147||LA141_20==149||LA141_20==153) ) {
					alt141=4;
				}

				else {
					int nvaeMark = input.mark();
					try {
						input.consume();
						NoViableAltException nvae =
							new NoViableAltException("", 141, 20, input);
						throw nvae;
					} finally {
						input.rewind(nvaeMark);
					}
				}

				}
				break;
			case IDENT:
			case K_ALL:
			case K_AS:
			case K_CLUSTERING:
			case K_COMPACT:
			case K_CONTAINS:
			case K_COUNT:
			case K_CUSTOM:
			case K_DISTINCT:
			case K_EXISTS:
			case K_FILTERING:
			case K_KEY:
			case K_KEYS:
			case K_KEYSPACES:
			case K_NOSUPERUSER:
			case K_PASSWORD:
			case K_PERMISSION:
			case K_PERMISSIONS:
			case K_STATIC:
			case K_STORAGE:
			case K_SUPERUSER:
			case K_TRIGGER:
			case K_TTL:
			case K_TYPE:
			case K_USER:
			case K_USERS:
			case K_VALUES:
			case K_WRITETIME:
			case QUOTED_NAME:
				{
				alt141=4;
				}
				break;
			case K_FROZEN:
				{
				int LA141_22 = input.LA(2);
				if ( (LA141_22==150) ) {
					alt141=5;
				}
				else if ( (LA141_22==EOF||LA141_22==K_PRIMARY||LA141_22==K_STATIC||LA141_22==143||LA141_22==145||LA141_22==147||LA141_22==149||LA141_22==153) ) {
					alt141=4;
				}

				else {
					int nvaeMark = input.mark();
					try {
						input.consume();
						NoViableAltException nvae =
							new NoViableAltException("", 141, 22, input);
						throw nvae;
					} finally {
						input.rewind(nvaeMark);
					}
				}

				}
				break;
			case STRING_LITERAL:
				{
				alt141=6;
				}
				break;
			default:
				NoViableAltException nvae =
					new NoViableAltException("", 141, 0, input);
				throw nvae;
			}
			switch (alt141) {
				case 1 :
					// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1123:7: n= native_type
					{
					pushFollow(FOLLOW_native_type_in_comparatorType7650);
					n=native_type();
					state._fsp--;

					 t = CQL3Type.Raw.from(n); 
					}
					break;
				case 2 :
					// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1124:7: c= collection_type
					{
					pushFollow(FOLLOW_collection_type_in_comparatorType7666);
					c=collection_type();
					state._fsp--;

					 t = c; 
					}
					break;
				case 3 :
					// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1125:7: tt= tuple_type
					{
					pushFollow(FOLLOW_tuple_type_in_comparatorType7678);
					tt=tuple_type();
					state._fsp--;

					 t = tt; 
					}
					break;
				case 4 :
					// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1126:7: id= userTypeName
					{
					pushFollow(FOLLOW_userTypeName_in_comparatorType7694);
					id=userTypeName();
					state._fsp--;

					 t = CQL3Type.Raw.userType(id); 
					}
					break;
				case 5 :
					// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1127:7: K_FROZEN '<' f= comparatorType '>'
					{
					match(input,K_FROZEN,FOLLOW_K_FROZEN_in_comparatorType7706); 
					match(input,150,FOLLOW_150_in_comparatorType7708); 
					pushFollow(FOLLOW_comparatorType_in_comparatorType7712);
					f=comparatorType();
					state._fsp--;

					match(input,153,FOLLOW_153_in_comparatorType7714); 

					        try {
					            t = CQL3Type.Raw.frozen(f);
					        } catch (InvalidRequestException e) {
					            addRecognitionError(e.getMessage());
					        }
					      
					}
					break;
				case 6 :
					// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1135:7: s= STRING_LITERAL
					{
					s=(Token)match(input,STRING_LITERAL,FOLLOW_STRING_LITERAL_in_comparatorType7732); 

					        try {
					            t = CQL3Type.Raw.from(new CQL3Type.Custom((s!=null?s.getText():null)));
					        } catch (SyntaxException e) {
					            addRecognitionError("Cannot parse type " + (s!=null?s.getText():null) + ": " + e.getMessage());
					        } catch (ConfigurationException e) {
					            addRecognitionError("Error setting type " + (s!=null?s.getText():null) + ": " + e.getMessage());
					        }
					      
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
		return t;
	}
	// $ANTLR end "comparatorType"



	// $ANTLR start "native_type"
	// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1147:1: native_type returns [CQL3Type t] : ( K_ASCII | K_BIGINT | K_BLOB | K_BOOLEAN | K_COUNTER | K_DECIMAL | K_DOUBLE | K_FLOAT | K_INET | K_INT | K_TEXT | K_TIMESTAMP | K_UUID | K_VARCHAR | K_VARINT | K_TIMEUUID );
	public final CQL3Type native_type() throws RecognitionException {
		CQL3Type t = null;


		try {
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1148:5: ( K_ASCII | K_BIGINT | K_BLOB | K_BOOLEAN | K_COUNTER | K_DECIMAL | K_DOUBLE | K_FLOAT | K_INET | K_INT | K_TEXT | K_TIMESTAMP | K_UUID | K_VARCHAR | K_VARINT | K_TIMEUUID )
			int alt142=16;
			switch ( input.LA(1) ) {
			case K_ASCII:
				{
				alt142=1;
				}
				break;
			case K_BIGINT:
				{
				alt142=2;
				}
				break;
			case K_BLOB:
				{
				alt142=3;
				}
				break;
			case K_BOOLEAN:
				{
				alt142=4;
				}
				break;
			case K_COUNTER:
				{
				alt142=5;
				}
				break;
			case K_DECIMAL:
				{
				alt142=6;
				}
				break;
			case K_DOUBLE:
				{
				alt142=7;
				}
				break;
			case K_FLOAT:
				{
				alt142=8;
				}
				break;
			case K_INET:
				{
				alt142=9;
				}
				break;
			case K_INT:
				{
				alt142=10;
				}
				break;
			case K_TEXT:
				{
				alt142=11;
				}
				break;
			case K_TIMESTAMP:
				{
				alt142=12;
				}
				break;
			case K_UUID:
				{
				alt142=13;
				}
				break;
			case K_VARCHAR:
				{
				alt142=14;
				}
				break;
			case K_VARINT:
				{
				alt142=15;
				}
				break;
			case K_TIMEUUID:
				{
				alt142=16;
				}
				break;
			default:
				NoViableAltException nvae =
					new NoViableAltException("", 142, 0, input);
				throw nvae;
			}
			switch (alt142) {
				case 1 :
					// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1148:7: K_ASCII
					{
					match(input,K_ASCII,FOLLOW_K_ASCII_in_native_type7761); 
					 t = CQL3Type.Native.ASCII; 
					}
					break;
				case 2 :
					// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1149:7: K_BIGINT
					{
					match(input,K_BIGINT,FOLLOW_K_BIGINT_in_native_type7775); 
					 t = CQL3Type.Native.BIGINT; 
					}
					break;
				case 3 :
					// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1150:7: K_BLOB
					{
					match(input,K_BLOB,FOLLOW_K_BLOB_in_native_type7788); 
					 t = CQL3Type.Native.BLOB; 
					}
					break;
				case 4 :
					// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1151:7: K_BOOLEAN
					{
					match(input,K_BOOLEAN,FOLLOW_K_BOOLEAN_in_native_type7803); 
					 t = CQL3Type.Native.BOOLEAN; 
					}
					break;
				case 5 :
					// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1152:7: K_COUNTER
					{
					match(input,K_COUNTER,FOLLOW_K_COUNTER_in_native_type7815); 
					 t = CQL3Type.Native.COUNTER; 
					}
					break;
				case 6 :
					// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1153:7: K_DECIMAL
					{
					match(input,K_DECIMAL,FOLLOW_K_DECIMAL_in_native_type7827); 
					 t = CQL3Type.Native.DECIMAL; 
					}
					break;
				case 7 :
					// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1154:7: K_DOUBLE
					{
					match(input,K_DOUBLE,FOLLOW_K_DOUBLE_in_native_type7839); 
					 t = CQL3Type.Native.DOUBLE; 
					}
					break;
				case 8 :
					// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1155:7: K_FLOAT
					{
					match(input,K_FLOAT,FOLLOW_K_FLOAT_in_native_type7852); 
					 t = CQL3Type.Native.FLOAT; 
					}
					break;
				case 9 :
					// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1156:7: K_INET
					{
					match(input,K_INET,FOLLOW_K_INET_in_native_type7866); 
					 t = CQL3Type.Native.INET;
					}
					break;
				case 10 :
					// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1157:7: K_INT
					{
					match(input,K_INT,FOLLOW_K_INT_in_native_type7881); 
					 t = CQL3Type.Native.INT; 
					}
					break;
				case 11 :
					// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1158:7: K_TEXT
					{
					match(input,K_TEXT,FOLLOW_K_TEXT_in_native_type7897); 
					 t = CQL3Type.Native.TEXT; 
					}
					break;
				case 12 :
					// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1159:7: K_TIMESTAMP
					{
					match(input,K_TIMESTAMP,FOLLOW_K_TIMESTAMP_in_native_type7912); 
					 t = CQL3Type.Native.TIMESTAMP; 
					}
					break;
				case 13 :
					// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1160:7: K_UUID
					{
					match(input,K_UUID,FOLLOW_K_UUID_in_native_type7922); 
					 t = CQL3Type.Native.UUID; 
					}
					break;
				case 14 :
					// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1161:7: K_VARCHAR
					{
					match(input,K_VARCHAR,FOLLOW_K_VARCHAR_in_native_type7937); 
					 t = CQL3Type.Native.VARCHAR; 
					}
					break;
				case 15 :
					// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1162:7: K_VARINT
					{
					match(input,K_VARINT,FOLLOW_K_VARINT_in_native_type7949); 
					 t = CQL3Type.Native.VARINT; 
					}
					break;
				case 16 :
					// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1163:7: K_TIMEUUID
					{
					match(input,K_TIMEUUID,FOLLOW_K_TIMEUUID_in_native_type7962); 
					 t = CQL3Type.Native.TIMEUUID; 
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
		return t;
	}
	// $ANTLR end "native_type"



	// $ANTLR start "collection_type"
	// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1166:1: collection_type returns [CQL3Type.Raw pt] : ( K_MAP '<' t1= comparatorType ',' t2= comparatorType '>' | K_LIST '<' t= comparatorType '>' | K_SET '<' t= comparatorType '>' );
	public final CQL3Type.Raw collection_type() throws RecognitionException {
		CQL3Type.Raw pt = null;


		CQL3Type.Raw t1 =null;
		CQL3Type.Raw t2 =null;
		CQL3Type.Raw t =null;

		try {
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1167:5: ( K_MAP '<' t1= comparatorType ',' t2= comparatorType '>' | K_LIST '<' t= comparatorType '>' | K_SET '<' t= comparatorType '>' )
			int alt143=3;
			switch ( input.LA(1) ) {
			case K_MAP:
				{
				alt143=1;
				}
				break;
			case K_LIST:
				{
				alt143=2;
				}
				break;
			case K_SET:
				{
				alt143=3;
				}
				break;
			default:
				NoViableAltException nvae =
					new NoViableAltException("", 143, 0, input);
				throw nvae;
			}
			switch (alt143) {
				case 1 :
					// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1167:7: K_MAP '<' t1= comparatorType ',' t2= comparatorType '>'
					{
					match(input,K_MAP,FOLLOW_K_MAP_in_collection_type7986); 
					match(input,150,FOLLOW_150_in_collection_type7989); 
					pushFollow(FOLLOW_comparatorType_in_collection_type7993);
					t1=comparatorType();
					state._fsp--;

					match(input,145,FOLLOW_145_in_collection_type7995); 
					pushFollow(FOLLOW_comparatorType_in_collection_type7999);
					t2=comparatorType();
					state._fsp--;

					match(input,153,FOLLOW_153_in_collection_type8001); 

					            // if we can't parse either t1 or t2, antlr will "recover" and we may have t1 or t2 null.
					            if (t1 != null && t2 != null)
					                pt = CQL3Type.Raw.map(t1, t2);
					        
					}
					break;
				case 2 :
					// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1173:7: K_LIST '<' t= comparatorType '>'
					{
					match(input,K_LIST,FOLLOW_K_LIST_in_collection_type8019); 
					match(input,150,FOLLOW_150_in_collection_type8021); 
					pushFollow(FOLLOW_comparatorType_in_collection_type8025);
					t=comparatorType();
					state._fsp--;

					match(input,153,FOLLOW_153_in_collection_type8027); 
					 if (t != null) pt = CQL3Type.Raw.list(t); 
					}
					break;
				case 3 :
					// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1175:7: K_SET '<' t= comparatorType '>'
					{
					match(input,K_SET,FOLLOW_K_SET_in_collection_type8045); 
					match(input,150,FOLLOW_150_in_collection_type8048); 
					pushFollow(FOLLOW_comparatorType_in_collection_type8052);
					t=comparatorType();
					state._fsp--;

					match(input,153,FOLLOW_153_in_collection_type8054); 
					 if (t != null) pt = CQL3Type.Raw.set(t); 
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
		return pt;
	}
	// $ANTLR end "collection_type"



	// $ANTLR start "tuple_type"
	// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1179:1: tuple_type returns [CQL3Type.Raw t] : K_TUPLE '<' t1= comparatorType ( ',' tn= comparatorType )* '>' ;
	public final CQL3Type.Raw tuple_type() throws RecognitionException {
		CQL3Type.Raw t = null;


		CQL3Type.Raw t1 =null;
		CQL3Type.Raw tn =null;

		try {
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1180:5: ( K_TUPLE '<' t1= comparatorType ( ',' tn= comparatorType )* '>' )
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1180:7: K_TUPLE '<' t1= comparatorType ( ',' tn= comparatorType )* '>'
			{
			match(input,K_TUPLE,FOLLOW_K_TUPLE_in_tuple_type8085); 
			match(input,150,FOLLOW_150_in_tuple_type8087); 
			 List<CQL3Type.Raw> types = new ArrayList<>(); 
			pushFollow(FOLLOW_comparatorType_in_tuple_type8102);
			t1=comparatorType();
			state._fsp--;

			 types.add(t1); 
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1181:47: ( ',' tn= comparatorType )*
			loop144:
			while (true) {
				int alt144=2;
				int LA144_0 = input.LA(1);
				if ( (LA144_0==145) ) {
					alt144=1;
				}

				switch (alt144) {
				case 1 :
					// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1181:48: ',' tn= comparatorType
					{
					match(input,145,FOLLOW_145_in_tuple_type8107); 
					pushFollow(FOLLOW_comparatorType_in_tuple_type8111);
					tn=comparatorType();
					state._fsp--;

					 types.add(tn); 
					}
					break;

				default :
					break loop144;
				}
			}

			match(input,153,FOLLOW_153_in_tuple_type8123); 
			 t = CQL3Type.Raw.tuple(types); 
			}

		}
		catch (RecognitionException re) {
			reportError(re);
			recover(input,re);
		}
		finally {
			// do for sure before leaving
		}
		return t;
	}
	// $ANTLR end "tuple_type"


	public static class username_return extends ParserRuleReturnScope {
	};


	// $ANTLR start "username"
	// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1185:1: username : ( IDENT | STRING_LITERAL );
	public final CqlParser.username_return username() throws RecognitionException {
		CqlParser.username_return retval = new CqlParser.username_return();
		retval.start = input.LT(1);

		try {
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1186:5: ( IDENT | STRING_LITERAL )
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:
			{
			if ( input.LA(1)==IDENT||input.LA(1)==STRING_LITERAL ) {
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
	// $ANTLR end "username"



	// $ANTLR start "non_type_ident"
	// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1192:1: non_type_ident returns [ColumnIdentifier id] : (t= IDENT |t= QUOTED_NAME |k= basic_unreserved_keyword |kk= K_KEY );
	public final ColumnIdentifier non_type_ident() throws RecognitionException {
		ColumnIdentifier id = null;


		Token t=null;
		Token kk=null;
		String k =null;

		try {
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1193:5: (t= IDENT |t= QUOTED_NAME |k= basic_unreserved_keyword |kk= K_KEY )
			int alt145=4;
			switch ( input.LA(1) ) {
			case IDENT:
				{
				alt145=1;
				}
				break;
			case QUOTED_NAME:
				{
				alt145=2;
				}
				break;
			case K_ALL:
			case K_AS:
			case K_CLUSTERING:
			case K_COMPACT:
			case K_CONTAINS:
			case K_CUSTOM:
			case K_DISTINCT:
			case K_EXISTS:
			case K_FILTERING:
			case K_FROZEN:
			case K_KEYS:
			case K_KEYSPACES:
			case K_LIST:
			case K_MAP:
			case K_NOSUPERUSER:
			case K_PASSWORD:
			case K_PERMISSION:
			case K_PERMISSIONS:
			case K_STATIC:
			case K_STORAGE:
			case K_SUPERUSER:
			case K_TRIGGER:
			case K_TUPLE:
			case K_TYPE:
			case K_USER:
			case K_USERS:
			case K_VALUES:
				{
				alt145=3;
				}
				break;
			case K_KEY:
				{
				alt145=4;
				}
				break;
			default:
				NoViableAltException nvae =
					new NoViableAltException("", 145, 0, input);
				throw nvae;
			}
			switch (alt145) {
				case 1 :
					// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1193:7: t= IDENT
					{
					t=(Token)match(input,IDENT,FOLLOW_IDENT_in_non_type_ident8175); 
					 if (reservedTypeNames.contains((t!=null?t.getText():null))) addRecognitionError("Invalid (reserved) user type name " + (t!=null?t.getText():null)); id = new ColumnIdentifier((t!=null?t.getText():null), false); 
					}
					break;
				case 2 :
					// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1194:7: t= QUOTED_NAME
					{
					t=(Token)match(input,QUOTED_NAME,FOLLOW_QUOTED_NAME_in_non_type_ident8206); 
					 id = new ColumnIdentifier((t!=null?t.getText():null), true); 
					}
					break;
				case 3 :
					// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1195:7: k= basic_unreserved_keyword
					{
					pushFollow(FOLLOW_basic_unreserved_keyword_in_non_type_ident8231);
					k=basic_unreserved_keyword();
					state._fsp--;

					 id = new ColumnIdentifier(k, false); 
					}
					break;
				case 4 :
					// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1196:7: kk= K_KEY
					{
					kk=(Token)match(input,K_KEY,FOLLOW_K_KEY_in_non_type_ident8243); 
					 id = new ColumnIdentifier((kk!=null?kk.getText():null), false); 
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
		return id;
	}
	// $ANTLR end "non_type_ident"



	// $ANTLR start "unreserved_keyword"
	// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1199:1: unreserved_keyword returns [String str] : (u= unreserved_function_keyword |k= ( K_TTL | K_COUNT | K_WRITETIME | K_KEY ) );
	public final String unreserved_keyword() throws RecognitionException {
		String str = null;


		Token k=null;
		String u =null;

		try {
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1200:5: (u= unreserved_function_keyword |k= ( K_TTL | K_COUNT | K_WRITETIME | K_KEY ) )
			int alt146=2;
			int LA146_0 = input.LA(1);
			if ( (LA146_0==K_ALL||LA146_0==K_AS||LA146_0==K_ASCII||(LA146_0 >= K_BIGINT && LA146_0 <= K_BOOLEAN)||LA146_0==K_CLUSTERING||(LA146_0 >= K_COMPACT && LA146_0 <= K_CONTAINS)||LA146_0==K_COUNTER||(LA146_0 >= K_CUSTOM && LA146_0 <= K_DECIMAL)||(LA146_0 >= K_DISTINCT && LA146_0 <= K_DOUBLE)||(LA146_0 >= K_EXISTS && LA146_0 <= K_FLOAT)||LA146_0==K_FROZEN||LA146_0==K_INET||LA146_0==K_INT||LA146_0==K_KEYS||LA146_0==K_KEYSPACES||(LA146_0 >= K_LIST && LA146_0 <= K_MAP)||LA146_0==K_NOSUPERUSER||(LA146_0 >= K_PASSWORD && LA146_0 <= K_PERMISSIONS)||(LA146_0 >= K_STATIC && LA146_0 <= K_TIMEUUID)||LA146_0==K_TRIGGER||(LA146_0 >= K_TUPLE && LA146_0 <= K_TYPE)||(LA146_0 >= K_USER && LA146_0 <= K_USERS)||(LA146_0 >= K_UUID && LA146_0 <= K_VARINT)) ) {
				alt146=1;
			}
			else if ( (LA146_0==K_COUNT||LA146_0==K_KEY||LA146_0==K_TTL||LA146_0==K_WRITETIME) ) {
				alt146=2;
			}

			else {
				NoViableAltException nvae =
					new NoViableAltException("", 146, 0, input);
				throw nvae;
			}

			switch (alt146) {
				case 1 :
					// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1200:7: u= unreserved_function_keyword
					{
					pushFollow(FOLLOW_unreserved_function_keyword_in_unreserved_keyword8286);
					u=unreserved_function_keyword();
					state._fsp--;

					 str = u; 
					}
					break;
				case 2 :
					// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1201:7: k= ( K_TTL | K_COUNT | K_WRITETIME | K_KEY )
					{
					k=input.LT(1);
					if ( input.LA(1)==K_COUNT||input.LA(1)==K_KEY||input.LA(1)==K_TTL||input.LA(1)==K_WRITETIME ) {
						input.consume();
						state.errorRecovery=false;
					}
					else {
						MismatchedSetException mse = new MismatchedSetException(null,input);
						throw mse;
					}
					 str = (k!=null?k.getText():null); 
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
		return str;
	}
	// $ANTLR end "unreserved_keyword"



	// $ANTLR start "unreserved_function_keyword"
	// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1204:1: unreserved_function_keyword returns [String str] : (u= basic_unreserved_keyword |t= native_type );
	public final String unreserved_function_keyword() throws RecognitionException {
		String str = null;


		String u =null;
		CQL3Type t =null;

		try {
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1205:5: (u= basic_unreserved_keyword |t= native_type )
			int alt147=2;
			int LA147_0 = input.LA(1);
			if ( (LA147_0==K_ALL||LA147_0==K_AS||LA147_0==K_CLUSTERING||(LA147_0 >= K_COMPACT && LA147_0 <= K_CONTAINS)||LA147_0==K_CUSTOM||LA147_0==K_DISTINCT||(LA147_0 >= K_EXISTS && LA147_0 <= K_FILTERING)||LA147_0==K_FROZEN||LA147_0==K_KEYS||LA147_0==K_KEYSPACES||(LA147_0 >= K_LIST && LA147_0 <= K_MAP)||LA147_0==K_NOSUPERUSER||(LA147_0 >= K_PASSWORD && LA147_0 <= K_PERMISSIONS)||(LA147_0 >= K_STATIC && LA147_0 <= K_SUPERUSER)||LA147_0==K_TRIGGER||(LA147_0 >= K_TUPLE && LA147_0 <= K_TYPE)||(LA147_0 >= K_USER && LA147_0 <= K_USERS)||LA147_0==K_VALUES) ) {
				alt147=1;
			}
			else if ( (LA147_0==K_ASCII||(LA147_0 >= K_BIGINT && LA147_0 <= K_BOOLEAN)||LA147_0==K_COUNTER||LA147_0==K_DECIMAL||LA147_0==K_DOUBLE||LA147_0==K_FLOAT||LA147_0==K_INET||LA147_0==K_INT||(LA147_0 >= K_TEXT && LA147_0 <= K_TIMEUUID)||LA147_0==K_UUID||(LA147_0 >= K_VARCHAR && LA147_0 <= K_VARINT)) ) {
				alt147=2;
			}

			else {
				NoViableAltException nvae =
					new NoViableAltException("", 147, 0, input);
				throw nvae;
			}

			switch (alt147) {
				case 1 :
					// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1205:7: u= basic_unreserved_keyword
					{
					pushFollow(FOLLOW_basic_unreserved_keyword_in_unreserved_function_keyword8341);
					u=basic_unreserved_keyword();
					state._fsp--;

					 str = u; 
					}
					break;
				case 2 :
					// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1206:7: t= native_type
					{
					pushFollow(FOLLOW_native_type_in_unreserved_function_keyword8353);
					t=native_type();
					state._fsp--;

					 str = t.toString(); 
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
		return str;
	}
	// $ANTLR end "unreserved_function_keyword"



	// $ANTLR start "basic_unreserved_keyword"
	// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1209:1: basic_unreserved_keyword returns [String str] : k= ( K_KEYS | K_AS | K_CLUSTERING | K_COMPACT | K_STORAGE | K_TYPE | K_VALUES | K_MAP | K_LIST | K_FILTERING | K_PERMISSION | K_PERMISSIONS | K_KEYSPACES | K_ALL | K_USER | K_USERS | K_SUPERUSER | K_NOSUPERUSER | K_PASSWORD | K_EXISTS | K_CUSTOM | K_TRIGGER | K_DISTINCT | K_CONTAINS | K_STATIC | K_FROZEN | K_TUPLE ) ;
	public final String basic_unreserved_keyword() throws RecognitionException {
		String str = null;


		Token k=null;

		try {
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1210:5: (k= ( K_KEYS | K_AS | K_CLUSTERING | K_COMPACT | K_STORAGE | K_TYPE | K_VALUES | K_MAP | K_LIST | K_FILTERING | K_PERMISSION | K_PERMISSIONS | K_KEYSPACES | K_ALL | K_USER | K_USERS | K_SUPERUSER | K_NOSUPERUSER | K_PASSWORD | K_EXISTS | K_CUSTOM | K_TRIGGER | K_DISTINCT | K_CONTAINS | K_STATIC | K_FROZEN | K_TUPLE ) )
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1210:7: k= ( K_KEYS | K_AS | K_CLUSTERING | K_COMPACT | K_STORAGE | K_TYPE | K_VALUES | K_MAP | K_LIST | K_FILTERING | K_PERMISSION | K_PERMISSIONS | K_KEYSPACES | K_ALL | K_USER | K_USERS | K_SUPERUSER | K_NOSUPERUSER | K_PASSWORD | K_EXISTS | K_CUSTOM | K_TRIGGER | K_DISTINCT | K_CONTAINS | K_STATIC | K_FROZEN | K_TUPLE )
			{
			k=input.LT(1);
			if ( input.LA(1)==K_ALL||input.LA(1)==K_AS||input.LA(1)==K_CLUSTERING||(input.LA(1) >= K_COMPACT && input.LA(1) <= K_CONTAINS)||input.LA(1)==K_CUSTOM||input.LA(1)==K_DISTINCT||(input.LA(1) >= K_EXISTS && input.LA(1) <= K_FILTERING)||input.LA(1)==K_FROZEN||input.LA(1)==K_KEYS||input.LA(1)==K_KEYSPACES||(input.LA(1) >= K_LIST && input.LA(1) <= K_MAP)||input.LA(1)==K_NOSUPERUSER||(input.LA(1) >= K_PASSWORD && input.LA(1) <= K_PERMISSIONS)||(input.LA(1) >= K_STATIC && input.LA(1) <= K_SUPERUSER)||input.LA(1)==K_TRIGGER||(input.LA(1) >= K_TUPLE && input.LA(1) <= K_TYPE)||(input.LA(1) >= K_USER && input.LA(1) <= K_USERS)||input.LA(1)==K_VALUES ) {
				input.consume();
				state.errorRecovery=false;
			}
			else {
				MismatchedSetException mse = new MismatchedSetException(null,input);
				throw mse;
			}
			 str = (k!=null?k.getText():null); 
			}

		}
		catch (RecognitionException re) {
			reportError(re);
			recover(input,re);
		}
		finally {
			// do for sure before leaving
		}
		return str;
	}
	// $ANTLR end "basic_unreserved_keyword"

	// Delegated rules



	public static final BitSet FOLLOW_cqlStatement_in_query72 = new BitSet(new long[]{0x0000000000000000L,0x0000000000000000L,0x0000000000200000L});
	public static final BitSet FOLLOW_149_in_query75 = new BitSet(new long[]{0x0000000000000000L,0x0000000000000000L,0x0000000000200000L});
	public static final BitSet FOLLOW_EOF_in_query79 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_selectStatement_in_cqlStatement113 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_insertStatement_in_cqlStatement138 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_updateStatement_in_cqlStatement163 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_batchStatement_in_cqlStatement188 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_deleteStatement_in_cqlStatement214 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_useStatement_in_cqlStatement239 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_truncateStatement_in_cqlStatement267 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_createKeyspaceStatement_in_cqlStatement290 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_createTableStatement_in_cqlStatement307 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_createIndexStatement_in_cqlStatement326 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_dropKeyspaceStatement_in_cqlStatement345 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_dropTableStatement_in_cqlStatement363 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_dropIndexStatement_in_cqlStatement384 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_alterTableStatement_in_cqlStatement405 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_alterKeyspaceStatement_in_cqlStatement425 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_grantStatement_in_cqlStatement442 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_revokeStatement_in_cqlStatement467 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_listPermissionsStatement_in_cqlStatement491 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_createUserStatement_in_cqlStatement506 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_alterUserStatement_in_cqlStatement526 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_dropUserStatement_in_cqlStatement547 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_listUsersStatement_in_cqlStatement569 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_createTriggerStatement_in_cqlStatement590 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_dropTriggerStatement_in_cqlStatement607 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_createTypeStatement_in_cqlStatement626 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_alterTypeStatement_in_cqlStatement646 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_dropTypeStatement_in_cqlStatement667 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_K_USE_in_useStatement702 = new BitSet(new long[]{0x05D9BD7142100000L,0x804F63A7E0E08D69L,0x0000000000000001L});
	public static final BitSet FOLLOW_keyspaceName_in_useStatement706 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_K_SELECT_in_selectStatement740 = new BitSet(new long[]{0x05D9BD7142100000L,0x004F63B7E0E08D69L,0x0000000010000001L});
	public static final BitSet FOLLOW_K_DISTINCT_in_selectStatement746 = new BitSet(new long[]{0x05D9BD7142100000L,0x004F63B7E0E08D69L,0x0000000010000001L});
	public static final BitSet FOLLOW_selectClause_in_selectStatement755 = new BitSet(new long[]{0x0200000000000000L});
	public static final BitSet FOLLOW_K_COUNT_in_selectStatement775 = new BitSet(new long[]{0x0000000000000000L,0x0000000000000000L,0x0000000000004000L});
	public static final BitSet FOLLOW_142_in_selectStatement777 = new BitSet(new long[]{0x0000000000200000L,0x0000000000000000L,0x0000000010000000L});
	public static final BitSet FOLLOW_selectCountClause_in_selectStatement781 = new BitSet(new long[]{0x0000000000000000L,0x0000000000000000L,0x0000000000008000L});
	public static final BitSet FOLLOW_143_in_selectStatement783 = new BitSet(new long[]{0x0200000040000000L});
	public static final BitSet FOLLOW_K_AS_in_selectStatement788 = new BitSet(new long[]{0x05D9BD7142100000L,0x004F63A7E0E08D69L,0x0000000000000001L});
	public static final BitSet FOLLOW_ident_in_selectStatement792 = new BitSet(new long[]{0x0200000000000000L});
	public static final BitSet FOLLOW_K_FROM_in_selectStatement807 = new BitSet(new long[]{0x05D9BD7142100000L,0x804F63A7E0E08D69L,0x0000000000000001L});
	public static final BitSet FOLLOW_columnFamilyName_in_selectStatement811 = new BitSet(new long[]{0x0000000004000002L,0x0010000000100200L});
	public static final BitSet FOLLOW_K_WHERE_in_selectStatement821 = new BitSet(new long[]{0x05D9BD7142100000L,0x004F63B7E0E08D69L,0x0000000000004001L});
	public static final BitSet FOLLOW_whereClause_in_selectStatement825 = new BitSet(new long[]{0x0000000004000002L,0x0000000000100200L});
	public static final BitSet FOLLOW_K_ORDER_in_selectStatement838 = new BitSet(new long[]{0x0000008000000000L});
	public static final BitSet FOLLOW_K_BY_in_selectStatement840 = new BitSet(new long[]{0x05D9BD7142100000L,0x004F63A7E0E08D69L,0x0000000000000001L});
	public static final BitSet FOLLOW_orderByClause_in_selectStatement842 = new BitSet(new long[]{0x0000000004000002L,0x0000000000000200L,0x0000000000020000L});
	public static final BitSet FOLLOW_145_in_selectStatement847 = new BitSet(new long[]{0x05D9BD7142100000L,0x004F63A7E0E08D69L,0x0000000000000001L});
	public static final BitSet FOLLOW_orderByClause_in_selectStatement849 = new BitSet(new long[]{0x0000000004000002L,0x0000000000000200L,0x0000000000020000L});
	public static final BitSet FOLLOW_K_LIMIT_in_selectStatement866 = new BitSet(new long[]{0x0000000004200000L,0x8000000000000000L,0x0000000000100000L});
	public static final BitSet FOLLOW_intValue_in_selectStatement870 = new BitSet(new long[]{0x0000000004000002L});
	public static final BitSet FOLLOW_K_ALLOW_in_selectStatement885 = new BitSet(new long[]{0x0080000000000000L});
	public static final BitSet FOLLOW_K_FILTERING_in_selectStatement887 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_selector_in_selectClause924 = new BitSet(new long[]{0x0000000000000002L,0x0000000000000000L,0x0000000000020000L});
	public static final BitSet FOLLOW_145_in_selectClause929 = new BitSet(new long[]{0x05D9BD7142100000L,0x004F63B7E0E08D69L,0x0000000000000001L});
	public static final BitSet FOLLOW_selector_in_selectClause933 = new BitSet(new long[]{0x0000000000000002L,0x0000000000000000L,0x0000000000020000L});
	public static final BitSet FOLLOW_156_in_selectClause945 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_unaliasedSelector_in_selector978 = new BitSet(new long[]{0x0000000040000002L});
	public static final BitSet FOLLOW_K_AS_in_selector981 = new BitSet(new long[]{0x05D9BD7142100000L,0x004F63A7E0E08D69L,0x0000000000000001L});
	public static final BitSet FOLLOW_ident_in_selector985 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_cident_in_unaliasedSelector1026 = new BitSet(new long[]{0x0000000000000002L,0x0000000000000000L,0x0000000000080000L});
	public static final BitSet FOLLOW_K_WRITETIME_in_unaliasedSelector1072 = new BitSet(new long[]{0x0000000000000000L,0x0000000000000000L,0x0000000000004000L});
	public static final BitSet FOLLOW_142_in_unaliasedSelector1074 = new BitSet(new long[]{0x05D9BD7142100000L,0x004F63A7E0E08D69L,0x0000000000000001L});
	public static final BitSet FOLLOW_cident_in_unaliasedSelector1078 = new BitSet(new long[]{0x0000000000000000L,0x0000000000000000L,0x0000000000008000L});
	public static final BitSet FOLLOW_143_in_unaliasedSelector1080 = new BitSet(new long[]{0x0000000000000002L,0x0000000000000000L,0x0000000000080000L});
	public static final BitSet FOLLOW_K_TTL_in_unaliasedSelector1106 = new BitSet(new long[]{0x0000000000000000L,0x0000000000000000L,0x0000000000004000L});
	public static final BitSet FOLLOW_142_in_unaliasedSelector1114 = new BitSet(new long[]{0x05D9BD7142100000L,0x004F63A7E0E08D69L,0x0000000000000001L});
	public static final BitSet FOLLOW_cident_in_unaliasedSelector1118 = new BitSet(new long[]{0x0000000000000000L,0x0000000000000000L,0x0000000000008000L});
	public static final BitSet FOLLOW_143_in_unaliasedSelector1120 = new BitSet(new long[]{0x0000000000000002L,0x0000000000000000L,0x0000000000080000L});
	public static final BitSet FOLLOW_functionName_in_unaliasedSelector1148 = new BitSet(new long[]{0x0000000000000000L,0x0000000000000000L,0x0000000000004000L});
	public static final BitSet FOLLOW_selectionFunctionArgs_in_unaliasedSelector1152 = new BitSet(new long[]{0x0000000000000002L,0x0000000000000000L,0x0000000000080000L});
	public static final BitSet FOLLOW_147_in_unaliasedSelector1167 = new BitSet(new long[]{0x05D9BD7142100000L,0x004F63A7E0E08D69L,0x0000000000000001L});
	public static final BitSet FOLLOW_cident_in_unaliasedSelector1171 = new BitSet(new long[]{0x0000000000000002L,0x0000000000000000L,0x0000000000080000L});
	public static final BitSet FOLLOW_142_in_selectionFunctionArgs1199 = new BitSet(new long[]{0x0000000000000000L,0x0000000000000000L,0x0000000000008000L});
	public static final BitSet FOLLOW_143_in_selectionFunctionArgs1201 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_142_in_selectionFunctionArgs1211 = new BitSet(new long[]{0x05D9BD7142100000L,0x004F63B7E0E08D69L,0x0000000000000001L});
	public static final BitSet FOLLOW_unaliasedSelector_in_selectionFunctionArgs1215 = new BitSet(new long[]{0x0000000000000000L,0x0000000000000000L,0x0000000000028000L});
	public static final BitSet FOLLOW_145_in_selectionFunctionArgs1231 = new BitSet(new long[]{0x05D9BD7142100000L,0x004F63B7E0E08D69L,0x0000000000000001L});
	public static final BitSet FOLLOW_unaliasedSelector_in_selectionFunctionArgs1235 = new BitSet(new long[]{0x0000000000000000L,0x0000000000000000L,0x0000000000028000L});
	public static final BitSet FOLLOW_143_in_selectionFunctionArgs1248 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_156_in_selectCountClause1271 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_INTEGER_in_selectCountClause1293 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_relation_in_whereClause1329 = new BitSet(new long[]{0x0000000010000002L});
	public static final BitSet FOLLOW_K_AND_in_whereClause1333 = new BitSet(new long[]{0x05D9BD7142100000L,0x004F63B7E0E08D69L,0x0000000000004001L});
	public static final BitSet FOLLOW_relation_in_whereClause1335 = new BitSet(new long[]{0x0000000010000002L});
	public static final BitSet FOLLOW_cident_in_orderByClause1366 = new BitSet(new long[]{0x0004000080000002L});
	public static final BitSet FOLLOW_K_ASC_in_orderByClause1371 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_K_DESC_in_orderByClause1375 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_K_INSERT_in_insertStatement1413 = new BitSet(new long[]{0x0000000000000000L,0x0000000000000010L});
	public static final BitSet FOLLOW_K_INTO_in_insertStatement1415 = new BitSet(new long[]{0x05D9BD7142100000L,0x804F63A7E0E08D69L,0x0000000000000001L});
	public static final BitSet FOLLOW_columnFamilyName_in_insertStatement1419 = new BitSet(new long[]{0x0000000000000000L,0x0000000000000000L,0x0000000000004000L});
	public static final BitSet FOLLOW_142_in_insertStatement1431 = new BitSet(new long[]{0x05D9BD7142100000L,0x004F63A7E0E08D69L,0x0000000000000001L});
	public static final BitSet FOLLOW_cident_in_insertStatement1435 = new BitSet(new long[]{0x0000000000000000L,0x0000000000000000L,0x0000000000028000L});
	public static final BitSet FOLLOW_145_in_insertStatement1442 = new BitSet(new long[]{0x05D9BD7142100000L,0x004F63A7E0E08D69L,0x0000000000000001L});
	public static final BitSet FOLLOW_cident_in_insertStatement1446 = new BitSet(new long[]{0x0000000000000000L,0x0000000000000000L,0x0000000000028000L});
	public static final BitSet FOLLOW_143_in_insertStatement1453 = new BitSet(new long[]{0x0000000000000000L,0x0002000000000000L});
	public static final BitSet FOLLOW_K_VALUES_in_insertStatement1463 = new BitSet(new long[]{0x0000000000000000L,0x0000000000000000L,0x0000000000004000L});
	public static final BitSet FOLLOW_142_in_insertStatement1475 = new BitSet(new long[]{0x05D9AD7142344040L,0x800F6337E0E2AD4BL,0x0000000048144048L});
	public static final BitSet FOLLOW_term_in_insertStatement1479 = new BitSet(new long[]{0x0000000000000000L,0x0000000000000000L,0x0000000000028000L});
	public static final BitSet FOLLOW_145_in_insertStatement1485 = new BitSet(new long[]{0x05D9AD7142344040L,0x800F6337E0E2AD4BL,0x0000000048144048L});
	public static final BitSet FOLLOW_term_in_insertStatement1489 = new BitSet(new long[]{0x0000000000000000L,0x0000000000000000L,0x0000000000028000L});
	public static final BitSet FOLLOW_143_in_insertStatement1496 = new BitSet(new long[]{0x2000000000000002L,0x0000800000000000L});
	public static final BitSet FOLLOW_K_IF_in_insertStatement1509 = new BitSet(new long[]{0x0000000000000000L,0x0000000000010000L});
	public static final BitSet FOLLOW_K_NOT_in_insertStatement1511 = new BitSet(new long[]{0x0040000000000000L});
	public static final BitSet FOLLOW_K_EXISTS_in_insertStatement1513 = new BitSet(new long[]{0x0000000000000002L,0x0000800000000000L});
	public static final BitSet FOLLOW_usingClause_in_insertStatement1530 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_K_USING_in_usingClause1560 = new BitSet(new long[]{0x0000000000000000L,0x0000008200000000L});
	public static final BitSet FOLLOW_usingClauseObjective_in_usingClause1562 = new BitSet(new long[]{0x0000000010000002L});
	public static final BitSet FOLLOW_K_AND_in_usingClause1567 = new BitSet(new long[]{0x0000000000000000L,0x0000008200000000L});
	public static final BitSet FOLLOW_usingClauseObjective_in_usingClause1569 = new BitSet(new long[]{0x0000000010000002L});
	public static final BitSet FOLLOW_K_TIMESTAMP_in_usingClauseObjective1591 = new BitSet(new long[]{0x0000000000200000L,0x8000000000000000L,0x0000000000100000L});
	public static final BitSet FOLLOW_intValue_in_usingClauseObjective1595 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_K_TTL_in_usingClauseObjective1605 = new BitSet(new long[]{0x0000000000200000L,0x8000000000000000L,0x0000000000100000L});
	public static final BitSet FOLLOW_intValue_in_usingClauseObjective1609 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_K_UPDATE_in_updateStatement1643 = new BitSet(new long[]{0x05D9BD7142100000L,0x804F63A7E0E08D69L,0x0000000000000001L});
	public static final BitSet FOLLOW_columnFamilyName_in_updateStatement1647 = new BitSet(new long[]{0x0000000000000000L,0x0000800010000000L});
	public static final BitSet FOLLOW_usingClause_in_updateStatement1657 = new BitSet(new long[]{0x0000000000000000L,0x0000000010000000L});
	public static final BitSet FOLLOW_K_SET_in_updateStatement1669 = new BitSet(new long[]{0x05D9BD7142100000L,0x004F63A7E0E08D69L,0x0000000000000001L});
	public static final BitSet FOLLOW_columnOperation_in_updateStatement1671 = new BitSet(new long[]{0x0000000000000000L,0x0010000000000000L,0x0000000000020000L});
	public static final BitSet FOLLOW_145_in_updateStatement1675 = new BitSet(new long[]{0x05D9BD7142100000L,0x004F63A7E0E08D69L,0x0000000000000001L});
	public static final BitSet FOLLOW_columnOperation_in_updateStatement1677 = new BitSet(new long[]{0x0000000000000000L,0x0010000000000000L,0x0000000000020000L});
	public static final BitSet FOLLOW_K_WHERE_in_updateStatement1688 = new BitSet(new long[]{0x05D9BD7142100000L,0x004F63B7E0E08D69L,0x0000000000004001L});
	public static final BitSet FOLLOW_whereClause_in_updateStatement1692 = new BitSet(new long[]{0x2000000000000002L});
	public static final BitSet FOLLOW_K_IF_in_updateStatement1702 = new BitSet(new long[]{0x05D9BD7142100000L,0x004F63A7E0E08D69L,0x0000000000000001L});
	public static final BitSet FOLLOW_K_EXISTS_in_updateStatement1706 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_updateConditions_in_updateStatement1714 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_columnCondition_in_updateConditions1756 = new BitSet(new long[]{0x0000000010000002L});
	public static final BitSet FOLLOW_K_AND_in_updateConditions1761 = new BitSet(new long[]{0x05D9BD7142100000L,0x004F63A7E0E08D69L,0x0000000000000001L});
	public static final BitSet FOLLOW_columnCondition_in_updateConditions1763 = new BitSet(new long[]{0x0000000010000002L});
	public static final BitSet FOLLOW_K_DELETE_in_deleteStatement1800 = new BitSet(new long[]{0x07D9BD7142100000L,0x004F63A7E0E08D69L,0x0000000000000001L});
	public static final BitSet FOLLOW_deleteSelection_in_deleteStatement1806 = new BitSet(new long[]{0x0200000000000000L});
	public static final BitSet FOLLOW_K_FROM_in_deleteStatement1819 = new BitSet(new long[]{0x05D9BD7142100000L,0x804F63A7E0E08D69L,0x0000000000000001L});
	public static final BitSet FOLLOW_columnFamilyName_in_deleteStatement1823 = new BitSet(new long[]{0x0000000000000000L,0x0010800000000000L});
	public static final BitSet FOLLOW_usingClauseDelete_in_deleteStatement1833 = new BitSet(new long[]{0x0000000000000000L,0x0010000000000000L});
	public static final BitSet FOLLOW_K_WHERE_in_deleteStatement1845 = new BitSet(new long[]{0x05D9BD7142100000L,0x004F63B7E0E08D69L,0x0000000000004001L});
	public static final BitSet FOLLOW_whereClause_in_deleteStatement1849 = new BitSet(new long[]{0x2000000000000002L});
	public static final BitSet FOLLOW_K_IF_in_deleteStatement1859 = new BitSet(new long[]{0x05D9BD7142100000L,0x004F63A7E0E08D69L,0x0000000000000001L});
	public static final BitSet FOLLOW_K_EXISTS_in_deleteStatement1863 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_updateConditions_in_deleteStatement1871 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_deleteOp_in_deleteSelection1918 = new BitSet(new long[]{0x0000000000000002L,0x0000000000000000L,0x0000000000020000L});
	public static final BitSet FOLLOW_145_in_deleteSelection1933 = new BitSet(new long[]{0x05D9BD7142100000L,0x004F63A7E0E08D69L,0x0000000000000001L});
	public static final BitSet FOLLOW_deleteOp_in_deleteSelection1937 = new BitSet(new long[]{0x0000000000000002L,0x0000000000000000L,0x0000000000020000L});
	public static final BitSet FOLLOW_cident_in_deleteOp1964 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_cident_in_deleteOp1991 = new BitSet(new long[]{0x0000000000000000L,0x0000000000000000L,0x0000000008000000L});
	public static final BitSet FOLLOW_155_in_deleteOp1993 = new BitSet(new long[]{0x05D9AD7142344040L,0x800F6337E0E2AD4BL,0x0000000048144048L});
	public static final BitSet FOLLOW_term_in_deleteOp1997 = new BitSet(new long[]{0x0000000000000000L,0x0000000000000000L,0x0000000020000000L});
	public static final BitSet FOLLOW_157_in_deleteOp1999 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_K_USING_in_usingClauseDelete2019 = new BitSet(new long[]{0x0000000000000000L,0x0000000200000000L});
	public static final BitSet FOLLOW_K_TIMESTAMP_in_usingClauseDelete2021 = new BitSet(new long[]{0x0000000000200000L,0x8000000000000000L,0x0000000000100000L});
	public static final BitSet FOLLOW_intValue_in_usingClauseDelete2025 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_K_BEGIN_in_batchStatement2059 = new BitSet(new long[]{0x0000200400000000L,0x0000040000000000L});
	public static final BitSet FOLLOW_K_UNLOGGED_in_batchStatement2069 = new BitSet(new long[]{0x0000000400000000L});
	public static final BitSet FOLLOW_K_COUNTER_in_batchStatement2075 = new BitSet(new long[]{0x0000000400000000L});
	public static final BitSet FOLLOW_K_BATCH_in_batchStatement2088 = new BitSet(new long[]{0x0002000020000000L,0x0000880000000004L});
	public static final BitSet FOLLOW_usingClause_in_batchStatement2092 = new BitSet(new long[]{0x0002000020000000L,0x0000080000000004L});
	public static final BitSet FOLLOW_batchStatementObjective_in_batchStatement2112 = new BitSet(new long[]{0x0002000020000000L,0x0000080000000004L,0x0000000000200000L});
	public static final BitSet FOLLOW_149_in_batchStatement2114 = new BitSet(new long[]{0x0002000020000000L,0x0000080000000004L});
	public static final BitSet FOLLOW_K_APPLY_in_batchStatement2128 = new BitSet(new long[]{0x0000000400000000L});
	public static final BitSet FOLLOW_K_BATCH_in_batchStatement2130 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_insertStatement_in_batchStatementObjective2161 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_updateStatement_in_batchStatementObjective2174 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_deleteStatement_in_batchStatementObjective2187 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_K_CREATE_in_createKeyspaceStatement2222 = new BitSet(new long[]{0x0000000000000000L,0x0000000000000080L});
	public static final BitSet FOLLOW_K_KEYSPACE_in_createKeyspaceStatement2224 = new BitSet(new long[]{0x25D9BD7142100000L,0x804F63A7E0E08D69L,0x0000000000000001L});
	public static final BitSet FOLLOW_K_IF_in_createKeyspaceStatement2227 = new BitSet(new long[]{0x0000000000000000L,0x0000000000010000L});
	public static final BitSet FOLLOW_K_NOT_in_createKeyspaceStatement2229 = new BitSet(new long[]{0x0040000000000000L});
	public static final BitSet FOLLOW_K_EXISTS_in_createKeyspaceStatement2231 = new BitSet(new long[]{0x05D9BD7142100000L,0x804F63A7E0E08D69L,0x0000000000000001L});
	public static final BitSet FOLLOW_keyspaceName_in_createKeyspaceStatement2240 = new BitSet(new long[]{0x0000000000000000L,0x0020000000000000L});
	public static final BitSet FOLLOW_K_WITH_in_createKeyspaceStatement2248 = new BitSet(new long[]{0x05D9BD7142100000L,0x004F63A7E0E08D69L,0x0000000000000001L});
	public static final BitSet FOLLOW_properties_in_createKeyspaceStatement2250 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_K_CREATE_in_createTableStatement2285 = new BitSet(new long[]{0x0000020000000000L});
	public static final BitSet FOLLOW_K_COLUMNFAMILY_in_createTableStatement2287 = new BitSet(new long[]{0x25D9BD7142100000L,0x804F63A7E0E08D69L,0x0000000000000001L});
	public static final BitSet FOLLOW_K_IF_in_createTableStatement2290 = new BitSet(new long[]{0x0000000000000000L,0x0000000000010000L});
	public static final BitSet FOLLOW_K_NOT_in_createTableStatement2292 = new BitSet(new long[]{0x0040000000000000L});
	public static final BitSet FOLLOW_K_EXISTS_in_createTableStatement2294 = new BitSet(new long[]{0x05D9BD7142100000L,0x804F63A7E0E08D69L,0x0000000000000001L});
	public static final BitSet FOLLOW_columnFamilyName_in_createTableStatement2309 = new BitSet(new long[]{0x0000000000000000L,0x0000000000000000L,0x0000000000004000L});
	public static final BitSet FOLLOW_cfamDefinition_in_createTableStatement2319 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_142_in_cfamDefinition2338 = new BitSet(new long[]{0x05D9BD7142100000L,0x004F63A7E1E08D69L,0x0000000000000001L});
	public static final BitSet FOLLOW_cfamColumns_in_cfamDefinition2340 = new BitSet(new long[]{0x0000000000000000L,0x0000000000000000L,0x0000000000028000L});
	public static final BitSet FOLLOW_145_in_cfamDefinition2345 = new BitSet(new long[]{0x05D9BD7142100000L,0x004F63A7E1E08D69L,0x0000000000028001L});
	public static final BitSet FOLLOW_cfamColumns_in_cfamDefinition2347 = new BitSet(new long[]{0x0000000000000000L,0x0000000000000000L,0x0000000000028000L});
	public static final BitSet FOLLOW_143_in_cfamDefinition2354 = new BitSet(new long[]{0x0000000000000002L,0x0020000000000000L});
	public static final BitSet FOLLOW_K_WITH_in_cfamDefinition2364 = new BitSet(new long[]{0x05D9BD7142100000L,0x004F63A7E0E08D69L,0x0000000000000001L});
	public static final BitSet FOLLOW_cfamProperty_in_cfamDefinition2366 = new BitSet(new long[]{0x0000000010000002L});
	public static final BitSet FOLLOW_K_AND_in_cfamDefinition2371 = new BitSet(new long[]{0x05D9BD7142100000L,0x004F63A7E0E08D69L,0x0000000000000001L});
	public static final BitSet FOLLOW_cfamProperty_in_cfamDefinition2373 = new BitSet(new long[]{0x0000000010000002L});
	public static final BitSet FOLLOW_ident_in_cfamColumns2399 = new BitSet(new long[]{0x05D9BD7142100000L,0x004F63A7F0E08D69L,0x0000000000000009L});
	public static final BitSet FOLLOW_comparatorType_in_cfamColumns2403 = new BitSet(new long[]{0x0000000000000002L,0x0000000021000000L});
	public static final BitSet FOLLOW_K_STATIC_in_cfamColumns2408 = new BitSet(new long[]{0x0000000000000002L,0x0000000001000000L});
	public static final BitSet FOLLOW_K_PRIMARY_in_cfamColumns2425 = new BitSet(new long[]{0x0000000000000000L,0x0000000000000020L});
	public static final BitSet FOLLOW_K_KEY_in_cfamColumns2427 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_K_PRIMARY_in_cfamColumns2439 = new BitSet(new long[]{0x0000000000000000L,0x0000000000000020L});
	public static final BitSet FOLLOW_K_KEY_in_cfamColumns2441 = new BitSet(new long[]{0x0000000000000000L,0x0000000000000000L,0x0000000000004000L});
	public static final BitSet FOLLOW_142_in_cfamColumns2443 = new BitSet(new long[]{0x05D9BD7142100000L,0x004F63A7E0E08D69L,0x0000000000004001L});
	public static final BitSet FOLLOW_pkDef_in_cfamColumns2445 = new BitSet(new long[]{0x0000000000000000L,0x0000000000000000L,0x0000000000028000L});
	public static final BitSet FOLLOW_145_in_cfamColumns2449 = new BitSet(new long[]{0x05D9BD7142100000L,0x004F63A7E0E08D69L,0x0000000000000001L});
	public static final BitSet FOLLOW_ident_in_cfamColumns2453 = new BitSet(new long[]{0x0000000000000000L,0x0000000000000000L,0x0000000000028000L});
	public static final BitSet FOLLOW_143_in_cfamColumns2460 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_ident_in_pkDef2480 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_142_in_pkDef2490 = new BitSet(new long[]{0x05D9BD7142100000L,0x004F63A7E0E08D69L,0x0000000000000001L});
	public static final BitSet FOLLOW_ident_in_pkDef2496 = new BitSet(new long[]{0x0000000000000000L,0x0000000000000000L,0x0000000000028000L});
	public static final BitSet FOLLOW_145_in_pkDef2502 = new BitSet(new long[]{0x05D9BD7142100000L,0x004F63A7E0E08D69L,0x0000000000000001L});
	public static final BitSet FOLLOW_ident_in_pkDef2506 = new BitSet(new long[]{0x0000000000000000L,0x0000000000000000L,0x0000000000028000L});
	public static final BitSet FOLLOW_143_in_pkDef2513 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_property_in_cfamProperty2533 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_K_COMPACT_in_cfamProperty2542 = new BitSet(new long[]{0x0000000000000000L,0x0000000040000000L});
	public static final BitSet FOLLOW_K_STORAGE_in_cfamProperty2544 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_K_CLUSTERING_in_cfamProperty2554 = new BitSet(new long[]{0x0000000000000000L,0x0000000000100000L});
	public static final BitSet FOLLOW_K_ORDER_in_cfamProperty2556 = new BitSet(new long[]{0x0000008000000000L});
	public static final BitSet FOLLOW_K_BY_in_cfamProperty2558 = new BitSet(new long[]{0x0000000000000000L,0x0000000000000000L,0x0000000000004000L});
	public static final BitSet FOLLOW_142_in_cfamProperty2560 = new BitSet(new long[]{0x05D9BD7142100000L,0x004F63A7E0E08D69L,0x0000000000000001L});
	public static final BitSet FOLLOW_cfamOrdering_in_cfamProperty2562 = new BitSet(new long[]{0x0000000000000000L,0x0000000000000000L,0x0000000000028000L});
	public static final BitSet FOLLOW_145_in_cfamProperty2566 = new BitSet(new long[]{0x05D9BD7142100000L,0x004F63A7E0E08D69L,0x0000000000000001L});
	public static final BitSet FOLLOW_cfamOrdering_in_cfamProperty2568 = new BitSet(new long[]{0x0000000000000000L,0x0000000000000000L,0x0000000000028000L});
	public static final BitSet FOLLOW_143_in_cfamProperty2573 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_ident_in_cfamOrdering2601 = new BitSet(new long[]{0x0004000080000000L});
	public static final BitSet FOLLOW_K_ASC_in_cfamOrdering2604 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_K_DESC_in_cfamOrdering2608 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_K_CREATE_in_createTypeStatement2647 = new BitSet(new long[]{0x0000000000000000L,0x0000020000000000L});
	public static final BitSet FOLLOW_K_TYPE_in_createTypeStatement2649 = new BitSet(new long[]{0x25D9BD7142100000L,0x004F63A7E0E08D69L,0x0000000000000001L});
	public static final BitSet FOLLOW_K_IF_in_createTypeStatement2652 = new BitSet(new long[]{0x0000000000000000L,0x0000000000010000L});
	public static final BitSet FOLLOW_K_NOT_in_createTypeStatement2654 = new BitSet(new long[]{0x0040000000000000L});
	public static final BitSet FOLLOW_K_EXISTS_in_createTypeStatement2656 = new BitSet(new long[]{0x05D9BD7142100000L,0x004F63A7E0E08D69L,0x0000000000000001L});
	public static final BitSet FOLLOW_userTypeName_in_createTypeStatement2674 = new BitSet(new long[]{0x0000000000000000L,0x0000000000000000L,0x0000000000004000L});
	public static final BitSet FOLLOW_142_in_createTypeStatement2687 = new BitSet(new long[]{0x05D9BD7142100000L,0x004F63A7E0E08D69L,0x0000000000000001L});
	public static final BitSet FOLLOW_typeColumns_in_createTypeStatement2689 = new BitSet(new long[]{0x0000000000000000L,0x0000000000000000L,0x0000000000028000L});
	public static final BitSet FOLLOW_145_in_createTypeStatement2694 = new BitSet(new long[]{0x05D9BD7142100000L,0x004F63A7E0E08D69L,0x0000000000028001L});
	public static final BitSet FOLLOW_typeColumns_in_createTypeStatement2696 = new BitSet(new long[]{0x0000000000000000L,0x0000000000000000L,0x0000000000028000L});
	public static final BitSet FOLLOW_143_in_createTypeStatement2703 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_ident_in_typeColumns2723 = new BitSet(new long[]{0x05D9BD7142100000L,0x004F63A7F0E08D69L,0x0000000000000009L});
	public static final BitSet FOLLOW_comparatorType_in_typeColumns2727 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_K_CREATE_in_createIndexStatement2762 = new BitSet(new long[]{0x8000800000000000L});
	public static final BitSet FOLLOW_K_CUSTOM_in_createIndexStatement2765 = new BitSet(new long[]{0x8000000000000000L});
	public static final BitSet FOLLOW_K_INDEX_in_createIndexStatement2771 = new BitSet(new long[]{0x25D9BD7142100000L,0x804F63A7E0E88D69L,0x0000000000000001L});
	public static final BitSet FOLLOW_K_IF_in_createIndexStatement2774 = new BitSet(new long[]{0x0000000000000000L,0x0000000000010000L});
	public static final BitSet FOLLOW_K_NOT_in_createIndexStatement2776 = new BitSet(new long[]{0x0040000000000000L});
	public static final BitSet FOLLOW_K_EXISTS_in_createIndexStatement2778 = new BitSet(new long[]{0x05D9BD7142100000L,0x804F63A7E0E88D69L,0x0000000000000001L});
	public static final BitSet FOLLOW_idxName_in_createIndexStatement2794 = new BitSet(new long[]{0x0000000000000000L,0x0000000000080000L});
	public static final BitSet FOLLOW_K_ON_in_createIndexStatement2799 = new BitSet(new long[]{0x05D9BD7142100000L,0x804F63A7E0E08D69L,0x0000000000000001L});
	public static final BitSet FOLLOW_columnFamilyName_in_createIndexStatement2803 = new BitSet(new long[]{0x0000000000000000L,0x0000000000000000L,0x0000000000004000L});
	public static final BitSet FOLLOW_142_in_createIndexStatement2805 = new BitSet(new long[]{0x0DD9BD7142100000L,0x004F63A7E0E08D69L,0x0000000000000001L});
	public static final BitSet FOLLOW_indexIdent_in_createIndexStatement2809 = new BitSet(new long[]{0x0000000000000000L,0x0000000000000000L,0x0000000000008000L});
	public static final BitSet FOLLOW_143_in_createIndexStatement2811 = new BitSet(new long[]{0x0000000000000002L,0x0020800000000000L});
	public static final BitSet FOLLOW_K_USING_in_createIndexStatement2822 = new BitSet(new long[]{0x0000000000000000L,0x0000000000000000L,0x0000000000000008L});
	public static final BitSet FOLLOW_STRING_LITERAL_in_createIndexStatement2826 = new BitSet(new long[]{0x0000000000000002L,0x0020000000000000L});
	public static final BitSet FOLLOW_K_WITH_in_createIndexStatement2841 = new BitSet(new long[]{0x05D9BD7142100000L,0x004F63A7E0E08D69L,0x0000000000000001L});
	public static final BitSet FOLLOW_properties_in_createIndexStatement2843 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_cident_in_indexIdent2877 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_K_KEYS_in_indexIdent2902 = new BitSet(new long[]{0x0000000000000000L,0x0000000000000000L,0x0000000000004000L});
	public static final BitSet FOLLOW_142_in_indexIdent2904 = new BitSet(new long[]{0x05D9BD7142100000L,0x004F63A7E0E08D69L,0x0000000000000001L});
	public static final BitSet FOLLOW_cident_in_indexIdent2908 = new BitSet(new long[]{0x0000000000000000L,0x0000000000000000L,0x0000000000008000L});
	public static final BitSet FOLLOW_143_in_indexIdent2910 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_K_FULL_in_indexIdent2920 = new BitSet(new long[]{0x0000000000000000L,0x0000000000000000L,0x0000000000004000L});
	public static final BitSet FOLLOW_142_in_indexIdent2922 = new BitSet(new long[]{0x05D9BD7142100000L,0x004F63A7E0E08D69L,0x0000000000000001L});
	public static final BitSet FOLLOW_cident_in_indexIdent2926 = new BitSet(new long[]{0x0000000000000000L,0x0000000000000000L,0x0000000000008000L});
	public static final BitSet FOLLOW_143_in_indexIdent2928 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_K_CREATE_in_createTriggerStatement2963 = new BitSet(new long[]{0x0000000000000000L,0x0000002000000000L});
	public static final BitSet FOLLOW_K_TRIGGER_in_createTriggerStatement2965 = new BitSet(new long[]{0x25D9BD7142100000L,0x004F63A7E0E08D69L,0x0000000000000001L});
	public static final BitSet FOLLOW_K_IF_in_createTriggerStatement2968 = new BitSet(new long[]{0x0000000000000000L,0x0000000000010000L});
	public static final BitSet FOLLOW_K_NOT_in_createTriggerStatement2970 = new BitSet(new long[]{0x0040000000000000L});
	public static final BitSet FOLLOW_K_EXISTS_in_createTriggerStatement2972 = new BitSet(new long[]{0x05D9BD7142100000L,0x004F63A7E0E08D69L,0x0000000000000001L});
	public static final BitSet FOLLOW_cident_in_createTriggerStatement2982 = new BitSet(new long[]{0x0000000000000000L,0x0000000000080000L});
	public static final BitSet FOLLOW_K_ON_in_createTriggerStatement2993 = new BitSet(new long[]{0x05D9BD7142100000L,0x804F63A7E0E08D69L,0x0000000000000001L});
	public static final BitSet FOLLOW_columnFamilyName_in_createTriggerStatement2997 = new BitSet(new long[]{0x0000000000000000L,0x0000800000000000L});
	public static final BitSet FOLLOW_K_USING_in_createTriggerStatement2999 = new BitSet(new long[]{0x0000000000000000L,0x0000000000000000L,0x0000000000000008L});
	public static final BitSet FOLLOW_STRING_LITERAL_in_createTriggerStatement3003 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_K_DROP_in_dropTriggerStatement3044 = new BitSet(new long[]{0x0000000000000000L,0x0000002000000000L});
	public static final BitSet FOLLOW_K_TRIGGER_in_dropTriggerStatement3046 = new BitSet(new long[]{0x25D9BD7142100000L,0x004F63A7E0E08D69L,0x0000000000000001L});
	public static final BitSet FOLLOW_K_IF_in_dropTriggerStatement3049 = new BitSet(new long[]{0x0040000000000000L});
	public static final BitSet FOLLOW_K_EXISTS_in_dropTriggerStatement3051 = new BitSet(new long[]{0x05D9BD7142100000L,0x004F63A7E0E08D69L,0x0000000000000001L});
	public static final BitSet FOLLOW_cident_in_dropTriggerStatement3061 = new BitSet(new long[]{0x0000000000000000L,0x0000000000080000L});
	public static final BitSet FOLLOW_K_ON_in_dropTriggerStatement3064 = new BitSet(new long[]{0x05D9BD7142100000L,0x804F63A7E0E08D69L,0x0000000000000001L});
	public static final BitSet FOLLOW_columnFamilyName_in_dropTriggerStatement3068 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_K_ALTER_in_alterKeyspaceStatement3108 = new BitSet(new long[]{0x0000000000000000L,0x0000000000000080L});
	public static final BitSet FOLLOW_K_KEYSPACE_in_alterKeyspaceStatement3110 = new BitSet(new long[]{0x05D9BD7142100000L,0x804F63A7E0E08D69L,0x0000000000000001L});
	public static final BitSet FOLLOW_keyspaceName_in_alterKeyspaceStatement3114 = new BitSet(new long[]{0x0000000000000000L,0x0020000000000000L});
	public static final BitSet FOLLOW_K_WITH_in_alterKeyspaceStatement3124 = new BitSet(new long[]{0x05D9BD7142100000L,0x004F63A7E0E08D69L,0x0000000000000001L});
	public static final BitSet FOLLOW_properties_in_alterKeyspaceStatement3126 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_K_ALTER_in_alterTableStatement3162 = new BitSet(new long[]{0x0000020000000000L});
	public static final BitSet FOLLOW_K_COLUMNFAMILY_in_alterTableStatement3164 = new BitSet(new long[]{0x05D9BD7142100000L,0x804F63A7E0E08D69L,0x0000000000000001L});
	public static final BitSet FOLLOW_columnFamilyName_in_alterTableStatement3168 = new BitSet(new long[]{0x0020000009000000L,0x0020000002000000L});
	public static final BitSet FOLLOW_K_ALTER_in_alterTableStatement3182 = new BitSet(new long[]{0x05D9BD7142100000L,0x004F63A7E0E08D69L,0x0000000000000001L});
	public static final BitSet FOLLOW_cident_in_alterTableStatement3186 = new BitSet(new long[]{0x0000000000000000L,0x0000020000000000L});
	public static final BitSet FOLLOW_K_TYPE_in_alterTableStatement3188 = new BitSet(new long[]{0x05D9BD7142100000L,0x004F63A7F0E08D69L,0x0000000000000009L});
	public static final BitSet FOLLOW_comparatorType_in_alterTableStatement3192 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_K_ADD_in_alterTableStatement3208 = new BitSet(new long[]{0x05D9BD7142100000L,0x004F63A7E0E08D69L,0x0000000000000001L});
	public static final BitSet FOLLOW_cident_in_alterTableStatement3214 = new BitSet(new long[]{0x05D9BD7142100000L,0x004F63A7F0E08D69L,0x0000000000000009L});
	public static final BitSet FOLLOW_comparatorType_in_alterTableStatement3218 = new BitSet(new long[]{0x0000000000000002L,0x0000000020000000L});
	public static final BitSet FOLLOW_K_STATIC_in_alterTableStatement3223 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_K_DROP_in_alterTableStatement3241 = new BitSet(new long[]{0x05D9BD7142100000L,0x004F63A7E0E08D69L,0x0000000000000001L});
	public static final BitSet FOLLOW_cident_in_alterTableStatement3246 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_K_WITH_in_alterTableStatement3286 = new BitSet(new long[]{0x05D9BD7142100000L,0x004F63A7E0E08D69L,0x0000000000000001L});
	public static final BitSet FOLLOW_properties_in_alterTableStatement3289 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_K_RENAME_in_alterTableStatement3322 = new BitSet(new long[]{0x05D9BD7142100000L,0x004F63A7E0E08D69L,0x0000000000000001L});
	public static final BitSet FOLLOW_cident_in_alterTableStatement3376 = new BitSet(new long[]{0x0000000000000000L,0x0000000800000000L});
	public static final BitSet FOLLOW_K_TO_in_alterTableStatement3378 = new BitSet(new long[]{0x05D9BD7142100000L,0x004F63A7E0E08D69L,0x0000000000000001L});
	public static final BitSet FOLLOW_cident_in_alterTableStatement3382 = new BitSet(new long[]{0x0000000010000002L});
	public static final BitSet FOLLOW_K_AND_in_alterTableStatement3403 = new BitSet(new long[]{0x05D9BD7142100000L,0x004F63A7E0E08D69L,0x0000000000000001L});
	public static final BitSet FOLLOW_cident_in_alterTableStatement3407 = new BitSet(new long[]{0x0000000000000000L,0x0000000800000000L});
	public static final BitSet FOLLOW_K_TO_in_alterTableStatement3409 = new BitSet(new long[]{0x05D9BD7142100000L,0x004F63A7E0E08D69L,0x0000000000000001L});
	public static final BitSet FOLLOW_cident_in_alterTableStatement3413 = new BitSet(new long[]{0x0000000010000002L});
	public static final BitSet FOLLOW_K_ALTER_in_alterTypeStatement3459 = new BitSet(new long[]{0x0000000000000000L,0x0000020000000000L});
	public static final BitSet FOLLOW_K_TYPE_in_alterTypeStatement3461 = new BitSet(new long[]{0x05D9BD7142100000L,0x004F63A7E0E08D69L,0x0000000000000001L});
	public static final BitSet FOLLOW_userTypeName_in_alterTypeStatement3465 = new BitSet(new long[]{0x0000000009000000L,0x0000000002000000L});
	public static final BitSet FOLLOW_K_ALTER_in_alterTypeStatement3479 = new BitSet(new long[]{0x05D9BD7142100000L,0x004F63A7E0E08D69L,0x0000000000000001L});
	public static final BitSet FOLLOW_ident_in_alterTypeStatement3483 = new BitSet(new long[]{0x0000000000000000L,0x0000020000000000L});
	public static final BitSet FOLLOW_K_TYPE_in_alterTypeStatement3485 = new BitSet(new long[]{0x05D9BD7142100000L,0x004F63A7F0E08D69L,0x0000000000000009L});
	public static final BitSet FOLLOW_comparatorType_in_alterTypeStatement3489 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_K_ADD_in_alterTypeStatement3505 = new BitSet(new long[]{0x05D9BD7142100000L,0x004F63A7E0E08D69L,0x0000000000000001L});
	public static final BitSet FOLLOW_ident_in_alterTypeStatement3511 = new BitSet(new long[]{0x05D9BD7142100000L,0x004F63A7F0E08D69L,0x0000000000000009L});
	public static final BitSet FOLLOW_comparatorType_in_alterTypeStatement3515 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_K_RENAME_in_alterTypeStatement3538 = new BitSet(new long[]{0x05D9BD7142100000L,0x004F63A7E0E08D69L,0x0000000000000001L});
	public static final BitSet FOLLOW_ident_in_alterTypeStatement3576 = new BitSet(new long[]{0x0000000000000000L,0x0000000800000000L});
	public static final BitSet FOLLOW_K_TO_in_alterTypeStatement3578 = new BitSet(new long[]{0x05D9BD7142100000L,0x004F63A7E0E08D69L,0x0000000000000001L});
	public static final BitSet FOLLOW_ident_in_alterTypeStatement3582 = new BitSet(new long[]{0x0000000010000002L});
	public static final BitSet FOLLOW_K_AND_in_alterTypeStatement3605 = new BitSet(new long[]{0x05D9BD7142100000L,0x004F63A7E0E08D69L,0x0000000000000001L});
	public static final BitSet FOLLOW_ident_in_alterTypeStatement3609 = new BitSet(new long[]{0x0000000000000000L,0x0000000800000000L});
	public static final BitSet FOLLOW_K_TO_in_alterTypeStatement3611 = new BitSet(new long[]{0x05D9BD7142100000L,0x004F63A7E0E08D69L,0x0000000000000001L});
	public static final BitSet FOLLOW_ident_in_alterTypeStatement3615 = new BitSet(new long[]{0x0000000010000002L});
	public static final BitSet FOLLOW_K_DROP_in_dropKeyspaceStatement3682 = new BitSet(new long[]{0x0000000000000000L,0x0000000000000080L});
	public static final BitSet FOLLOW_K_KEYSPACE_in_dropKeyspaceStatement3684 = new BitSet(new long[]{0x25D9BD7142100000L,0x804F63A7E0E08D69L,0x0000000000000001L});
	public static final BitSet FOLLOW_K_IF_in_dropKeyspaceStatement3687 = new BitSet(new long[]{0x0040000000000000L});
	public static final BitSet FOLLOW_K_EXISTS_in_dropKeyspaceStatement3689 = new BitSet(new long[]{0x05D9BD7142100000L,0x804F63A7E0E08D69L,0x0000000000000001L});
	public static final BitSet FOLLOW_keyspaceName_in_dropKeyspaceStatement3698 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_K_DROP_in_dropTableStatement3732 = new BitSet(new long[]{0x0000020000000000L});
	public static final BitSet FOLLOW_K_COLUMNFAMILY_in_dropTableStatement3734 = new BitSet(new long[]{0x25D9BD7142100000L,0x804F63A7E0E08D69L,0x0000000000000001L});
	public static final BitSet FOLLOW_K_IF_in_dropTableStatement3737 = new BitSet(new long[]{0x0040000000000000L});
	public static final BitSet FOLLOW_K_EXISTS_in_dropTableStatement3739 = new BitSet(new long[]{0x05D9BD7142100000L,0x804F63A7E0E08D69L,0x0000000000000001L});
	public static final BitSet FOLLOW_columnFamilyName_in_dropTableStatement3748 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_K_DROP_in_dropTypeStatement3782 = new BitSet(new long[]{0x0000000000000000L,0x0000020000000000L});
	public static final BitSet FOLLOW_K_TYPE_in_dropTypeStatement3784 = new BitSet(new long[]{0x25D9BD7142100000L,0x004F63A7E0E08D69L,0x0000000000000001L});
	public static final BitSet FOLLOW_K_IF_in_dropTypeStatement3787 = new BitSet(new long[]{0x0040000000000000L});
	public static final BitSet FOLLOW_K_EXISTS_in_dropTypeStatement3789 = new BitSet(new long[]{0x05D9BD7142100000L,0x004F63A7E0E08D69L,0x0000000000000001L});
	public static final BitSet FOLLOW_userTypeName_in_dropTypeStatement3798 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_K_DROP_in_dropIndexStatement3832 = new BitSet(new long[]{0x8000000000000000L});
	public static final BitSet FOLLOW_K_INDEX_in_dropIndexStatement3834 = new BitSet(new long[]{0x25D9BD7142100000L,0x804F63A7E0E08D69L,0x0000000000000001L});
	public static final BitSet FOLLOW_K_IF_in_dropIndexStatement3837 = new BitSet(new long[]{0x0040000000000000L});
	public static final BitSet FOLLOW_K_EXISTS_in_dropIndexStatement3839 = new BitSet(new long[]{0x05D9BD7142100000L,0x804F63A7E0E08D69L,0x0000000000000001L});
	public static final BitSet FOLLOW_indexName_in_dropIndexStatement3848 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_K_TRUNCATE_in_truncateStatement3879 = new BitSet(new long[]{0x05D9BD7142100000L,0x804F63A7E0E08D69L,0x0000000000000001L});
	public static final BitSet FOLLOW_columnFamilyName_in_truncateStatement3883 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_K_GRANT_in_grantStatement3908 = new BitSet(new long[]{0x002040020A000000L,0x0000000008001000L});
	public static final BitSet FOLLOW_permissionOrAll_in_grantStatement3920 = new BitSet(new long[]{0x0000000000000000L,0x0000000000080000L});
	public static final BitSet FOLLOW_K_ON_in_grantStatement3928 = new BitSet(new long[]{0x05D9BF7142100000L,0x804F63A7E0E08DE9L,0x0000000000000001L});
	public static final BitSet FOLLOW_resource_in_grantStatement3940 = new BitSet(new long[]{0x0000000000000000L,0x0000000800000000L});
	public static final BitSet FOLLOW_K_TO_in_grantStatement3948 = new BitSet(new long[]{0x0000000000100000L,0x0000000000000000L,0x0000000000000008L});
	public static final BitSet FOLLOW_username_in_grantStatement3960 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_K_REVOKE_in_revokeStatement3991 = new BitSet(new long[]{0x002040020A000000L,0x0000000008001000L});
	public static final BitSet FOLLOW_permissionOrAll_in_revokeStatement4003 = new BitSet(new long[]{0x0000000000000000L,0x0000000000080000L});
	public static final BitSet FOLLOW_K_ON_in_revokeStatement4011 = new BitSet(new long[]{0x05D9BF7142100000L,0x804F63A7E0E08DE9L,0x0000000000000001L});
	public static final BitSet FOLLOW_resource_in_revokeStatement4023 = new BitSet(new long[]{0x0200000000000000L});
	public static final BitSet FOLLOW_K_FROM_in_revokeStatement4031 = new BitSet(new long[]{0x0000000000100000L,0x0000000000000000L,0x0000000000000008L});
	public static final BitSet FOLLOW_username_in_revokeStatement4043 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_K_LIST_in_listPermissionsStatement4081 = new BitSet(new long[]{0x002040020A000000L,0x0000000008001000L});
	public static final BitSet FOLLOW_permissionOrAll_in_listPermissionsStatement4093 = new BitSet(new long[]{0x0000000000000002L,0x00000000000C4000L});
	public static final BitSet FOLLOW_K_ON_in_listPermissionsStatement4103 = new BitSet(new long[]{0x05D9BF7142100000L,0x804F63A7E0E08DE9L,0x0000000000000001L});
	public static final BitSet FOLLOW_resource_in_listPermissionsStatement4105 = new BitSet(new long[]{0x0000000000000002L,0x0000000000044000L});
	public static final BitSet FOLLOW_K_OF_in_listPermissionsStatement4120 = new BitSet(new long[]{0x0000000000100000L,0x0000000000000000L,0x0000000000000008L});
	public static final BitSet FOLLOW_username_in_listPermissionsStatement4122 = new BitSet(new long[]{0x0000000000000002L,0x0000000000004000L});
	public static final BitSet FOLLOW_K_NORECURSIVE_in_listPermissionsStatement4137 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_set_in_permission4173 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_K_ALL_in_permissionOrAll4222 = new BitSet(new long[]{0x0000000000000002L,0x0000000000800000L});
	public static final BitSet FOLLOW_K_PERMISSIONS_in_permissionOrAll4226 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_permission_in_permissionOrAll4247 = new BitSet(new long[]{0x0000000000000002L,0x0000000000400000L});
	public static final BitSet FOLLOW_K_PERMISSION_in_permissionOrAll4251 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_dataResource_in_resource4279 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_K_ALL_in_dataResource4302 = new BitSet(new long[]{0x0000000000000000L,0x0000000000000100L});
	public static final BitSet FOLLOW_K_KEYSPACES_in_dataResource4304 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_K_KEYSPACE_in_dataResource4314 = new BitSet(new long[]{0x05D9BD7142100000L,0x804F63A7E0E08D69L,0x0000000000000001L});
	public static final BitSet FOLLOW_keyspaceName_in_dataResource4320 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_K_COLUMNFAMILY_in_dataResource4332 = new BitSet(new long[]{0x05D9BD7142100000L,0x804F63A7E0E08D69L,0x0000000000000001L});
	public static final BitSet FOLLOW_columnFamilyName_in_dataResource4341 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_K_CREATE_in_createUserStatement4381 = new BitSet(new long[]{0x0000000000000000L,0x0000200000000000L});
	public static final BitSet FOLLOW_K_USER_in_createUserStatement4383 = new BitSet(new long[]{0x2000000000100000L,0x0000000000000000L,0x0000000000000008L});
	public static final BitSet FOLLOW_K_IF_in_createUserStatement4386 = new BitSet(new long[]{0x0000000000000000L,0x0000000000010000L});
	public static final BitSet FOLLOW_K_NOT_in_createUserStatement4388 = new BitSet(new long[]{0x0040000000000000L});
	public static final BitSet FOLLOW_K_EXISTS_in_createUserStatement4390 = new BitSet(new long[]{0x0000000000100000L,0x0000000000000000L,0x0000000000000008L});
	public static final BitSet FOLLOW_username_in_createUserStatement4396 = new BitSet(new long[]{0x0000000000000002L,0x0020000080008000L});
	public static final BitSet FOLLOW_K_WITH_in_createUserStatement4406 = new BitSet(new long[]{0x0000000000000000L,0x0000000000200000L});
	public static final BitSet FOLLOW_userOptions_in_createUserStatement4408 = new BitSet(new long[]{0x0000000000000002L,0x0000000080008000L});
	public static final BitSet FOLLOW_K_SUPERUSER_in_createUserStatement4422 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_K_NOSUPERUSER_in_createUserStatement4428 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_K_ALTER_in_alterUserStatement4473 = new BitSet(new long[]{0x0000000000000000L,0x0000200000000000L});
	public static final BitSet FOLLOW_K_USER_in_alterUserStatement4475 = new BitSet(new long[]{0x0000000000100000L,0x0000000000000000L,0x0000000000000008L});
	public static final BitSet FOLLOW_username_in_alterUserStatement4477 = new BitSet(new long[]{0x0000000000000002L,0x0020000080008000L});
	public static final BitSet FOLLOW_K_WITH_in_alterUserStatement4487 = new BitSet(new long[]{0x0000000000000000L,0x0000000000200000L});
	public static final BitSet FOLLOW_userOptions_in_alterUserStatement4489 = new BitSet(new long[]{0x0000000000000002L,0x0000000080008000L});
	public static final BitSet FOLLOW_K_SUPERUSER_in_alterUserStatement4503 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_K_NOSUPERUSER_in_alterUserStatement4509 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_K_DROP_in_dropUserStatement4554 = new BitSet(new long[]{0x0000000000000000L,0x0000200000000000L});
	public static final BitSet FOLLOW_K_USER_in_dropUserStatement4556 = new BitSet(new long[]{0x2000000000100000L,0x0000000000000000L,0x0000000000000008L});
	public static final BitSet FOLLOW_K_IF_in_dropUserStatement4559 = new BitSet(new long[]{0x0040000000000000L});
	public static final BitSet FOLLOW_K_EXISTS_in_dropUserStatement4561 = new BitSet(new long[]{0x0000000000100000L,0x0000000000000000L,0x0000000000000008L});
	public static final BitSet FOLLOW_username_in_dropUserStatement4567 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_K_LIST_in_listUsersStatement4592 = new BitSet(new long[]{0x0000000000000000L,0x0000400000000000L});
	public static final BitSet FOLLOW_K_USERS_in_listUsersStatement4594 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_userOption_in_userOptions4614 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_K_PASSWORD_in_userOption4635 = new BitSet(new long[]{0x0000000000000000L,0x0000000000000000L,0x0000000000000008L});
	public static final BitSet FOLLOW_STRING_LITERAL_in_userOption4639 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_IDENT_in_cident4670 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_QUOTED_NAME_in_cident4695 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_unreserved_keyword_in_cident4714 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_IDENT_in_ident4740 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_QUOTED_NAME_in_ident4765 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_unreserved_keyword_in_ident4784 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_ksName_in_keyspaceName4817 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_ksName_in_indexName4851 = new BitSet(new long[]{0x0000000000000000L,0x0000000000000000L,0x0000000000080000L});
	public static final BitSet FOLLOW_147_in_indexName4854 = new BitSet(new long[]{0x05D9BD7142100000L,0x804F63A7E0E08D69L,0x0000000000000001L});
	public static final BitSet FOLLOW_idxName_in_indexName4858 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_ksName_in_columnFamilyName4890 = new BitSet(new long[]{0x0000000000000000L,0x0000000000000000L,0x0000000000080000L});
	public static final BitSet FOLLOW_147_in_columnFamilyName4893 = new BitSet(new long[]{0x05D9BD7142100000L,0x804F63A7E0E08D69L,0x0000000000000001L});
	public static final BitSet FOLLOW_cfName_in_columnFamilyName4897 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_ident_in_userTypeName4922 = new BitSet(new long[]{0x0000000000000000L,0x0000000000000000L,0x0000000000080000L});
	public static final BitSet FOLLOW_147_in_userTypeName4924 = new BitSet(new long[]{0x04C88D0042100000L,0x00026320E0E08D60L,0x0000000000000001L});
	public static final BitSet FOLLOW_non_type_ident_in_userTypeName4930 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_IDENT_in_ksName4952 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_QUOTED_NAME_in_ksName4977 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_unreserved_keyword_in_ksName4996 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_QMARK_in_ksName5006 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_IDENT_in_cfName5028 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_QUOTED_NAME_in_cfName5053 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_unreserved_keyword_in_cfName5072 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_QMARK_in_cfName5082 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_IDENT_in_idxName5104 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_QUOTED_NAME_in_idxName5129 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_unreserved_keyword_in_idxName5148 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_QMARK_in_idxName5158 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_STRING_LITERAL_in_constant5183 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_INTEGER_in_constant5195 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_FLOAT_in_constant5214 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_BOOLEAN_in_constant5235 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_UUID_in_constant5254 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_HEXNUMBER_in_constant5276 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_146_in_constant5294 = new BitSet(new long[]{0x0000000000000000L,0x0000000000002002L});
	public static final BitSet FOLLOW_set_in_constant5303 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_158_in_mapLiteral5332 = new BitSet(new long[]{0x05D9AD7142344040L,0x800F6337E0E2AD4BL,0x00000000C8144048L});
	public static final BitSet FOLLOW_term_in_mapLiteral5350 = new BitSet(new long[]{0x0000000000000000L,0x0000000000000000L,0x0000000000100000L});
	public static final BitSet FOLLOW_148_in_mapLiteral5352 = new BitSet(new long[]{0x05D9AD7142344040L,0x800F6337E0E2AD4BL,0x0000000048144048L});
	public static final BitSet FOLLOW_term_in_mapLiteral5356 = new BitSet(new long[]{0x0000000000000000L,0x0000000000000000L,0x0000000080020000L});
	public static final BitSet FOLLOW_145_in_mapLiteral5362 = new BitSet(new long[]{0x05D9AD7142344040L,0x800F6337E0E2AD4BL,0x0000000048144048L});
	public static final BitSet FOLLOW_term_in_mapLiteral5366 = new BitSet(new long[]{0x0000000000000000L,0x0000000000000000L,0x0000000000100000L});
	public static final BitSet FOLLOW_148_in_mapLiteral5368 = new BitSet(new long[]{0x05D9AD7142344040L,0x800F6337E0E2AD4BL,0x0000000048144048L});
	public static final BitSet FOLLOW_term_in_mapLiteral5372 = new BitSet(new long[]{0x0000000000000000L,0x0000000000000000L,0x0000000080020000L});
	public static final BitSet FOLLOW_159_in_mapLiteral5388 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_148_in_setOrMapLiteral5412 = new BitSet(new long[]{0x05D9AD7142344040L,0x800F6337E0E2AD4BL,0x0000000048144048L});
	public static final BitSet FOLLOW_term_in_setOrMapLiteral5416 = new BitSet(new long[]{0x0000000000000002L,0x0000000000000000L,0x0000000000020000L});
	public static final BitSet FOLLOW_145_in_setOrMapLiteral5432 = new BitSet(new long[]{0x05D9AD7142344040L,0x800F6337E0E2AD4BL,0x0000000048144048L});
	public static final BitSet FOLLOW_term_in_setOrMapLiteral5436 = new BitSet(new long[]{0x0000000000000000L,0x0000000000000000L,0x0000000000100000L});
	public static final BitSet FOLLOW_148_in_setOrMapLiteral5438 = new BitSet(new long[]{0x05D9AD7142344040L,0x800F6337E0E2AD4BL,0x0000000048144048L});
	public static final BitSet FOLLOW_term_in_setOrMapLiteral5442 = new BitSet(new long[]{0x0000000000000002L,0x0000000000000000L,0x0000000000020000L});
	public static final BitSet FOLLOW_145_in_setOrMapLiteral5477 = new BitSet(new long[]{0x05D9AD7142344040L,0x800F6337E0E2AD4BL,0x0000000048144048L});
	public static final BitSet FOLLOW_term_in_setOrMapLiteral5481 = new BitSet(new long[]{0x0000000000000002L,0x0000000000000000L,0x0000000000020000L});
	public static final BitSet FOLLOW_155_in_collectionLiteral5515 = new BitSet(new long[]{0x05D9AD7142344040L,0x800F6337E0E2AD4BL,0x0000000068144048L});
	public static final BitSet FOLLOW_term_in_collectionLiteral5533 = new BitSet(new long[]{0x0000000000000000L,0x0000000000000000L,0x0000000020020000L});
	public static final BitSet FOLLOW_145_in_collectionLiteral5539 = new BitSet(new long[]{0x05D9AD7142344040L,0x800F6337E0E2AD4BL,0x0000000048144048L});
	public static final BitSet FOLLOW_term_in_collectionLiteral5543 = new BitSet(new long[]{0x0000000000000000L,0x0000000000000000L,0x0000000020020000L});
	public static final BitSet FOLLOW_157_in_collectionLiteral5559 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_158_in_collectionLiteral5569 = new BitSet(new long[]{0x05D9AD7142344040L,0x800F6337E0E2AD4BL,0x0000000048144048L});
	public static final BitSet FOLLOW_term_in_collectionLiteral5573 = new BitSet(new long[]{0x0000000000000000L,0x0000000000000000L,0x0000000080120000L});
	public static final BitSet FOLLOW_setOrMapLiteral_in_collectionLiteral5577 = new BitSet(new long[]{0x0000000000000000L,0x0000000000000000L,0x0000000080000000L});
	public static final BitSet FOLLOW_159_in_collectionLiteral5582 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_158_in_collectionLiteral5600 = new BitSet(new long[]{0x0000000000000000L,0x0000000000000000L,0x0000000080000000L});
	public static final BitSet FOLLOW_159_in_collectionLiteral5602 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_158_in_usertypeLiteral5646 = new BitSet(new long[]{0x05D9BD7142100000L,0x004F63A7E0E08D69L,0x0000000000000001L});
	public static final BitSet FOLLOW_ident_in_usertypeLiteral5650 = new BitSet(new long[]{0x0000000000000000L,0x0000000000000000L,0x0000000000100000L});
	public static final BitSet FOLLOW_148_in_usertypeLiteral5652 = new BitSet(new long[]{0x05D9AD7142344040L,0x800F6337E0E2AD4BL,0x0000000048144048L});
	public static final BitSet FOLLOW_term_in_usertypeLiteral5656 = new BitSet(new long[]{0x0000000000000000L,0x0000000000000000L,0x0000000080020000L});
	public static final BitSet FOLLOW_145_in_usertypeLiteral5662 = new BitSet(new long[]{0x05D9BD7142100000L,0x004F63A7E0E08D69L,0x0000000000000001L});
	public static final BitSet FOLLOW_ident_in_usertypeLiteral5666 = new BitSet(new long[]{0x0000000000000000L,0x0000000000000000L,0x0000000000100000L});
	public static final BitSet FOLLOW_148_in_usertypeLiteral5668 = new BitSet(new long[]{0x05D9AD7142344040L,0x800F6337E0E2AD4BL,0x0000000048144048L});
	public static final BitSet FOLLOW_term_in_usertypeLiteral5672 = new BitSet(new long[]{0x0000000000000000L,0x0000000000000000L,0x0000000080020000L});
	public static final BitSet FOLLOW_159_in_usertypeLiteral5679 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_142_in_tupleLiteral5716 = new BitSet(new long[]{0x05D9AD7142344040L,0x800F6337E0E2AD4BL,0x0000000048144048L});
	public static final BitSet FOLLOW_term_in_tupleLiteral5720 = new BitSet(new long[]{0x0000000000000000L,0x0000000000000000L,0x0000000000028000L});
	public static final BitSet FOLLOW_145_in_tupleLiteral5726 = new BitSet(new long[]{0x05D9AD7142344040L,0x800F6337E0E2AD4BL,0x0000000048144048L});
	public static final BitSet FOLLOW_term_in_tupleLiteral5730 = new BitSet(new long[]{0x0000000000000000L,0x0000000000000000L,0x0000000000028000L});
	public static final BitSet FOLLOW_143_in_tupleLiteral5737 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_constant_in_value5760 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_collectionLiteral_in_value5782 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_usertypeLiteral_in_value5795 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_tupleLiteral_in_value5810 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_K_NULL_in_value5826 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_148_in_value5850 = new BitSet(new long[]{0x05D9BD7142100000L,0x004F63A7E0E08D69L,0x0000000000000001L});
	public static final BitSet FOLLOW_ident_in_value5854 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_QMARK_in_value5872 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_INTEGER_in_intValue5918 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_148_in_intValue5932 = new BitSet(new long[]{0x05D9BD7142100000L,0x004F63A7E0E08D69L,0x0000000000000001L});
	public static final BitSet FOLLOW_ident_in_intValue5936 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_QMARK_in_intValue5947 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_IDENT_in_functionName5980 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_unreserved_function_keyword_in_functionName6014 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_K_TOKEN_in_functionName6024 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_functionName_in_function6071 = new BitSet(new long[]{0x0000000000000000L,0x0000000000000000L,0x0000000000004000L});
	public static final BitSet FOLLOW_142_in_function6073 = new BitSet(new long[]{0x0000000000000000L,0x0000000000000000L,0x0000000000008000L});
	public static final BitSet FOLLOW_143_in_function6075 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_functionName_in_function6105 = new BitSet(new long[]{0x0000000000000000L,0x0000000000000000L,0x0000000000004000L});
	public static final BitSet FOLLOW_142_in_function6107 = new BitSet(new long[]{0x05D9AD7142344040L,0x800F6337E0E2AD4BL,0x0000000048144048L});
	public static final BitSet FOLLOW_functionArgs_in_function6111 = new BitSet(new long[]{0x0000000000000000L,0x0000000000000000L,0x0000000000008000L});
	public static final BitSet FOLLOW_143_in_function6113 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_term_in_functionArgs6146 = new BitSet(new long[]{0x0000000000000002L,0x0000000000000000L,0x0000000000020000L});
	public static final BitSet FOLLOW_145_in_functionArgs6152 = new BitSet(new long[]{0x05D9AD7142344040L,0x800F6337E0E2AD4BL,0x0000000048144048L});
	public static final BitSet FOLLOW_term_in_functionArgs6156 = new BitSet(new long[]{0x0000000000000002L,0x0000000000000000L,0x0000000000020000L});
	public static final BitSet FOLLOW_value_in_term6184 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_function_in_term6221 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_142_in_term6253 = new BitSet(new long[]{0x05D9BD7142100000L,0x004F63A7F0E08D69L,0x0000000000000009L});
	public static final BitSet FOLLOW_comparatorType_in_term6257 = new BitSet(new long[]{0x0000000000000000L,0x0000000000000000L,0x0000000000008000L});
	public static final BitSet FOLLOW_143_in_term6259 = new BitSet(new long[]{0x05D9AD7142344040L,0x800F6337E0E2AD4BL,0x0000000048144048L});
	public static final BitSet FOLLOW_term_in_term6263 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_cident_in_columnOperation6286 = new BitSet(new long[]{0x0000000000000000L,0x0000000000000000L,0x0000000009000000L});
	public static final BitSet FOLLOW_columnOperationDifferentiator_in_columnOperation6288 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_152_in_columnOperationDifferentiator6307 = new BitSet(new long[]{0x05D9BD7142344040L,0x804F63B7E0E2AD6BL,0x0000000048144049L});
	public static final BitSet FOLLOW_normalColumnOperation_in_columnOperationDifferentiator6309 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_155_in_columnOperationDifferentiator6318 = new BitSet(new long[]{0x05D9AD7142344040L,0x800F6337E0E2AD4BL,0x0000000048144048L});
	public static final BitSet FOLLOW_term_in_columnOperationDifferentiator6322 = new BitSet(new long[]{0x0000000000000000L,0x0000000000000000L,0x0000000020000000L});
	public static final BitSet FOLLOW_157_in_columnOperationDifferentiator6324 = new BitSet(new long[]{0x0000000000000000L,0x0000000000000000L,0x0000000001000000L});
	public static final BitSet FOLLOW_specializedColumnOperation_in_columnOperationDifferentiator6326 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_term_in_normalColumnOperation6347 = new BitSet(new long[]{0x0000000000000002L,0x0000000000000000L,0x0000000000010000L});
	public static final BitSet FOLLOW_144_in_normalColumnOperation6350 = new BitSet(new long[]{0x05D9BD7142100000L,0x004F63A7E0E08D69L,0x0000000000000001L});
	public static final BitSet FOLLOW_cident_in_normalColumnOperation6354 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_cident_in_normalColumnOperation6375 = new BitSet(new long[]{0x0000000000000000L,0x0000000000000000L,0x0000000000050000L});
	public static final BitSet FOLLOW_set_in_normalColumnOperation6379 = new BitSet(new long[]{0x05D9AD7142344040L,0x800F6337E0E2AD4BL,0x0000000048144048L});
	public static final BitSet FOLLOW_term_in_normalColumnOperation6389 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_cident_in_normalColumnOperation6407 = new BitSet(new long[]{0x0000000000200000L});
	public static final BitSet FOLLOW_INTEGER_in_normalColumnOperation6411 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_152_in_specializedColumnOperation6437 = new BitSet(new long[]{0x05D9AD7142344040L,0x800F6337E0E2AD4BL,0x0000000048144048L});
	public static final BitSet FOLLOW_term_in_specializedColumnOperation6441 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_cident_in_columnCondition6474 = new BitSet(new long[]{0x4000000000000000L,0x0000000000000000L,0x000000000FC02000L});
	public static final BitSet FOLLOW_relationType_in_columnCondition6488 = new BitSet(new long[]{0x05D9AD7142344040L,0x800F6337E0E2AD4BL,0x0000000048144048L});
	public static final BitSet FOLLOW_term_in_columnCondition6492 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_K_IN_in_columnCondition6506 = new BitSet(new long[]{0x0000000000000000L,0x8000000000000000L,0x0000000000104000L});
	public static final BitSet FOLLOW_singleColumnInValues_in_columnCondition6524 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_inMarker_in_columnCondition6544 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_155_in_columnCondition6572 = new BitSet(new long[]{0x05D9AD7142344040L,0x800F6337E0E2AD4BL,0x0000000048144048L});
	public static final BitSet FOLLOW_term_in_columnCondition6576 = new BitSet(new long[]{0x0000000000000000L,0x0000000000000000L,0x0000000020000000L});
	public static final BitSet FOLLOW_157_in_columnCondition6578 = new BitSet(new long[]{0x4000000000000000L,0x0000000000000000L,0x0000000007C02000L});
	public static final BitSet FOLLOW_relationType_in_columnCondition6596 = new BitSet(new long[]{0x05D9AD7142344040L,0x800F6337E0E2AD4BL,0x0000000048144048L});
	public static final BitSet FOLLOW_term_in_columnCondition6600 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_K_IN_in_columnCondition6618 = new BitSet(new long[]{0x0000000000000000L,0x8000000000000000L,0x0000000000104000L});
	public static final BitSet FOLLOW_singleColumnInValues_in_columnCondition6640 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_inMarker_in_columnCondition6664 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_property_in_properties6726 = new BitSet(new long[]{0x0000000010000002L});
	public static final BitSet FOLLOW_K_AND_in_properties6730 = new BitSet(new long[]{0x05D9BD7142100000L,0x004F63A7E0E08D69L,0x0000000000000001L});
	public static final BitSet FOLLOW_property_in_properties6732 = new BitSet(new long[]{0x0000000010000002L});
	public static final BitSet FOLLOW_ident_in_property6755 = new BitSet(new long[]{0x0000000000000000L,0x0000000000000000L,0x0000000001000000L});
	public static final BitSet FOLLOW_152_in_property6757 = new BitSet(new long[]{0x05D9BD7142244040L,0x004F63A7E0E0AD6BL,0x0000000040040048L});
	public static final BitSet FOLLOW_propertyValue_in_property6762 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_mapLiteral_in_property6790 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_constant_in_propertyValue6819 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_unreserved_keyword_in_propertyValue6841 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_152_in_relationType6864 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_150_in_relationType6875 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_151_in_relationType6886 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_153_in_relationType6896 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_154_in_relationType6907 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_141_in_relationType6917 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_cident_in_relation6939 = new BitSet(new long[]{0x0000000000000000L,0x0000000000000000L,0x0000000007C02000L});
	public static final BitSet FOLLOW_relationType_in_relation6943 = new BitSet(new long[]{0x05D9AD7142344040L,0x800F6337E0E2AD4BL,0x0000000048144048L});
	public static final BitSet FOLLOW_term_in_relation6947 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_K_TOKEN_in_relation6957 = new BitSet(new long[]{0x0000000000000000L,0x0000000000000000L,0x0000000000004000L});
	public static final BitSet FOLLOW_tupleOfIdentifiers_in_relation6961 = new BitSet(new long[]{0x0000000000000000L,0x0000000000000000L,0x0000000007C02000L});
	public static final BitSet FOLLOW_relationType_in_relation6965 = new BitSet(new long[]{0x05D9AD7142344040L,0x800F6337E0E2AD4BL,0x0000000048144048L});
	public static final BitSet FOLLOW_term_in_relation6969 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_cident_in_relation6989 = new BitSet(new long[]{0x4000000000000000L});
	public static final BitSet FOLLOW_K_IN_in_relation6991 = new BitSet(new long[]{0x0000000000000000L,0x8000000000000000L,0x0000000000100000L});
	public static final BitSet FOLLOW_inMarker_in_relation6995 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_cident_in_relation7015 = new BitSet(new long[]{0x4000000000000000L});
	public static final BitSet FOLLOW_K_IN_in_relation7017 = new BitSet(new long[]{0x0000000000000000L,0x0000000000000000L,0x0000000000004000L});
	public static final BitSet FOLLOW_singleColumnInValues_in_relation7021 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_cident_in_relation7041 = new BitSet(new long[]{0x0000080000000000L});
	public static final BitSet FOLLOW_K_CONTAINS_in_relation7043 = new BitSet(new long[]{0x05D9AD7142344040L,0x800F6337E0E2AD6BL,0x0000000048144048L});
	public static final BitSet FOLLOW_K_KEY_in_relation7048 = new BitSet(new long[]{0x05D9AD7142344040L,0x800F6337E0E2AD4BL,0x0000000048144048L});
	public static final BitSet FOLLOW_term_in_relation7064 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_tupleOfIdentifiers_in_relation7076 = new BitSet(new long[]{0x4000000000000000L,0x0000000000000000L,0x0000000007C02000L});
	public static final BitSet FOLLOW_K_IN_in_relation7086 = new BitSet(new long[]{0x0000000000000000L,0x8000000000000000L,0x0000000000104000L});
	public static final BitSet FOLLOW_142_in_relation7100 = new BitSet(new long[]{0x0000000000000000L,0x0000000000000000L,0x0000000000008000L});
	public static final BitSet FOLLOW_143_in_relation7102 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_inMarkerForTuple_in_relation7134 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_tupleOfTupleLiterals_in_relation7168 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_tupleOfMarkersForTuples_in_relation7202 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_relationType_in_relation7244 = new BitSet(new long[]{0x0000000000000000L,0x0000000000000000L,0x0000000000004000L});
	public static final BitSet FOLLOW_tupleLiteral_in_relation7248 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_relationType_in_relation7274 = new BitSet(new long[]{0x0000000000000000L,0x8000000000000000L,0x0000000000100000L});
	public static final BitSet FOLLOW_markerForTuple_in_relation7278 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_142_in_relation7308 = new BitSet(new long[]{0x05D9BD7142100000L,0x004F63B7E0E08D69L,0x0000000000004001L});
	public static final BitSet FOLLOW_relation_in_relation7310 = new BitSet(new long[]{0x0000000000000000L,0x0000000000000000L,0x0000000000008000L});
	public static final BitSet FOLLOW_143_in_relation7313 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_QMARK_in_inMarker7334 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_148_in_inMarker7344 = new BitSet(new long[]{0x05D9BD7142100000L,0x004F63A7E0E08D69L,0x0000000000000001L});
	public static final BitSet FOLLOW_ident_in_inMarker7348 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_142_in_tupleOfIdentifiers7380 = new BitSet(new long[]{0x05D9BD7142100000L,0x004F63A7E0E08D69L,0x0000000000000001L});
	public static final BitSet FOLLOW_cident_in_tupleOfIdentifiers7384 = new BitSet(new long[]{0x0000000000000000L,0x0000000000000000L,0x0000000000028000L});
	public static final BitSet FOLLOW_145_in_tupleOfIdentifiers7389 = new BitSet(new long[]{0x05D9BD7142100000L,0x004F63A7E0E08D69L,0x0000000000000001L});
	public static final BitSet FOLLOW_cident_in_tupleOfIdentifiers7393 = new BitSet(new long[]{0x0000000000000000L,0x0000000000000000L,0x0000000000028000L});
	public static final BitSet FOLLOW_143_in_tupleOfIdentifiers7399 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_142_in_singleColumnInValues7429 = new BitSet(new long[]{0x05D9AD7142344040L,0x800F6337E0E2AD4BL,0x000000004814C048L});
	public static final BitSet FOLLOW_term_in_singleColumnInValues7437 = new BitSet(new long[]{0x0000000000000000L,0x0000000000000000L,0x0000000000028000L});
	public static final BitSet FOLLOW_145_in_singleColumnInValues7442 = new BitSet(new long[]{0x05D9AD7142344040L,0x800F6337E0E2AD4BL,0x0000000048144048L});
	public static final BitSet FOLLOW_term_in_singleColumnInValues7446 = new BitSet(new long[]{0x0000000000000000L,0x0000000000000000L,0x0000000000028000L});
	public static final BitSet FOLLOW_143_in_singleColumnInValues7455 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_142_in_tupleOfTupleLiterals7485 = new BitSet(new long[]{0x0000000000000000L,0x0000000000000000L,0x0000000000004000L});
	public static final BitSet FOLLOW_tupleLiteral_in_tupleOfTupleLiterals7489 = new BitSet(new long[]{0x0000000000000000L,0x0000000000000000L,0x0000000000028000L});
	public static final BitSet FOLLOW_145_in_tupleOfTupleLiterals7494 = new BitSet(new long[]{0x0000000000000000L,0x0000000000000000L,0x0000000000004000L});
	public static final BitSet FOLLOW_tupleLiteral_in_tupleOfTupleLiterals7498 = new BitSet(new long[]{0x0000000000000000L,0x0000000000000000L,0x0000000000028000L});
	public static final BitSet FOLLOW_143_in_tupleOfTupleLiterals7504 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_QMARK_in_markerForTuple7525 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_148_in_markerForTuple7535 = new BitSet(new long[]{0x05D9BD7142100000L,0x004F63A7E0E08D69L,0x0000000000000001L});
	public static final BitSet FOLLOW_ident_in_markerForTuple7539 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_142_in_tupleOfMarkersForTuples7571 = new BitSet(new long[]{0x0000000000000000L,0x8000000000000000L,0x0000000000100000L});
	public static final BitSet FOLLOW_markerForTuple_in_tupleOfMarkersForTuples7575 = new BitSet(new long[]{0x0000000000000000L,0x0000000000000000L,0x0000000000028000L});
	public static final BitSet FOLLOW_145_in_tupleOfMarkersForTuples7580 = new BitSet(new long[]{0x0000000000000000L,0x8000000000000000L,0x0000000000100000L});
	public static final BitSet FOLLOW_markerForTuple_in_tupleOfMarkersForTuples7584 = new BitSet(new long[]{0x0000000000000000L,0x0000000000000000L,0x0000000000028000L});
	public static final BitSet FOLLOW_143_in_tupleOfMarkersForTuples7590 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_QMARK_in_inMarkerForTuple7611 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_148_in_inMarkerForTuple7621 = new BitSet(new long[]{0x05D9BD7142100000L,0x004F63A7E0E08D69L,0x0000000000000001L});
	public static final BitSet FOLLOW_ident_in_inMarkerForTuple7625 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_native_type_in_comparatorType7650 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_collection_type_in_comparatorType7666 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_tuple_type_in_comparatorType7678 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_userTypeName_in_comparatorType7694 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_K_FROZEN_in_comparatorType7706 = new BitSet(new long[]{0x0000000000000000L,0x0000000000000000L,0x0000000000400000L});
	public static final BitSet FOLLOW_150_in_comparatorType7708 = new BitSet(new long[]{0x05D9BD7142100000L,0x004F63A7F0E08D69L,0x0000000000000009L});
	public static final BitSet FOLLOW_comparatorType_in_comparatorType7712 = new BitSet(new long[]{0x0000000000000000L,0x0000000000000000L,0x0000000002000000L});
	public static final BitSet FOLLOW_153_in_comparatorType7714 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_STRING_LITERAL_in_comparatorType7732 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_K_ASCII_in_native_type7761 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_K_BIGINT_in_native_type7775 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_K_BLOB_in_native_type7788 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_K_BOOLEAN_in_native_type7803 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_K_COUNTER_in_native_type7815 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_K_DECIMAL_in_native_type7827 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_K_DOUBLE_in_native_type7839 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_K_FLOAT_in_native_type7852 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_K_INET_in_native_type7866 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_K_INT_in_native_type7881 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_K_TEXT_in_native_type7897 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_K_TIMESTAMP_in_native_type7912 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_K_UUID_in_native_type7922 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_K_VARCHAR_in_native_type7937 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_K_VARINT_in_native_type7949 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_K_TIMEUUID_in_native_type7962 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_K_MAP_in_collection_type7986 = new BitSet(new long[]{0x0000000000000000L,0x0000000000000000L,0x0000000000400000L});
	public static final BitSet FOLLOW_150_in_collection_type7989 = new BitSet(new long[]{0x05D9BD7142100000L,0x004F63A7F0E08D69L,0x0000000000000009L});
	public static final BitSet FOLLOW_comparatorType_in_collection_type7993 = new BitSet(new long[]{0x0000000000000000L,0x0000000000000000L,0x0000000000020000L});
	public static final BitSet FOLLOW_145_in_collection_type7995 = new BitSet(new long[]{0x05D9BD7142100000L,0x004F63A7F0E08D69L,0x0000000000000009L});
	public static final BitSet FOLLOW_comparatorType_in_collection_type7999 = new BitSet(new long[]{0x0000000000000000L,0x0000000000000000L,0x0000000002000000L});
	public static final BitSet FOLLOW_153_in_collection_type8001 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_K_LIST_in_collection_type8019 = new BitSet(new long[]{0x0000000000000000L,0x0000000000000000L,0x0000000000400000L});
	public static final BitSet FOLLOW_150_in_collection_type8021 = new BitSet(new long[]{0x05D9BD7142100000L,0x004F63A7F0E08D69L,0x0000000000000009L});
	public static final BitSet FOLLOW_comparatorType_in_collection_type8025 = new BitSet(new long[]{0x0000000000000000L,0x0000000000000000L,0x0000000002000000L});
	public static final BitSet FOLLOW_153_in_collection_type8027 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_K_SET_in_collection_type8045 = new BitSet(new long[]{0x0000000000000000L,0x0000000000000000L,0x0000000000400000L});
	public static final BitSet FOLLOW_150_in_collection_type8048 = new BitSet(new long[]{0x05D9BD7142100000L,0x004F63A7F0E08D69L,0x0000000000000009L});
	public static final BitSet FOLLOW_comparatorType_in_collection_type8052 = new BitSet(new long[]{0x0000000000000000L,0x0000000000000000L,0x0000000002000000L});
	public static final BitSet FOLLOW_153_in_collection_type8054 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_K_TUPLE_in_tuple_type8085 = new BitSet(new long[]{0x0000000000000000L,0x0000000000000000L,0x0000000000400000L});
	public static final BitSet FOLLOW_150_in_tuple_type8087 = new BitSet(new long[]{0x05D9BD7142100000L,0x004F63A7F0E08D69L,0x0000000000000009L});
	public static final BitSet FOLLOW_comparatorType_in_tuple_type8102 = new BitSet(new long[]{0x0000000000000000L,0x0000000000000000L,0x0000000002020000L});
	public static final BitSet FOLLOW_145_in_tuple_type8107 = new BitSet(new long[]{0x05D9BD7142100000L,0x004F63A7F0E08D69L,0x0000000000000009L});
	public static final BitSet FOLLOW_comparatorType_in_tuple_type8111 = new BitSet(new long[]{0x0000000000000000L,0x0000000000000000L,0x0000000002020000L});
	public static final BitSet FOLLOW_153_in_tuple_type8123 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_IDENT_in_non_type_ident8175 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_QUOTED_NAME_in_non_type_ident8206 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_basic_unreserved_keyword_in_non_type_ident8231 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_K_KEY_in_non_type_ident8243 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_unreserved_function_keyword_in_unreserved_keyword8286 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_set_in_unreserved_keyword8302 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_basic_unreserved_keyword_in_unreserved_function_keyword8341 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_native_type_in_unreserved_function_keyword8353 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_set_in_basic_unreserved_keyword8391 = new BitSet(new long[]{0x0000000000000002L});
}
