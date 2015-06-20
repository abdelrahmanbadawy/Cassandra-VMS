// $ANTLR 3.5.2 /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g 2015-05-19 16:38:02

    package org.apache.cassandra.cql3;

    import org.apache.cassandra.exceptions.SyntaxException;


import org.antlr.runtime.*;
import java.util.Stack;
import java.util.List;
import java.util.ArrayList;

@SuppressWarnings("all")
public class CqlLexer extends Lexer {
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

	    List<Token> tokens = new ArrayList<Token>();

	    public void emit(Token token)
	    {
	        state.token = token;
	        tokens.add(token);
	    }

	    public Token nextToken()
	    {
	        super.nextToken();
	        if (tokens.size() == 0)
	            return new CommonToken(Token.EOF);
	        return tokens.remove(0);
	    }

	    private final List<ErrorListener> listeners = new ArrayList<ErrorListener>();

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


	// delegates
	// delegators
	public Lexer[] getDelegates() {
		return new Lexer[] {};
	}

	public CqlLexer() {} 
	public CqlLexer(CharStream input) {
		this(input, new RecognizerSharedState());
	}
	public CqlLexer(CharStream input, RecognizerSharedState state) {
		super(input,state);
	}
	@Override public String getGrammarFileName() { return "/home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g"; }

	// $ANTLR start "T__141"
	public final void mT__141() throws RecognitionException {
		try {
			int _type = T__141;
			int _channel = DEFAULT_TOKEN_CHANNEL;
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:48:8: ( '!=' )
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:48:10: '!='
			{
			match("!="); 

			}

			state.type = _type;
			state.channel = _channel;
		}
		finally {
			// do for sure before leaving
		}
	}
	// $ANTLR end "T__141"

	// $ANTLR start "T__142"
	public final void mT__142() throws RecognitionException {
		try {
			int _type = T__142;
			int _channel = DEFAULT_TOKEN_CHANNEL;
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:49:8: ( '(' )
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:49:10: '('
			{
			match('('); 
			}

			state.type = _type;
			state.channel = _channel;
		}
		finally {
			// do for sure before leaving
		}
	}
	// $ANTLR end "T__142"

	// $ANTLR start "T__143"
	public final void mT__143() throws RecognitionException {
		try {
			int _type = T__143;
			int _channel = DEFAULT_TOKEN_CHANNEL;
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:50:8: ( ')' )
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:50:10: ')'
			{
			match(')'); 
			}

			state.type = _type;
			state.channel = _channel;
		}
		finally {
			// do for sure before leaving
		}
	}
	// $ANTLR end "T__143"

	// $ANTLR start "T__144"
	public final void mT__144() throws RecognitionException {
		try {
			int _type = T__144;
			int _channel = DEFAULT_TOKEN_CHANNEL;
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:51:8: ( '+' )
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:51:10: '+'
			{
			match('+'); 
			}

			state.type = _type;
			state.channel = _channel;
		}
		finally {
			// do for sure before leaving
		}
	}
	// $ANTLR end "T__144"

	// $ANTLR start "T__145"
	public final void mT__145() throws RecognitionException {
		try {
			int _type = T__145;
			int _channel = DEFAULT_TOKEN_CHANNEL;
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:52:8: ( ',' )
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:52:10: ','
			{
			match(','); 
			}

			state.type = _type;
			state.channel = _channel;
		}
		finally {
			// do for sure before leaving
		}
	}
	// $ANTLR end "T__145"

	// $ANTLR start "T__146"
	public final void mT__146() throws RecognitionException {
		try {
			int _type = T__146;
			int _channel = DEFAULT_TOKEN_CHANNEL;
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:53:8: ( '-' )
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:53:10: '-'
			{
			match('-'); 
			}

			state.type = _type;
			state.channel = _channel;
		}
		finally {
			// do for sure before leaving
		}
	}
	// $ANTLR end "T__146"

	// $ANTLR start "T__147"
	public final void mT__147() throws RecognitionException {
		try {
			int _type = T__147;
			int _channel = DEFAULT_TOKEN_CHANNEL;
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:54:8: ( '.' )
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:54:10: '.'
			{
			match('.'); 
			}

			state.type = _type;
			state.channel = _channel;
		}
		finally {
			// do for sure before leaving
		}
	}
	// $ANTLR end "T__147"

	// $ANTLR start "T__148"
	public final void mT__148() throws RecognitionException {
		try {
			int _type = T__148;
			int _channel = DEFAULT_TOKEN_CHANNEL;
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:55:8: ( ':' )
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:55:10: ':'
			{
			match(':'); 
			}

			state.type = _type;
			state.channel = _channel;
		}
		finally {
			// do for sure before leaving
		}
	}
	// $ANTLR end "T__148"

	// $ANTLR start "T__149"
	public final void mT__149() throws RecognitionException {
		try {
			int _type = T__149;
			int _channel = DEFAULT_TOKEN_CHANNEL;
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:56:8: ( ';' )
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:56:10: ';'
			{
			match(';'); 
			}

			state.type = _type;
			state.channel = _channel;
		}
		finally {
			// do for sure before leaving
		}
	}
	// $ANTLR end "T__149"

	// $ANTLR start "T__150"
	public final void mT__150() throws RecognitionException {
		try {
			int _type = T__150;
			int _channel = DEFAULT_TOKEN_CHANNEL;
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:57:8: ( '<' )
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:57:10: '<'
			{
			match('<'); 
			}

			state.type = _type;
			state.channel = _channel;
		}
		finally {
			// do for sure before leaving
		}
	}
	// $ANTLR end "T__150"

	// $ANTLR start "T__151"
	public final void mT__151() throws RecognitionException {
		try {
			int _type = T__151;
			int _channel = DEFAULT_TOKEN_CHANNEL;
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:58:8: ( '<=' )
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:58:10: '<='
			{
			match("<="); 

			}

			state.type = _type;
			state.channel = _channel;
		}
		finally {
			// do for sure before leaving
		}
	}
	// $ANTLR end "T__151"

	// $ANTLR start "T__152"
	public final void mT__152() throws RecognitionException {
		try {
			int _type = T__152;
			int _channel = DEFAULT_TOKEN_CHANNEL;
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:59:8: ( '=' )
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:59:10: '='
			{
			match('='); 
			}

			state.type = _type;
			state.channel = _channel;
		}
		finally {
			// do for sure before leaving
		}
	}
	// $ANTLR end "T__152"

	// $ANTLR start "T__153"
	public final void mT__153() throws RecognitionException {
		try {
			int _type = T__153;
			int _channel = DEFAULT_TOKEN_CHANNEL;
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:60:8: ( '>' )
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:60:10: '>'
			{
			match('>'); 
			}

			state.type = _type;
			state.channel = _channel;
		}
		finally {
			// do for sure before leaving
		}
	}
	// $ANTLR end "T__153"

	// $ANTLR start "T__154"
	public final void mT__154() throws RecognitionException {
		try {
			int _type = T__154;
			int _channel = DEFAULT_TOKEN_CHANNEL;
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:61:8: ( '>=' )
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:61:10: '>='
			{
			match(">="); 

			}

			state.type = _type;
			state.channel = _channel;
		}
		finally {
			// do for sure before leaving
		}
	}
	// $ANTLR end "T__154"

	// $ANTLR start "T__155"
	public final void mT__155() throws RecognitionException {
		try {
			int _type = T__155;
			int _channel = DEFAULT_TOKEN_CHANNEL;
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:62:8: ( '[' )
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:62:10: '['
			{
			match('['); 
			}

			state.type = _type;
			state.channel = _channel;
		}
		finally {
			// do for sure before leaving
		}
	}
	// $ANTLR end "T__155"

	// $ANTLR start "T__156"
	public final void mT__156() throws RecognitionException {
		try {
			int _type = T__156;
			int _channel = DEFAULT_TOKEN_CHANNEL;
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:63:8: ( '\\*' )
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:63:10: '\\*'
			{
			match('*'); 
			}

			state.type = _type;
			state.channel = _channel;
		}
		finally {
			// do for sure before leaving
		}
	}
	// $ANTLR end "T__156"

	// $ANTLR start "T__157"
	public final void mT__157() throws RecognitionException {
		try {
			int _type = T__157;
			int _channel = DEFAULT_TOKEN_CHANNEL;
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:64:8: ( ']' )
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:64:10: ']'
			{
			match(']'); 
			}

			state.type = _type;
			state.channel = _channel;
		}
		finally {
			// do for sure before leaving
		}
	}
	// $ANTLR end "T__157"

	// $ANTLR start "T__158"
	public final void mT__158() throws RecognitionException {
		try {
			int _type = T__158;
			int _channel = DEFAULT_TOKEN_CHANNEL;
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:65:8: ( '{' )
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:65:10: '{'
			{
			match('{'); 
			}

			state.type = _type;
			state.channel = _channel;
		}
		finally {
			// do for sure before leaving
		}
	}
	// $ANTLR end "T__158"

	// $ANTLR start "T__159"
	public final void mT__159() throws RecognitionException {
		try {
			int _type = T__159;
			int _channel = DEFAULT_TOKEN_CHANNEL;
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:66:8: ( '}' )
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:66:10: '}'
			{
			match('}'); 
			}

			state.type = _type;
			state.channel = _channel;
		}
		finally {
			// do for sure before leaving
		}
	}
	// $ANTLR end "T__159"

	// $ANTLR start "K_SELECT"
	public final void mK_SELECT() throws RecognitionException {
		try {
			int _type = K_SELECT;
			int _channel = DEFAULT_TOKEN_CHANNEL;
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1242:9: ( S E L E C T )
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1242:16: S E L E C T
			{
			mS(); 

			mE(); 

			mL(); 

			mE(); 

			mC(); 

			mT(); 

			}

			state.type = _type;
			state.channel = _channel;
		}
		finally {
			// do for sure before leaving
		}
	}
	// $ANTLR end "K_SELECT"

	// $ANTLR start "K_FROM"
	public final void mK_FROM() throws RecognitionException {
		try {
			int _type = K_FROM;
			int _channel = DEFAULT_TOKEN_CHANNEL;
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1243:7: ( F R O M )
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1243:16: F R O M
			{
			mF(); 

			mR(); 

			mO(); 

			mM(); 

			}

			state.type = _type;
			state.channel = _channel;
		}
		finally {
			// do for sure before leaving
		}
	}
	// $ANTLR end "K_FROM"

	// $ANTLR start "K_AS"
	public final void mK_AS() throws RecognitionException {
		try {
			int _type = K_AS;
			int _channel = DEFAULT_TOKEN_CHANNEL;
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1244:5: ( A S )
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1244:16: A S
			{
			mA(); 

			mS(); 

			}

			state.type = _type;
			state.channel = _channel;
		}
		finally {
			// do for sure before leaving
		}
	}
	// $ANTLR end "K_AS"

	// $ANTLR start "K_WHERE"
	public final void mK_WHERE() throws RecognitionException {
		try {
			int _type = K_WHERE;
			int _channel = DEFAULT_TOKEN_CHANNEL;
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1245:8: ( W H E R E )
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1245:16: W H E R E
			{
			mW(); 

			mH(); 

			mE(); 

			mR(); 

			mE(); 

			}

			state.type = _type;
			state.channel = _channel;
		}
		finally {
			// do for sure before leaving
		}
	}
	// $ANTLR end "K_WHERE"

	// $ANTLR start "K_AND"
	public final void mK_AND() throws RecognitionException {
		try {
			int _type = K_AND;
			int _channel = DEFAULT_TOKEN_CHANNEL;
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1246:6: ( A N D )
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1246:16: A N D
			{
			mA(); 

			mN(); 

			mD(); 

			}

			state.type = _type;
			state.channel = _channel;
		}
		finally {
			// do for sure before leaving
		}
	}
	// $ANTLR end "K_AND"

	// $ANTLR start "K_KEY"
	public final void mK_KEY() throws RecognitionException {
		try {
			int _type = K_KEY;
			int _channel = DEFAULT_TOKEN_CHANNEL;
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1247:6: ( K E Y )
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1247:16: K E Y
			{
			mK(); 

			mE(); 

			mY(); 

			}

			state.type = _type;
			state.channel = _channel;
		}
		finally {
			// do for sure before leaving
		}
	}
	// $ANTLR end "K_KEY"

	// $ANTLR start "K_KEYS"
	public final void mK_KEYS() throws RecognitionException {
		try {
			int _type = K_KEYS;
			int _channel = DEFAULT_TOKEN_CHANNEL;
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1248:7: ( K E Y S )
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1248:16: K E Y S
			{
			mK(); 

			mE(); 

			mY(); 

			mS(); 

			}

			state.type = _type;
			state.channel = _channel;
		}
		finally {
			// do for sure before leaving
		}
	}
	// $ANTLR end "K_KEYS"

	// $ANTLR start "K_FULL"
	public final void mK_FULL() throws RecognitionException {
		try {
			int _type = K_FULL;
			int _channel = DEFAULT_TOKEN_CHANNEL;
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1249:7: ( F U L L )
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1249:16: F U L L
			{
			mF(); 

			mU(); 

			mL(); 

			mL(); 

			}

			state.type = _type;
			state.channel = _channel;
		}
		finally {
			// do for sure before leaving
		}
	}
	// $ANTLR end "K_FULL"

	// $ANTLR start "K_INSERT"
	public final void mK_INSERT() throws RecognitionException {
		try {
			int _type = K_INSERT;
			int _channel = DEFAULT_TOKEN_CHANNEL;
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1250:9: ( I N S E R T )
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1250:16: I N S E R T
			{
			mI(); 

			mN(); 

			mS(); 

			mE(); 

			mR(); 

			mT(); 

			}

			state.type = _type;
			state.channel = _channel;
		}
		finally {
			// do for sure before leaving
		}
	}
	// $ANTLR end "K_INSERT"

	// $ANTLR start "K_UPDATE"
	public final void mK_UPDATE() throws RecognitionException {
		try {
			int _type = K_UPDATE;
			int _channel = DEFAULT_TOKEN_CHANNEL;
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1251:9: ( U P D A T E )
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1251:16: U P D A T E
			{
			mU(); 

			mP(); 

			mD(); 

			mA(); 

			mT(); 

			mE(); 

			}

			state.type = _type;
			state.channel = _channel;
		}
		finally {
			// do for sure before leaving
		}
	}
	// $ANTLR end "K_UPDATE"

	// $ANTLR start "K_WITH"
	public final void mK_WITH() throws RecognitionException {
		try {
			int _type = K_WITH;
			int _channel = DEFAULT_TOKEN_CHANNEL;
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1252:7: ( W I T H )
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1252:16: W I T H
			{
			mW(); 

			mI(); 

			mT(); 

			mH(); 

			}

			state.type = _type;
			state.channel = _channel;
		}
		finally {
			// do for sure before leaving
		}
	}
	// $ANTLR end "K_WITH"

	// $ANTLR start "K_LIMIT"
	public final void mK_LIMIT() throws RecognitionException {
		try {
			int _type = K_LIMIT;
			int _channel = DEFAULT_TOKEN_CHANNEL;
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1253:8: ( L I M I T )
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1253:16: L I M I T
			{
			mL(); 

			mI(); 

			mM(); 

			mI(); 

			mT(); 

			}

			state.type = _type;
			state.channel = _channel;
		}
		finally {
			// do for sure before leaving
		}
	}
	// $ANTLR end "K_LIMIT"

	// $ANTLR start "K_USING"
	public final void mK_USING() throws RecognitionException {
		try {
			int _type = K_USING;
			int _channel = DEFAULT_TOKEN_CHANNEL;
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1254:8: ( U S I N G )
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1254:16: U S I N G
			{
			mU(); 

			mS(); 

			mI(); 

			mN(); 

			mG(); 

			}

			state.type = _type;
			state.channel = _channel;
		}
		finally {
			// do for sure before leaving
		}
	}
	// $ANTLR end "K_USING"

	// $ANTLR start "K_USE"
	public final void mK_USE() throws RecognitionException {
		try {
			int _type = K_USE;
			int _channel = DEFAULT_TOKEN_CHANNEL;
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1255:6: ( U S E )
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1255:16: U S E
			{
			mU(); 

			mS(); 

			mE(); 

			}

			state.type = _type;
			state.channel = _channel;
		}
		finally {
			// do for sure before leaving
		}
	}
	// $ANTLR end "K_USE"

	// $ANTLR start "K_DISTINCT"
	public final void mK_DISTINCT() throws RecognitionException {
		try {
			int _type = K_DISTINCT;
			int _channel = DEFAULT_TOKEN_CHANNEL;
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1256:11: ( D I S T I N C T )
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1256:16: D I S T I N C T
			{
			mD(); 

			mI(); 

			mS(); 

			mT(); 

			mI(); 

			mN(); 

			mC(); 

			mT(); 

			}

			state.type = _type;
			state.channel = _channel;
		}
		finally {
			// do for sure before leaving
		}
	}
	// $ANTLR end "K_DISTINCT"

	// $ANTLR start "K_COUNT"
	public final void mK_COUNT() throws RecognitionException {
		try {
			int _type = K_COUNT;
			int _channel = DEFAULT_TOKEN_CHANNEL;
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1257:8: ( C O U N T )
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1257:16: C O U N T
			{
			mC(); 

			mO(); 

			mU(); 

			mN(); 

			mT(); 

			}

			state.type = _type;
			state.channel = _channel;
		}
		finally {
			// do for sure before leaving
		}
	}
	// $ANTLR end "K_COUNT"

	// $ANTLR start "K_SET"
	public final void mK_SET() throws RecognitionException {
		try {
			int _type = K_SET;
			int _channel = DEFAULT_TOKEN_CHANNEL;
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1258:6: ( S E T )
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1258:16: S E T
			{
			mS(); 

			mE(); 

			mT(); 

			}

			state.type = _type;
			state.channel = _channel;
		}
		finally {
			// do for sure before leaving
		}
	}
	// $ANTLR end "K_SET"

	// $ANTLR start "K_BEGIN"
	public final void mK_BEGIN() throws RecognitionException {
		try {
			int _type = K_BEGIN;
			int _channel = DEFAULT_TOKEN_CHANNEL;
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1259:8: ( B E G I N )
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1259:16: B E G I N
			{
			mB(); 

			mE(); 

			mG(); 

			mI(); 

			mN(); 

			}

			state.type = _type;
			state.channel = _channel;
		}
		finally {
			// do for sure before leaving
		}
	}
	// $ANTLR end "K_BEGIN"

	// $ANTLR start "K_UNLOGGED"
	public final void mK_UNLOGGED() throws RecognitionException {
		try {
			int _type = K_UNLOGGED;
			int _channel = DEFAULT_TOKEN_CHANNEL;
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1260:11: ( U N L O G G E D )
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1260:16: U N L O G G E D
			{
			mU(); 

			mN(); 

			mL(); 

			mO(); 

			mG(); 

			mG(); 

			mE(); 

			mD(); 

			}

			state.type = _type;
			state.channel = _channel;
		}
		finally {
			// do for sure before leaving
		}
	}
	// $ANTLR end "K_UNLOGGED"

	// $ANTLR start "K_BATCH"
	public final void mK_BATCH() throws RecognitionException {
		try {
			int _type = K_BATCH;
			int _channel = DEFAULT_TOKEN_CHANNEL;
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1261:8: ( B A T C H )
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1261:16: B A T C H
			{
			mB(); 

			mA(); 

			mT(); 

			mC(); 

			mH(); 

			}

			state.type = _type;
			state.channel = _channel;
		}
		finally {
			// do for sure before leaving
		}
	}
	// $ANTLR end "K_BATCH"

	// $ANTLR start "K_APPLY"
	public final void mK_APPLY() throws RecognitionException {
		try {
			int _type = K_APPLY;
			int _channel = DEFAULT_TOKEN_CHANNEL;
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1262:8: ( A P P L Y )
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1262:16: A P P L Y
			{
			mA(); 

			mP(); 

			mP(); 

			mL(); 

			mY(); 

			}

			state.type = _type;
			state.channel = _channel;
		}
		finally {
			// do for sure before leaving
		}
	}
	// $ANTLR end "K_APPLY"

	// $ANTLR start "K_TRUNCATE"
	public final void mK_TRUNCATE() throws RecognitionException {
		try {
			int _type = K_TRUNCATE;
			int _channel = DEFAULT_TOKEN_CHANNEL;
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1263:11: ( T R U N C A T E )
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1263:16: T R U N C A T E
			{
			mT(); 

			mR(); 

			mU(); 

			mN(); 

			mC(); 

			mA(); 

			mT(); 

			mE(); 

			}

			state.type = _type;
			state.channel = _channel;
		}
		finally {
			// do for sure before leaving
		}
	}
	// $ANTLR end "K_TRUNCATE"

	// $ANTLR start "K_DELETE"
	public final void mK_DELETE() throws RecognitionException {
		try {
			int _type = K_DELETE;
			int _channel = DEFAULT_TOKEN_CHANNEL;
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1264:9: ( D E L E T E )
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1264:16: D E L E T E
			{
			mD(); 

			mE(); 

			mL(); 

			mE(); 

			mT(); 

			mE(); 

			}

			state.type = _type;
			state.channel = _channel;
		}
		finally {
			// do for sure before leaving
		}
	}
	// $ANTLR end "K_DELETE"

	// $ANTLR start "K_IN"
	public final void mK_IN() throws RecognitionException {
		try {
			int _type = K_IN;
			int _channel = DEFAULT_TOKEN_CHANNEL;
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1265:5: ( I N )
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1265:16: I N
			{
			mI(); 

			mN(); 

			}

			state.type = _type;
			state.channel = _channel;
		}
		finally {
			// do for sure before leaving
		}
	}
	// $ANTLR end "K_IN"

	// $ANTLR start "K_CREATE"
	public final void mK_CREATE() throws RecognitionException {
		try {
			int _type = K_CREATE;
			int _channel = DEFAULT_TOKEN_CHANNEL;
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1266:9: ( C R E A T E )
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1266:16: C R E A T E
			{
			mC(); 

			mR(); 

			mE(); 

			mA(); 

			mT(); 

			mE(); 

			}

			state.type = _type;
			state.channel = _channel;
		}
		finally {
			// do for sure before leaving
		}
	}
	// $ANTLR end "K_CREATE"

	// $ANTLR start "K_KEYSPACE"
	public final void mK_KEYSPACE() throws RecognitionException {
		try {
			int _type = K_KEYSPACE;
			int _channel = DEFAULT_TOKEN_CHANNEL;
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1267:11: ( ( K E Y S P A C E | S C H E M A ) )
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1267:16: ( K E Y S P A C E | S C H E M A )
			{
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1267:16: ( K E Y S P A C E | S C H E M A )
			int alt1=2;
			int LA1_0 = input.LA(1);
			if ( (LA1_0=='K'||LA1_0=='k') ) {
				alt1=1;
			}
			else if ( (LA1_0=='S'||LA1_0=='s') ) {
				alt1=2;
			}

			else {
				NoViableAltException nvae =
					new NoViableAltException("", 1, 0, input);
				throw nvae;
			}

			switch (alt1) {
				case 1 :
					// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1267:18: K E Y S P A C E
					{
					mK(); 

					mE(); 

					mY(); 

					mS(); 

					mP(); 

					mA(); 

					mC(); 

					mE(); 

					}
					break;
				case 2 :
					// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1268:20: S C H E M A
					{
					mS(); 

					mC(); 

					mH(); 

					mE(); 

					mM(); 

					mA(); 

					}
					break;

			}

			}

			state.type = _type;
			state.channel = _channel;
		}
		finally {
			// do for sure before leaving
		}
	}
	// $ANTLR end "K_KEYSPACE"

	// $ANTLR start "K_KEYSPACES"
	public final void mK_KEYSPACES() throws RecognitionException {
		try {
			int _type = K_KEYSPACES;
			int _channel = DEFAULT_TOKEN_CHANNEL;
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1269:12: ( K E Y S P A C E S )
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1269:16: K E Y S P A C E S
			{
			mK(); 

			mE(); 

			mY(); 

			mS(); 

			mP(); 

			mA(); 

			mC(); 

			mE(); 

			mS(); 

			}

			state.type = _type;
			state.channel = _channel;
		}
		finally {
			// do for sure before leaving
		}
	}
	// $ANTLR end "K_KEYSPACES"

	// $ANTLR start "K_COLUMNFAMILY"
	public final void mK_COLUMNFAMILY() throws RecognitionException {
		try {
			int _type = K_COLUMNFAMILY;
			int _channel = DEFAULT_TOKEN_CHANNEL;
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1270:15: ( ( C O L U M N F A M I L Y | T A B L E ) )
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1270:16: ( C O L U M N F A M I L Y | T A B L E )
			{
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1270:16: ( C O L U M N F A M I L Y | T A B L E )
			int alt2=2;
			int LA2_0 = input.LA(1);
			if ( (LA2_0=='C'||LA2_0=='c') ) {
				alt2=1;
			}
			else if ( (LA2_0=='T'||LA2_0=='t') ) {
				alt2=2;
			}

			else {
				NoViableAltException nvae =
					new NoViableAltException("", 2, 0, input);
				throw nvae;
			}

			switch (alt2) {
				case 1 :
					// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1270:18: C O L U M N F A M I L Y
					{
					mC(); 

					mO(); 

					mL(); 

					mU(); 

					mM(); 

					mN(); 

					mF(); 

					mA(); 

					mM(); 

					mI(); 

					mL(); 

					mY(); 

					}
					break;
				case 2 :
					// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1271:20: T A B L E
					{
					mT(); 

					mA(); 

					mB(); 

					mL(); 

					mE(); 

					}
					break;

			}

			}

			state.type = _type;
			state.channel = _channel;
		}
		finally {
			// do for sure before leaving
		}
	}
	// $ANTLR end "K_COLUMNFAMILY"

	// $ANTLR start "K_INDEX"
	public final void mK_INDEX() throws RecognitionException {
		try {
			int _type = K_INDEX;
			int _channel = DEFAULT_TOKEN_CHANNEL;
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1272:8: ( I N D E X )
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1272:16: I N D E X
			{
			mI(); 

			mN(); 

			mD(); 

			mE(); 

			mX(); 

			}

			state.type = _type;
			state.channel = _channel;
		}
		finally {
			// do for sure before leaving
		}
	}
	// $ANTLR end "K_INDEX"

	// $ANTLR start "K_CUSTOM"
	public final void mK_CUSTOM() throws RecognitionException {
		try {
			int _type = K_CUSTOM;
			int _channel = DEFAULT_TOKEN_CHANNEL;
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1273:9: ( C U S T O M )
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1273:16: C U S T O M
			{
			mC(); 

			mU(); 

			mS(); 

			mT(); 

			mO(); 

			mM(); 

			}

			state.type = _type;
			state.channel = _channel;
		}
		finally {
			// do for sure before leaving
		}
	}
	// $ANTLR end "K_CUSTOM"

	// $ANTLR start "K_ON"
	public final void mK_ON() throws RecognitionException {
		try {
			int _type = K_ON;
			int _channel = DEFAULT_TOKEN_CHANNEL;
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1274:5: ( O N )
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1274:16: O N
			{
			mO(); 

			mN(); 

			}

			state.type = _type;
			state.channel = _channel;
		}
		finally {
			// do for sure before leaving
		}
	}
	// $ANTLR end "K_ON"

	// $ANTLR start "K_TO"
	public final void mK_TO() throws RecognitionException {
		try {
			int _type = K_TO;
			int _channel = DEFAULT_TOKEN_CHANNEL;
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1275:5: ( T O )
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1275:16: T O
			{
			mT(); 

			mO(); 

			}

			state.type = _type;
			state.channel = _channel;
		}
		finally {
			// do for sure before leaving
		}
	}
	// $ANTLR end "K_TO"

	// $ANTLR start "K_DROP"
	public final void mK_DROP() throws RecognitionException {
		try {
			int _type = K_DROP;
			int _channel = DEFAULT_TOKEN_CHANNEL;
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1276:7: ( D R O P )
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1276:16: D R O P
			{
			mD(); 

			mR(); 

			mO(); 

			mP(); 

			}

			state.type = _type;
			state.channel = _channel;
		}
		finally {
			// do for sure before leaving
		}
	}
	// $ANTLR end "K_DROP"

	// $ANTLR start "K_PRIMARY"
	public final void mK_PRIMARY() throws RecognitionException {
		try {
			int _type = K_PRIMARY;
			int _channel = DEFAULT_TOKEN_CHANNEL;
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1277:10: ( P R I M A R Y )
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1277:16: P R I M A R Y
			{
			mP(); 

			mR(); 

			mI(); 

			mM(); 

			mA(); 

			mR(); 

			mY(); 

			}

			state.type = _type;
			state.channel = _channel;
		}
		finally {
			// do for sure before leaving
		}
	}
	// $ANTLR end "K_PRIMARY"

	// $ANTLR start "K_INTO"
	public final void mK_INTO() throws RecognitionException {
		try {
			int _type = K_INTO;
			int _channel = DEFAULT_TOKEN_CHANNEL;
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1278:7: ( I N T O )
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1278:16: I N T O
			{
			mI(); 

			mN(); 

			mT(); 

			mO(); 

			}

			state.type = _type;
			state.channel = _channel;
		}
		finally {
			// do for sure before leaving
		}
	}
	// $ANTLR end "K_INTO"

	// $ANTLR start "K_VALUES"
	public final void mK_VALUES() throws RecognitionException {
		try {
			int _type = K_VALUES;
			int _channel = DEFAULT_TOKEN_CHANNEL;
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1279:9: ( V A L U E S )
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1279:16: V A L U E S
			{
			mV(); 

			mA(); 

			mL(); 

			mU(); 

			mE(); 

			mS(); 

			}

			state.type = _type;
			state.channel = _channel;
		}
		finally {
			// do for sure before leaving
		}
	}
	// $ANTLR end "K_VALUES"

	// $ANTLR start "K_TIMESTAMP"
	public final void mK_TIMESTAMP() throws RecognitionException {
		try {
			int _type = K_TIMESTAMP;
			int _channel = DEFAULT_TOKEN_CHANNEL;
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1280:12: ( T I M E S T A M P )
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1280:16: T I M E S T A M P
			{
			mT(); 

			mI(); 

			mM(); 

			mE(); 

			mS(); 

			mT(); 

			mA(); 

			mM(); 

			mP(); 

			}

			state.type = _type;
			state.channel = _channel;
		}
		finally {
			// do for sure before leaving
		}
	}
	// $ANTLR end "K_TIMESTAMP"

	// $ANTLR start "K_TTL"
	public final void mK_TTL() throws RecognitionException {
		try {
			int _type = K_TTL;
			int _channel = DEFAULT_TOKEN_CHANNEL;
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1281:6: ( T T L )
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1281:16: T T L
			{
			mT(); 

			mT(); 

			mL(); 

			}

			state.type = _type;
			state.channel = _channel;
		}
		finally {
			// do for sure before leaving
		}
	}
	// $ANTLR end "K_TTL"

	// $ANTLR start "K_ALTER"
	public final void mK_ALTER() throws RecognitionException {
		try {
			int _type = K_ALTER;
			int _channel = DEFAULT_TOKEN_CHANNEL;
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1282:8: ( A L T E R )
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1282:16: A L T E R
			{
			mA(); 

			mL(); 

			mT(); 

			mE(); 

			mR(); 

			}

			state.type = _type;
			state.channel = _channel;
		}
		finally {
			// do for sure before leaving
		}
	}
	// $ANTLR end "K_ALTER"

	// $ANTLR start "K_RENAME"
	public final void mK_RENAME() throws RecognitionException {
		try {
			int _type = K_RENAME;
			int _channel = DEFAULT_TOKEN_CHANNEL;
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1283:9: ( R E N A M E )
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1283:16: R E N A M E
			{
			mR(); 

			mE(); 

			mN(); 

			mA(); 

			mM(); 

			mE(); 

			}

			state.type = _type;
			state.channel = _channel;
		}
		finally {
			// do for sure before leaving
		}
	}
	// $ANTLR end "K_RENAME"

	// $ANTLR start "K_ADD"
	public final void mK_ADD() throws RecognitionException {
		try {
			int _type = K_ADD;
			int _channel = DEFAULT_TOKEN_CHANNEL;
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1284:6: ( A D D )
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1284:16: A D D
			{
			mA(); 

			mD(); 

			mD(); 

			}

			state.type = _type;
			state.channel = _channel;
		}
		finally {
			// do for sure before leaving
		}
	}
	// $ANTLR end "K_ADD"

	// $ANTLR start "K_TYPE"
	public final void mK_TYPE() throws RecognitionException {
		try {
			int _type = K_TYPE;
			int _channel = DEFAULT_TOKEN_CHANNEL;
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1285:7: ( T Y P E )
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1285:16: T Y P E
			{
			mT(); 

			mY(); 

			mP(); 

			mE(); 

			}

			state.type = _type;
			state.channel = _channel;
		}
		finally {
			// do for sure before leaving
		}
	}
	// $ANTLR end "K_TYPE"

	// $ANTLR start "K_COMPACT"
	public final void mK_COMPACT() throws RecognitionException {
		try {
			int _type = K_COMPACT;
			int _channel = DEFAULT_TOKEN_CHANNEL;
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1286:10: ( C O M P A C T )
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1286:16: C O M P A C T
			{
			mC(); 

			mO(); 

			mM(); 

			mP(); 

			mA(); 

			mC(); 

			mT(); 

			}

			state.type = _type;
			state.channel = _channel;
		}
		finally {
			// do for sure before leaving
		}
	}
	// $ANTLR end "K_COMPACT"

	// $ANTLR start "K_STORAGE"
	public final void mK_STORAGE() throws RecognitionException {
		try {
			int _type = K_STORAGE;
			int _channel = DEFAULT_TOKEN_CHANNEL;
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1287:10: ( S T O R A G E )
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1287:16: S T O R A G E
			{
			mS(); 

			mT(); 

			mO(); 

			mR(); 

			mA(); 

			mG(); 

			mE(); 

			}

			state.type = _type;
			state.channel = _channel;
		}
		finally {
			// do for sure before leaving
		}
	}
	// $ANTLR end "K_STORAGE"

	// $ANTLR start "K_ORDER"
	public final void mK_ORDER() throws RecognitionException {
		try {
			int _type = K_ORDER;
			int _channel = DEFAULT_TOKEN_CHANNEL;
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1288:8: ( O R D E R )
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1288:16: O R D E R
			{
			mO(); 

			mR(); 

			mD(); 

			mE(); 

			mR(); 

			}

			state.type = _type;
			state.channel = _channel;
		}
		finally {
			// do for sure before leaving
		}
	}
	// $ANTLR end "K_ORDER"

	// $ANTLR start "K_BY"
	public final void mK_BY() throws RecognitionException {
		try {
			int _type = K_BY;
			int _channel = DEFAULT_TOKEN_CHANNEL;
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1289:5: ( B Y )
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1289:16: B Y
			{
			mB(); 

			mY(); 

			}

			state.type = _type;
			state.channel = _channel;
		}
		finally {
			// do for sure before leaving
		}
	}
	// $ANTLR end "K_BY"

	// $ANTLR start "K_ASC"
	public final void mK_ASC() throws RecognitionException {
		try {
			int _type = K_ASC;
			int _channel = DEFAULT_TOKEN_CHANNEL;
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1290:6: ( A S C )
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1290:16: A S C
			{
			mA(); 

			mS(); 

			mC(); 

			}

			state.type = _type;
			state.channel = _channel;
		}
		finally {
			// do for sure before leaving
		}
	}
	// $ANTLR end "K_ASC"

	// $ANTLR start "K_DESC"
	public final void mK_DESC() throws RecognitionException {
		try {
			int _type = K_DESC;
			int _channel = DEFAULT_TOKEN_CHANNEL;
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1291:7: ( D E S C )
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1291:16: D E S C
			{
			mD(); 

			mE(); 

			mS(); 

			mC(); 

			}

			state.type = _type;
			state.channel = _channel;
		}
		finally {
			// do for sure before leaving
		}
	}
	// $ANTLR end "K_DESC"

	// $ANTLR start "K_ALLOW"
	public final void mK_ALLOW() throws RecognitionException {
		try {
			int _type = K_ALLOW;
			int _channel = DEFAULT_TOKEN_CHANNEL;
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1292:8: ( A L L O W )
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1292:16: A L L O W
			{
			mA(); 

			mL(); 

			mL(); 

			mO(); 

			mW(); 

			}

			state.type = _type;
			state.channel = _channel;
		}
		finally {
			// do for sure before leaving
		}
	}
	// $ANTLR end "K_ALLOW"

	// $ANTLR start "K_FILTERING"
	public final void mK_FILTERING() throws RecognitionException {
		try {
			int _type = K_FILTERING;
			int _channel = DEFAULT_TOKEN_CHANNEL;
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1293:12: ( F I L T E R I N G )
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1293:16: F I L T E R I N G
			{
			mF(); 

			mI(); 

			mL(); 

			mT(); 

			mE(); 

			mR(); 

			mI(); 

			mN(); 

			mG(); 

			}

			state.type = _type;
			state.channel = _channel;
		}
		finally {
			// do for sure before leaving
		}
	}
	// $ANTLR end "K_FILTERING"

	// $ANTLR start "K_IF"
	public final void mK_IF() throws RecognitionException {
		try {
			int _type = K_IF;
			int _channel = DEFAULT_TOKEN_CHANNEL;
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1294:5: ( I F )
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1294:16: I F
			{
			mI(); 

			mF(); 

			}

			state.type = _type;
			state.channel = _channel;
		}
		finally {
			// do for sure before leaving
		}
	}
	// $ANTLR end "K_IF"

	// $ANTLR start "K_CONTAINS"
	public final void mK_CONTAINS() throws RecognitionException {
		try {
			int _type = K_CONTAINS;
			int _channel = DEFAULT_TOKEN_CHANNEL;
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1295:11: ( C O N T A I N S )
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1295:16: C O N T A I N S
			{
			mC(); 

			mO(); 

			mN(); 

			mT(); 

			mA(); 

			mI(); 

			mN(); 

			mS(); 

			}

			state.type = _type;
			state.channel = _channel;
		}
		finally {
			// do for sure before leaving
		}
	}
	// $ANTLR end "K_CONTAINS"

	// $ANTLR start "K_GRANT"
	public final void mK_GRANT() throws RecognitionException {
		try {
			int _type = K_GRANT;
			int _channel = DEFAULT_TOKEN_CHANNEL;
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1297:8: ( G R A N T )
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1297:16: G R A N T
			{
			mG(); 

			mR(); 

			mA(); 

			mN(); 

			mT(); 

			}

			state.type = _type;
			state.channel = _channel;
		}
		finally {
			// do for sure before leaving
		}
	}
	// $ANTLR end "K_GRANT"

	// $ANTLR start "K_ALL"
	public final void mK_ALL() throws RecognitionException {
		try {
			int _type = K_ALL;
			int _channel = DEFAULT_TOKEN_CHANNEL;
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1298:6: ( A L L )
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1298:16: A L L
			{
			mA(); 

			mL(); 

			mL(); 

			}

			state.type = _type;
			state.channel = _channel;
		}
		finally {
			// do for sure before leaving
		}
	}
	// $ANTLR end "K_ALL"

	// $ANTLR start "K_PERMISSION"
	public final void mK_PERMISSION() throws RecognitionException {
		try {
			int _type = K_PERMISSION;
			int _channel = DEFAULT_TOKEN_CHANNEL;
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1299:13: ( P E R M I S S I O N )
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1299:16: P E R M I S S I O N
			{
			mP(); 

			mE(); 

			mR(); 

			mM(); 

			mI(); 

			mS(); 

			mS(); 

			mI(); 

			mO(); 

			mN(); 

			}

			state.type = _type;
			state.channel = _channel;
		}
		finally {
			// do for sure before leaving
		}
	}
	// $ANTLR end "K_PERMISSION"

	// $ANTLR start "K_PERMISSIONS"
	public final void mK_PERMISSIONS() throws RecognitionException {
		try {
			int _type = K_PERMISSIONS;
			int _channel = DEFAULT_TOKEN_CHANNEL;
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1300:14: ( P E R M I S S I O N S )
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1300:16: P E R M I S S I O N S
			{
			mP(); 

			mE(); 

			mR(); 

			mM(); 

			mI(); 

			mS(); 

			mS(); 

			mI(); 

			mO(); 

			mN(); 

			mS(); 

			}

			state.type = _type;
			state.channel = _channel;
		}
		finally {
			// do for sure before leaving
		}
	}
	// $ANTLR end "K_PERMISSIONS"

	// $ANTLR start "K_OF"
	public final void mK_OF() throws RecognitionException {
		try {
			int _type = K_OF;
			int _channel = DEFAULT_TOKEN_CHANNEL;
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1301:5: ( O F )
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1301:16: O F
			{
			mO(); 

			mF(); 

			}

			state.type = _type;
			state.channel = _channel;
		}
		finally {
			// do for sure before leaving
		}
	}
	// $ANTLR end "K_OF"

	// $ANTLR start "K_REVOKE"
	public final void mK_REVOKE() throws RecognitionException {
		try {
			int _type = K_REVOKE;
			int _channel = DEFAULT_TOKEN_CHANNEL;
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1302:9: ( R E V O K E )
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1302:16: R E V O K E
			{
			mR(); 

			mE(); 

			mV(); 

			mO(); 

			mK(); 

			mE(); 

			}

			state.type = _type;
			state.channel = _channel;
		}
		finally {
			// do for sure before leaving
		}
	}
	// $ANTLR end "K_REVOKE"

	// $ANTLR start "K_MODIFY"
	public final void mK_MODIFY() throws RecognitionException {
		try {
			int _type = K_MODIFY;
			int _channel = DEFAULT_TOKEN_CHANNEL;
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1303:9: ( M O D I F Y )
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1303:16: M O D I F Y
			{
			mM(); 

			mO(); 

			mD(); 

			mI(); 

			mF(); 

			mY(); 

			}

			state.type = _type;
			state.channel = _channel;
		}
		finally {
			// do for sure before leaving
		}
	}
	// $ANTLR end "K_MODIFY"

	// $ANTLR start "K_AUTHORIZE"
	public final void mK_AUTHORIZE() throws RecognitionException {
		try {
			int _type = K_AUTHORIZE;
			int _channel = DEFAULT_TOKEN_CHANNEL;
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1304:12: ( A U T H O R I Z E )
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1304:16: A U T H O R I Z E
			{
			mA(); 

			mU(); 

			mT(); 

			mH(); 

			mO(); 

			mR(); 

			mI(); 

			mZ(); 

			mE(); 

			}

			state.type = _type;
			state.channel = _channel;
		}
		finally {
			// do for sure before leaving
		}
	}
	// $ANTLR end "K_AUTHORIZE"

	// $ANTLR start "K_NORECURSIVE"
	public final void mK_NORECURSIVE() throws RecognitionException {
		try {
			int _type = K_NORECURSIVE;
			int _channel = DEFAULT_TOKEN_CHANNEL;
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1305:14: ( N O R E C U R S I V E )
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1305:16: N O R E C U R S I V E
			{
			mN(); 

			mO(); 

			mR(); 

			mE(); 

			mC(); 

			mU(); 

			mR(); 

			mS(); 

			mI(); 

			mV(); 

			mE(); 

			}

			state.type = _type;
			state.channel = _channel;
		}
		finally {
			// do for sure before leaving
		}
	}
	// $ANTLR end "K_NORECURSIVE"

	// $ANTLR start "K_USER"
	public final void mK_USER() throws RecognitionException {
		try {
			int _type = K_USER;
			int _channel = DEFAULT_TOKEN_CHANNEL;
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1307:7: ( U S E R )
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1307:16: U S E R
			{
			mU(); 

			mS(); 

			mE(); 

			mR(); 

			}

			state.type = _type;
			state.channel = _channel;
		}
		finally {
			// do for sure before leaving
		}
	}
	// $ANTLR end "K_USER"

	// $ANTLR start "K_USERS"
	public final void mK_USERS() throws RecognitionException {
		try {
			int _type = K_USERS;
			int _channel = DEFAULT_TOKEN_CHANNEL;
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1308:8: ( U S E R S )
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1308:16: U S E R S
			{
			mU(); 

			mS(); 

			mE(); 

			mR(); 

			mS(); 

			}

			state.type = _type;
			state.channel = _channel;
		}
		finally {
			// do for sure before leaving
		}
	}
	// $ANTLR end "K_USERS"

	// $ANTLR start "K_SUPERUSER"
	public final void mK_SUPERUSER() throws RecognitionException {
		try {
			int _type = K_SUPERUSER;
			int _channel = DEFAULT_TOKEN_CHANNEL;
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1309:12: ( S U P E R U S E R )
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1309:16: S U P E R U S E R
			{
			mS(); 

			mU(); 

			mP(); 

			mE(); 

			mR(); 

			mU(); 

			mS(); 

			mE(); 

			mR(); 

			}

			state.type = _type;
			state.channel = _channel;
		}
		finally {
			// do for sure before leaving
		}
	}
	// $ANTLR end "K_SUPERUSER"

	// $ANTLR start "K_NOSUPERUSER"
	public final void mK_NOSUPERUSER() throws RecognitionException {
		try {
			int _type = K_NOSUPERUSER;
			int _channel = DEFAULT_TOKEN_CHANNEL;
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1310:14: ( N O S U P E R U S E R )
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1310:16: N O S U P E R U S E R
			{
			mN(); 

			mO(); 

			mS(); 

			mU(); 

			mP(); 

			mE(); 

			mR(); 

			mU(); 

			mS(); 

			mE(); 

			mR(); 

			}

			state.type = _type;
			state.channel = _channel;
		}
		finally {
			// do for sure before leaving
		}
	}
	// $ANTLR end "K_NOSUPERUSER"

	// $ANTLR start "K_PASSWORD"
	public final void mK_PASSWORD() throws RecognitionException {
		try {
			int _type = K_PASSWORD;
			int _channel = DEFAULT_TOKEN_CHANNEL;
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1311:11: ( P A S S W O R D )
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1311:16: P A S S W O R D
			{
			mP(); 

			mA(); 

			mS(); 

			mS(); 

			mW(); 

			mO(); 

			mR(); 

			mD(); 

			}

			state.type = _type;
			state.channel = _channel;
		}
		finally {
			// do for sure before leaving
		}
	}
	// $ANTLR end "K_PASSWORD"

	// $ANTLR start "K_CLUSTERING"
	public final void mK_CLUSTERING() throws RecognitionException {
		try {
			int _type = K_CLUSTERING;
			int _channel = DEFAULT_TOKEN_CHANNEL;
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1313:13: ( C L U S T E R I N G )
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1313:16: C L U S T E R I N G
			{
			mC(); 

			mL(); 

			mU(); 

			mS(); 

			mT(); 

			mE(); 

			mR(); 

			mI(); 

			mN(); 

			mG(); 

			}

			state.type = _type;
			state.channel = _channel;
		}
		finally {
			// do for sure before leaving
		}
	}
	// $ANTLR end "K_CLUSTERING"

	// $ANTLR start "K_ASCII"
	public final void mK_ASCII() throws RecognitionException {
		try {
			int _type = K_ASCII;
			int _channel = DEFAULT_TOKEN_CHANNEL;
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1314:8: ( A S C I I )
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1314:16: A S C I I
			{
			mA(); 

			mS(); 

			mC(); 

			mI(); 

			mI(); 

			}

			state.type = _type;
			state.channel = _channel;
		}
		finally {
			// do for sure before leaving
		}
	}
	// $ANTLR end "K_ASCII"

	// $ANTLR start "K_BIGINT"
	public final void mK_BIGINT() throws RecognitionException {
		try {
			int _type = K_BIGINT;
			int _channel = DEFAULT_TOKEN_CHANNEL;
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1315:9: ( B I G I N T )
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1315:16: B I G I N T
			{
			mB(); 

			mI(); 

			mG(); 

			mI(); 

			mN(); 

			mT(); 

			}

			state.type = _type;
			state.channel = _channel;
		}
		finally {
			// do for sure before leaving
		}
	}
	// $ANTLR end "K_BIGINT"

	// $ANTLR start "K_BLOB"
	public final void mK_BLOB() throws RecognitionException {
		try {
			int _type = K_BLOB;
			int _channel = DEFAULT_TOKEN_CHANNEL;
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1316:7: ( B L O B )
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1316:16: B L O B
			{
			mB(); 

			mL(); 

			mO(); 

			mB(); 

			}

			state.type = _type;
			state.channel = _channel;
		}
		finally {
			// do for sure before leaving
		}
	}
	// $ANTLR end "K_BLOB"

	// $ANTLR start "K_BOOLEAN"
	public final void mK_BOOLEAN() throws RecognitionException {
		try {
			int _type = K_BOOLEAN;
			int _channel = DEFAULT_TOKEN_CHANNEL;
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1317:10: ( B O O L E A N )
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1317:16: B O O L E A N
			{
			mB(); 

			mO(); 

			mO(); 

			mL(); 

			mE(); 

			mA(); 

			mN(); 

			}

			state.type = _type;
			state.channel = _channel;
		}
		finally {
			// do for sure before leaving
		}
	}
	// $ANTLR end "K_BOOLEAN"

	// $ANTLR start "K_COUNTER"
	public final void mK_COUNTER() throws RecognitionException {
		try {
			int _type = K_COUNTER;
			int _channel = DEFAULT_TOKEN_CHANNEL;
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1318:10: ( C O U N T E R )
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1318:16: C O U N T E R
			{
			mC(); 

			mO(); 

			mU(); 

			mN(); 

			mT(); 

			mE(); 

			mR(); 

			}

			state.type = _type;
			state.channel = _channel;
		}
		finally {
			// do for sure before leaving
		}
	}
	// $ANTLR end "K_COUNTER"

	// $ANTLR start "K_DECIMAL"
	public final void mK_DECIMAL() throws RecognitionException {
		try {
			int _type = K_DECIMAL;
			int _channel = DEFAULT_TOKEN_CHANNEL;
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1319:10: ( D E C I M A L )
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1319:16: D E C I M A L
			{
			mD(); 

			mE(); 

			mC(); 

			mI(); 

			mM(); 

			mA(); 

			mL(); 

			}

			state.type = _type;
			state.channel = _channel;
		}
		finally {
			// do for sure before leaving
		}
	}
	// $ANTLR end "K_DECIMAL"

	// $ANTLR start "K_DOUBLE"
	public final void mK_DOUBLE() throws RecognitionException {
		try {
			int _type = K_DOUBLE;
			int _channel = DEFAULT_TOKEN_CHANNEL;
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1320:9: ( D O U B L E )
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1320:16: D O U B L E
			{
			mD(); 

			mO(); 

			mU(); 

			mB(); 

			mL(); 

			mE(); 

			}

			state.type = _type;
			state.channel = _channel;
		}
		finally {
			// do for sure before leaving
		}
	}
	// $ANTLR end "K_DOUBLE"

	// $ANTLR start "K_FLOAT"
	public final void mK_FLOAT() throws RecognitionException {
		try {
			int _type = K_FLOAT;
			int _channel = DEFAULT_TOKEN_CHANNEL;
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1321:8: ( F L O A T )
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1321:16: F L O A T
			{
			mF(); 

			mL(); 

			mO(); 

			mA(); 

			mT(); 

			}

			state.type = _type;
			state.channel = _channel;
		}
		finally {
			// do for sure before leaving
		}
	}
	// $ANTLR end "K_FLOAT"

	// $ANTLR start "K_INET"
	public final void mK_INET() throws RecognitionException {
		try {
			int _type = K_INET;
			int _channel = DEFAULT_TOKEN_CHANNEL;
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1322:7: ( I N E T )
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1322:16: I N E T
			{
			mI(); 

			mN(); 

			mE(); 

			mT(); 

			}

			state.type = _type;
			state.channel = _channel;
		}
		finally {
			// do for sure before leaving
		}
	}
	// $ANTLR end "K_INET"

	// $ANTLR start "K_INT"
	public final void mK_INT() throws RecognitionException {
		try {
			int _type = K_INT;
			int _channel = DEFAULT_TOKEN_CHANNEL;
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1323:6: ( I N T )
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1323:16: I N T
			{
			mI(); 

			mN(); 

			mT(); 

			}

			state.type = _type;
			state.channel = _channel;
		}
		finally {
			// do for sure before leaving
		}
	}
	// $ANTLR end "K_INT"

	// $ANTLR start "K_TEXT"
	public final void mK_TEXT() throws RecognitionException {
		try {
			int _type = K_TEXT;
			int _channel = DEFAULT_TOKEN_CHANNEL;
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1324:7: ( T E X T )
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1324:16: T E X T
			{
			mT(); 

			mE(); 

			mX(); 

			mT(); 

			}

			state.type = _type;
			state.channel = _channel;
		}
		finally {
			// do for sure before leaving
		}
	}
	// $ANTLR end "K_TEXT"

	// $ANTLR start "K_UUID"
	public final void mK_UUID() throws RecognitionException {
		try {
			int _type = K_UUID;
			int _channel = DEFAULT_TOKEN_CHANNEL;
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1325:7: ( U U I D )
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1325:16: U U I D
			{
			mU(); 

			mU(); 

			mI(); 

			mD(); 

			}

			state.type = _type;
			state.channel = _channel;
		}
		finally {
			// do for sure before leaving
		}
	}
	// $ANTLR end "K_UUID"

	// $ANTLR start "K_VARCHAR"
	public final void mK_VARCHAR() throws RecognitionException {
		try {
			int _type = K_VARCHAR;
			int _channel = DEFAULT_TOKEN_CHANNEL;
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1326:10: ( V A R C H A R )
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1326:16: V A R C H A R
			{
			mV(); 

			mA(); 

			mR(); 

			mC(); 

			mH(); 

			mA(); 

			mR(); 

			}

			state.type = _type;
			state.channel = _channel;
		}
		finally {
			// do for sure before leaving
		}
	}
	// $ANTLR end "K_VARCHAR"

	// $ANTLR start "K_VARINT"
	public final void mK_VARINT() throws RecognitionException {
		try {
			int _type = K_VARINT;
			int _channel = DEFAULT_TOKEN_CHANNEL;
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1327:9: ( V A R I N T )
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1327:16: V A R I N T
			{
			mV(); 

			mA(); 

			mR(); 

			mI(); 

			mN(); 

			mT(); 

			}

			state.type = _type;
			state.channel = _channel;
		}
		finally {
			// do for sure before leaving
		}
	}
	// $ANTLR end "K_VARINT"

	// $ANTLR start "K_TIMEUUID"
	public final void mK_TIMEUUID() throws RecognitionException {
		try {
			int _type = K_TIMEUUID;
			int _channel = DEFAULT_TOKEN_CHANNEL;
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1328:11: ( T I M E U U I D )
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1328:16: T I M E U U I D
			{
			mT(); 

			mI(); 

			mM(); 

			mE(); 

			mU(); 

			mU(); 

			mI(); 

			mD(); 

			}

			state.type = _type;
			state.channel = _channel;
		}
		finally {
			// do for sure before leaving
		}
	}
	// $ANTLR end "K_TIMEUUID"

	// $ANTLR start "K_TOKEN"
	public final void mK_TOKEN() throws RecognitionException {
		try {
			int _type = K_TOKEN;
			int _channel = DEFAULT_TOKEN_CHANNEL;
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1329:8: ( T O K E N )
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1329:16: T O K E N
			{
			mT(); 

			mO(); 

			mK(); 

			mE(); 

			mN(); 

			}

			state.type = _type;
			state.channel = _channel;
		}
		finally {
			// do for sure before leaving
		}
	}
	// $ANTLR end "K_TOKEN"

	// $ANTLR start "K_WRITETIME"
	public final void mK_WRITETIME() throws RecognitionException {
		try {
			int _type = K_WRITETIME;
			int _channel = DEFAULT_TOKEN_CHANNEL;
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1330:12: ( W R I T E T I M E )
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1330:16: W R I T E T I M E
			{
			mW(); 

			mR(); 

			mI(); 

			mT(); 

			mE(); 

			mT(); 

			mI(); 

			mM(); 

			mE(); 

			}

			state.type = _type;
			state.channel = _channel;
		}
		finally {
			// do for sure before leaving
		}
	}
	// $ANTLR end "K_WRITETIME"

	// $ANTLR start "K_NULL"
	public final void mK_NULL() throws RecognitionException {
		try {
			int _type = K_NULL;
			int _channel = DEFAULT_TOKEN_CHANNEL;
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1332:7: ( N U L L )
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1332:16: N U L L
			{
			mN(); 

			mU(); 

			mL(); 

			mL(); 

			}

			state.type = _type;
			state.channel = _channel;
		}
		finally {
			// do for sure before leaving
		}
	}
	// $ANTLR end "K_NULL"

	// $ANTLR start "K_NOT"
	public final void mK_NOT() throws RecognitionException {
		try {
			int _type = K_NOT;
			int _channel = DEFAULT_TOKEN_CHANNEL;
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1333:6: ( N O T )
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1333:16: N O T
			{
			mN(); 

			mO(); 

			mT(); 

			}

			state.type = _type;
			state.channel = _channel;
		}
		finally {
			// do for sure before leaving
		}
	}
	// $ANTLR end "K_NOT"

	// $ANTLR start "K_EXISTS"
	public final void mK_EXISTS() throws RecognitionException {
		try {
			int _type = K_EXISTS;
			int _channel = DEFAULT_TOKEN_CHANNEL;
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1334:9: ( E X I S T S )
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1334:16: E X I S T S
			{
			mE(); 

			mX(); 

			mI(); 

			mS(); 

			mT(); 

			mS(); 

			}

			state.type = _type;
			state.channel = _channel;
		}
		finally {
			// do for sure before leaving
		}
	}
	// $ANTLR end "K_EXISTS"

	// $ANTLR start "K_MAP"
	public final void mK_MAP() throws RecognitionException {
		try {
			int _type = K_MAP;
			int _channel = DEFAULT_TOKEN_CHANNEL;
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1336:6: ( M A P )
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1336:16: M A P
			{
			mM(); 

			mA(); 

			mP(); 

			}

			state.type = _type;
			state.channel = _channel;
		}
		finally {
			// do for sure before leaving
		}
	}
	// $ANTLR end "K_MAP"

	// $ANTLR start "K_LIST"
	public final void mK_LIST() throws RecognitionException {
		try {
			int _type = K_LIST;
			int _channel = DEFAULT_TOKEN_CHANNEL;
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1337:7: ( L I S T )
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1337:16: L I S T
			{
			mL(); 

			mI(); 

			mS(); 

			mT(); 

			}

			state.type = _type;
			state.channel = _channel;
		}
		finally {
			// do for sure before leaving
		}
	}
	// $ANTLR end "K_LIST"

	// $ANTLR start "K_NAN"
	public final void mK_NAN() throws RecognitionException {
		try {
			int _type = K_NAN;
			int _channel = DEFAULT_TOKEN_CHANNEL;
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1338:6: ( N A N )
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1338:16: N A N
			{
			mN(); 

			mA(); 

			mN(); 

			}

			state.type = _type;
			state.channel = _channel;
		}
		finally {
			// do for sure before leaving
		}
	}
	// $ANTLR end "K_NAN"

	// $ANTLR start "K_INFINITY"
	public final void mK_INFINITY() throws RecognitionException {
		try {
			int _type = K_INFINITY;
			int _channel = DEFAULT_TOKEN_CHANNEL;
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1339:11: ( I N F I N I T Y )
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1339:16: I N F I N I T Y
			{
			mI(); 

			mN(); 

			mF(); 

			mI(); 

			mN(); 

			mI(); 

			mT(); 

			mY(); 

			}

			state.type = _type;
			state.channel = _channel;
		}
		finally {
			// do for sure before leaving
		}
	}
	// $ANTLR end "K_INFINITY"

	// $ANTLR start "K_TUPLE"
	public final void mK_TUPLE() throws RecognitionException {
		try {
			int _type = K_TUPLE;
			int _channel = DEFAULT_TOKEN_CHANNEL;
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1340:8: ( T U P L E )
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1340:16: T U P L E
			{
			mT(); 

			mU(); 

			mP(); 

			mL(); 

			mE(); 

			}

			state.type = _type;
			state.channel = _channel;
		}
		finally {
			// do for sure before leaving
		}
	}
	// $ANTLR end "K_TUPLE"

	// $ANTLR start "K_TRIGGER"
	public final void mK_TRIGGER() throws RecognitionException {
		try {
			int _type = K_TRIGGER;
			int _channel = DEFAULT_TOKEN_CHANNEL;
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1342:10: ( T R I G G E R )
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1342:16: T R I G G E R
			{
			mT(); 

			mR(); 

			mI(); 

			mG(); 

			mG(); 

			mE(); 

			mR(); 

			}

			state.type = _type;
			state.channel = _channel;
		}
		finally {
			// do for sure before leaving
		}
	}
	// $ANTLR end "K_TRIGGER"

	// $ANTLR start "K_STATIC"
	public final void mK_STATIC() throws RecognitionException {
		try {
			int _type = K_STATIC;
			int _channel = DEFAULT_TOKEN_CHANNEL;
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1343:9: ( S T A T I C )
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1343:16: S T A T I C
			{
			mS(); 

			mT(); 

			mA(); 

			mT(); 

			mI(); 

			mC(); 

			}

			state.type = _type;
			state.channel = _channel;
		}
		finally {
			// do for sure before leaving
		}
	}
	// $ANTLR end "K_STATIC"

	// $ANTLR start "K_FROZEN"
	public final void mK_FROZEN() throws RecognitionException {
		try {
			int _type = K_FROZEN;
			int _channel = DEFAULT_TOKEN_CHANNEL;
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1344:9: ( F R O Z E N )
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1344:16: F R O Z E N
			{
			mF(); 

			mR(); 

			mO(); 

			mZ(); 

			mE(); 

			mN(); 

			}

			state.type = _type;
			state.channel = _channel;
		}
		finally {
			// do for sure before leaving
		}
	}
	// $ANTLR end "K_FROZEN"

	// $ANTLR start "A"
	public final void mA() throws RecognitionException {
		try {
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1347:11: ( ( 'a' | 'A' ) )
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:
			{
			if ( input.LA(1)=='A'||input.LA(1)=='a' ) {
				input.consume();
			}
			else {
				MismatchedSetException mse = new MismatchedSetException(null,input);
				recover(mse);
				throw mse;
			}
			}

		}
		finally {
			// do for sure before leaving
		}
	}
	// $ANTLR end "A"

	// $ANTLR start "B"
	public final void mB() throws RecognitionException {
		try {
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1348:11: ( ( 'b' | 'B' ) )
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:
			{
			if ( input.LA(1)=='B'||input.LA(1)=='b' ) {
				input.consume();
			}
			else {
				MismatchedSetException mse = new MismatchedSetException(null,input);
				recover(mse);
				throw mse;
			}
			}

		}
		finally {
			// do for sure before leaving
		}
	}
	// $ANTLR end "B"

	// $ANTLR start "C"
	public final void mC() throws RecognitionException {
		try {
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1349:11: ( ( 'c' | 'C' ) )
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:
			{
			if ( input.LA(1)=='C'||input.LA(1)=='c' ) {
				input.consume();
			}
			else {
				MismatchedSetException mse = new MismatchedSetException(null,input);
				recover(mse);
				throw mse;
			}
			}

		}
		finally {
			// do for sure before leaving
		}
	}
	// $ANTLR end "C"

	// $ANTLR start "D"
	public final void mD() throws RecognitionException {
		try {
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1350:11: ( ( 'd' | 'D' ) )
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:
			{
			if ( input.LA(1)=='D'||input.LA(1)=='d' ) {
				input.consume();
			}
			else {
				MismatchedSetException mse = new MismatchedSetException(null,input);
				recover(mse);
				throw mse;
			}
			}

		}
		finally {
			// do for sure before leaving
		}
	}
	// $ANTLR end "D"

	// $ANTLR start "E"
	public final void mE() throws RecognitionException {
		try {
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1351:11: ( ( 'e' | 'E' ) )
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:
			{
			if ( input.LA(1)=='E'||input.LA(1)=='e' ) {
				input.consume();
			}
			else {
				MismatchedSetException mse = new MismatchedSetException(null,input);
				recover(mse);
				throw mse;
			}
			}

		}
		finally {
			// do for sure before leaving
		}
	}
	// $ANTLR end "E"

	// $ANTLR start "F"
	public final void mF() throws RecognitionException {
		try {
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1352:11: ( ( 'f' | 'F' ) )
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:
			{
			if ( input.LA(1)=='F'||input.LA(1)=='f' ) {
				input.consume();
			}
			else {
				MismatchedSetException mse = new MismatchedSetException(null,input);
				recover(mse);
				throw mse;
			}
			}

		}
		finally {
			// do for sure before leaving
		}
	}
	// $ANTLR end "F"

	// $ANTLR start "G"
	public final void mG() throws RecognitionException {
		try {
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1353:11: ( ( 'g' | 'G' ) )
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:
			{
			if ( input.LA(1)=='G'||input.LA(1)=='g' ) {
				input.consume();
			}
			else {
				MismatchedSetException mse = new MismatchedSetException(null,input);
				recover(mse);
				throw mse;
			}
			}

		}
		finally {
			// do for sure before leaving
		}
	}
	// $ANTLR end "G"

	// $ANTLR start "H"
	public final void mH() throws RecognitionException {
		try {
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1354:11: ( ( 'h' | 'H' ) )
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:
			{
			if ( input.LA(1)=='H'||input.LA(1)=='h' ) {
				input.consume();
			}
			else {
				MismatchedSetException mse = new MismatchedSetException(null,input);
				recover(mse);
				throw mse;
			}
			}

		}
		finally {
			// do for sure before leaving
		}
	}
	// $ANTLR end "H"

	// $ANTLR start "I"
	public final void mI() throws RecognitionException {
		try {
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1355:11: ( ( 'i' | 'I' ) )
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:
			{
			if ( input.LA(1)=='I'||input.LA(1)=='i' ) {
				input.consume();
			}
			else {
				MismatchedSetException mse = new MismatchedSetException(null,input);
				recover(mse);
				throw mse;
			}
			}

		}
		finally {
			// do for sure before leaving
		}
	}
	// $ANTLR end "I"

	// $ANTLR start "J"
	public final void mJ() throws RecognitionException {
		try {
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1356:11: ( ( 'j' | 'J' ) )
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:
			{
			if ( input.LA(1)=='J'||input.LA(1)=='j' ) {
				input.consume();
			}
			else {
				MismatchedSetException mse = new MismatchedSetException(null,input);
				recover(mse);
				throw mse;
			}
			}

		}
		finally {
			// do for sure before leaving
		}
	}
	// $ANTLR end "J"

	// $ANTLR start "K"
	public final void mK() throws RecognitionException {
		try {
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1357:11: ( ( 'k' | 'K' ) )
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:
			{
			if ( input.LA(1)=='K'||input.LA(1)=='k' ) {
				input.consume();
			}
			else {
				MismatchedSetException mse = new MismatchedSetException(null,input);
				recover(mse);
				throw mse;
			}
			}

		}
		finally {
			// do for sure before leaving
		}
	}
	// $ANTLR end "K"

	// $ANTLR start "L"
	public final void mL() throws RecognitionException {
		try {
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1358:11: ( ( 'l' | 'L' ) )
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:
			{
			if ( input.LA(1)=='L'||input.LA(1)=='l' ) {
				input.consume();
			}
			else {
				MismatchedSetException mse = new MismatchedSetException(null,input);
				recover(mse);
				throw mse;
			}
			}

		}
		finally {
			// do for sure before leaving
		}
	}
	// $ANTLR end "L"

	// $ANTLR start "M"
	public final void mM() throws RecognitionException {
		try {
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1359:11: ( ( 'm' | 'M' ) )
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:
			{
			if ( input.LA(1)=='M'||input.LA(1)=='m' ) {
				input.consume();
			}
			else {
				MismatchedSetException mse = new MismatchedSetException(null,input);
				recover(mse);
				throw mse;
			}
			}

		}
		finally {
			// do for sure before leaving
		}
	}
	// $ANTLR end "M"

	// $ANTLR start "N"
	public final void mN() throws RecognitionException {
		try {
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1360:11: ( ( 'n' | 'N' ) )
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:
			{
			if ( input.LA(1)=='N'||input.LA(1)=='n' ) {
				input.consume();
			}
			else {
				MismatchedSetException mse = new MismatchedSetException(null,input);
				recover(mse);
				throw mse;
			}
			}

		}
		finally {
			// do for sure before leaving
		}
	}
	// $ANTLR end "N"

	// $ANTLR start "O"
	public final void mO() throws RecognitionException {
		try {
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1361:11: ( ( 'o' | 'O' ) )
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:
			{
			if ( input.LA(1)=='O'||input.LA(1)=='o' ) {
				input.consume();
			}
			else {
				MismatchedSetException mse = new MismatchedSetException(null,input);
				recover(mse);
				throw mse;
			}
			}

		}
		finally {
			// do for sure before leaving
		}
	}
	// $ANTLR end "O"

	// $ANTLR start "P"
	public final void mP() throws RecognitionException {
		try {
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1362:11: ( ( 'p' | 'P' ) )
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:
			{
			if ( input.LA(1)=='P'||input.LA(1)=='p' ) {
				input.consume();
			}
			else {
				MismatchedSetException mse = new MismatchedSetException(null,input);
				recover(mse);
				throw mse;
			}
			}

		}
		finally {
			// do for sure before leaving
		}
	}
	// $ANTLR end "P"

	// $ANTLR start "Q"
	public final void mQ() throws RecognitionException {
		try {
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1363:11: ( ( 'q' | 'Q' ) )
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:
			{
			if ( input.LA(1)=='Q'||input.LA(1)=='q' ) {
				input.consume();
			}
			else {
				MismatchedSetException mse = new MismatchedSetException(null,input);
				recover(mse);
				throw mse;
			}
			}

		}
		finally {
			// do for sure before leaving
		}
	}
	// $ANTLR end "Q"

	// $ANTLR start "R"
	public final void mR() throws RecognitionException {
		try {
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1364:11: ( ( 'r' | 'R' ) )
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:
			{
			if ( input.LA(1)=='R'||input.LA(1)=='r' ) {
				input.consume();
			}
			else {
				MismatchedSetException mse = new MismatchedSetException(null,input);
				recover(mse);
				throw mse;
			}
			}

		}
		finally {
			// do for sure before leaving
		}
	}
	// $ANTLR end "R"

	// $ANTLR start "S"
	public final void mS() throws RecognitionException {
		try {
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1365:11: ( ( 's' | 'S' ) )
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:
			{
			if ( input.LA(1)=='S'||input.LA(1)=='s' ) {
				input.consume();
			}
			else {
				MismatchedSetException mse = new MismatchedSetException(null,input);
				recover(mse);
				throw mse;
			}
			}

		}
		finally {
			// do for sure before leaving
		}
	}
	// $ANTLR end "S"

	// $ANTLR start "T"
	public final void mT() throws RecognitionException {
		try {
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1366:11: ( ( 't' | 'T' ) )
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:
			{
			if ( input.LA(1)=='T'||input.LA(1)=='t' ) {
				input.consume();
			}
			else {
				MismatchedSetException mse = new MismatchedSetException(null,input);
				recover(mse);
				throw mse;
			}
			}

		}
		finally {
			// do for sure before leaving
		}
	}
	// $ANTLR end "T"

	// $ANTLR start "U"
	public final void mU() throws RecognitionException {
		try {
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1367:11: ( ( 'u' | 'U' ) )
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:
			{
			if ( input.LA(1)=='U'||input.LA(1)=='u' ) {
				input.consume();
			}
			else {
				MismatchedSetException mse = new MismatchedSetException(null,input);
				recover(mse);
				throw mse;
			}
			}

		}
		finally {
			// do for sure before leaving
		}
	}
	// $ANTLR end "U"

	// $ANTLR start "V"
	public final void mV() throws RecognitionException {
		try {
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1368:11: ( ( 'v' | 'V' ) )
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:
			{
			if ( input.LA(1)=='V'||input.LA(1)=='v' ) {
				input.consume();
			}
			else {
				MismatchedSetException mse = new MismatchedSetException(null,input);
				recover(mse);
				throw mse;
			}
			}

		}
		finally {
			// do for sure before leaving
		}
	}
	// $ANTLR end "V"

	// $ANTLR start "W"
	public final void mW() throws RecognitionException {
		try {
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1369:11: ( ( 'w' | 'W' ) )
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:
			{
			if ( input.LA(1)=='W'||input.LA(1)=='w' ) {
				input.consume();
			}
			else {
				MismatchedSetException mse = new MismatchedSetException(null,input);
				recover(mse);
				throw mse;
			}
			}

		}
		finally {
			// do for sure before leaving
		}
	}
	// $ANTLR end "W"

	// $ANTLR start "X"
	public final void mX() throws RecognitionException {
		try {
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1370:11: ( ( 'x' | 'X' ) )
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:
			{
			if ( input.LA(1)=='X'||input.LA(1)=='x' ) {
				input.consume();
			}
			else {
				MismatchedSetException mse = new MismatchedSetException(null,input);
				recover(mse);
				throw mse;
			}
			}

		}
		finally {
			// do for sure before leaving
		}
	}
	// $ANTLR end "X"

	// $ANTLR start "Y"
	public final void mY() throws RecognitionException {
		try {
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1371:11: ( ( 'y' | 'Y' ) )
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:
			{
			if ( input.LA(1)=='Y'||input.LA(1)=='y' ) {
				input.consume();
			}
			else {
				MismatchedSetException mse = new MismatchedSetException(null,input);
				recover(mse);
				throw mse;
			}
			}

		}
		finally {
			// do for sure before leaving
		}
	}
	// $ANTLR end "Y"

	// $ANTLR start "Z"
	public final void mZ() throws RecognitionException {
		try {
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1372:11: ( ( 'z' | 'Z' ) )
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:
			{
			if ( input.LA(1)=='Z'||input.LA(1)=='z' ) {
				input.consume();
			}
			else {
				MismatchedSetException mse = new MismatchedSetException(null,input);
				recover(mse);
				throw mse;
			}
			}

		}
		finally {
			// do for sure before leaving
		}
	}
	// $ANTLR end "Z"

	// $ANTLR start "STRING_LITERAL"
	public final void mSTRING_LITERAL() throws RecognitionException {
		try {
			int _type = STRING_LITERAL;
			int _channel = DEFAULT_TOKEN_CHANNEL;
			int c;

			 StringBuilder b = new StringBuilder(); 
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1377:5: ( '\\'' (c=~ ( '\\'' ) | '\\'' '\\'' )* '\\'' )
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1377:7: '\\'' (c=~ ( '\\'' ) | '\\'' '\\'' )* '\\''
			{
			match('\''); 
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1377:12: (c=~ ( '\\'' ) | '\\'' '\\'' )*
			loop3:
			while (true) {
				int alt3=3;
				int LA3_0 = input.LA(1);
				if ( (LA3_0=='\'') ) {
					int LA3_1 = input.LA(2);
					if ( (LA3_1=='\'') ) {
						alt3=2;
					}

				}
				else if ( ((LA3_0 >= '\u0000' && LA3_0 <= '&')||(LA3_0 >= '(' && LA3_0 <= '\uFFFF')) ) {
					alt3=1;
				}

				switch (alt3) {
				case 1 :
					// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1377:13: c=~ ( '\\'' )
					{
					c= input.LA(1);
					if ( (input.LA(1) >= '\u0000' && input.LA(1) <= '&')||(input.LA(1) >= '(' && input.LA(1) <= '\uFFFF') ) {
						input.consume();
					}
					else {
						MismatchedSetException mse = new MismatchedSetException(null,input);
						recover(mse);
						throw mse;
					}
					 b.appendCodePoint(c);
					}
					break;
				case 2 :
					// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1377:50: '\\'' '\\''
					{
					match('\''); 
					match('\''); 
					 b.appendCodePoint('\''); 
					}
					break;

				default :
					break loop3;
				}
			}

			match('\''); 
			}

			state.type = _type;
			state.channel = _channel;
			 setText(b.toString()); 
		}
		finally {
			// do for sure before leaving
		}
	}
	// $ANTLR end "STRING_LITERAL"

	// $ANTLR start "QUOTED_NAME"
	public final void mQUOTED_NAME() throws RecognitionException {
		try {
			int _type = QUOTED_NAME;
			int _channel = DEFAULT_TOKEN_CHANNEL;
			int c;

			 StringBuilder b = new StringBuilder(); 
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1383:5: ( '\\\"' (c=~ ( '\\\"' ) | '\\\"' '\\\"' )+ '\\\"' )
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1383:7: '\\\"' (c=~ ( '\\\"' ) | '\\\"' '\\\"' )+ '\\\"'
			{
			match('\"'); 
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1383:12: (c=~ ( '\\\"' ) | '\\\"' '\\\"' )+
			int cnt4=0;
			loop4:
			while (true) {
				int alt4=3;
				int LA4_0 = input.LA(1);
				if ( (LA4_0=='\"') ) {
					int LA4_1 = input.LA(2);
					if ( (LA4_1=='\"') ) {
						alt4=2;
					}

				}
				else if ( ((LA4_0 >= '\u0000' && LA4_0 <= '!')||(LA4_0 >= '#' && LA4_0 <= '\uFFFF')) ) {
					alt4=1;
				}

				switch (alt4) {
				case 1 :
					// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1383:13: c=~ ( '\\\"' )
					{
					c= input.LA(1);
					if ( (input.LA(1) >= '\u0000' && input.LA(1) <= '!')||(input.LA(1) >= '#' && input.LA(1) <= '\uFFFF') ) {
						input.consume();
					}
					else {
						MismatchedSetException mse = new MismatchedSetException(null,input);
						recover(mse);
						throw mse;
					}
					 b.appendCodePoint(c); 
					}
					break;
				case 2 :
					// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1383:51: '\\\"' '\\\"'
					{
					match('\"'); 
					match('\"'); 
					 b.appendCodePoint('\"'); 
					}
					break;

				default :
					if ( cnt4 >= 1 ) break loop4;
					EarlyExitException eee = new EarlyExitException(4, input);
					throw eee;
				}
				cnt4++;
			}

			match('\"'); 
			}

			state.type = _type;
			state.channel = _channel;
			 setText(b.toString()); 
		}
		finally {
			// do for sure before leaving
		}
	}
	// $ANTLR end "QUOTED_NAME"

	// $ANTLR start "DIGIT"
	public final void mDIGIT() throws RecognitionException {
		try {
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1387:5: ( '0' .. '9' )
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:
			{
			if ( (input.LA(1) >= '0' && input.LA(1) <= '9') ) {
				input.consume();
			}
			else {
				MismatchedSetException mse = new MismatchedSetException(null,input);
				recover(mse);
				throw mse;
			}
			}

		}
		finally {
			// do for sure before leaving
		}
	}
	// $ANTLR end "DIGIT"

	// $ANTLR start "LETTER"
	public final void mLETTER() throws RecognitionException {
		try {
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1391:5: ( ( 'A' .. 'Z' | 'a' .. 'z' ) )
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:
			{
			if ( (input.LA(1) >= 'A' && input.LA(1) <= 'Z')||(input.LA(1) >= 'a' && input.LA(1) <= 'z') ) {
				input.consume();
			}
			else {
				MismatchedSetException mse = new MismatchedSetException(null,input);
				recover(mse);
				throw mse;
			}
			}

		}
		finally {
			// do for sure before leaving
		}
	}
	// $ANTLR end "LETTER"

	// $ANTLR start "HEX"
	public final void mHEX() throws RecognitionException {
		try {
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1395:5: ( ( 'A' .. 'F' | 'a' .. 'f' | '0' .. '9' ) )
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:
			{
			if ( (input.LA(1) >= '0' && input.LA(1) <= '9')||(input.LA(1) >= 'A' && input.LA(1) <= 'F')||(input.LA(1) >= 'a' && input.LA(1) <= 'f') ) {
				input.consume();
			}
			else {
				MismatchedSetException mse = new MismatchedSetException(null,input);
				recover(mse);
				throw mse;
			}
			}

		}
		finally {
			// do for sure before leaving
		}
	}
	// $ANTLR end "HEX"

	// $ANTLR start "EXPONENT"
	public final void mEXPONENT() throws RecognitionException {
		try {
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1399:5: ( E ( '+' | '-' )? ( DIGIT )+ )
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1399:7: E ( '+' | '-' )? ( DIGIT )+
			{
			mE(); 

			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1399:9: ( '+' | '-' )?
			int alt5=2;
			int LA5_0 = input.LA(1);
			if ( (LA5_0=='+'||LA5_0=='-') ) {
				alt5=1;
			}
			switch (alt5) {
				case 1 :
					// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:
					{
					if ( input.LA(1)=='+'||input.LA(1)=='-' ) {
						input.consume();
					}
					else {
						MismatchedSetException mse = new MismatchedSetException(null,input);
						recover(mse);
						throw mse;
					}
					}
					break;

			}

			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1399:22: ( DIGIT )+
			int cnt6=0;
			loop6:
			while (true) {
				int alt6=2;
				int LA6_0 = input.LA(1);
				if ( ((LA6_0 >= '0' && LA6_0 <= '9')) ) {
					alt6=1;
				}

				switch (alt6) {
				case 1 :
					// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:
					{
					if ( (input.LA(1) >= '0' && input.LA(1) <= '9') ) {
						input.consume();
					}
					else {
						MismatchedSetException mse = new MismatchedSetException(null,input);
						recover(mse);
						throw mse;
					}
					}
					break;

				default :
					if ( cnt6 >= 1 ) break loop6;
					EarlyExitException eee = new EarlyExitException(6, input);
					throw eee;
				}
				cnt6++;
			}

			}

		}
		finally {
			// do for sure before leaving
		}
	}
	// $ANTLR end "EXPONENT"

	// $ANTLR start "INTEGER"
	public final void mINTEGER() throws RecognitionException {
		try {
			int _type = INTEGER;
			int _channel = DEFAULT_TOKEN_CHANNEL;
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1403:5: ( ( '-' )? ( DIGIT )+ )
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1403:7: ( '-' )? ( DIGIT )+
			{
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1403:7: ( '-' )?
			int alt7=2;
			int LA7_0 = input.LA(1);
			if ( (LA7_0=='-') ) {
				alt7=1;
			}
			switch (alt7) {
				case 1 :
					// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1403:7: '-'
					{
					match('-'); 
					}
					break;

			}

			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1403:12: ( DIGIT )+
			int cnt8=0;
			loop8:
			while (true) {
				int alt8=2;
				int LA8_0 = input.LA(1);
				if ( ((LA8_0 >= '0' && LA8_0 <= '9')) ) {
					alt8=1;
				}

				switch (alt8) {
				case 1 :
					// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:
					{
					if ( (input.LA(1) >= '0' && input.LA(1) <= '9') ) {
						input.consume();
					}
					else {
						MismatchedSetException mse = new MismatchedSetException(null,input);
						recover(mse);
						throw mse;
					}
					}
					break;

				default :
					if ( cnt8 >= 1 ) break loop8;
					EarlyExitException eee = new EarlyExitException(8, input);
					throw eee;
				}
				cnt8++;
			}

			}

			state.type = _type;
			state.channel = _channel;
		}
		finally {
			// do for sure before leaving
		}
	}
	// $ANTLR end "INTEGER"

	// $ANTLR start "QMARK"
	public final void mQMARK() throws RecognitionException {
		try {
			int _type = QMARK;
			int _channel = DEFAULT_TOKEN_CHANNEL;
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1407:5: ( '?' )
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1407:7: '?'
			{
			match('?'); 
			}

			state.type = _type;
			state.channel = _channel;
		}
		finally {
			// do for sure before leaving
		}
	}
	// $ANTLR end "QMARK"

	// $ANTLR start "FLOAT"
	public final void mFLOAT() throws RecognitionException {
		try {
			int _type = FLOAT;
			int _channel = DEFAULT_TOKEN_CHANNEL;
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1415:5: ( INTEGER EXPONENT | INTEGER '.' ( DIGIT )* ( EXPONENT )? )
			int alt11=2;
			alt11 = dfa11.predict(input);
			switch (alt11) {
				case 1 :
					// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1415:7: INTEGER EXPONENT
					{
					mINTEGER(); 

					mEXPONENT(); 

					}
					break;
				case 2 :
					// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1416:7: INTEGER '.' ( DIGIT )* ( EXPONENT )?
					{
					mINTEGER(); 

					match('.'); 
					// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1416:19: ( DIGIT )*
					loop9:
					while (true) {
						int alt9=2;
						int LA9_0 = input.LA(1);
						if ( ((LA9_0 >= '0' && LA9_0 <= '9')) ) {
							alt9=1;
						}

						switch (alt9) {
						case 1 :
							// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:
							{
							if ( (input.LA(1) >= '0' && input.LA(1) <= '9') ) {
								input.consume();
							}
							else {
								MismatchedSetException mse = new MismatchedSetException(null,input);
								recover(mse);
								throw mse;
							}
							}
							break;

						default :
							break loop9;
						}
					}

					// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1416:26: ( EXPONENT )?
					int alt10=2;
					int LA10_0 = input.LA(1);
					if ( (LA10_0=='E'||LA10_0=='e') ) {
						alt10=1;
					}
					switch (alt10) {
						case 1 :
							// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1416:26: EXPONENT
							{
							mEXPONENT(); 

							}
							break;

					}

					}
					break;

			}
			state.type = _type;
			state.channel = _channel;
		}
		finally {
			// do for sure before leaving
		}
	}
	// $ANTLR end "FLOAT"

	// $ANTLR start "BOOLEAN"
	public final void mBOOLEAN() throws RecognitionException {
		try {
			int _type = BOOLEAN;
			int _channel = DEFAULT_TOKEN_CHANNEL;
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1423:5: ( T R U E | F A L S E )
			int alt12=2;
			int LA12_0 = input.LA(1);
			if ( (LA12_0=='T'||LA12_0=='t') ) {
				alt12=1;
			}
			else if ( (LA12_0=='F'||LA12_0=='f') ) {
				alt12=2;
			}

			else {
				NoViableAltException nvae =
					new NoViableAltException("", 12, 0, input);
				throw nvae;
			}

			switch (alt12) {
				case 1 :
					// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1423:7: T R U E
					{
					mT(); 

					mR(); 

					mU(); 

					mE(); 

					}
					break;
				case 2 :
					// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1423:17: F A L S E
					{
					mF(); 

					mA(); 

					mL(); 

					mS(); 

					mE(); 

					}
					break;

			}
			state.type = _type;
			state.channel = _channel;
		}
		finally {
			// do for sure before leaving
		}
	}
	// $ANTLR end "BOOLEAN"

	// $ANTLR start "IDENT"
	public final void mIDENT() throws RecognitionException {
		try {
			int _type = IDENT;
			int _channel = DEFAULT_TOKEN_CHANNEL;
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1427:5: ( LETTER ( LETTER | DIGIT | '_' )* )
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1427:7: LETTER ( LETTER | DIGIT | '_' )*
			{
			mLETTER(); 

			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1427:14: ( LETTER | DIGIT | '_' )*
			loop13:
			while (true) {
				int alt13=2;
				int LA13_0 = input.LA(1);
				if ( ((LA13_0 >= '0' && LA13_0 <= '9')||(LA13_0 >= 'A' && LA13_0 <= 'Z')||LA13_0=='_'||(LA13_0 >= 'a' && LA13_0 <= 'z')) ) {
					alt13=1;
				}

				switch (alt13) {
				case 1 :
					// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:
					{
					if ( (input.LA(1) >= '0' && input.LA(1) <= '9')||(input.LA(1) >= 'A' && input.LA(1) <= 'Z')||input.LA(1)=='_'||(input.LA(1) >= 'a' && input.LA(1) <= 'z') ) {
						input.consume();
					}
					else {
						MismatchedSetException mse = new MismatchedSetException(null,input);
						recover(mse);
						throw mse;
					}
					}
					break;

				default :
					break loop13;
				}
			}

			}

			state.type = _type;
			state.channel = _channel;
		}
		finally {
			// do for sure before leaving
		}
	}
	// $ANTLR end "IDENT"

	// $ANTLR start "HEXNUMBER"
	public final void mHEXNUMBER() throws RecognitionException {
		try {
			int _type = HEXNUMBER;
			int _channel = DEFAULT_TOKEN_CHANNEL;
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1431:5: ( '0' X ( HEX )* )
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1431:7: '0' X ( HEX )*
			{
			match('0'); 
			mX(); 

			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1431:13: ( HEX )*
			loop14:
			while (true) {
				int alt14=2;
				int LA14_0 = input.LA(1);
				if ( ((LA14_0 >= '0' && LA14_0 <= '9')||(LA14_0 >= 'A' && LA14_0 <= 'F')||(LA14_0 >= 'a' && LA14_0 <= 'f')) ) {
					alt14=1;
				}

				switch (alt14) {
				case 1 :
					// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:
					{
					if ( (input.LA(1) >= '0' && input.LA(1) <= '9')||(input.LA(1) >= 'A' && input.LA(1) <= 'F')||(input.LA(1) >= 'a' && input.LA(1) <= 'f') ) {
						input.consume();
					}
					else {
						MismatchedSetException mse = new MismatchedSetException(null,input);
						recover(mse);
						throw mse;
					}
					}
					break;

				default :
					break loop14;
				}
			}

			}

			state.type = _type;
			state.channel = _channel;
		}
		finally {
			// do for sure before leaving
		}
	}
	// $ANTLR end "HEXNUMBER"

	// $ANTLR start "UUID"
	public final void mUUID() throws RecognitionException {
		try {
			int _type = UUID;
			int _channel = DEFAULT_TOKEN_CHANNEL;
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1435:5: ( HEX HEX HEX HEX HEX HEX HEX HEX '-' HEX HEX HEX HEX '-' HEX HEX HEX HEX '-' HEX HEX HEX HEX '-' HEX HEX HEX HEX HEX HEX HEX HEX HEX HEX HEX HEX )
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1435:7: HEX HEX HEX HEX HEX HEX HEX HEX '-' HEX HEX HEX HEX '-' HEX HEX HEX HEX '-' HEX HEX HEX HEX '-' HEX HEX HEX HEX HEX HEX HEX HEX HEX HEX HEX HEX
			{
			mHEX(); 

			mHEX(); 

			mHEX(); 

			mHEX(); 

			mHEX(); 

			mHEX(); 

			mHEX(); 

			mHEX(); 

			match('-'); 
			mHEX(); 

			mHEX(); 

			mHEX(); 

			mHEX(); 

			match('-'); 
			mHEX(); 

			mHEX(); 

			mHEX(); 

			mHEX(); 

			match('-'); 
			mHEX(); 

			mHEX(); 

			mHEX(); 

			mHEX(); 

			match('-'); 
			mHEX(); 

			mHEX(); 

			mHEX(); 

			mHEX(); 

			mHEX(); 

			mHEX(); 

			mHEX(); 

			mHEX(); 

			mHEX(); 

			mHEX(); 

			mHEX(); 

			mHEX(); 

			}

			state.type = _type;
			state.channel = _channel;
		}
		finally {
			// do for sure before leaving
		}
	}
	// $ANTLR end "UUID"

	// $ANTLR start "WS"
	public final void mWS() throws RecognitionException {
		try {
			int _type = WS;
			int _channel = DEFAULT_TOKEN_CHANNEL;
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1443:5: ( ( ' ' | '\\t' | '\\n' | '\\r' )+ )
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1443:7: ( ' ' | '\\t' | '\\n' | '\\r' )+
			{
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1443:7: ( ' ' | '\\t' | '\\n' | '\\r' )+
			int cnt15=0;
			loop15:
			while (true) {
				int alt15=2;
				int LA15_0 = input.LA(1);
				if ( ((LA15_0 >= '\t' && LA15_0 <= '\n')||LA15_0=='\r'||LA15_0==' ') ) {
					alt15=1;
				}

				switch (alt15) {
				case 1 :
					// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:
					{
					if ( (input.LA(1) >= '\t' && input.LA(1) <= '\n')||input.LA(1)=='\r'||input.LA(1)==' ' ) {
						input.consume();
					}
					else {
						MismatchedSetException mse = new MismatchedSetException(null,input);
						recover(mse);
						throw mse;
					}
					}
					break;

				default :
					if ( cnt15 >= 1 ) break loop15;
					EarlyExitException eee = new EarlyExitException(15, input);
					throw eee;
				}
				cnt15++;
			}

			 _channel = HIDDEN; 
			}

			state.type = _type;
			state.channel = _channel;
		}
		finally {
			// do for sure before leaving
		}
	}
	// $ANTLR end "WS"

	// $ANTLR start "COMMENT"
	public final void mCOMMENT() throws RecognitionException {
		try {
			int _type = COMMENT;
			int _channel = DEFAULT_TOKEN_CHANNEL;
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1447:5: ( ( '--' | '//' ) ( . )* ( '\\n' | '\\r' ) )
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1447:7: ( '--' | '//' ) ( . )* ( '\\n' | '\\r' )
			{
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1447:7: ( '--' | '//' )
			int alt16=2;
			int LA16_0 = input.LA(1);
			if ( (LA16_0=='-') ) {
				alt16=1;
			}
			else if ( (LA16_0=='/') ) {
				alt16=2;
			}

			else {
				NoViableAltException nvae =
					new NoViableAltException("", 16, 0, input);
				throw nvae;
			}

			switch (alt16) {
				case 1 :
					// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1447:8: '--'
					{
					match("--"); 

					}
					break;
				case 2 :
					// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1447:15: '//'
					{
					match("//"); 

					}
					break;

			}

			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1447:21: ( . )*
			loop17:
			while (true) {
				int alt17=2;
				int LA17_0 = input.LA(1);
				if ( (LA17_0=='\n'||LA17_0=='\r') ) {
					alt17=2;
				}
				else if ( ((LA17_0 >= '\u0000' && LA17_0 <= '\t')||(LA17_0 >= '\u000B' && LA17_0 <= '\f')||(LA17_0 >= '\u000E' && LA17_0 <= '\uFFFF')) ) {
					alt17=1;
				}

				switch (alt17) {
				case 1 :
					// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1447:21: .
					{
					matchAny(); 
					}
					break;

				default :
					break loop17;
				}
			}

			if ( input.LA(1)=='\n'||input.LA(1)=='\r' ) {
				input.consume();
			}
			else {
				MismatchedSetException mse = new MismatchedSetException(null,input);
				recover(mse);
				throw mse;
			}
			 _channel = HIDDEN; 
			}

			state.type = _type;
			state.channel = _channel;
		}
		finally {
			// do for sure before leaving
		}
	}
	// $ANTLR end "COMMENT"

	// $ANTLR start "MULTILINE_COMMENT"
	public final void mMULTILINE_COMMENT() throws RecognitionException {
		try {
			int _type = MULTILINE_COMMENT;
			int _channel = DEFAULT_TOKEN_CHANNEL;
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1451:5: ( '/*' ( . )* '*/' )
			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1451:7: '/*' ( . )* '*/'
			{
			match("/*"); 

			// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1451:12: ( . )*
			loop18:
			while (true) {
				int alt18=2;
				int LA18_0 = input.LA(1);
				if ( (LA18_0=='*') ) {
					int LA18_1 = input.LA(2);
					if ( (LA18_1=='/') ) {
						alt18=2;
					}
					else if ( ((LA18_1 >= '\u0000' && LA18_1 <= '.')||(LA18_1 >= '0' && LA18_1 <= '\uFFFF')) ) {
						alt18=1;
					}

				}
				else if ( ((LA18_0 >= '\u0000' && LA18_0 <= ')')||(LA18_0 >= '+' && LA18_0 <= '\uFFFF')) ) {
					alt18=1;
				}

				switch (alt18) {
				case 1 :
					// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1451:12: .
					{
					matchAny(); 
					}
					break;

				default :
					break loop18;
				}
			}

			match("*/"); 

			 _channel = HIDDEN; 
			}

			state.type = _type;
			state.channel = _channel;
		}
		finally {
			// do for sure before leaving
		}
	}
	// $ANTLR end "MULTILINE_COMMENT"

	@Override
	public void mTokens() throws RecognitionException {
		// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1:8: ( T__141 | T__142 | T__143 | T__144 | T__145 | T__146 | T__147 | T__148 | T__149 | T__150 | T__151 | T__152 | T__153 | T__154 | T__155 | T__156 | T__157 | T__158 | T__159 | K_SELECT | K_FROM | K_AS | K_WHERE | K_AND | K_KEY | K_KEYS | K_FULL | K_INSERT | K_UPDATE | K_WITH | K_LIMIT | K_USING | K_USE | K_DISTINCT | K_COUNT | K_SET | K_BEGIN | K_UNLOGGED | K_BATCH | K_APPLY | K_TRUNCATE | K_DELETE | K_IN | K_CREATE | K_KEYSPACE | K_KEYSPACES | K_COLUMNFAMILY | K_INDEX | K_CUSTOM | K_ON | K_TO | K_DROP | K_PRIMARY | K_INTO | K_VALUES | K_TIMESTAMP | K_TTL | K_ALTER | K_RENAME | K_ADD | K_TYPE | K_COMPACT | K_STORAGE | K_ORDER | K_BY | K_ASC | K_DESC | K_ALLOW | K_FILTERING | K_IF | K_CONTAINS | K_GRANT | K_ALL | K_PERMISSION | K_PERMISSIONS | K_OF | K_REVOKE | K_MODIFY | K_AUTHORIZE | K_NORECURSIVE | K_USER | K_USERS | K_SUPERUSER | K_NOSUPERUSER | K_PASSWORD | K_CLUSTERING | K_ASCII | K_BIGINT | K_BLOB | K_BOOLEAN | K_COUNTER | K_DECIMAL | K_DOUBLE | K_FLOAT | K_INET | K_INT | K_TEXT | K_UUID | K_VARCHAR | K_VARINT | K_TIMEUUID | K_TOKEN | K_WRITETIME | K_NULL | K_NOT | K_EXISTS | K_MAP | K_LIST | K_NAN | K_INFINITY | K_TUPLE | K_TRIGGER | K_STATIC | K_FROZEN | STRING_LITERAL | QUOTED_NAME | INTEGER | QMARK | FLOAT | BOOLEAN | IDENT | HEXNUMBER | UUID | WS | COMMENT | MULTILINE_COMMENT )
		int alt19=126;
		alt19 = dfa19.predict(input);
		switch (alt19) {
			case 1 :
				// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1:10: T__141
				{
				mT__141(); 

				}
				break;
			case 2 :
				// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1:17: T__142
				{
				mT__142(); 

				}
				break;
			case 3 :
				// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1:24: T__143
				{
				mT__143(); 

				}
				break;
			case 4 :
				// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1:31: T__144
				{
				mT__144(); 

				}
				break;
			case 5 :
				// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1:38: T__145
				{
				mT__145(); 

				}
				break;
			case 6 :
				// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1:45: T__146
				{
				mT__146(); 

				}
				break;
			case 7 :
				// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1:52: T__147
				{
				mT__147(); 

				}
				break;
			case 8 :
				// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1:59: T__148
				{
				mT__148(); 

				}
				break;
			case 9 :
				// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1:66: T__149
				{
				mT__149(); 

				}
				break;
			case 10 :
				// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1:73: T__150
				{
				mT__150(); 

				}
				break;
			case 11 :
				// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1:80: T__151
				{
				mT__151(); 

				}
				break;
			case 12 :
				// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1:87: T__152
				{
				mT__152(); 

				}
				break;
			case 13 :
				// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1:94: T__153
				{
				mT__153(); 

				}
				break;
			case 14 :
				// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1:101: T__154
				{
				mT__154(); 

				}
				break;
			case 15 :
				// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1:108: T__155
				{
				mT__155(); 

				}
				break;
			case 16 :
				// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1:115: T__156
				{
				mT__156(); 

				}
				break;
			case 17 :
				// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1:122: T__157
				{
				mT__157(); 

				}
				break;
			case 18 :
				// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1:129: T__158
				{
				mT__158(); 

				}
				break;
			case 19 :
				// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1:136: T__159
				{
				mT__159(); 

				}
				break;
			case 20 :
				// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1:143: K_SELECT
				{
				mK_SELECT(); 

				}
				break;
			case 21 :
				// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1:152: K_FROM
				{
				mK_FROM(); 

				}
				break;
			case 22 :
				// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1:159: K_AS
				{
				mK_AS(); 

				}
				break;
			case 23 :
				// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1:164: K_WHERE
				{
				mK_WHERE(); 

				}
				break;
			case 24 :
				// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1:172: K_AND
				{
				mK_AND(); 

				}
				break;
			case 25 :
				// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1:178: K_KEY
				{
				mK_KEY(); 

				}
				break;
			case 26 :
				// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1:184: K_KEYS
				{
				mK_KEYS(); 

				}
				break;
			case 27 :
				// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1:191: K_FULL
				{
				mK_FULL(); 

				}
				break;
			case 28 :
				// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1:198: K_INSERT
				{
				mK_INSERT(); 

				}
				break;
			case 29 :
				// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1:207: K_UPDATE
				{
				mK_UPDATE(); 

				}
				break;
			case 30 :
				// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1:216: K_WITH
				{
				mK_WITH(); 

				}
				break;
			case 31 :
				// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1:223: K_LIMIT
				{
				mK_LIMIT(); 

				}
				break;
			case 32 :
				// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1:231: K_USING
				{
				mK_USING(); 

				}
				break;
			case 33 :
				// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1:239: K_USE
				{
				mK_USE(); 

				}
				break;
			case 34 :
				// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1:245: K_DISTINCT
				{
				mK_DISTINCT(); 

				}
				break;
			case 35 :
				// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1:256: K_COUNT
				{
				mK_COUNT(); 

				}
				break;
			case 36 :
				// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1:264: K_SET
				{
				mK_SET(); 

				}
				break;
			case 37 :
				// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1:270: K_BEGIN
				{
				mK_BEGIN(); 

				}
				break;
			case 38 :
				// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1:278: K_UNLOGGED
				{
				mK_UNLOGGED(); 

				}
				break;
			case 39 :
				// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1:289: K_BATCH
				{
				mK_BATCH(); 

				}
				break;
			case 40 :
				// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1:297: K_APPLY
				{
				mK_APPLY(); 

				}
				break;
			case 41 :
				// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1:305: K_TRUNCATE
				{
				mK_TRUNCATE(); 

				}
				break;
			case 42 :
				// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1:316: K_DELETE
				{
				mK_DELETE(); 

				}
				break;
			case 43 :
				// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1:325: K_IN
				{
				mK_IN(); 

				}
				break;
			case 44 :
				// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1:330: K_CREATE
				{
				mK_CREATE(); 

				}
				break;
			case 45 :
				// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1:339: K_KEYSPACE
				{
				mK_KEYSPACE(); 

				}
				break;
			case 46 :
				// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1:350: K_KEYSPACES
				{
				mK_KEYSPACES(); 

				}
				break;
			case 47 :
				// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1:362: K_COLUMNFAMILY
				{
				mK_COLUMNFAMILY(); 

				}
				break;
			case 48 :
				// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1:377: K_INDEX
				{
				mK_INDEX(); 

				}
				break;
			case 49 :
				// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1:385: K_CUSTOM
				{
				mK_CUSTOM(); 

				}
				break;
			case 50 :
				// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1:394: K_ON
				{
				mK_ON(); 

				}
				break;
			case 51 :
				// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1:399: K_TO
				{
				mK_TO(); 

				}
				break;
			case 52 :
				// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1:404: K_DROP
				{
				mK_DROP(); 

				}
				break;
			case 53 :
				// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1:411: K_PRIMARY
				{
				mK_PRIMARY(); 

				}
				break;
			case 54 :
				// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1:421: K_INTO
				{
				mK_INTO(); 

				}
				break;
			case 55 :
				// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1:428: K_VALUES
				{
				mK_VALUES(); 

				}
				break;
			case 56 :
				// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1:437: K_TIMESTAMP
				{
				mK_TIMESTAMP(); 

				}
				break;
			case 57 :
				// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1:449: K_TTL
				{
				mK_TTL(); 

				}
				break;
			case 58 :
				// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1:455: K_ALTER
				{
				mK_ALTER(); 

				}
				break;
			case 59 :
				// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1:463: K_RENAME
				{
				mK_RENAME(); 

				}
				break;
			case 60 :
				// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1:472: K_ADD
				{
				mK_ADD(); 

				}
				break;
			case 61 :
				// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1:478: K_TYPE
				{
				mK_TYPE(); 

				}
				break;
			case 62 :
				// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1:485: K_COMPACT
				{
				mK_COMPACT(); 

				}
				break;
			case 63 :
				// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1:495: K_STORAGE
				{
				mK_STORAGE(); 

				}
				break;
			case 64 :
				// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1:505: K_ORDER
				{
				mK_ORDER(); 

				}
				break;
			case 65 :
				// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1:513: K_BY
				{
				mK_BY(); 

				}
				break;
			case 66 :
				// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1:518: K_ASC
				{
				mK_ASC(); 

				}
				break;
			case 67 :
				// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1:524: K_DESC
				{
				mK_DESC(); 

				}
				break;
			case 68 :
				// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1:531: K_ALLOW
				{
				mK_ALLOW(); 

				}
				break;
			case 69 :
				// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1:539: K_FILTERING
				{
				mK_FILTERING(); 

				}
				break;
			case 70 :
				// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1:551: K_IF
				{
				mK_IF(); 

				}
				break;
			case 71 :
				// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1:556: K_CONTAINS
				{
				mK_CONTAINS(); 

				}
				break;
			case 72 :
				// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1:567: K_GRANT
				{
				mK_GRANT(); 

				}
				break;
			case 73 :
				// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1:575: K_ALL
				{
				mK_ALL(); 

				}
				break;
			case 74 :
				// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1:581: K_PERMISSION
				{
				mK_PERMISSION(); 

				}
				break;
			case 75 :
				// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1:594: K_PERMISSIONS
				{
				mK_PERMISSIONS(); 

				}
				break;
			case 76 :
				// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1:608: K_OF
				{
				mK_OF(); 

				}
				break;
			case 77 :
				// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1:613: K_REVOKE
				{
				mK_REVOKE(); 

				}
				break;
			case 78 :
				// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1:622: K_MODIFY
				{
				mK_MODIFY(); 

				}
				break;
			case 79 :
				// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1:631: K_AUTHORIZE
				{
				mK_AUTHORIZE(); 

				}
				break;
			case 80 :
				// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1:643: K_NORECURSIVE
				{
				mK_NORECURSIVE(); 

				}
				break;
			case 81 :
				// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1:657: K_USER
				{
				mK_USER(); 

				}
				break;
			case 82 :
				// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1:664: K_USERS
				{
				mK_USERS(); 

				}
				break;
			case 83 :
				// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1:672: K_SUPERUSER
				{
				mK_SUPERUSER(); 

				}
				break;
			case 84 :
				// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1:684: K_NOSUPERUSER
				{
				mK_NOSUPERUSER(); 

				}
				break;
			case 85 :
				// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1:698: K_PASSWORD
				{
				mK_PASSWORD(); 

				}
				break;
			case 86 :
				// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1:709: K_CLUSTERING
				{
				mK_CLUSTERING(); 

				}
				break;
			case 87 :
				// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1:722: K_ASCII
				{
				mK_ASCII(); 

				}
				break;
			case 88 :
				// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1:730: K_BIGINT
				{
				mK_BIGINT(); 

				}
				break;
			case 89 :
				// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1:739: K_BLOB
				{
				mK_BLOB(); 

				}
				break;
			case 90 :
				// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1:746: K_BOOLEAN
				{
				mK_BOOLEAN(); 

				}
				break;
			case 91 :
				// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1:756: K_COUNTER
				{
				mK_COUNTER(); 

				}
				break;
			case 92 :
				// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1:766: K_DECIMAL
				{
				mK_DECIMAL(); 

				}
				break;
			case 93 :
				// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1:776: K_DOUBLE
				{
				mK_DOUBLE(); 

				}
				break;
			case 94 :
				// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1:785: K_FLOAT
				{
				mK_FLOAT(); 

				}
				break;
			case 95 :
				// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1:793: K_INET
				{
				mK_INET(); 

				}
				break;
			case 96 :
				// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1:800: K_INT
				{
				mK_INT(); 

				}
				break;
			case 97 :
				// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1:806: K_TEXT
				{
				mK_TEXT(); 

				}
				break;
			case 98 :
				// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1:813: K_UUID
				{
				mK_UUID(); 

				}
				break;
			case 99 :
				// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1:820: K_VARCHAR
				{
				mK_VARCHAR(); 

				}
				break;
			case 100 :
				// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1:830: K_VARINT
				{
				mK_VARINT(); 

				}
				break;
			case 101 :
				// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1:839: K_TIMEUUID
				{
				mK_TIMEUUID(); 

				}
				break;
			case 102 :
				// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1:850: K_TOKEN
				{
				mK_TOKEN(); 

				}
				break;
			case 103 :
				// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1:858: K_WRITETIME
				{
				mK_WRITETIME(); 

				}
				break;
			case 104 :
				// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1:870: K_NULL
				{
				mK_NULL(); 

				}
				break;
			case 105 :
				// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1:877: K_NOT
				{
				mK_NOT(); 

				}
				break;
			case 106 :
				// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1:883: K_EXISTS
				{
				mK_EXISTS(); 

				}
				break;
			case 107 :
				// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1:892: K_MAP
				{
				mK_MAP(); 

				}
				break;
			case 108 :
				// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1:898: K_LIST
				{
				mK_LIST(); 

				}
				break;
			case 109 :
				// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1:905: K_NAN
				{
				mK_NAN(); 

				}
				break;
			case 110 :
				// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1:911: K_INFINITY
				{
				mK_INFINITY(); 

				}
				break;
			case 111 :
				// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1:922: K_TUPLE
				{
				mK_TUPLE(); 

				}
				break;
			case 112 :
				// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1:930: K_TRIGGER
				{
				mK_TRIGGER(); 

				}
				break;
			case 113 :
				// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1:940: K_STATIC
				{
				mK_STATIC(); 

				}
				break;
			case 114 :
				// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1:949: K_FROZEN
				{
				mK_FROZEN(); 

				}
				break;
			case 115 :
				// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1:958: STRING_LITERAL
				{
				mSTRING_LITERAL(); 

				}
				break;
			case 116 :
				// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1:973: QUOTED_NAME
				{
				mQUOTED_NAME(); 

				}
				break;
			case 117 :
				// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1:985: INTEGER
				{
				mINTEGER(); 

				}
				break;
			case 118 :
				// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1:993: QMARK
				{
				mQMARK(); 

				}
				break;
			case 119 :
				// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1:999: FLOAT
				{
				mFLOAT(); 

				}
				break;
			case 120 :
				// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1:1005: BOOLEAN
				{
				mBOOLEAN(); 

				}
				break;
			case 121 :
				// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1:1013: IDENT
				{
				mIDENT(); 

				}
				break;
			case 122 :
				// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1:1019: HEXNUMBER
				{
				mHEXNUMBER(); 

				}
				break;
			case 123 :
				// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1:1029: UUID
				{
				mUUID(); 

				}
				break;
			case 124 :
				// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1:1034: WS
				{
				mWS(); 

				}
				break;
			case 125 :
				// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1:1037: COMMENT
				{
				mCOMMENT(); 

				}
				break;
			case 126 :
				// /home/abdelrahman/apache-cassandra-2.1.5-src/src/java/org/apache/cassandra/cql3/Cql.g:1:1045: MULTILINE_COMMENT
				{
				mMULTILINE_COMMENT(); 

				}
				break;

		}
	}


	protected DFA11 dfa11 = new DFA11(this);
	protected DFA19 dfa19 = new DFA19(this);
	static final String DFA11_eotS =
		"\5\uffff";
	static final String DFA11_eofS =
		"\5\uffff";
	static final String DFA11_minS =
		"\1\55\1\60\1\56\2\uffff";
	static final String DFA11_maxS =
		"\2\71\1\145\2\uffff";
	static final String DFA11_acceptS =
		"\3\uffff\1\1\1\2";
	static final String DFA11_specialS =
		"\5\uffff}>";
	static final String[] DFA11_transitionS = {
			"\1\1\2\uffff\12\2",
			"\12\2",
			"\1\4\1\uffff\12\2\13\uffff\1\3\37\uffff\1\3",
			"",
			""
	};

	static final short[] DFA11_eot = DFA.unpackEncodedString(DFA11_eotS);
	static final short[] DFA11_eof = DFA.unpackEncodedString(DFA11_eofS);
	static final char[] DFA11_min = DFA.unpackEncodedStringToUnsignedChars(DFA11_minS);
	static final char[] DFA11_max = DFA.unpackEncodedStringToUnsignedChars(DFA11_maxS);
	static final short[] DFA11_accept = DFA.unpackEncodedString(DFA11_acceptS);
	static final short[] DFA11_special = DFA.unpackEncodedString(DFA11_specialS);
	static final short[][] DFA11_transition;

	static {
		int numStates = DFA11_transitionS.length;
		DFA11_transition = new short[numStates][];
		for (int i=0; i<numStates; i++) {
			DFA11_transition[i] = DFA.unpackEncodedString(DFA11_transitionS[i]);
		}
	}

	protected class DFA11 extends DFA {

		public DFA11(BaseRecognizer recognizer) {
			this.recognizer = recognizer;
			this.decisionNumber = 11;
			this.eot = DFA11_eot;
			this.eof = DFA11_eof;
			this.min = DFA11_min;
			this.max = DFA11_max;
			this.accept = DFA11_accept;
			this.special = DFA11_special;
			this.transition = DFA11_transition;
		}
		@Override
		public String getDescription() {
			return "1414:1: FLOAT : ( INTEGER EXPONENT | INTEGER '.' ( DIGIT )* ( EXPONENT )? );";
		}
	}

	static final String DFA19_eotS =
		"\6\uffff\1\57\3\uffff\1\62\1\uffff\1\64\5\uffff\24\52\2\uffff\1\165\2"+
		"\uffff\1\165\4\uffff\1\165\4\uffff\12\52\1\u0088\11\52\1\u0094\1\u009a"+
		"\17\52\1\u00b1\5\52\1\u00b8\5\52\1\u00bf\1\52\1\u00c1\14\52\1\uffff\1"+
		"\165\5\uffff\1\52\1\u00d6\12\52\1\uffff\1\u00e2\1\u00e4\2\52\1\u00e7\1"+
		"\u00e9\4\52\1\u00ee\1\uffff\2\52\1\u00f2\2\52\1\uffff\2\52\1\u00f8\23"+
		"\52\1\uffff\6\52\1\uffff\2\52\1\u0116\3\52\1\uffff\1\52\1\uffff\11\52"+
		"\1\u0125\2\52\1\u0128\1\52\1\u012a\1\52\1\165\1\uffff\1\170\1\52\1\uffff"+
		"\4\52\1\u0134\1\52\1\u0136\4\52\1\uffff\1\52\1\uffff\2\52\1\uffff\1\52"+
		"\1\uffff\2\52\1\u0141\1\52\1\uffff\1\u0143\2\52\1\uffff\1\u0147\1\u0148"+
		"\3\52\1\uffff\1\u014c\1\52\1\u014f\1\52\1\u0151\2\52\1\u0154\1\52\1\u0156"+
		"\13\52\1\u0162\2\52\1\u0165\4\52\1\uffff\1\u016b\1\u016c\14\52\1\uffff"+
		"\2\52\1\uffff\1\u017b\1\uffff\1\52\1\165\1\uffff\1\170\5\52\1\uffff\1"+
		"\52\1\uffff\1\52\1\u0187\1\u0165\1\52\1\u0189\1\u018a\1\u018b\1\u018c"+
		"\1\52\1\u018e\1\uffff\1\52\1\uffff\2\52\1\u0192\2\uffff\2\52\1\u0195\1"+
		"\uffff\1\u0196\1\52\1\uffff\1\u0198\1\uffff\2\52\1\uffff\1\52\1\uffff"+
		"\1\52\1\u019d\6\52\1\u01a5\1\u01a6\1\52\1\uffff\2\52\1\uffff\1\52\1\u01ab"+
		"\1\u01ac\2\52\2\uffff\1\u01af\1\u01b0\10\52\1\u01b9\3\52\1\uffff\1\52"+
		"\1\165\1\uffff\1\170\1\u01c1\1\u01c2\1\52\1\u01c4\1\52\1\u01c6\1\52\1"+
		"\uffff\1\52\4\uffff\1\52\1\uffff\2\52\1\u01cc\1\uffff\1\52\1\u01ce\2\uffff"+
		"\1\52\1\uffff\1\52\1\u01d1\1\52\1\u01d3\1\uffff\4\52\1\u01d8\1\u01d9\1"+
		"\52\2\uffff\1\u01db\3\52\2\uffff\2\52\2\uffff\3\52\1\u01e4\1\52\1\u01e6"+
		"\1\u01e7\1\u01e8\1\uffff\1\u01e9\2\52\1\u01ec\1\165\1\uffff\1\170\2\uffff"+
		"\1\u01f0\1\uffff\1\52\1\uffff\5\52\1\uffff\1\52\1\uffff\2\52\1\uffff\1"+
		"\u01fa\1\uffff\1\u01fb\1\52\1\u01fd\1\52\2\uffff\1\52\1\uffff\1\u0200"+
		"\1\52\1\u0202\2\52\1\u0205\2\52\1\uffff\1\u0208\4\uffff\2\52\1\uffff\1"+
		"\165\1\uffff\1\170\1\uffff\5\52\1\u01c2\1\u0213\1\u0214\1\u0215\2\uffff"+
		"\1\52\1\uffff\1\u0217\1\52\1\uffff\1\u0219\1\uffff\1\52\1\u021b\1\uffff"+
		"\1\52\1\u021d\1\uffff\2\52\1\165\1\uffff\1\170\1\u0221\1\u0222\1\u0223"+
		"\1\u0224\1\u0225\3\uffff\1\52\1\uffff\1\52\1\uffff\1\u0228\1\uffff\1\52"+
		"\1\uffff\2\52\6\uffff\1\52\1\u022e\1\uffff\1\u022f\2\52\1\170\1\52\2\uffff"+
		"\1\u0235\1\u0236\1\u0237\1\170\1\u01ab\3\uffff\2\170";
	static final String DFA19_eofS =
		"\u023a\uffff";
	static final String DFA19_minS =
		"\1\11\5\uffff\1\55\3\uffff\1\75\1\uffff\1\75\5\uffff\1\103\2\60\1\110"+
		"\1\105\1\106\1\116\1\111\3\60\1\101\1\106\2\101\1\105\1\122\2\101\1\60"+
		"\2\uffff\1\56\2\uffff\1\56\1\uffff\1\52\2\uffff\1\56\4\uffff\1\114\1\110"+
		"\1\101\1\120\1\117\2\114\1\117\3\60\1\104\1\120\1\114\1\60\1\124\1\105"+
		"\1\124\1\111\1\131\2\60\1\104\1\105\1\114\1\111\1\115\1\123\1\60\1\117"+
		"\1\125\1\114\1\105\1\123\1\125\3\60\1\107\2\117\1\111\1\102\1\60\1\115"+
		"\1\114\1\120\1\130\1\120\1\60\1\104\1\60\1\111\1\122\1\123\1\114\1\116"+
		"\1\101\1\104\1\120\1\122\1\114\1\116\1\111\1\uffff\1\56\1\53\4\uffff\1"+
		"\105\1\60\1\105\1\122\1\124\1\105\1\115\1\114\1\124\1\101\1\123\1\60\1"+
		"\uffff\2\60\1\114\1\105\2\60\1\110\1\122\1\110\1\124\1\60\1\uffff\2\105"+
		"\1\60\1\124\1\111\1\uffff\1\101\1\116\1\60\1\117\1\104\1\111\2\124\1\105"+
		"\1\103\1\60\1\120\1\102\1\116\1\125\1\120\1\124\1\101\1\124\1\123\1\111"+
		"\1\103\1\uffff\1\111\1\102\1\114\1\105\1\107\1\114\1\uffff\2\105\1\60"+
		"\1\105\1\124\1\114\1\uffff\1\105\1\uffff\2\115\1\123\1\125\1\103\1\101"+
		"\1\117\1\116\1\111\1\60\1\105\1\125\1\60\1\114\1\60\1\123\1\56\1\53\1"+
		"\60\1\103\1\uffff\1\115\1\101\1\111\1\122\1\60\1\105\1\60\1\105\1\124"+
		"\1\105\1\60\1\uffff\1\111\1\uffff\1\131\1\122\1\uffff\1\127\1\uffff\1"+
		"\117\1\105\1\60\1\105\1\uffff\1\60\1\122\1\130\1\uffff\2\60\1\116\1\124"+
		"\1\107\1\uffff\1\60\1\107\1\60\1\124\1\60\1\111\1\124\1\60\1\115\1\60"+
		"\1\114\1\124\1\115\2\101\1\124\1\117\1\124\1\116\1\110\1\116\1\60\1\105"+
		"\1\103\1\60\1\107\1\105\1\116\1\123\1\uffff\2\60\1\105\1\122\1\101\1\111"+
		"\1\127\1\105\1\110\1\116\1\115\1\113\1\124\1\106\1\uffff\1\103\1\120\1"+
		"\uffff\1\60\1\uffff\1\124\1\56\1\53\1\60\1\124\1\101\1\107\1\103\1\125"+
		"\1\uffff\1\116\1\uffff\1\122\7\60\1\122\1\60\1\uffff\1\124\1\uffff\1\101"+
		"\1\124\1\60\2\uffff\1\111\1\105\1\60\1\uffff\1\60\1\107\1\uffff\1\60\1"+
		"\uffff\1\116\1\105\1\uffff\1\101\1\uffff\1\105\1\60\1\116\1\103\1\111"+
		"\1\105\1\115\1\105\2\60\1\124\1\uffff\2\101\1\uffff\1\105\2\60\1\124\1"+
		"\125\2\uffff\2\60\1\122\1\123\1\117\1\123\1\101\1\124\2\105\1\60\1\131"+
		"\1\125\1\105\1\uffff\1\123\1\56\1\53\3\60\1\105\1\60\1\123\1\60\1\111"+
		"\1\uffff\1\60\4\uffff\1\111\1\uffff\1\111\1\103\1\60\1\uffff\1\124\1\60"+
		"\2\uffff\1\105\1\uffff\1\103\1\60\1\114\1\60\1\uffff\1\122\1\106\1\124"+
		"\1\116\2\60\1\122\2\uffff\1\60\1\116\1\124\1\122\2\uffff\1\101\1\111\2"+
		"\uffff\1\131\1\123\1\122\1\60\1\122\3\60\1\uffff\1\60\2\122\1\60\1\56"+
		"\1\53\1\60\2\uffff\1\60\1\uffff\1\105\1\uffff\1\116\1\60\1\132\1\115\1"+
		"\105\1\uffff\1\131\1\uffff\1\104\1\124\1\uffff\1\60\1\uffff\1\60\1\101"+
		"\1\60\1\123\2\uffff\1\111\1\uffff\1\60\1\105\1\60\1\115\1\104\1\60\1\111"+
		"\1\104\1\uffff\1\60\4\uffff\1\123\1\125\1\uffff\1\56\1\53\1\60\1\uffff"+
		"\1\122\1\107\1\55\2\105\4\60\2\uffff\1\115\1\uffff\1\60\1\116\1\uffff"+
		"\1\60\1\uffff\1\120\1\60\1\uffff\1\117\1\60\1\uffff\1\111\1\123\1\55\1"+
		"\53\1\55\5\60\3\uffff\1\111\1\uffff\1\107\1\uffff\1\60\1\uffff\1\116\1"+
		"\uffff\1\126\1\105\1\60\5\uffff\1\114\1\60\1\uffff\1\60\1\105\1\122\1"+
		"\60\1\131\2\uffff\5\60\3\uffff\1\60\1\55";
	static final String DFA19_maxS =
		"\1\175\5\uffff\1\71\3\uffff\1\75\1\uffff\1\75\5\uffff\3\165\1\162\1\145"+
		"\1\156\1\165\1\151\1\162\1\165\2\171\2\162\1\141\1\145\1\162\1\157\1\165"+
		"\1\170\2\uffff\1\170\2\uffff\1\146\1\uffff\1\57\2\uffff\1\145\4\uffff"+
		"\1\164\1\150\1\157\1\160\1\157\2\154\1\157\1\154\1\146\1\172\1\144\1\160"+
		"\1\164\1\146\1\164\1\145\1\164\1\151\1\171\2\172\1\144\1\151\1\154\1\151"+
		"\3\163\1\157\2\165\1\145\1\163\1\165\1\147\1\164\1\172\1\147\2\157\1\165"+
		"\1\142\1\172\1\155\1\154\1\160\1\170\1\160\1\172\1\144\1\172\1\151\1\162"+
		"\1\163\1\162\1\166\1\141\1\144\1\160\1\164\1\154\1\156\1\151\1\uffff\2"+
		"\146\4\uffff\1\145\1\172\1\145\1\162\1\164\1\145\1\172\1\154\1\164\1\141"+
		"\1\163\1\146\1\uffff\2\172\1\154\1\145\2\172\1\150\1\162\1\150\1\164\1"+
		"\172\1\uffff\2\145\1\172\1\164\1\151\1\uffff\1\141\1\156\1\172\1\157\1"+
		"\144\1\151\2\164\1\145\1\143\1\151\1\160\1\142\1\156\1\165\1\160\1\164"+
		"\1\141\1\164\1\163\1\151\1\143\1\uffff\1\151\1\142\1\154\1\156\1\147\1"+
		"\154\1\uffff\2\145\1\172\1\145\1\164\1\154\1\uffff\1\145\1\uffff\2\155"+
		"\1\163\1\165\1\151\1\141\1\157\1\156\1\151\1\172\1\145\1\165\1\172\1\154"+
		"\1\172\1\163\3\146\1\143\1\uffff\1\155\1\141\1\151\1\162\1\172\1\145\1"+
		"\172\1\145\1\164\1\145\1\146\1\uffff\1\151\1\uffff\1\171\1\162\1\uffff"+
		"\1\167\1\uffff\1\157\1\145\1\172\1\145\1\uffff\1\172\1\162\1\170\1\uffff"+
		"\2\172\1\156\1\164\1\147\1\uffff\1\172\1\147\1\172\1\164\1\172\1\151\1"+
		"\164\1\172\1\155\1\172\1\154\1\164\1\155\2\141\1\164\1\157\1\164\1\156"+
		"\1\150\1\156\1\172\1\145\1\143\1\172\1\147\1\145\1\156\1\165\1\uffff\2"+
		"\172\1\145\1\162\1\141\1\151\1\167\1\145\1\150\1\156\1\155\1\153\1\164"+
		"\1\146\1\uffff\1\143\1\160\1\uffff\1\172\1\uffff\1\164\3\146\1\164\1\141"+
		"\1\147\1\143\1\165\1\uffff\1\156\1\uffff\1\162\2\172\1\146\4\172\1\162"+
		"\1\172\1\uffff\1\164\1\uffff\1\141\1\164\1\172\2\uffff\1\151\1\145\1\172"+
		"\1\uffff\1\172\1\147\1\uffff\1\172\1\uffff\1\156\1\145\1\uffff\1\141\1"+
		"\uffff\1\145\1\172\1\156\1\143\1\151\1\145\1\155\1\145\2\172\1\164\1\uffff"+
		"\2\141\1\uffff\1\145\2\172\1\164\1\165\2\uffff\2\172\1\162\1\163\1\157"+
		"\1\163\1\141\1\164\2\145\1\172\1\171\1\165\1\145\1\uffff\1\163\3\146\2"+
		"\172\1\145\1\172\1\163\1\172\1\151\1\uffff\1\146\4\uffff\1\151\1\uffff"+
		"\1\151\1\143\1\172\1\uffff\1\164\1\172\2\uffff\1\145\1\uffff\1\143\1\172"+
		"\1\154\1\172\1\uffff\1\162\1\146\1\164\1\156\2\172\1\162\2\uffff\1\172"+
		"\1\156\1\164\1\162\2\uffff\1\141\1\151\2\uffff\1\171\1\163\1\162\1\172"+
		"\1\162\3\172\1\uffff\1\172\2\162\1\172\3\146\2\uffff\1\172\1\uffff\1\145"+
		"\1\uffff\1\156\1\146\1\172\1\155\1\145\1\uffff\1\171\1\uffff\1\144\1\164"+
		"\1\uffff\1\172\1\uffff\1\172\1\141\1\172\1\163\2\uffff\1\151\1\uffff\1"+
		"\172\1\145\1\172\1\155\1\144\1\172\1\151\1\144\1\uffff\1\172\4\uffff\1"+
		"\163\1\165\1\uffff\3\146\1\uffff\1\162\1\147\1\55\2\145\4\172\2\uffff"+
		"\1\155\1\uffff\1\172\1\156\1\uffff\1\172\1\uffff\1\160\1\172\1\uffff\1"+
		"\157\1\172\1\uffff\1\151\1\163\1\145\1\71\1\55\5\172\3\uffff\1\151\1\uffff"+
		"\1\147\1\uffff\1\172\1\uffff\1\156\1\uffff\1\166\1\145\1\146\5\uffff\1"+
		"\154\1\172\1\uffff\1\172\1\145\1\162\1\146\1\171\2\uffff\3\172\1\146\1"+
		"\172\3\uffff\1\146\1\55";
	static final String DFA19_acceptS =
		"\1\uffff\1\1\1\2\1\3\1\4\1\5\1\uffff\1\7\1\10\1\11\1\uffff\1\14\1\uffff"+
		"\1\17\1\20\1\21\1\22\1\23\24\uffff\1\163\1\164\1\uffff\1\166\1\171\1\uffff"+
		"\1\174\1\uffff\1\175\1\6\1\uffff\1\13\1\12\1\16\1\15\100\uffff\1\165\2"+
		"\uffff\1\167\1\172\1\173\1\176\14\uffff\1\26\13\uffff\1\53\5\uffff\1\106"+
		"\26\uffff\1\101\6\uffff\1\63\6\uffff\1\62\1\uffff\1\114\24\uffff\1\44"+
		"\13\uffff\1\102\1\uffff\1\30\2\uffff\1\111\1\uffff\1\74\4\uffff\1\31\3"+
		"\uffff\1\140\5\uffff\1\41\35\uffff\1\71\16\uffff\1\153\2\uffff\1\151\1"+
		"\uffff\1\155\11\uffff\1\25\1\uffff\1\33\12\uffff\1\36\1\uffff\1\32\3\uffff"+
		"\1\66\1\137\3\uffff\1\121\2\uffff\1\142\1\uffff\1\154\2\uffff\1\103\1"+
		"\uffff\1\64\13\uffff\1\131\2\uffff\1\170\5\uffff\1\75\1\141\16\uffff\1"+
		"\150\13\uffff\1\136\1\uffff\1\127\1\50\1\72\1\104\1\uffff\1\27\3\uffff"+
		"\1\60\2\uffff\1\40\1\122\1\uffff\1\37\4\uffff\1\43\7\uffff\1\45\1\47\4"+
		"\uffff\1\57\1\146\2\uffff\1\157\1\100\10\uffff\1\110\7\uffff\1\24\1\55"+
		"\1\uffff\1\161\1\uffff\1\162\5\uffff\1\34\1\uffff\1\35\2\uffff\1\52\1"+
		"\uffff\1\135\4\uffff\1\54\1\61\1\uffff\1\130\10\uffff\1\67\1\uffff\1\144"+
		"\1\73\1\115\1\116\2\uffff\1\152\3\uffff\1\77\11\uffff\1\134\1\133\1\uffff"+
		"\1\76\2\uffff\1\132\1\uffff\1\160\2\uffff\1\65\2\uffff\1\143\12\uffff"+
		"\1\156\1\46\1\42\1\uffff\1\107\1\uffff\1\51\1\uffff\1\145\1\uffff\1\125"+
		"\3\uffff\1\123\1\105\1\117\1\147\1\56\2\uffff\1\70\5\uffff\1\126\1\112"+
		"\5\uffff\1\113\1\120\1\124\2\uffff";
	static final String DFA19_specialS =
		"\u023a\uffff}>";
	static final String[] DFA19_transitionS = {
			"\2\54\2\uffff\1\54\22\uffff\1\54\1\1\1\47\4\uffff\1\46\1\2\1\3\1\16\1"+
			"\4\1\5\1\6\1\7\1\55\1\50\11\53\1\10\1\11\1\12\1\13\1\14\1\51\1\uffff"+
			"\1\24\1\34\1\33\1\32\1\45\1\23\1\42\1\52\1\27\1\52\1\26\1\31\1\43\1\44"+
			"\1\36\1\37\1\52\1\41\1\22\1\35\1\30\1\40\1\25\3\52\1\15\1\uffff\1\17"+
			"\3\uffff\1\24\1\34\1\33\1\32\1\45\1\23\1\42\1\52\1\27\1\52\1\26\1\31"+
			"\1\43\1\44\1\36\1\37\1\52\1\41\1\22\1\35\1\30\1\40\1\25\3\52\1\20\1\uffff"+
			"\1\21",
			"",
			"",
			"",
			"",
			"",
			"\1\56\2\uffff\12\60",
			"",
			"",
			"",
			"\1\61",
			"",
			"\1\63",
			"",
			"",
			"",
			"",
			"",
			"\1\66\1\uffff\1\65\16\uffff\1\67\1\70\15\uffff\1\66\1\uffff\1\65\16"+
			"\uffff\1\67\1\70",
			"\12\76\7\uffff\1\75\5\76\2\uffff\1\73\2\uffff\1\74\5\uffff\1\71\2\uffff"+
			"\1\72\13\uffff\1\75\5\76\2\uffff\1\73\2\uffff\1\74\5\uffff\1\71\2\uffff"+
			"\1\72",
			"\12\76\7\uffff\3\76\1\103\2\76\5\uffff\1\102\1\uffff\1\100\1\uffff\1"+
			"\101\2\uffff\1\77\1\uffff\1\104\13\uffff\3\76\1\103\2\76\5\uffff\1\102"+
			"\1\uffff\1\100\1\uffff\1\101\2\uffff\1\77\1\uffff\1\104",
			"\1\105\1\106\10\uffff\1\107\25\uffff\1\105\1\106\10\uffff\1\107",
			"\1\110\37\uffff\1\110",
			"\1\112\7\uffff\1\111\27\uffff\1\112\7\uffff\1\111",
			"\1\115\1\uffff\1\113\2\uffff\1\114\1\uffff\1\116\30\uffff\1\115\1\uffff"+
			"\1\113\2\uffff\1\114\1\uffff\1\116",
			"\1\117\37\uffff\1\117",
			"\12\76\7\uffff\4\76\1\121\1\76\2\uffff\1\120\5\uffff\1\123\2\uffff\1"+
			"\122\16\uffff\4\76\1\121\1\76\2\uffff\1\120\5\uffff\1\123\2\uffff\1\122",
			"\12\76\7\uffff\6\76\5\uffff\1\127\2\uffff\1\124\2\uffff\1\125\2\uffff"+
			"\1\126\13\uffff\6\76\5\uffff\1\127\2\uffff\1\124\2\uffff\1\125\2\uffff"+
			"\1\126",
			"\12\76\7\uffff\1\131\3\76\1\130\1\76\2\uffff\1\133\2\uffff\1\134\2\uffff"+
			"\1\135\11\uffff\1\132\7\uffff\1\131\3\76\1\130\1\76\2\uffff\1\133\2\uffff"+
			"\1\134\2\uffff\1\135\11\uffff\1\132",
			"\1\137\3\uffff\1\144\3\uffff\1\141\5\uffff\1\140\2\uffff\1\136\1\uffff"+
			"\1\142\1\145\3\uffff\1\143\7\uffff\1\137\3\uffff\1\144\3\uffff\1\141"+
			"\5\uffff\1\140\2\uffff\1\136\1\uffff\1\142\1\145\3\uffff\1\143",
			"\1\150\7\uffff\1\146\3\uffff\1\147\23\uffff\1\150\7\uffff\1\146\3\uffff"+
			"\1\147",
			"\1\153\3\uffff\1\152\14\uffff\1\151\16\uffff\1\153\3\uffff\1\152\14"+
			"\uffff\1\151",
			"\1\154\37\uffff\1\154",
			"\1\155\37\uffff\1\155",
			"\1\156\37\uffff\1\156",
			"\1\160\15\uffff\1\157\21\uffff\1\160\15\uffff\1\157",
			"\1\163\15\uffff\1\161\5\uffff\1\162\13\uffff\1\163\15\uffff\1\161\5"+
			"\uffff\1\162",
			"\12\76\7\uffff\6\76\21\uffff\1\164\10\uffff\6\76\21\uffff\1\164",
			"",
			"",
			"\1\170\1\uffff\12\166\7\uffff\4\172\1\167\1\172\21\uffff\1\171\10\uffff"+
			"\4\172\1\167\1\172\21\uffff\1\171",
			"",
			"",
			"\1\170\1\uffff\12\166\7\uffff\4\172\1\167\1\172\32\uffff\4\172\1\167"+
			"\1\172",
			"",
			"\1\173\4\uffff\1\56",
			"",
			"",
			"\1\170\1\uffff\12\60\13\uffff\1\170\37\uffff\1\170",
			"",
			"",
			"",
			"",
			"\1\174\7\uffff\1\175\27\uffff\1\174\7\uffff\1\175",
			"\1\176\37\uffff\1\176",
			"\1\u0080\15\uffff\1\177\21\uffff\1\u0080\15\uffff\1\177",
			"\1\u0081\37\uffff\1\u0081",
			"\1\u0082\37\uffff\1\u0082",
			"\1\u0083\37\uffff\1\u0083",
			"\1\u0084\37\uffff\1\u0084",
			"\1\u0085\37\uffff\1\u0085",
			"\12\u0087\7\uffff\6\u0087\5\uffff\1\u0086\24\uffff\6\u0087\5\uffff\1"+
			"\u0086",
			"\12\u0087\7\uffff\6\u0087\32\uffff\6\u0087",
			"\12\52\7\uffff\2\52\1\u0089\27\52\4\uffff\1\52\1\uffff\2\52\1\u0089"+
			"\27\52",
			"\1\u008a\37\uffff\1\u008a",
			"\1\u008b\37\uffff\1\u008b",
			"\1\u008d\7\uffff\1\u008c\27\uffff\1\u008d\7\uffff\1\u008c",
			"\12\u0087\7\uffff\3\u0087\1\u008e\2\u0087\32\uffff\3\u0087\1\u008e\2"+
			"\u0087",
			"\1\u008f\37\uffff\1\u008f",
			"\1\u0090\37\uffff\1\u0090",
			"\1\u0091\37\uffff\1\u0091",
			"\1\u0092\37\uffff\1\u0092",
			"\1\u0093\37\uffff\1\u0093",
			"\12\52\7\uffff\3\52\1\u0096\1\u0098\1\u0099\14\52\1\u0095\1\u0097\6"+
			"\52\4\uffff\1\52\1\uffff\3\52\1\u0096\1\u0098\1\u0099\14\52\1\u0095\1"+
			"\u0097\6\52",
			"\12\52\7\uffff\32\52\4\uffff\1\52\1\uffff\32\52",
			"\1\u009b\37\uffff\1\u009b",
			"\1\u009d\3\uffff\1\u009c\33\uffff\1\u009d\3\uffff\1\u009c",
			"\1\u009e\37\uffff\1\u009e",
			"\1\u009f\37\uffff\1\u009f",
			"\1\u00a0\5\uffff\1\u00a1\31\uffff\1\u00a0\5\uffff\1\u00a1",
			"\1\u00a2\37\uffff\1\u00a2",
			"\12\u0087\7\uffff\2\u0087\1\u00a5\3\u0087\5\uffff\1\u00a3\6\uffff\1"+
			"\u00a4\15\uffff\2\u0087\1\u00a5\3\u0087\5\uffff\1\u00a3\6\uffff\1\u00a4",
			"\1\u00a6\37\uffff\1\u00a6",
			"\1\u00a7\37\uffff\1\u00a7",
			"\1\u00a9\1\u00aa\1\u00ab\6\uffff\1\u00a8\26\uffff\1\u00a9\1\u00aa\1"+
			"\u00ab\6\uffff\1\u00a8",
			"\1\u00ac\37\uffff\1\u00ac",
			"\1\u00ad\37\uffff\1\u00ad",
			"\1\u00ae\37\uffff\1\u00ae",
			"\12\u0087\7\uffff\6\u0087\1\u00af\31\uffff\6\u0087\1\u00af",
			"\12\u0087\7\uffff\6\u0087\15\uffff\1\u00b0\14\uffff\6\u0087\15\uffff"+
			"\1\u00b0",
			"\12\52\7\uffff\32\52\4\uffff\1\52\1\uffff\32\52",
			"\1\u00b2\37\uffff\1\u00b2",
			"\1\u00b3\37\uffff\1\u00b3",
			"\1\u00b4\37\uffff\1\u00b4",
			"\1\u00b6\13\uffff\1\u00b5\23\uffff\1\u00b6\13\uffff\1\u00b5",
			"\1\u00b7\37\uffff\1\u00b7",
			"\12\52\7\uffff\12\52\1\u00b9\17\52\4\uffff\1\52\1\uffff\12\52\1\u00b9"+
			"\17\52",
			"\1\u00ba\37\uffff\1\u00ba",
			"\1\u00bb\37\uffff\1\u00bb",
			"\1\u00bc\37\uffff\1\u00bc",
			"\1\u00bd\37\uffff\1\u00bd",
			"\1\u00be\37\uffff\1\u00be",
			"\12\52\7\uffff\32\52\4\uffff\1\52\1\uffff\32\52",
			"\1\u00c0\37\uffff\1\u00c0",
			"\12\52\7\uffff\32\52\4\uffff\1\52\1\uffff\32\52",
			"\1\u00c2\37\uffff\1\u00c2",
			"\1\u00c3\37\uffff\1\u00c3",
			"\1\u00c4\37\uffff\1\u00c4",
			"\1\u00c5\5\uffff\1\u00c6\31\uffff\1\u00c5\5\uffff\1\u00c6",
			"\1\u00c7\7\uffff\1\u00c8\27\uffff\1\u00c7\7\uffff\1\u00c8",
			"\1\u00c9\37\uffff\1\u00c9",
			"\1\u00ca\37\uffff\1\u00ca",
			"\1\u00cb\37\uffff\1\u00cb",
			"\1\u00cc\1\u00cd\1\u00ce\35\uffff\1\u00cc\1\u00cd\1\u00ce",
			"\1\u00cf\37\uffff\1\u00cf",
			"\1\u00d0\37\uffff\1\u00d0",
			"\1\u00d1\37\uffff\1\u00d1",
			"",
			"\1\170\1\uffff\12\u00d2\7\uffff\4\172\1\u00d3\1\172\32\uffff\4\172\1"+
			"\u00d3\1\172",
			"\1\170\1\uffff\1\170\2\uffff\12\u00d4\7\uffff\6\172\32\uffff\6\172",
			"",
			"",
			"",
			"",
			"\1\u00d5\37\uffff\1\u00d5",
			"\12\52\7\uffff\32\52\4\uffff\1\52\1\uffff\32\52",
			"\1\u00d7\37\uffff\1\u00d7",
			"\1\u00d8\37\uffff\1\u00d8",
			"\1\u00d9\37\uffff\1\u00d9",
			"\1\u00da\37\uffff\1\u00da",
			"\1\u00db\14\uffff\1\u00dc\22\uffff\1\u00db\14\uffff\1\u00dc",
			"\1\u00dd\37\uffff\1\u00dd",
			"\1\u00de\37\uffff\1\u00de",
			"\1\u00df\37\uffff\1\u00df",
			"\1\u00e0\37\uffff\1\u00e0",
			"\12\u00e1\7\uffff\6\u00e1\32\uffff\6\u00e1",
			"",
			"\12\52\7\uffff\10\52\1\u00e3\21\52\4\uffff\1\52\1\uffff\10\52\1\u00e3"+
			"\21\52",
			"\12\52\7\uffff\32\52\4\uffff\1\52\1\uffff\32\52",
			"\1\u00e5\37\uffff\1\u00e5",
			"\1\u00e6\37\uffff\1\u00e6",
			"\12\52\7\uffff\16\52\1\u00e8\13\52\4\uffff\1\52\1\uffff\16\52\1\u00e8"+
			"\13\52",
			"\12\u00e1\7\uffff\6\u00e1\24\52\4\uffff\1\52\1\uffff\6\u00e1\24\52",
			"\1\u00ea\37\uffff\1\u00ea",
			"\1\u00eb\37\uffff\1\u00eb",
			"\1\u00ec\37\uffff\1\u00ec",
			"\1\u00ed\37\uffff\1\u00ed",
			"\12\52\7\uffff\22\52\1\u00ef\7\52\4\uffff\1\52\1\uffff\22\52\1\u00ef"+
			"\7\52",
			"",
			"\1\u00f0\37\uffff\1\u00f0",
			"\1\u00f1\37\uffff\1\u00f1",
			"\12\52\7\uffff\16\52\1\u00f3\13\52\4\uffff\1\52\1\uffff\16\52\1\u00f3"+
			"\13\52",
			"\1\u00f4\37\uffff\1\u00f4",
			"\1\u00f5\37\uffff\1\u00f5",
			"",
			"\1\u00f6\37\uffff\1\u00f6",
			"\1\u00f7\37\uffff\1\u00f7",
			"\12\52\7\uffff\21\52\1\u00f9\10\52\4\uffff\1\52\1\uffff\21\52\1\u00f9"+
			"\10\52",
			"\1\u00fa\37\uffff\1\u00fa",
			"\1\u00fb\37\uffff\1\u00fb",
			"\1\u00fc\37\uffff\1\u00fc",
			"\1\u00fd\37\uffff\1\u00fd",
			"\1\u00fe\37\uffff\1\u00fe",
			"\1\u00ff\37\uffff\1\u00ff",
			"\1\u0100\37\uffff\1\u0100",
			"\12\u00e1\7\uffff\6\u00e1\2\uffff\1\u0101\27\uffff\6\u00e1\2\uffff\1"+
			"\u0101",
			"\1\u0102\37\uffff\1\u0102",
			"\1\u0103\37\uffff\1\u0103",
			"\1\u0104\37\uffff\1\u0104",
			"\1\u0105\37\uffff\1\u0105",
			"\1\u0106\37\uffff\1\u0106",
			"\1\u0107\37\uffff\1\u0107",
			"\1\u0108\37\uffff\1\u0108",
			"\1\u0109\37\uffff\1\u0109",
			"\1\u010a\37\uffff\1\u010a",
			"\1\u010b\37\uffff\1\u010b",
			"\1\u010c\37\uffff\1\u010c",
			"",
			"\1\u010d\37\uffff\1\u010d",
			"\1\u010e\37\uffff\1\u010e",
			"\1\u010f\37\uffff\1\u010f",
			"\1\u0111\10\uffff\1\u0110\26\uffff\1\u0111\10\uffff\1\u0110",
			"\1\u0112\37\uffff\1\u0112",
			"\1\u0113\37\uffff\1\u0113",
			"",
			"\1\u0114\37\uffff\1\u0114",
			"\1\u0115\37\uffff\1\u0115",
			"\12\52\7\uffff\32\52\4\uffff\1\52\1\uffff\32\52",
			"\1\u0117\37\uffff\1\u0117",
			"\1\u0118\37\uffff\1\u0118",
			"\1\u0119\37\uffff\1\u0119",
			"",
			"\1\u011a\37\uffff\1\u011a",
			"",
			"\1\u011b\37\uffff\1\u011b",
			"\1\u011c\37\uffff\1\u011c",
			"\1\u011d\37\uffff\1\u011d",
			"\1\u011e\37\uffff\1\u011e",
			"\1\u011f\5\uffff\1\u0120\31\uffff\1\u011f\5\uffff\1\u0120",
			"\1\u0121\37\uffff\1\u0121",
			"\1\u0122\37\uffff\1\u0122",
			"\1\u0123\37\uffff\1\u0123",
			"\1\u0124\37\uffff\1\u0124",
			"\12\52\7\uffff\32\52\4\uffff\1\52\1\uffff\32\52",
			"\1\u0126\37\uffff\1\u0126",
			"\1\u0127\37\uffff\1\u0127",
			"\12\52\7\uffff\32\52\4\uffff\1\52\1\uffff\32\52",
			"\1\u0129\37\uffff\1\u0129",
			"\12\52\7\uffff\32\52\4\uffff\1\52\1\uffff\32\52",
			"\1\u012b\37\uffff\1\u012b",
			"\1\170\1\uffff\12\u012c\7\uffff\4\172\1\u012d\1\172\32\uffff\4\172\1"+
			"\u012d\1\172",
			"\1\170\1\uffff\1\170\2\uffff\12\u012e\7\uffff\6\172\32\uffff\6\172",
			"\12\u012e\7\uffff\6\172\32\uffff\6\172",
			"\1\u012f\37\uffff\1\u012f",
			"",
			"\1\u0130\37\uffff\1\u0130",
			"\1\u0131\37\uffff\1\u0131",
			"\1\u0132\37\uffff\1\u0132",
			"\1\u0133\37\uffff\1\u0133",
			"\12\52\7\uffff\32\52\4\uffff\1\52\1\uffff\32\52",
			"\1\u0135\37\uffff\1\u0135",
			"\12\52\7\uffff\32\52\4\uffff\1\52\1\uffff\32\52",
			"\1\u0137\37\uffff\1\u0137",
			"\1\u0138\37\uffff\1\u0138",
			"\1\u0139\37\uffff\1\u0139",
			"\12\u013a\7\uffff\6\u013a\32\uffff\6\u013a",
			"",
			"\1\u013b\37\uffff\1\u013b",
			"",
			"\1\u013c\37\uffff\1\u013c",
			"\1\u013d\37\uffff\1\u013d",
			"",
			"\1\u013e\37\uffff\1\u013e",
			"",
			"\1\u013f\37\uffff\1\u013f",
			"\1\u0140\37\uffff\1\u0140",
			"\12\52\7\uffff\32\52\4\uffff\1\52\1\uffff\32\52",
			"\1\u0142\37\uffff\1\u0142",
			"",
			"\12\52\7\uffff\17\52\1\u0144\12\52\4\uffff\1\52\1\uffff\17\52\1\u0144"+
			"\12\52",
			"\1\u0145\37\uffff\1\u0145",
			"\1\u0146\37\uffff\1\u0146",
			"",
			"\12\52\7\uffff\32\52\4\uffff\1\52\1\uffff\32\52",
			"\12\52\7\uffff\32\52\4\uffff\1\52\1\uffff\32\52",
			"\1\u0149\37\uffff\1\u0149",
			"\1\u014a\37\uffff\1\u014a",
			"\1\u014b\37\uffff\1\u014b",
			"",
			"\12\52\7\uffff\22\52\1\u014d\7\52\4\uffff\1\52\1\uffff\22\52\1\u014d"+
			"\7\52",
			"\1\u014e\37\uffff\1\u014e",
			"\12\52\7\uffff\32\52\4\uffff\1\52\1\uffff\32\52",
			"\1\u0150\37\uffff\1\u0150",
			"\12\52\7\uffff\32\52\4\uffff\1\52\1\uffff\32\52",
			"\1\u0152\37\uffff\1\u0152",
			"\1\u0153\37\uffff\1\u0153",
			"\12\52\7\uffff\32\52\4\uffff\1\52\1\uffff\32\52",
			"\1\u0155\37\uffff\1\u0155",
			"\12\52\7\uffff\32\52\4\uffff\1\52\1\uffff\32\52",
			"\1\u0157\37\uffff\1\u0157",
			"\1\u0158\37\uffff\1\u0158",
			"\1\u0159\37\uffff\1\u0159",
			"\1\u015a\37\uffff\1\u015a",
			"\1\u015b\37\uffff\1\u015b",
			"\1\u015c\37\uffff\1\u015c",
			"\1\u015d\37\uffff\1\u015d",
			"\1\u015e\37\uffff\1\u015e",
			"\1\u015f\37\uffff\1\u015f",
			"\1\u0160\37\uffff\1\u0160",
			"\1\u0161\37\uffff\1\u0161",
			"\12\52\7\uffff\32\52\4\uffff\1\52\1\uffff\32\52",
			"\1\u0163\37\uffff\1\u0163",
			"\1\u0164\37\uffff\1\u0164",
			"\12\52\7\uffff\32\52\4\uffff\1\52\1\uffff\32\52",
			"\1\u0166\37\uffff\1\u0166",
			"\1\u0167\37\uffff\1\u0167",
			"\1\u0168\37\uffff\1\u0168",
			"\1\u0169\1\uffff\1\u016a\35\uffff\1\u0169\1\uffff\1\u016a",
			"",
			"\12\52\7\uffff\32\52\4\uffff\1\52\1\uffff\32\52",
			"\12\52\7\uffff\32\52\4\uffff\1\52\1\uffff\32\52",
			"\1\u016d\37\uffff\1\u016d",
			"\1\u016e\37\uffff\1\u016e",
			"\1\u016f\37\uffff\1\u016f",
			"\1\u0170\37\uffff\1\u0170",
			"\1\u0171\37\uffff\1\u0171",
			"\1\u0172\37\uffff\1\u0172",
			"\1\u0173\37\uffff\1\u0173",
			"\1\u0174\37\uffff\1\u0174",
			"\1\u0175\37\uffff\1\u0175",
			"\1\u0176\37\uffff\1\u0176",
			"\1\u0177\37\uffff\1\u0177",
			"\1\u0178\37\uffff\1\u0178",
			"",
			"\1\u0179\37\uffff\1\u0179",
			"\1\u017a\37\uffff\1\u017a",
			"",
			"\12\52\7\uffff\32\52\4\uffff\1\52\1\uffff\32\52",
			"",
			"\1\u017c\37\uffff\1\u017c",
			"\1\170\1\uffff\12\u017d\7\uffff\4\172\1\u017e\1\172\32\uffff\4\172\1"+
			"\u017e\1\172",
			"\1\170\1\uffff\1\170\2\uffff\12\u017f\7\uffff\6\172\32\uffff\6\172",
			"\12\u017f\7\uffff\6\172\32\uffff\6\172",
			"\1\u0180\37\uffff\1\u0180",
			"\1\u0181\37\uffff\1\u0181",
			"\1\u0182\37\uffff\1\u0182",
			"\1\u0183\37\uffff\1\u0183",
			"\1\u0184\37\uffff\1\u0184",
			"",
			"\1\u0185\37\uffff\1\u0185",
			"",
			"\1\u0186\37\uffff\1\u0186",
			"\12\52\7\uffff\32\52\4\uffff\1\52\1\uffff\32\52",
			"\12\52\7\uffff\32\52\4\uffff\1\52\1\uffff\32\52",
			"\12\u0188\7\uffff\6\u0188\32\uffff\6\u0188",
			"\12\52\7\uffff\32\52\4\uffff\1\52\1\uffff\32\52",
			"\12\52\7\uffff\32\52\4\uffff\1\52\1\uffff\32\52",
			"\12\52\7\uffff\32\52\4\uffff\1\52\1\uffff\32\52",
			"\12\52\7\uffff\32\52\4\uffff\1\52\1\uffff\32\52",
			"\1\u018d\37\uffff\1\u018d",
			"\12\52\7\uffff\32\52\4\uffff\1\52\1\uffff\32\52",
			"",
			"\1\u018f\37\uffff\1\u018f",
			"",
			"\1\u0190\37\uffff\1\u0190",
			"\1\u0191\37\uffff\1\u0191",
			"\12\52\7\uffff\32\52\4\uffff\1\52\1\uffff\32\52",
			"",
			"",
			"\1\u0193\37\uffff\1\u0193",
			"\1\u0194\37\uffff\1\u0194",
			"\12\52\7\uffff\32\52\4\uffff\1\52\1\uffff\32\52",
			"",
			"\12\52\7\uffff\32\52\4\uffff\1\52\1\uffff\32\52",
			"\1\u0197\37\uffff\1\u0197",
			"",
			"\12\52\7\uffff\32\52\4\uffff\1\52\1\uffff\32\52",
			"",
			"\1\u0199\37\uffff\1\u0199",
			"\1\u019a\37\uffff\1\u019a",
			"",
			"\1\u019b\37\uffff\1\u019b",
			"",
			"\1\u019c\37\uffff\1\u019c",
			"\12\52\7\uffff\4\52\1\u019e\25\52\4\uffff\1\52\1\uffff\4\52\1\u019e"+
			"\25\52",
			"\1\u019f\37\uffff\1\u019f",
			"\1\u01a0\37\uffff\1\u01a0",
			"\1\u01a1\37\uffff\1\u01a1",
			"\1\u01a2\37\uffff\1\u01a2",
			"\1\u01a3\37\uffff\1\u01a3",
			"\1\u01a4\37\uffff\1\u01a4",
			"\12\52\7\uffff\32\52\4\uffff\1\52\1\uffff\32\52",
			"\12\52\7\uffff\32\52\4\uffff\1\52\1\uffff\32\52",
			"\1\u01a7\37\uffff\1\u01a7",
			"",
			"\1\u01a8\37\uffff\1\u01a8",
			"\1\u01a9\37\uffff\1\u01a9",
			"",
			"\1\u01aa\37\uffff\1\u01aa",
			"\12\52\7\uffff\32\52\4\uffff\1\52\1\uffff\32\52",
			"\12\52\7\uffff\32\52\4\uffff\1\52\1\uffff\32\52",
			"\1\u01ad\37\uffff\1\u01ad",
			"\1\u01ae\37\uffff\1\u01ae",
			"",
			"",
			"\12\52\7\uffff\32\52\4\uffff\1\52\1\uffff\32\52",
			"\12\52\7\uffff\32\52\4\uffff\1\52\1\uffff\32\52",
			"\1\u01b1\37\uffff\1\u01b1",
			"\1\u01b2\37\uffff\1\u01b2",
			"\1\u01b3\37\uffff\1\u01b3",
			"\1\u01b4\37\uffff\1\u01b4",
			"\1\u01b5\37\uffff\1\u01b5",
			"\1\u01b6\37\uffff\1\u01b6",
			"\1\u01b7\37\uffff\1\u01b7",
			"\1\u01b8\37\uffff\1\u01b8",
			"\12\52\7\uffff\32\52\4\uffff\1\52\1\uffff\32\52",
			"\1\u01ba\37\uffff\1\u01ba",
			"\1\u01bb\37\uffff\1\u01bb",
			"\1\u01bc\37\uffff\1\u01bc",
			"",
			"\1\u01bd\37\uffff\1\u01bd",
			"\1\170\1\uffff\12\u01be\7\uffff\4\172\1\u01bf\1\172\32\uffff\4\172\1"+
			"\u01bf\1\172",
			"\1\170\1\uffff\1\170\2\uffff\12\u01c0\7\uffff\6\172\32\uffff\6\172",
			"\12\u01c0\7\uffff\6\172\32\uffff\6\172",
			"\12\52\7\uffff\32\52\4\uffff\1\52\1\uffff\32\52",
			"\12\52\7\uffff\32\52\4\uffff\1\52\1\uffff\32\52",
			"\1\u01c3\37\uffff\1\u01c3",
			"\12\52\7\uffff\32\52\4\uffff\1\52\1\uffff\32\52",
			"\1\u01c5\37\uffff\1\u01c5",
			"\12\52\7\uffff\32\52\4\uffff\1\52\1\uffff\32\52",
			"\1\u01c7\37\uffff\1\u01c7",
			"",
			"\12\u01c8\7\uffff\6\u01c8\32\uffff\6\u01c8",
			"",
			"",
			"",
			"",
			"\1\u01c9\37\uffff\1\u01c9",
			"",
			"\1\u01ca\37\uffff\1\u01ca",
			"\1\u01cb\37\uffff\1\u01cb",
			"\12\52\7\uffff\32\52\4\uffff\1\52\1\uffff\32\52",
			"",
			"\1\u01cd\37\uffff\1\u01cd",
			"\12\52\7\uffff\32\52\4\uffff\1\52\1\uffff\32\52",
			"",
			"",
			"\1\u01cf\37\uffff\1\u01cf",
			"",
			"\1\u01d0\37\uffff\1\u01d0",
			"\12\52\7\uffff\32\52\4\uffff\1\52\1\uffff\32\52",
			"\1\u01d2\37\uffff\1\u01d2",
			"\12\52\7\uffff\32\52\4\uffff\1\52\1\uffff\32\52",
			"",
			"\1\u01d4\37\uffff\1\u01d4",
			"\1\u01d5\37\uffff\1\u01d5",
			"\1\u01d6\37\uffff\1\u01d6",
			"\1\u01d7\37\uffff\1\u01d7",
			"\12\52\7\uffff\32\52\4\uffff\1\52\1\uffff\32\52",
			"\12\52\7\uffff\32\52\4\uffff\1\52\1\uffff\32\52",
			"\1\u01da\37\uffff\1\u01da",
			"",
			"",
			"\12\52\7\uffff\32\52\4\uffff\1\52\1\uffff\32\52",
			"\1\u01dc\37\uffff\1\u01dc",
			"\1\u01dd\37\uffff\1\u01dd",
			"\1\u01de\37\uffff\1\u01de",
			"",
			"",
			"\1\u01df\37\uffff\1\u01df",
			"\1\u01e0\37\uffff\1\u01e0",
			"",
			"",
			"\1\u01e1\37\uffff\1\u01e1",
			"\1\u01e2\37\uffff\1\u01e2",
			"\1\u01e3\37\uffff\1\u01e3",
			"\12\52\7\uffff\32\52\4\uffff\1\52\1\uffff\32\52",
			"\1\u01e5\37\uffff\1\u01e5",
			"\12\52\7\uffff\32\52\4\uffff\1\52\1\uffff\32\52",
			"\12\52\7\uffff\32\52\4\uffff\1\52\1\uffff\32\52",
			"\12\52\7\uffff\32\52\4\uffff\1\52\1\uffff\32\52",
			"",
			"\12\52\7\uffff\32\52\4\uffff\1\52\1\uffff\32\52",
			"\1\u01ea\37\uffff\1\u01ea",
			"\1\u01eb\37\uffff\1\u01eb",
			"\12\52\7\uffff\32\52\4\uffff\1\52\1\uffff\32\52",
			"\1\170\1\uffff\12\u01ed\7\uffff\4\172\1\u01ee\1\172\32\uffff\4\172\1"+
			"\u01ee\1\172",
			"\1\170\1\uffff\1\170\2\uffff\12\u01ef\7\uffff\6\172\32\uffff\6\172",
			"\12\u01ef\7\uffff\6\172\32\uffff\6\172",
			"",
			"",
			"\12\52\7\uffff\32\52\4\uffff\1\52\1\uffff\32\52",
			"",
			"\1\u01f1\37\uffff\1\u01f1",
			"",
			"\1\u01f2\37\uffff\1\u01f2",
			"\12\u01f3\7\uffff\6\u01f3\32\uffff\6\u01f3",
			"\1\u01f4\37\uffff\1\u01f4",
			"\1\u01f5\37\uffff\1\u01f5",
			"\1\u01f6\37\uffff\1\u01f6",
			"",
			"\1\u01f7\37\uffff\1\u01f7",
			"",
			"\1\u01f8\37\uffff\1\u01f8",
			"\1\u01f9\37\uffff\1\u01f9",
			"",
			"\12\52\7\uffff\32\52\4\uffff\1\52\1\uffff\32\52",
			"",
			"\12\52\7\uffff\32\52\4\uffff\1\52\1\uffff\32\52",
			"\1\u01fc\37\uffff\1\u01fc",
			"\12\52\7\uffff\32\52\4\uffff\1\52\1\uffff\32\52",
			"\1\u01fe\37\uffff\1\u01fe",
			"",
			"",
			"\1\u01ff\37\uffff\1\u01ff",
			"",
			"\12\52\7\uffff\32\52\4\uffff\1\52\1\uffff\32\52",
			"\1\u0201\37\uffff\1\u0201",
			"\12\52\7\uffff\32\52\4\uffff\1\52\1\uffff\32\52",
			"\1\u0203\37\uffff\1\u0203",
			"\1\u0204\37\uffff\1\u0204",
			"\12\52\7\uffff\32\52\4\uffff\1\52\1\uffff\32\52",
			"\1\u0206\37\uffff\1\u0206",
			"\1\u0207\37\uffff\1\u0207",
			"",
			"\12\52\7\uffff\32\52\4\uffff\1\52\1\uffff\32\52",
			"",
			"",
			"",
			"",
			"\1\u0209\37\uffff\1\u0209",
			"\1\u020a\37\uffff\1\u020a",
			"",
			"\1\170\1\uffff\12\u020b\7\uffff\4\172\1\u020c\1\172\32\uffff\4\172\1"+
			"\u020c\1\172",
			"\1\170\1\uffff\1\170\2\uffff\12\u020d\7\uffff\6\172\32\uffff\6\172",
			"\12\u020d\7\uffff\6\172\32\uffff\6\172",
			"",
			"\1\u020e\37\uffff\1\u020e",
			"\1\u020f\37\uffff\1\u020f",
			"\1\172",
			"\1\u0210\37\uffff\1\u0210",
			"\1\u0211\37\uffff\1\u0211",
			"\12\52\7\uffff\22\52\1\u0212\7\52\4\uffff\1\52\1\uffff\22\52\1\u0212"+
			"\7\52",
			"\12\52\7\uffff\32\52\4\uffff\1\52\1\uffff\32\52",
			"\12\52\7\uffff\32\52\4\uffff\1\52\1\uffff\32\52",
			"\12\52\7\uffff\32\52\4\uffff\1\52\1\uffff\32\52",
			"",
			"",
			"\1\u0216\37\uffff\1\u0216",
			"",
			"\12\52\7\uffff\32\52\4\uffff\1\52\1\uffff\32\52",
			"\1\u0218\37\uffff\1\u0218",
			"",
			"\12\52\7\uffff\32\52\4\uffff\1\52\1\uffff\32\52",
			"",
			"\1\u021a\37\uffff\1\u021a",
			"\12\52\7\uffff\32\52\4\uffff\1\52\1\uffff\32\52",
			"",
			"\1\u021c\37\uffff\1\u021c",
			"\12\52\7\uffff\32\52\4\uffff\1\52\1\uffff\32\52",
			"",
			"\1\u021e\37\uffff\1\u021e",
			"\1\u021f\37\uffff\1\u021f",
			"\1\172\1\170\1\uffff\12\60\13\uffff\1\170\37\uffff\1\170",
			"\1\170\1\uffff\1\u0220\2\uffff\12\170",
			"\1\172",
			"\12\52\7\uffff\32\52\4\uffff\1\52\1\uffff\32\52",
			"\12\52\7\uffff\32\52\4\uffff\1\52\1\uffff\32\52",
			"\12\52\7\uffff\32\52\4\uffff\1\52\1\uffff\32\52",
			"\12\52\7\uffff\32\52\4\uffff\1\52\1\uffff\32\52",
			"\12\52\7\uffff\32\52\4\uffff\1\52\1\uffff\32\52",
			"",
			"",
			"",
			"\1\u0226\37\uffff\1\u0226",
			"",
			"\1\u0227\37\uffff\1\u0227",
			"",
			"\12\52\7\uffff\32\52\4\uffff\1\52\1\uffff\32\52",
			"",
			"\1\u0229\37\uffff\1\u0229",
			"",
			"\1\u022a\37\uffff\1\u022a",
			"\1\u022b\37\uffff\1\u022b",
			"\12\u022c\7\uffff\6\172\32\uffff\6\172",
			"",
			"",
			"",
			"",
			"",
			"\1\u022d\37\uffff\1\u022d",
			"\12\52\7\uffff\32\52\4\uffff\1\52\1\uffff\32\52",
			"",
			"\12\52\7\uffff\22\52\1\u0230\7\52\4\uffff\1\52\1\uffff\22\52\1\u0230"+
			"\7\52",
			"\1\u0231\37\uffff\1\u0231",
			"\1\u0232\37\uffff\1\u0232",
			"\12\u0233\7\uffff\6\172\32\uffff\6\172",
			"\1\u0234\37\uffff\1\u0234",
			"",
			"",
			"\12\52\7\uffff\32\52\4\uffff\1\52\1\uffff\32\52",
			"\12\52\7\uffff\32\52\4\uffff\1\52\1\uffff\32\52",
			"\12\52\7\uffff\32\52\4\uffff\1\52\1\uffff\32\52",
			"\12\u0238\7\uffff\6\172\32\uffff\6\172",
			"\12\52\7\uffff\32\52\4\uffff\1\52\1\uffff\32\52",
			"",
			"",
			"",
			"\12\u0239\7\uffff\6\172\32\uffff\6\172",
			"\1\172"
	};

	static final short[] DFA19_eot = DFA.unpackEncodedString(DFA19_eotS);
	static final short[] DFA19_eof = DFA.unpackEncodedString(DFA19_eofS);
	static final char[] DFA19_min = DFA.unpackEncodedStringToUnsignedChars(DFA19_minS);
	static final char[] DFA19_max = DFA.unpackEncodedStringToUnsignedChars(DFA19_maxS);
	static final short[] DFA19_accept = DFA.unpackEncodedString(DFA19_acceptS);
	static final short[] DFA19_special = DFA.unpackEncodedString(DFA19_specialS);
	static final short[][] DFA19_transition;

	static {
		int numStates = DFA19_transitionS.length;
		DFA19_transition = new short[numStates][];
		for (int i=0; i<numStates; i++) {
			DFA19_transition[i] = DFA.unpackEncodedString(DFA19_transitionS[i]);
		}
	}

	protected class DFA19 extends DFA {

		public DFA19(BaseRecognizer recognizer) {
			this.recognizer = recognizer;
			this.decisionNumber = 19;
			this.eot = DFA19_eot;
			this.eof = DFA19_eof;
			this.min = DFA19_min;
			this.max = DFA19_max;
			this.accept = DFA19_accept;
			this.special = DFA19_special;
			this.transition = DFA19_transition;
		}
		@Override
		public String getDescription() {
			return "1:1: Tokens : ( T__141 | T__142 | T__143 | T__144 | T__145 | T__146 | T__147 | T__148 | T__149 | T__150 | T__151 | T__152 | T__153 | T__154 | T__155 | T__156 | T__157 | T__158 | T__159 | K_SELECT | K_FROM | K_AS | K_WHERE | K_AND | K_KEY | K_KEYS | K_FULL | K_INSERT | K_UPDATE | K_WITH | K_LIMIT | K_USING | K_USE | K_DISTINCT | K_COUNT | K_SET | K_BEGIN | K_UNLOGGED | K_BATCH | K_APPLY | K_TRUNCATE | K_DELETE | K_IN | K_CREATE | K_KEYSPACE | K_KEYSPACES | K_COLUMNFAMILY | K_INDEX | K_CUSTOM | K_ON | K_TO | K_DROP | K_PRIMARY | K_INTO | K_VALUES | K_TIMESTAMP | K_TTL | K_ALTER | K_RENAME | K_ADD | K_TYPE | K_COMPACT | K_STORAGE | K_ORDER | K_BY | K_ASC | K_DESC | K_ALLOW | K_FILTERING | K_IF | K_CONTAINS | K_GRANT | K_ALL | K_PERMISSION | K_PERMISSIONS | K_OF | K_REVOKE | K_MODIFY | K_AUTHORIZE | K_NORECURSIVE | K_USER | K_USERS | K_SUPERUSER | K_NOSUPERUSER | K_PASSWORD | K_CLUSTERING | K_ASCII | K_BIGINT | K_BLOB | K_BOOLEAN | K_COUNTER | K_DECIMAL | K_DOUBLE | K_FLOAT | K_INET | K_INT | K_TEXT | K_UUID | K_VARCHAR | K_VARINT | K_TIMEUUID | K_TOKEN | K_WRITETIME | K_NULL | K_NOT | K_EXISTS | K_MAP | K_LIST | K_NAN | K_INFINITY | K_TUPLE | K_TRIGGER | K_STATIC | K_FROZEN | STRING_LITERAL | QUOTED_NAME | INTEGER | QMARK | FLOAT | BOOLEAN | IDENT | HEXNUMBER | UUID | WS | COMMENT | MULTILINE_COMMENT );";
		}
	}

}
