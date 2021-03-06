/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.cassandra.transport;

import java.io.PrintWriter;
import java.io.StringReader;
import java.text.SimpleDateFormat;
import java.util.*;
import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.FileHandler;

import io.netty.buffer.ByteBuf;
import io.netty.channel.*;
import io.netty.handler.codec.MessageToMessageDecoder;
import io.netty.handler.codec.MessageToMessageEncoder;

import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableSet;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.config.KSMetaData;
import org.apache.cassandra.config.Schema;
import org.apache.cassandra.db.ColumnFamily;
import org.apache.cassandra.thrift.Cassandra;
import org.apache.cassandra.thrift.KsDef;
import org.json.simple.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.cassandra.transport.messages.*;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.utils.JVMStabilityInspector;

/**
 * A message from the CQL binary protocol.
 */
public abstract class Message {

	protected static final Logger logger = LoggerFactory
			.getLogger(Message.class);

	static Logger commitLogger = LoggerFactory.getLogger("commitLogger");

	static long transactionId = 0;

	/**
	 * When we encounter an unexpected IOException we look for these
	 * {@link Throwable#getMessage() messages} (because we have no better way to
	 * distinguish) and log them at DEBUG rather than INFO, since they are
	 * generally caused by unclean client disconnects rather than an actual
	 * problem.
	 */
	private static final Set<String> ioExceptionsAtDebugLevel = ImmutableSet
			.<String> builder().add("Connection reset by peer")
			.add("Broken pipe").add("Connection timed out").build();

	public interface Codec<M extends Message> extends CBCodec<M> {
	}

	public enum Direction {
		REQUEST, RESPONSE;

		public static Direction extractFromVersion(int versionWithDirection) {
			return (versionWithDirection & 0x80) == 0 ? REQUEST : RESPONSE;
		}

		public int addToVersion(int rawVersion) {
			return this == REQUEST ? (rawVersion & 0x7F) : (rawVersion | 0x80);
		}
	}

	public enum Type {
		ERROR(0, Direction.RESPONSE, ErrorMessage.codec), STARTUP(1,
				Direction.REQUEST, StartupMessage.codec), READY(2,
						Direction.RESPONSE, ReadyMessage.codec), AUTHENTICATE(3,
								Direction.RESPONSE, AuthenticateMessage.codec), CREDENTIALS(4,
										Direction.REQUEST, CredentialsMessage.codec), OPTIONS(5,
												Direction.REQUEST, OptionsMessage.codec), SUPPORTED(6,
														Direction.RESPONSE, SupportedMessage.codec), QUERY(7,
																Direction.REQUEST, QueryMessage.codec), RESULT(8,
																		Direction.RESPONSE, ResultMessage.codec), PREPARE(9,
																				Direction.REQUEST, PrepareMessage.codec), EXECUTE(10,
																						Direction.REQUEST, ExecuteMessage.codec), REGISTER(11,
																								Direction.REQUEST, RegisterMessage.codec), EVENT(12,
																										Direction.RESPONSE, EventMessage.codec), BATCH(13,
																												Direction.REQUEST, BatchMessage.codec), AUTH_CHALLENGE(14,
																														Direction.RESPONSE, AuthChallenge.codec), AUTH_RESPONSE(15,
																																Direction.REQUEST, AuthResponse.codec), AUTH_SUCCESS(16,
																																		Direction.RESPONSE, AuthSuccess.codec);

		public final int opcode;
		public final Direction direction;
		public final Codec<?> codec;

		private static final Type[] opcodeIdx;
		static {
			int maxOpcode = -1;
			for (Type type : Type.values())
				maxOpcode = Math.max(maxOpcode, type.opcode);
			opcodeIdx = new Type[maxOpcode + 1];
			for (Type type : Type.values()) {
				if (opcodeIdx[type.opcode] != null)
					throw new IllegalStateException("Duplicate opcode");
				opcodeIdx[type.opcode] = type;
			}
		}

		private Type(int opcode, Direction direction, Codec<?> codec) {
			this.opcode = opcode;
			this.direction = direction;
			this.codec = codec;
		}

		public static Type fromOpcode(int opcode, Direction direction) {
			if (opcode >= opcodeIdx.length)
				throw new ProtocolException(String.format("Unknown opcode %d",
						opcode));
			Type t = opcodeIdx[opcode];
			if (t == null)
				throw new ProtocolException(String.format("Unknown opcode %d",
						opcode));
			if (t.direction != direction)
				throw new ProtocolException(
						String.format(
								"Wrong protocol direction (expected %s, got %s) for opcode %d (%s)",
								t.direction, direction, opcode, t));
			return t;
		}
	}

	public final Type type;
	protected Connection connection;
	private int streamId;
	private Frame sourceFrame;

	protected Message(Type type) {
		this.type = type;

	}

	public void attach(Connection connection) {
		this.connection = connection;
	}

	public Connection connection() {
		return connection;
	}

	public Message setStreamId(int streamId) {
		this.streamId = streamId;
		return this;
	}

	public int getStreamId() {
		return streamId;
	}

	public void setSourceFrame(Frame sourceFrame) {
		this.sourceFrame = sourceFrame;
	}

	public Frame getSourceFrame() {
		return sourceFrame;
	}

	public static abstract class Request extends Message {
		protected boolean tracingRequested;

		protected Request(Type type) {
			super(type);

			if (type.direction != Direction.REQUEST)
				throw new IllegalArgumentException();
		}

		public abstract Response execute(QueryState queryState);

		public void setTracingRequested() {
			this.tracingRequested = true;
		}

		public boolean isTracingRequested() {
			return tracingRequested;
		}
	}

	public static abstract class Response extends Message {
		protected UUID tracingId;

		protected Response(Type type) {
			super(type);

			if (type.direction != Direction.RESPONSE)
				throw new IllegalArgumentException();
		}

		public Message setTracingId(UUID tracingId) {
			this.tracingId = tracingId;
			return this;
		}

		public UUID getTracingId() {
			return tracingId;
		}
	}

	@ChannelHandler.Sharable
	public static class ProtocolDecoder extends MessageToMessageDecoder<Frame> {
		public void decode(ChannelHandlerContext ctx, Frame frame, List results) {
			boolean isRequest = frame.header.type.direction == Direction.REQUEST;
			boolean isTracing = frame.header.flags
					.contains(Frame.Header.Flag.TRACING);

			UUID tracingId = isRequest || !isTracing ? null : CBUtil
					.readUUID(frame.body);

			try {
				Message message = frame.header.type.codec.decode(frame.body,
						frame.header.version);
				message.setStreamId(frame.header.streamId);
				message.setSourceFrame(frame);

				if (isRequest) {
					assert message instanceof Request;
					Request req = (Request) message;
					Connection connection = ctx.channel()
							.attr(Connection.attributeKey).get();
					req.attach(connection);
					if (isTracing)
						req.setTracingRequested();
				} else {
					assert message instanceof Response;
					if (isTracing)
						((Response) message).setTracingId(tracingId);
				}

				results.add(message);
			} catch (Throwable ex) {
				frame.release();
				// Remember the streamId
				throw ErrorMessage.wrap(ex, frame.header.streamId);
			}
		}
	}

	@ChannelHandler.Sharable
	public static class ProtocolEncoder extends
	MessageToMessageEncoder<Message> {
		public void encode(ChannelHandlerContext ctx, Message message,
				List results) {
			Connection connection = ctx.channel().attr(Connection.attributeKey)
					.get();
			// The only case the connection can be null is when we send the
			// initial STARTUP message (client side thus)
			int version = connection == null ? Server.CURRENT_VERSION
					: connection.getVersion();

			EnumSet<Frame.Header.Flag> flags = EnumSet
					.noneOf(Frame.Header.Flag.class);

			Codec<Message> codec = (Codec<Message>) message.type.codec;
			try {
				int messageSize = codec.encodedSize(message, version);
				ByteBuf body;
				if (message instanceof Response) {
					UUID tracingId = ((Response) message).getTracingId();
					if (tracingId != null) {
						body = CBUtil.allocator.buffer(CBUtil
								.sizeOfUUID(tracingId) + messageSize);
						CBUtil.writeUUID(tracingId, body);
						flags.add(Frame.Header.Flag.TRACING);
					} else {
						body = CBUtil.allocator.buffer(messageSize);
					}
				} else {
					assert message instanceof Request;
					body = CBUtil.allocator.buffer(messageSize);
					if (((Request) message).isTracingRequested())
						flags.add(Frame.Header.Flag.TRACING);
				}

				try {
					codec.encode(message, body, version);
				} catch (Throwable e) {
					body.release();
					throw e;
				}

				results.add(Frame.create(message.type, message.getStreamId(),
						version, flags, body));
			} catch (Throwable e) {
				throw ErrorMessage.wrap(e, message.getStreamId());
			}
		}
	}

	@ChannelHandler.Sharable
	public static class Dispatcher extends SimpleChannelInboundHandler<Request> {
		private static class FlushItem {
			final ChannelHandlerContext ctx;
			final Object response;
			final Frame sourceFrame;

			private FlushItem(ChannelHandlerContext ctx, Object response,
					Frame sourceFrame) {
				this.ctx = ctx;
				this.sourceFrame = sourceFrame;
				this.response = response;
			}
		}

		private final class Flusher implements Runnable {
			final EventLoop eventLoop;
			final ConcurrentLinkedQueue<FlushItem> queued = new ConcurrentLinkedQueue<>();
			final AtomicBoolean running = new AtomicBoolean(false);
			final HashSet<ChannelHandlerContext> channels = new HashSet<>();
			final List<FlushItem> flushed = new ArrayList<>();
			int runsSinceFlush = 0;
			int runsWithNoWork = 0;

			private Flusher(EventLoop eventLoop) {
				this.eventLoop = eventLoop;
			}

			void start() {
				if (!running.get() && running.compareAndSet(false, true)) {
					this.eventLoop.execute(this);
				}
			}

			public void run() {

				boolean doneWork = false;
				FlushItem flush;
				while (null != (flush = queued.poll())) {
					channels.add(flush.ctx);
					flush.ctx.write(flush.response, flush.ctx.voidPromise());
					flushed.add(flush);
					doneWork = true;
				}

				runsSinceFlush++;

				if (!doneWork || runsSinceFlush > 2 || flushed.size() > 50) {
					for (ChannelHandlerContext channel : channels)
						channel.flush();
					for (FlushItem item : flushed)
						item.sourceFrame.release();

					channels.clear();
					flushed.clear();
					runsSinceFlush = 0;
				}

				if (doneWork) {
					runsWithNoWork = 0;
				} else {
					// either reschedule or cancel
					if (++runsWithNoWork > 5) {
						running.set(false);
						if (queued.isEmpty()
								|| !running.compareAndSet(false, true))
							return;
					}
				}

				eventLoop.schedule(this, 10000, TimeUnit.NANOSECONDS);
			}
		}

		private static final ConcurrentMap<EventLoop, Flusher> flusherLookup = new ConcurrentHashMap<>();

		public Dispatcher() {
			super(false);
		}

		@Override
		public void channelRead0(ChannelHandlerContext ctx, Request request) {

			final Response response;
			final ServerConnection connection;

			try {
				assert request.connection() instanceof ServerConnection;
				connection = (ServerConnection) request.connection();
				QueryState qstate = connection.validateNewMessage(request.type,
						connection.getVersion(), request.getStreamId());

				/*logger.debug("Received: {}, v={}", request,
						connection.getVersion());*/

				response = request.execute(qstate);
				response.setStreamId(request.getStreamId());
				response.attach(connection);
				connection.applyStateTransition(request.type, response.type);


				/*boolean applied = true;


				if(response.toString().contains("[applied]") && response.toString().split("\n")[1].split(" ")[2].equals("false")){
					applied = false;
				}


				if (applied && ! request.toString().toLowerCase().contains("selection")
						&& ! request.toString().toLowerCase().contains("delta_")
						&& ! request.toString().toLowerCase().contains("inner_")
						&& ! request.toString().toLowerCase().contains("having_")
						&& ! request.toString().toLowerCase().contains("leftjoin_")
						&& ! request.toString().toLowerCase().contains("rightjoin_")
						&& ! request.toString().toLowerCase().contains("left_")
						&& ! request.toString().toLowerCase().contains("right_")
						&& ! request.toString().toLowerCase().contains("join_agg")
						&& (request.toString().toLowerCase().contains("insert")
								|| request.toString().toLowerCase()
								.contains("update") || (request
										.toString().toLowerCase().contains("delete")))) {

					//this.parseInputForViewMaintenance(request.toString() + '\n');

				}

				if(applied && request.toString().toLowerCase().contains("groupby") && (request.toString().toLowerCase().contains("insert")
						|| request.toString().toLowerCase()
						.contains("update") || (request
								.toString().toLowerCase().contains("delete"))));
					//this.parseInputForViewMaintenance(request.toString() + '\n');*/


			} catch (Throwable t) {
				JVMStabilityInspector.inspectThrowable(t);
				UnexpectedChannelExceptionHandler handler = new UnexpectedChannelExceptionHandler(
						ctx.channel(), true);
				flush(new FlushItem(ctx, ErrorMessage.fromException(t, handler)
						.setStreamId(request.getStreamId()),
						request.getSourceFrame()));
				return;
			}

			/*logger.debug("Responding: {}, v={}", response,
					connection.getVersion());*/
			flush(new FlushItem(ctx, response, request.getSourceFrame()));
		}

		/*private void parseInputForViewMaintenance(String rawInput) {

			transactionId++;

			String queryType = rawInput.split(" ")[1].toLowerCase();
			String keySpaceName;
			String tableName;

			// update
			if (queryType.toLowerCase().equals("update")) {

				String [] table_keyspace = (rawInput.split(" ")[2]).split("\\.");
				tableName = table_keyspace[1];
				keySpaceName = table_keyspace[0];

				String[] splitRaw = rawInput.split(" WHERE ");
				String rawSetString = splitRaw[0].split(" SET ")[1];

				String[] splitRawSetString = rawSetString.split(", ");

				String[] set_data_columns = new String[splitRawSetString.length];
				String[] set_data_values = new String[splitRawSetString.length];


				for (int i = 0; i < splitRawSetString.length; i++) {
					String[] set = splitRawSetString[i].split("= ");
					set_data_columns[i] = set[0];
					set_data_values[i] = set[1];
				}

				String rawConditionString = splitRaw[1].split(";")[0].split(" IF ")[0];

				String[] splitRawConditionSetString = rawConditionString
						.split(" AND ");

				String[] condition_columns = new String[splitRawConditionSetString.length];
				String[] condition_values = new String[splitRawConditionSetString.length];

				for (int i = 0; i < splitRawConditionSetString.length; i++) {
					String[] condition = splitRawConditionSetString[i]
							.split("= ");
					condition_columns[i] = condition[0];
					condition_values[i] = condition[1];
				}

				if ( !tableName.contains("delta") && !(set_data_columns.length==1 && set_data_columns[0].contains("signature")))
					commitLogger.info(convertUpdateToJSON(queryType, keySpaceName,
							tableName, condition_columns, condition_values,
							set_data_columns, set_data_values, transactionId)
							.toJSONString());

			} else {
				// insert



				if (queryType.toLowerCase().equals("insert")) {
					String [] table_keyspace = (rawInput.split(" ")[3]).split("\\.");



					tableName = table_keyspace[1];
					keySpaceName = table_keyspace[0];

					String[] splitRaw = rawInput.split(" VALUES ");
					String[] columns = splitRaw[0].split(" " + keySpaceName+"."+tableName + " ")[1]
							.replace("(", "").replace(")", "").split(", ");

					String[] values = splitRaw[1].split(";")[0].split(" IF ")[0]
							.replace("(", "").replace(")", "").split(", ");

					if (!tableName.contains("SelectView") && !tableName.contains("delta") )
						commitLogger.info(convertInsertToJSON(queryType,
								keySpaceName, tableName, columns, values,
								transactionId).toJSONString());

				}
				// delete
				else {

					String[] table_keyspace = rawInput.split(" ")[3].split("\\.");

					tableName = table_keyspace[1];

					if(!tableName.contains("preag_agg") && !tableName.contains("rj") && !tableName.contains("groupby") ){

						keySpaceName = table_keyspace[0];

						String[] splitRaw = rawInput.split(" WHERE ");

						String rawConditionString = splitRaw[1].split(";")[0];

						rawConditionString = rawConditionString.split(" IF ")[0];

						String[] condition_columns; 
						String[] condition_values;


						if(!rawConditionString.contains("IN")){

							condition_columns = new String[1];
							String condition_values_string = "" ;

							String[] condition = rawConditionString.split(" = ");
							condition_columns[0] = condition[0];
							condition_values_string = condition[1];

							commitLogger.info(convertDeleteToJSON(queryType,
									keySpaceName, tableName, condition_columns,
									condition_values_string, transactionId).toJSONString());

						}else{

							String[] splitRawConditionSetString = rawConditionString
									.split(" IN ");

							condition_columns = new String[1];

							condition_columns[0] = splitRawConditionSetString[0];
							splitRawConditionSetString[1] = splitRawConditionSetString[1].replace("(", "").replace(")", "");

							String[] inValues = splitRawConditionSetString[1].split(",");
							condition_values = new String[inValues.length];

							for (int i = 0; i < inValues.length; i++) {				
								commitLogger.info(convertDeleteToJSON(queryType,
										keySpaceName, tableName, condition_columns,
										inValues[i], transactionId).toJSONString());
							}

						}
					}
				}

			}

		}

		/*
		 * Converts Insert query to JSON Object
		 */
		/*	private JSONObject convertInsertToJSON(String type, String ks,
				String table, String[] columns, String[] values, long tid) {
			JSONObject jsonObject = new JSONObject();

			jsonObject.put("type", type.toLowerCase());
			jsonObject.put("tid", tid);
			jsonObject.put("keyspace", ks);
			jsonObject.put("table", table);

			JSONObject set_data = new JSONObject();
			for (int i = 0; i < columns.length; i++) {
				set_data.put(columns[i].replaceAll(" ", ""), values[i]);
			}
			jsonObject.put("data", set_data);
			return jsonObject;

		}

		/*
		 * Converts Update query to JSON Object
		 */
		/*private JSONObject convertUpdateToJSON(String type, String ks,
				String table, String[] condition_columns,
				String[] condition_values, String[] set_data_columns,
				String[] set_data_values, long tid) {
			JSONObject jsonObject = new JSONObject();

			jsonObject.put("type", type.toLowerCase());
			jsonObject.put("tid", tid);
			jsonObject.put("keyspace", ks);
			jsonObject.put("table", table);

			JSONObject set_data = new JSONObject();
			for (int i = 0; i < set_data_columns.length; i++) {
				set_data.put(set_data_columns[i], set_data_values[i]);
			}
			jsonObject.put("set_data", set_data);

			JSONObject condition = new JSONObject();
			for (int i = 0; i < condition_columns.length; i++) {
				condition.put(condition_columns[i], condition_values[i]);
			}
			jsonObject.put("condition", condition);

			return jsonObject;
		}*/

		/*
		 * Converts delete query to JSON Object
		 */
		/*	private JSONObject convertDeleteToJSON(String type, String ks,
				String table, String[] condition_columns,
				String inValues, long tid) {

			JSONObject jsonObject = new JSONObject();

			jsonObject.put("type", type.toLowerCase()+"-row");
			jsonObject.put("tid", tid);
			jsonObject.put("keyspace", ks);
			jsonObject.put("table", table);

			JSONObject condition = new JSONObject();
			for (int i = 0; i < condition_columns.length; i++) {
				condition.put(condition_columns[i], inValues);
			}
			jsonObject.put("condition", condition);

			return jsonObject;

		}*/

		private void flush(FlushItem item) {
			EventLoop loop = item.ctx.channel().eventLoop();
			Flusher flusher = flusherLookup.get(loop);
			if (flusher == null) {
				Flusher alt = flusherLookup.putIfAbsent(loop,
						flusher = new Flusher(loop));
				if (alt != null)
					flusher = alt;
			}

			flusher.queued.add(item);
			flusher.start();
		}

		@Override
		public void exceptionCaught(final ChannelHandlerContext ctx,
				Throwable cause) throws Exception {
			if (ctx.channel().isOpen()) {
				UnexpectedChannelExceptionHandler handler = new UnexpectedChannelExceptionHandler(
						ctx.channel(), false);
				ChannelFuture future = ctx.writeAndFlush(ErrorMessage
						.fromException(cause, handler));
				// On protocol exception, close the channel as soon as the
				// message have been sent
				if (cause instanceof ProtocolException) {
					future.addListener(new ChannelFutureListener() {
						public void operationComplete(ChannelFuture future) {
							ctx.close();
						}
					});
				}
			}
		}
	}

	/**
	 * Include the channel info in the logged information for unexpected errors,
	 * and (if {@link #alwaysLogAtError} is false then choose the log level
	 * based on the type of exception (some are clearly client issues and
	 * shouldn't be logged at server ERROR level)
	 */
	static final class UnexpectedChannelExceptionHandler implements
	Predicate<Throwable> {
		private final Channel channel;
		private final boolean alwaysLogAtError;

		UnexpectedChannelExceptionHandler(Channel channel,
				boolean alwaysLogAtError) {
			this.channel = channel;
			this.alwaysLogAtError = alwaysLogAtError;
		}

		@Override
		public boolean apply(Throwable exception) {
			String message;
			try {
				message = "Unexpected exception during request; channel = "
						+ channel;
			} catch (Exception ignore) {
				// We don't want to make things worse if String.valueOf() throws
				// an exception
				message = "Unexpected exception during request; channel = <unprintable>";
			}

			if (!alwaysLogAtError && exception instanceof IOException) {
				if (ioExceptionsAtDebugLevel.contains(exception.getMessage())) {
					// Likely unclean client disconnects
					logger.debug(message, exception);
				} else {
					// Generally unhandled IO exceptions are network issues, not
					// actual ERRORS
					logger.info(message, exception);
				}
			} else {
				// Anything else is probably a bug in server of client binary
				// protocol handling
				logger.error(message, exception);
			}

			// We handled the exception.
			return true;
		}
	}

}
