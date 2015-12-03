package ViewManager;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.nio.ByteBuffer;

import com.datastax.driver.core.utils.Bytes;

public class Serialize {

	public static ByteBuffer serializeStream(Stream ms){

		ByteArrayOutputStream b = new ByteArrayOutputStream();
		ObjectOutputStream o;
		try {
			o = new ObjectOutputStream(b);
			o.writeObject(ms);
			o.close();
			
		} catch (IOException e) {
			e.printStackTrace();
		}


		return ByteBuffer.wrap(b.toByteArray());

	}

	public static String serializeStream2(Stream ms){

		ByteArrayOutputStream b = new ByteArrayOutputStream();
		ObjectOutputStream o;
		try {
			o = new ObjectOutputStream(b);
			o.writeObject(ms);
			o.close();
		} catch (IOException e) {
			e.printStackTrace();
		}

		return Bytes.toHexString(b.toByteArray());

	}


	public static Stream deserializeStream(String bufferString){

		Stream stream = null;
		bufferString = bufferString.trim();
		ByteBuffer buffer = Bytes.fromHexString(bufferString);

		byte[] bytes = new byte[buffer.remaining()];
		buffer.get(bytes, 0, bytes.length);

		ByteArrayInputStream b = new ByteArrayInputStream(bytes);
		ObjectInputStream o;
		try {
			o = new ObjectInputStream(b);
			stream = (Stream) o.readObject();
			o.close();
			
		} catch (IOException e) {
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}
		return stream;

	}
}