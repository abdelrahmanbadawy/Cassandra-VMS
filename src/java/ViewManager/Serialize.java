package ViewManager;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.nio.ByteBuffer;

public class Serialize {

	public ByteBuffer serializeStream(Stream ms){

		ByteArrayOutputStream b = new ByteArrayOutputStream();
		ObjectOutputStream o;
		try {
			o = new ObjectOutputStream(b);
			o.writeObject(ms);
		} catch (IOException e) {
			e.printStackTrace();
		}

		
		return ByteBuffer.wrap(b.toByteArray());
		
	}

	public Stream deserializeStream(ByteBuffer buffer){

		Stream stream = null;
		
	    byte[] bytes = new byte[buffer.remaining()];
	    buffer.get(bytes, 0, bytes.length);

		ByteArrayInputStream b = new ByteArrayInputStream(bytes);
		ObjectInputStream o;
		try {
			o = new ObjectInputStream(b);
			stream = (Stream) o.readObject();
		} catch (IOException e) {
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}
		return stream;

	}

}
