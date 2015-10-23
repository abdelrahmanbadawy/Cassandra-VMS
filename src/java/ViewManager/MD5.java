package ViewManager;


import java.nio.ByteBuffer;

import org.apache.commons.codec.digest.DigestUtils;



public class MD5 implements HashFunction{

	@Override
	public Integer hash(String value) {
		
		ByteBuffer wrapped = ByteBuffer.wrap(DigestUtils.md5(value));
		return wrapped.getInt();

		
	}

}