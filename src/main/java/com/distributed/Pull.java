package com.distributed;

import org.zeromq.ZMQ;

public class Pull {

	ZMQ.Context context ;
	ZMQ.Socket pull ;

	public Pull(int port){
		context = ZMQ.context(1);
pull = context.socket(ZMQ.PULL);

		pull.connect("tcp://localhost:"+port);

	}


	public byte[] run(){
			byte [] data = pull.recv();
			return data;
	}

}