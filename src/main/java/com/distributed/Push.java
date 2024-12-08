package com.distributed;
 
import org.zeromq.ZMQ;
 
public class Push {

	ZMQ.Context context ;
	ZMQ.Socket push ;

	public Push(int port){
		 context = ZMQ.context(1);
		 push  = context.socket(ZMQ.PUSH);
		push.bind("tcp://*:"+port);
	}
	public void send(byte[] data){
		push.send(data);
	}
}