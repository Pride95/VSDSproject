package it.unitn.ds1;

import java.io.Serializable;
import java.util.concurrent.TimeUnit;
import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.HashMap;
import java.util.Collections;
import akka.actor.ActorRef;
import akka.actor.AbstractActor;
import akka.actor.ActorSystem;
import akka.actor.Props;
import akka.actor.Cancellable;
import scala.concurrent.duration.Duration;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.Config;

public class NodeApp {

	static private String remotePath = null; // Akka path of the bootstrapping peer

	
	//classe per incapsulare tutti i dati di un attore inerenti ad una view
	private static class actorData implements Serializable{
		ActorRef ref;
		genericMessage lastMessage;
		int timer;

		public actorData(ActorRef ref, genericMessage lastMessage, int timer) {
			this.ref = ref;
			this.lastMessage = lastMessage;
			this.timer = timer;
		}
		
	}
	
	public static class genericMessage implements Serializable{}
	
	public static class joinRequest extends genericMessage implements Serializable{
		ActorRef ref;
		public joinRequest(ActorRef ref){
			this.ref = ref;
		}
	}
	
	public static class joinResponse extends genericMessage implements Serializable{
		int IDactor;

		public joinResponse(int IDactor) {
			this.IDactor = IDactor;
		}
		
	}
	
	public static class changeView extends genericMessage implements Serializable{
		int newIDView;
		Map<Integer, actorData> newView;

		public changeView(int newIDView, Map<Integer, actorData> newView) {
			this.newIDView = newIDView;
			this.newView = newView;
		}
	}
	
	public static class FLUSH extends genericMessage implements Serializable{
		int ID;
		public FLUSH(int ID) {
			this.ID = ID;
		}
	}
	
	
	
	
	
	public static class Join  implements Serializable {
		int id;
		public Join(int id) {
			this.id = id;
		}
	}
	public static class RequestNodelist implements Serializable {}
	public static class Nodelist implements Serializable {
		//messaggio contente la view
		Map<Integer, ActorRef> nodes;
		public Nodelist(Map<Integer, ActorRef> nodes) {
			this.nodes = Collections.unmodifiableMap(new HashMap<Integer, ActorRef>(nodes));
		}
	}

	
	
	public static class Node extends AbstractActor {

		// The table of all nodes in the system id->ref
		
		//questa è la view
		//private Map<Integer, ActorRef> nodes = new HashMap<>();
		
		private Map<Integer, actorData> view = new HashMap<>();
		private int IDview;
		
		private Map<Integer, actorData> newView = new HashMap<>();
		private int newIDview;
		
		
		private boolean onJoin = false;
		private boolean onChange = false;
		
		private String remotePath = null;
		private int id;
		
		private int IDactor = 1;

		
		
		/* -- Actor constructor --------------------------------------------------- */
		public Node(String remotePath) {
			this.remotePath = remotePath;
			
		}

		static public Props props(int id, String remotePath) {
			return Props.create(Node.class, () -> new Node(remotePath));
		}
		
		

		public void preStart() {
			if (this.remotePath != null) {
				//sono qui solo se non sono il coordinator e sono appena entrato
				
				//mi metto nello stato di sto aspettando di entrare
				onJoin = true;
				//invio la richiesta di entrare
				getContext().actorSelection(remotePath).tell(new joinRequest( getSelf() ) , getSelf());
			}
			else{
				//il coordinatore si riconosce e si autosetta il proprio id
				//e si inserisce nella view.
				id = 0;
				view.put(id, new actorData(getSelf(), null, 0));
			}
		}
		
		
		
		private void onJoinRequest (joinRequest mess){
			newIDview = IDview+1;
			newView.clear();
			newView.putAll(view);
			newView.put(IDactor, new actorData(mess.ref, null, 0));
			
			
			//pulire tutti i last message perche non li devo mandare in giro.
			
			for (int key : newView.keySet()){
				newView.get(key).lastMessage = null;
				newView.get(key).timer = 0;
			}
			//
			mess.ref.tell(new joinResponse(IDactor), getSelf());
			
			
			//ciclo for per mandare a tutti nella nuova view il messaggio dio changeview !
			
			for (int key : newView.keySet()){
				newView.get(key).ref.tell(new changeView(newIDview, newView), getSelf());
			}
			
			IDactor++;
			
		}
		
		
		
		private void onChangeView (changeView mess){
			
		}
		
		private void onFLUSH (FLUSH mess){
			
		}

		private void onRequestNodelist(RequestNodelist message) {
			getSender().tell(new Nodelist(nodes), getSelf());
		}

		private void onNodelist(Nodelist message) {
			nodes.putAll(message.nodes);
			for (ActorRef n : nodes.values()) {
				n.tell(new Join(this.id), getSelf());
			}
		}

		private void onJoin(Join message) {
			int id = message.id;
			System.out.println("Node " + id + " joined");
			nodes.put(id, getSender());
		}

		@Override
		public Receive createReceive() {
			return receiveBuilder()
					.match(RequestNodelist.class, this::onRequestNodelist)
					.match(Nodelist.class, this::onNodelist)
					.match(Join.class, this::onJoin)
					.build();
		}
	}

	public static void main(String[] args) {

		// Load the configuration file
		Config config = ConfigFactory.load();
		int myId = config.getInt("nodeapp.id");
		String remotePath = null;

		if (config.hasPath("nodeapp.remote_ip")) {
			String remote_ip = config.getString("nodeapp.remote_ip");
			int remote_port = config.getInt("nodeapp.remote_port");
			// Starting with a bootstrapping node
			// The Akka path to the bootstrapping peer
			remotePath = "akka.tcp://mysystem@" + remote_ip + ":" + remote_port + "/user/node";
			System.out.println("Starting node " + myId + "; bootstrapping node: " + remote_ip + ":" + remote_port);
		} else {
			System.out.println("Starting disconnected node " + myId);
		}
		// Create the actor system
		final ActorSystem system = ActorSystem.create("mysystem", config);

		// Create a single node actor locally
		final ActorRef receiver = system.actorOf(
				Node.props(myId, remotePath),
				"node" // actor name
		);
	}
}
