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
			//this.newView = newView;
			this.newView = new HashMap<>();
			for(int key : newView.keySet()){
				this.newView.put(key, new actorData(newView.get(key).ref, null, 0));
			}
			
		}
	}
	
	public static class FLUSH extends genericMessage implements Serializable{
		int ID;
		public FLUSH(int ID) {
			this.ID = ID;
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
				System.out.println("cerco di entrare e chiedo a " + remotePath);
				getContext().actorSelection(remotePath).tell(new joinRequest( getSelf() ) , getSelf());
				System.out.println("ho mandato il messaggio");
			}
			else{
				//il coordinatore si riconosce e si autosetta il proprio id
				//e si inserisce nella view.
				id = 0;
				System.out.println("sono il Coordinator");
				view.put(id, new actorData(getSelf(), null, 0));
				System.out.println("e questa e' la view: " + view.toString() );
				
			}
		}
		
		
		
		private void onJoinRequest (joinRequest mess){
			System.out.println("mi e' arrivata una richiesta da: " + mess.ref);
			newIDview = IDview+1;
			newView.clear();
			newView.putAll(view);
			newView.put(IDactor, new actorData(mess.ref, null, 0));
			
			//pulire tutti i last message perche non li devo mandare in giro.
			
			for (int key : newView.keySet()){
				newView.get(key).lastMessage = null;
				newView.get(key).timer = 0;
			}
			
			//mando un messaggio a chi vuole entrare
			System.out.println("sto mandando il messaggio a: " + IDactor);
			mess.ref.tell(new joinResponse(IDactor), getSelf());
			
			//ciclo for per mandare a tutti nella nuova view il messaggio di changeview !
			
			//mettere questo ciclo in una funzione e sara nominata multicast
			
			for (int key : newView.keySet()){
				System.out.println("mando il messaggio di changeview a: " + key + " --- " + newView.get(key).ref.toString());
				newView.get(key).ref.tell(new changeView(newIDview, newView), getSelf());
			}
			
			IDactor++;
		}
		
		private void onJoinResponse(joinResponse mess){
			
			this.id = mess.IDactor;
			System.out.println("mi è arrivata la risposta di join e sono: " + mess.IDactor);
			
		}
		
		
		private void onChangeView (changeView mess){
			this.onChange = true;
			
			this.newIDview = mess.newIDView;
			this.newView = mess.newView;
			
			System.out.println("devo cambiare view");
			if(!onJoin){
				//qui bisognerebbe mettere il fatto che inviamo a tutti la nostra cache 
			}
			else{
				System.out.println("sono nuovo e devo mandare solo i FLUSH");
			}
			
			
			for (int key : newView.keySet()){
				//newView.get(key).lastMessage = null;
				if(newView.get(key).lastMessage != null){
					System.out.println("il messaggio che e' gia nella view e': " + newView.get(key).lastMessage.toString());
				}
				
				newView.get(key).ref.tell(new FLUSH(id), getSelf());
				
				System.out.println("mando il FLUSH a: " + key + " --- " + newView.get(key).ref );
			}
			
			
		}
		
		
		
		
		private void onFLUSH (FLUSH mess){
			
			System.out.println("arrivato messaggio da: " + mess.ID);
			
			this.newView.get(mess.ID).lastMessage = mess;
			
			
			//ciclo su tutti e se tutti sono in flush io installo la view
			
			boolean install = true;
			for (int key : newView.keySet()){
				
				if (this.newView.get(key).lastMessage != null ){
					System.out.println("vedo il " + key + " con il messaggio : " + this.newView.get(key).lastMessage.toString());
					if (! (this.newView.get(key).lastMessage instanceof FLUSH)){
						install = false;
					}
				}
				else{
					System.out.println("il last messaggio da " + key + " era NULL");
					install = false;
				}
			}
			
			if(install){
				System.out.println("view stabile ora installo");
				installView();
			}
			
		}
		
		
		private void installView(){
			//pulisco la nuova view dai messaggi di FLUSH salvati
			for (int key : newView.keySet()){
				newView.get(key).lastMessage = null;
			}
			//
			this.view = this.newView;
			this.newView = null;
			
			this.IDview = this.newIDview;
			
		}

		

		@Override
		public Receive createReceive() {
			return receiveBuilder()
					.match(joinRequest.class, this::onJoinRequest)
					.match(joinResponse.class, this::onJoinResponse)
					.match(changeView.class, this::onChangeView)
					.match(FLUSH.class, this::onFLUSH)
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
