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
import java.util.Random;
import java.util.logging.Level;
import java.util.logging.Logger;

public class NodeApp {

	static private String remotePath = null; // Akka path of the bootstrapping peer

	
	//classe per incapsulare tutti i dati di un attore inerenti ad una view
	private static class actorData implements Serializable{
		ActorRef ref;
		genericMessage lastMessage;
		long timer;

		public actorData(ActorRef ref, genericMessage lastMessage, long timer) {
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
	
	public static class cacheMessage extends genericMessage implements Serializable{
                //il senderid e l'id chi chi invia effettivamente il messaggii
                int newsenderID;
                
                //il creatorId invece e l'id di chi ha creato il messaggio che sta venendo mandato in forma di cache
                int creatorID;
		int messageID;
		int IDview;

                public cacheMessage(int newsenderID, int creatorID, int messageID, int IDview) {
                    this.newsenderID = newsenderID;
                    this.creatorID = creatorID;
                    this.messageID = messageID;
                    this.IDview = IDview;
                }
        }
	
	public static class normalMessage extends genericMessage implements Serializable{
		//id di chi lo manda
		int senderID;
		int messageID;
		int IDview;

		public normalMessage(int senderID, int messageID, int v) {
			this.senderID = senderID;
			this.messageID = messageID;
			this.IDview = v;
		}
	}

	
	
        
        
        
        
	public static class Node extends AbstractActor {

		// The table of all nodes in the system id->ref
		
		//questa è la view
		//private Map<Integer, ActorRef> nodes = new HashMap<>();
		
		private Map<Integer, actorData> view = new HashMap<>();
		private int IDview;
		
		private Map<Integer, actorData> newView = null;
		
		private int newIDview;
		
		private Map<Integer, genericMessage> tempFLUSH = new HashMap<>();
		
		
		private boolean onJoin = false;
		private boolean onChange = false;
		
		private String remotePath = null;
		
		//questo è il proprio id di ciascun nodo
		private int id;
		
		//questo è l'id assegnato agli attori che si uniscono ed è uso esclusivo del coordinator
		private int IDactor = 1;
		
		private int messageID = 0;
		
		Random rand = new Random();
		

		
		
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
			newView = new HashMap<>();
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
			
			if(tempFLUSH != null){
				for(int key : tempFLUSH.keySet()){
					newView.get(key).lastMessage = tempFLUSH.get(key);
				}
				tempFLUSH = null;
			}
			
			System.out.println("devo cambiare view, la nuova view e' :" + printView(newView) );
			if(!onJoin){
				
				//INVIO CACHE
				int numMessaggi = 0;
				
				System.out.println("INVIO CACHE");
				System.out.println("la vecchia view e' : " + printView(view));
				
				for(int key : view.keySet()){
					if(view.get(key).lastMessage != null){
                                            
                                                normalMessage last = (normalMessage)view.get(key).lastMessage;
                                                
						System.out.println("inizio invio messaggio " + last.messageID +
								" inviato dal peer " + last.senderID + 
								" appartenente alla view " + last.IDview);
                                                
                                                cacheMessage c = new cacheMessage(id, last.senderID, last.messageID, last.IDview);
                                                
						multicast(newView, c);
						numMessaggi++;
					}
				}
				
				System.out.println("sono stati inviati " + numMessaggi + " messaggi di cache ");
				
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
			
			System.out.println("arrivato un messaggio di FLUSH da: " + mess.ID);
			
			if(newView == null && !onChange){
				System.out.println("mi è arrivato un FLUSH prima che fossi in onChangeView");
				if(tempFLUSH == null){
					tempFLUSH = new HashMap<>();
				}
				tempFLUSH.put(mess.ID, mess);
			}
			else{
			
				this.newView.get(mess.ID).lastMessage = mess;


				//ciclo su tutti e se tutti sono in flush io installo la view

				boolean install = true;
				for (int key : newView.keySet()){

					if (this.newView.get(key).lastMessage != null ){
						//System.out.println("vedo il " + key + " con il messaggio : " + this.newView.get(key).lastMessage.toString());
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
		}
		
		private void onNormalMessage(normalMessage mess){
			if(!onJoin){
				//faccio l'handling
				int idSenderActor = mess.senderID;
				int idMessage = mess.messageID;
				normalMessage last = (normalMessage) view.get(idSenderActor).lastMessage;
				
				//sono il coordinator
				if(id == 0){
					if(!onChange){
						//non si sta cambiando la view quindi lavoro sulla vecchia view
						view.get(idSenderActor).timer = System.currentTimeMillis();
						timeCheck(view);
					}
					else{
						//si sta cambiando la view quindi questi sono messaggi di cache
						//quindi lavoro sulla newView
						
						ActorRef f = getSender();
						
						System.out.println("Arrivato messaggio di cache inerente al peer " + idSenderActor + "  ");
						
						int realSender = getKeyFromValue(newView, f);
						
						actorData acData = newView.get(realSender);
						if( acData != null){
							newView.get(realSender).timer = System.currentTimeMillis();
						}
						else{
							//in questo branch non puo entrare 
							System.out.println("c'e' un errore da controllare alla riga 324");
						}
						timeCheck(newView);
					}
				}
				
				if(last != null){
					if(last.messageID < idMessage){
						//ne è arrivato uno più recente adesso li cambio
						view.get(idSenderActor).lastMessage = mess;
						deliver(mess);
					}
					else{
						System.out.println("Ho ricevuto il messaggio: " + idMessage + " da " + idSenderActor + " vecchio, lo considero");
						//non lo considero perche o è uguale o è più vecchio e quindi ne avro di sicuro fatto il delivery
					}
				}
				else{
					view.get(idSenderActor).lastMessage = mess;
					deliver(mess);
				}
				
				System.out.println("la view nel in questo momento e' : " + printView(view));
				
				//metto un controllo cosi ne invia solo un numero fisso
				if(!onChange && idSenderActor == id && messageID < 400){
					normalMessage m = new normalMessage(id, messageID, IDview);
					messageID++;
					
					System.out.println("nella onNormalMessage inizio invio messaggio " + m.messageID + " appartenente alla view " + IDview);
					
					multicast(view, m);
				}
				
			}
			else{
				//sono in onJoin quindi li scarto cioè termino il metodo senza fare niente
				System.out.println("sono nuovo e quindi mi e' arrivato un messaggio e lo scarto");
			}
		}
				
		private void installView(){
			//pulisco la nuova view dai messaggi di FLUSH salvati
			for (int key : newView.keySet()){
				newView.get(key).lastMessage = null;
				newView.get(key).timer = 0;
			}
			
			this.view = this.newView;
			this.newView = null;
			
			this.IDview = this.newIDview;
			
			this.onChange = false;
			this.onJoin = false;
			
			//metto un controllo cosi ne invia solo un numero fisso
			if(messageID < 400){
				//qui cominciamo a inviare oppure a riprendere

				normalMessage m = new normalMessage(id, messageID, IDview);
				messageID++;
				
				System.out.println("nella installView inizio invio messaggio " + m.messageID + " appartenente alla view " + IDview);
				
				multicast(view, m);
			}
		}
		
		private void timeCheck(Map<Integer, actorData> currentview){
			
			long current = System.currentTimeMillis();
			
			int timeout = 4000; //sono millisecond
			
			Map<Integer, actorData> tempView = new HashMap<>();
			tempView.putAll(currentview);
			
			boolean crash = false;
			
			for(int key : currentview.keySet()){
				if(currentview.get(key).timer != 0 ){
					long difference = current - currentview.get(key).timer;
					
					boolean wasFLUSH = false;
					
					genericMessage last = currentview.get(key).lastMessage;
					
					if(last != null){
						if(last instanceof FLUSH){
							wasFLUSH = true;
						}
					}
					
					if(difference > timeout && !wasFLUSH){
						crash = true;
						System.out.println("CRASH !!!!!! il peer " + key + " non risponde da " + difference);
						tempView.remove(key);
					}
				}
				else{
					currentview.get(key).timer = current;
				}
				
			}
			
			if(crash){
				newIDview = IDview+1;
				newView = new HashMap<>();
				newView.putAll(tempView);
				
				for (int key : newView.keySet()){
					newView.get(key).lastMessage = null;
					newView.get(key).timer = 0;
				}
				
				System.out.println("CRASH individuato e la nuova view e' : " + printView(newView));
				
				for (int key : newView.keySet()){
					System.out.println("mando il messaggio di changeview a: " + key + " --- " + newView.get(key).ref.toString());
					newView.get(key).ref.tell(new changeView(newIDview, newView), getSelf());
				}
				
			}
		}
		
		
		private int getKeyFromValue(Map<Integer, actorData> m, ActorRef re){
			int returnedKey = 0;
			for(int key : m.keySet()){
				if(m.get(key).ref == re){
					returnedKey = key;
				}
			}
			return returnedKey;
		}
		
		private String printView(Map<Integer, actorData> m){
			String word = "";
			word = word + "{ \n" ;
			for(int key : m.keySet()){
				word = word + key + " = \n \t" ;
				word = word + "ref : " + m.get(key).ref + " \n \t" ;
				if(m.get(key).lastMessage != null){
					word = word + "last message = messageID : " + ((normalMessage)m.get(key).lastMessage).messageID +
							" senderID : " + ((normalMessage)m.get(key).lastMessage).senderID +
							" \n \t" ;
				}else{
					word = word + "last message = NULL \n \t";
				}
				word = word + "timer : " + m.get(key).timer + " \n";
			}
			word = word + "} \n" ;
			return word;
		}
		
		private void deliver(normalMessage m){
			System.out.println("Deliver nuovo messaggio con id : " + m.messageID + " mandato da: " + m.senderID + " inviata nella view : " + m.IDview + " siamo nella view " + IDview);
		}
		
		private void multicast(Map<Integer, actorData> v, genericMessage m){
			int timeSleep = rand.nextInt(1000) + 1000 ;
			try {
				Thread.sleep(timeSleep);
			} catch (InterruptedException ex) {
				Logger.getLogger(NodeApp.class.getName()).log(Level.SEVERE, null, ex);
			}
			for(int key : v.keySet()){
				v.get(key).ref.tell(m, getSelf());
			}
		}

		@Override
		public Receive createReceive() {
			return receiveBuilder()
					.match(joinRequest.class, this::onJoinRequest)
					.match(joinResponse.class, this::onJoinResponse)
					.match(changeView.class, this::onChangeView)
					.match(FLUSH.class, this::onFLUSH)
					.match(normalMessage.class, this::onNormalMessage)
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
